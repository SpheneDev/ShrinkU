using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System;
using Penumbra.Api.Enums;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ShrinkU.Services;

public sealed class TextureConversionService : IDisposable
{
    private readonly ILogger _logger;
    private readonly PenumbraIpc _penumbraIpc;
    private readonly TextureBackupService _backupService;
    private readonly ShrinkUConfigService _configService;

    private readonly Progress<(string, int)> _conversionProgress = new();
    private readonly Progress<(string, int, int)> _backupProgress = new();
    private CancellationTokenSource _cts = new();
    private volatile bool _cancelRequested = false;
    public bool IsConverting { get; private set; } = false;

    public event Action<(string, int)>? OnConversionProgress;
    public event Action<(string, int, int)>? OnBackupProgress;
    public event Action? OnConversionCompleted;
    public event Action<(string modName, int current, int total, int fileTotal)>? OnModProgress;
    public event Action? OnPenumbraModsChanged;
    public event Action<string>? OnPenumbraModAdded;
    public event Action<string>? OnPenumbraModDeleted;
    public event Action<bool>? OnPenumbraEnabledChanged;
    public event Action<string>? OnExternalTexturesChanged;
    public event Action? OnPlayerResourcesChanged;
    private DateTime _lastModSettingChangedAt = DateTime.MinValue;
    private DateTime _lastAutoAttemptUtc = DateTime.MinValue;
    private Dictionary<string, string[]>? _lastAutoCandidates;
    private CancellationTokenSource? _autoPollCts;
    private Task? _autoPollTask;

    public TextureConversionService(ILogger logger, PenumbraIpc penumbraIpc, TextureBackupService backupService, ShrinkUConfigService configService)
    {
        _logger = logger;
        _penumbraIpc = penumbraIpc;
        _backupService = backupService;
        _configService = configService;

        _conversionProgress.ProgressChanged += (_, e) => OnConversionProgress?.Invoke(e);
        _backupProgress.ProgressChanged += (_, e) => OnBackupProgress?.Invoke(e);

        // Forward Penumbra change broadcasts to UI consumers, keep delegate refs for unsubscription
        _onModAdded = dir => OnPenumbraModAdded?.Invoke(dir);
        _onModDeleted = dir => OnPenumbraModDeleted?.Invoke(dir);
        _onEnabledChanged = enabled => OnPenumbraEnabledChanged?.Invoke(enabled);
        _onModSettingChanged = (change, collectionId, modDir, inherited) =>
        {
            _lastModSettingChangedAt = DateTime.UtcNow;
            OnPenumbraModSettingChanged?.Invoke(change, collectionId, modDir, inherited);
            // When a mod setting changes (enable/disable, priority, etc.),
            // attempt an automatic conversion run if in Automatic mode.
            TryScheduleAutomaticConversion("mod-setting-changed");
        };
        _onModsChanged = () =>
        {
            OnPenumbraModsChanged?.Invoke();
            // ModsAdded/Deleted/Path changes may reflect newly used files.
            TryScheduleAutomaticConversion("mods-changed");
        };

        _onPlayerResourcesChanged = () =>
        {
            OnPlayerResourcesChanged?.Invoke();
            TryScheduleAutomaticConversion("player-resources-changed");
        };

        _penumbraIpc.ModAdded += _onModAdded;
        _penumbraIpc.ModDeleted += _onModDeleted;
        _penumbraIpc.PenumbraEnabledChanged += _onEnabledChanged;
        _penumbraIpc.ModSettingChanged += _onModSettingChanged;
        _penumbraIpc.ModsChanged += _onModsChanged;
        _penumbraIpc.PlayerResourcesChanged += _onPlayerResourcesChanged;

        // Start lightweight auto-conversion watcher for standalone mode
        StartAutoConversionWatcher();
    }

    public void Cancel()
    {
        _cancelRequested = true;
        try { _cts.Cancel(); } catch { }
    }

    public event Action<ModSettingChange, Guid, string, bool>? OnPenumbraModSettingChanged;

    // Debounce/coalesce ModsChanged events to avoid repeated heavy scans
    private void HandleModsChanged() { /* intentionally ignored */ }

    private void TryScheduleAutomaticConversion(string reason)
    {
        try
        {
            if (_configService.Current.TextureProcessingMode != TextureProcessingMode.Automatic)
                return;
            if (_configService.Current.AutomaticHandledBySphene)
            {
                try { _logger.LogDebug("Automatic conversion handled by Sphene; skipping trigger: {reason}", reason); } catch { }
                return;
            }
            if (IsConverting)
                return;

            var now = DateTime.UtcNow;
            if (now - _lastAutoAttemptUtc < TimeSpan.FromSeconds(2))
                return;
            _lastAutoAttemptUtc = now;

            _ = Task.Run(async () =>
            {
                try
                {
                    var candidates = await GetAutomaticCandidateTexturesAsync().ConfigureAwait(false);
                    if (candidates == null || candidates.Count == 0)
                    {
                        try { _logger.LogDebug("No automatic candidates found on trigger: {reason}", reason); } catch { }
                        return;
                    }

                    if (_lastAutoCandidates != null && candidates.Count == _lastAutoCandidates.Count)
                    {
                        bool same = true;
                        foreach (var k in candidates.Keys)
                        {
                            if (!_lastAutoCandidates.TryGetValue(k, out var prev))
                            {
                                same = false;
                                break;
                            }
                            var curr = candidates[k];
                            if ((curr?.Length ?? 0) != (prev?.Length ?? 0))
                            {
                                same = false;
                                break;
                            }
                        }
                        if (same)
                        {
                            try { _logger.LogDebug("Skipping automatic conversion; candidates unchanged since last run"); } catch { }
                            return;
                        }
                    }

                    _lastAutoCandidates = candidates;
                    try { _logger.LogDebug("Starting automatic conversion (standalone) due to: {reason}", reason); } catch { }
                    await StartConversionAsync(candidates).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    try { _logger.LogDebug(ex, "Automatic conversion trigger failed: {reason}", reason); } catch { }
                }
            });
        }
        catch (Exception ex)
        {
            try { _logger.LogDebug(ex, "Failed to schedule automatic conversion: {reason}", reason); } catch { }
        }
    }

    private void StartAutoConversionWatcher()
    {
        try
        {
            StopAutoConversionWatcher();
            _autoPollCts = new CancellationTokenSource();
            var token = _autoPollCts.Token;
            _autoPollTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(2000, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            break;

                        // Only act in Automatic mode and when not controlled by Sphene
                        if (_configService.Current.TextureProcessingMode != TextureProcessingMode.Automatic)
                            continue;
                        if (_configService.Current.AutomaticHandledBySphene)
                            continue;
                        if (IsConverting)
                            continue;

                        // Attempt automatic conversion based on current candidates
                        TryScheduleAutomaticConversion("auto-poll");
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex)
                    {
                        try { _logger.LogDebug(ex, "Auto-conversion watcher iteration failed"); } catch { }
                    }
                }
            }, token);
            try { _logger.LogDebug("Auto-conversion watcher started"); } catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogDebug(ex, "Failed to start auto-conversion watcher"); } catch { }
        }
    }

    private void StopAutoConversionWatcher()
    {
        try { _autoPollCts?.Cancel(); } catch { }
        try { _autoPollCts?.Dispose(); } catch { }
        _autoPollCts = null;
        _autoPollTask = null;
        try { _logger.LogDebug("Auto-conversion watcher stopped"); } catch { }
    }

    public async Task StartConversionAsync(Dictionary<string, string[]> textures)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; conversion aborted");
            return;
        }

        try
        {
            IsConverting = true;
            // Reset cancellation state for this run
            _cts.Dispose();
            _cts = new CancellationTokenSource();
            var token = _cts.Token;

            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            var byMod = new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in textures)
            {
                var source = kvp.Key;
                var rel = !string.IsNullOrWhiteSpace(root) ? Path.GetRelativePath(root, source) : source;
                var parts = rel.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var modName = parts.Length > 1 ? parts[0] : "<unknown>";

                if (!byMod.TryGetValue(modName, out var modDict))
                {
                    modDict = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
                    byMod[modName] = modDict;
                }
                modDict[source] = kvp.Value;
            }

            var totalMods = byMod.Count;
            var currentModIndex = 0;
            foreach (var (modName, modTextures) in byMod)
            {
                currentModIndex++;
                var modFileTotal = modTextures.Count;
                OnModProgress?.Invoke((modName, currentModIndex, totalMods, modFileTotal));

                if (_configService.Current.EnableBackupBeforeConversion || _configService.Current.EnableFullModBackupBeforeConversion)
                {
                    // Finish backup for the current mod before honoring cancellation.
                    await _backupService.BackupAsync(modTextures, _backupProgress, token).ConfigureAwait(false);
                }

                var isLastPlannedMod = currentModIndex == totalMods;
                var redrawAfter = isLastPlannedMod || _cancelRequested;

                // Convert textures for the current mod; support cancellation between files.
                await _penumbraIpc.ConvertTextureFilesAsync(_logger, modTextures, _conversionProgress, token, redrawAfter).ConfigureAwait(false);

                if (_cancelRequested || token.IsCancellationRequested)
                {
                    break;
                }

                // After conversion: evaluate per-mod savings and auto-restore if conversion made it larger.
                try
                {
                    var perMod = await _backupService.ComputePerModSavingsAsync().ConfigureAwait(false);
                    if (perMod.TryGetValue(modName, out var stats) && stats != null && stats.ComparedFiles > 0)
                    {
                        if (stats.CurrentBytes > stats.OriginalBytes)
                        {
                            // Persist inefficient mod marker
                            _configService.Current.InefficientMods ??= new List<string>();
                            if (!_configService.Current.InefficientMods.Any(m => string.Equals(m, modName, StringComparison.OrdinalIgnoreCase)))
                            {
                                _configService.Current.InefficientMods.Add(modName);
                                _configService.Save();
                                _logger.LogDebug("Marked mod {modName} as inefficient (larger after conversion)", modName);
                            }

                            // Auto-restore latest backup for this mod if enabled
                            if (_configService.Current.AutoRestoreInefficientMods)
                            {
                                _logger.LogDebug("Auto-restoring mod {modName} due to increased size after conversion", modName);
                                try
                                {
                                    await _backupService.RestoreLatestForModAsync(modName, _backupProgress, token).ConfigureAwait(false);
                                }
                                catch (Exception rex)
                                {
                                    _logger.LogDebug(rex, "Auto-restore failed for mod {modName}", modName);
                                }
                            }
                        }
                        else
                        {
                            // Remove inefficient marker if the mod is no longer larger after conversion
                            var list = _configService.Current.InefficientMods;
                            if (list != null && list.Any(m => string.Equals(m, modName, StringComparison.OrdinalIgnoreCase)))
                            {
                                _configService.Current.InefficientMods = list.Where(m => !string.Equals(m, modName, StringComparison.OrdinalIgnoreCase)).ToList();
                                _configService.Save();
                                _logger.LogDebug("Cleared inefficient marker for mod {modName} (no longer larger)", modName);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Failed to compute per-mod savings for {modName}", modName);
                }

                // Yield between mods to keep UI responsive
                await Task.Yield();
            }
        }
        finally
        {
            _cancelRequested = false;
            IsConverting = false;
            OnConversionCompleted?.Invoke();
        }
    }

    public async Task<Dictionary<string, string[]>> GetAutomaticCandidateTexturesAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; auto-scan aborted");
            return new Dictionary<string, string[]>();
        }
        // Scan all mod textures, then filter to only currently-used textures,
        // and include only files that are not yet backed up (incremental mode).
        var all = await _penumbraIpc.ScanModTexturesAsync().ConfigureAwait(false);
        if (all.Count == 0)
            return all;

        var used = await _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().ConfigureAwait(false);
        if (used.Count == 0)
            return new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        var root = _penumbraIpc.ModDirectory ?? string.Empty;
        var modsToCheck = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in used)
        {
            try
            {
                var rel = !string.IsNullOrWhiteSpace(root) ? Path.GetRelativePath(root, path) : path;
                var parts = rel.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var modName = parts.Length > 1 ? parts[0] : string.Empty;
                if (!string.IsNullOrWhiteSpace(modName))
                    modsToCheck.Add(modName);
            }
            catch { }
        }

        // Query backed keys per mod to exclude already-backed files while allowing new ones.
        var backedKeysPerMod = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var tasks = modsToCheck
                .Select(m => _backupService.GetBackedKeysForModAsync(m)
                    .ContinueWith(t => (Mod: m, Keys: t.IsCompletedSuccessfully ? t.Result : new HashSet<string>(StringComparer.OrdinalIgnoreCase)), TaskScheduler.Default))
                .ToArray();
            await Task.WhenAll(tasks).ConfigureAwait(false);
            foreach (var t in tasks)
            {
                var (mod, keys) = t.Result;
                backedKeysPerMod[mod] = keys ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }
        }
        catch { }

        var result = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in all)
        {
            var file = kv.Key;
            if (!used.Contains(file))
                continue;

            try
            {
                var rel = !string.IsNullOrWhiteSpace(root) ? Path.GetRelativePath(root, file) : file;
                var parts = rel.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var modName = parts.Length > 1 ? parts[0] : string.Empty;
                if (!string.IsNullOrWhiteSpace(modName))
                {
                    // Build a prefixed path like BackupService uses to match keys
                    var prefixed = file;
                    if (!string.IsNullOrWhiteSpace(root))
                    {
                        prefixed = prefixed.Replace(root, root.EndsWith('\\') ? "{penumbra}\\" : "{penumbra}", StringComparison.OrdinalIgnoreCase);
                        while (prefixed.Contains("\\\\", StringComparison.Ordinal))
                            prefixed = prefixed.Replace("\\\\", "\\", StringComparison.Ordinal);
                    }

                    if (backedKeysPerMod.TryGetValue(modName, out var keys) && keys != null && keys.Contains(prefixed))
                        continue; // skip already-backed files
                }
            }
            catch { }

            result[file] = kv.Value;
        }

        return result;
    }

    private static bool SafeBool(Task<bool> t)
    {
        try { return t.IsCompletedSuccessfully ? t.Result : false; } catch { return false; }
    }

    // Notify UI consumers that external texture changes occurred (e.g., conversions/restores done outside ShrinkU).
    public void NotifyExternalTextureChange(string reason)
    {
        try { _logger.LogDebug("External texture change notification received: {reason}", reason); } catch { }
        try { OnExternalTexturesChanged?.Invoke(reason); } catch { }
    }

    public async Task<Dictionary<string, List<string>>> GetGroupedCandidateTexturesAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; grouped auto-scan aborted");
            return new Dictionary<string, List<string>>();
        }
        return await _penumbraIpc.ScanModTexturesGroupedAsync().ConfigureAwait(false);
    }

    public async Task<Dictionary<string, string>> GetModDisplayNamesAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; display name query aborted");
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
        return await _penumbraIpc.GetModDisplayNamesAsync().ConfigureAwait(false);
    }

    public async Task StartAutomaticConversionForModWithDelayAsync(string modFolder, int delayMs)
    {
        if (_configService.Current.TextureProcessingMode != TextureProcessingMode.Automatic)
            return;
        await Task.Delay(Math.Max(0, delayMs)).ConfigureAwait(false);
        var files = await GetModTextureFilesAsync(modFolder).ConfigureAwait(false);
        if (files == null || files.Count == 0)
            return;
        var dict = new Dictionary<string, string[]>(StringComparer.Ordinal);
        foreach (var f in files)
        {
            if (string.IsNullOrWhiteSpace(f)) continue;
            dict[f] = Array.Empty<string>();
        }
        await StartConversionAsync(dict).ConfigureAwait(false);
    }

    public Task<List<string>> GetModTextureFilesAsync(string modFolder)
    {
        var files = new List<string>();
        try
        {
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            if (string.IsNullOrWhiteSpace(root))
                return Task.FromResult(files);
            var modPath = Path.Combine(root, modFolder);
            if (!Directory.Exists(modPath))
                return Task.FromResult(files);
            foreach (var f in Directory.EnumerateFiles(modPath, "*.*", SearchOption.AllDirectories)
                                       .Where(p => p.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
                                                || p.EndsWith(".dds", StringComparison.OrdinalIgnoreCase)))
            {
                files.Add(f);
            }
        }
        catch { }
        return Task.FromResult(files);
    }

    public async Task<List<string>> GetAllModFoldersAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; all mods query aborted");
            return new List<string>();
        }
        return await _penumbraIpc.GetAllModFoldersAsync().ConfigureAwait(false);
    }

    public async Task<HashSet<string>> GetUsedModTexturePathsAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; used textures query aborted");
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
        return await _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().ConfigureAwait(false);
    }

    public async Task<Dictionary<string, List<string>>> GetModTagsAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; mod tags query aborted");
            return new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        }
        return await _penumbraIpc.GetModTagsAsync().ConfigureAwait(false);
    }

    public void OpenModInPenumbra(string modDirectory, string? modName)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; open mod aborted");
            return;
        }
        try
        {
            _penumbraIpc.OpenModInPenumbra(modDirectory, modName ?? modDirectory);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to open mod {modDirectory} in Penumbra", modDirectory);
        }
    }

    public async Task RunAutomaticConversionOnceAsync(string reason)
    {
        if (_configService.Current.TextureProcessingMode != TextureProcessingMode.Automatic)
            return;
        if (_configService.Current.AutomaticHandledBySphene)
            return;
        try
        {
            var candidates = await GetAutomaticCandidateTexturesAsync().ConfigureAwait(false);
            if (candidates == null || candidates.Count == 0)
                return;
            await _penumbraIpc.ConvertTextureFilesAsync(_logger, candidates, _conversionProgress, System.Threading.CancellationToken.None, true).ConfigureAwait(false);
            try { OnConversionCompleted?.Invoke(); } catch { }
        }
        catch
        {
        }
    }

    // Retrieve hierarchical mod paths from Penumbra for folder-structured views.
    public async Task<Dictionary<string, string>> GetModPathsAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; mod paths query aborted");
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
        return await _penumbraIpc.GetModPathsAsync().ConfigureAwait(false);
    }

    // Retrieve all collections and the current active collection from Penumbra.
    public async Task<Dictionary<Guid, string>> GetCollectionsAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; collections query aborted");
            return new Dictionary<Guid, string>();
        }
        return await _penumbraIpc.GetCollectionsAsync().ConfigureAwait(false);
    }

    public async Task<(Guid Id, string Name)?> GetCurrentCollectionAsync()
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; current collection query aborted");
            return null;
        }
        return await _penumbraIpc.GetCurrentCollectionAsync().ConfigureAwait(false);
    }

    // Retrieve enabled states for all mods within the specified collection.
    public async Task<Dictionary<string, (bool Enabled, int Priority, bool Inherited, bool Temporary)>> GetAllModEnabledStatesAsync(Guid collectionId)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; enabled states query aborted");
            return new Dictionary<string, (bool, int, bool, bool)>(StringComparer.OrdinalIgnoreCase);
        }
        return await _penumbraIpc.GetAllModEnabledStatesAsync(collectionId).ConfigureAwait(false);
    }

    private readonly Action<string>? _onModAdded;
    private readonly Action<string>? _onModDeleted;
    private readonly Action<bool>? _onEnabledChanged;
    private readonly Action<ModSettingChange, Guid, string, bool>? _onModSettingChanged;
    private readonly Action? _onModsChanged;
    private readonly Action? _onPlayerResourcesChanged;

    public void Dispose()
    {
        try { _penumbraIpc.ModAdded -= _onModAdded; } catch { }
        try { _penumbraIpc.ModDeleted -= _onModDeleted; } catch { }
        try { _penumbraIpc.PenumbraEnabledChanged -= _onEnabledChanged; } catch { }
        try { _penumbraIpc.ModSettingChanged -= _onModSettingChanged; } catch { }
        try { _penumbraIpc.ModsChanged -= _onModsChanged; } catch { }
        try { _penumbraIpc.PlayerResourcesChanged -= _onPlayerResourcesChanged; } catch { }
        StopAutoConversionWatcher();
        try { _cts.Cancel(); } catch { }
        try { _cts.Dispose(); } catch { }
    }
}