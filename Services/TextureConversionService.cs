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
        };
        _onModsChanged = () => OnPenumbraModsChanged?.Invoke();

        _onPlayerResourcesChanged = () => OnPlayerResourcesChanged?.Invoke();

        _penumbraIpc.ModAdded += _onModAdded;
        _penumbraIpc.ModDeleted += _onModDeleted;
        _penumbraIpc.PenumbraEnabledChanged += _onEnabledChanged;
        _penumbraIpc.ModSettingChanged += _onModSettingChanged;
        _penumbraIpc.ModsChanged += _onModsChanged;
        _penumbraIpc.PlayerResourcesChanged += _onPlayerResourcesChanged;
    }

    public void Cancel()
    {
        _cancelRequested = true;
        try { _cts.Cancel(); } catch { }
    }

    public event Action<ModSettingChange, Guid, string, bool>? OnPenumbraModSettingChanged;

    // Debounce/coalesce ModsChanged events to avoid repeated heavy scans
    private void HandleModsChanged() { /* intentionally ignored */ }

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

                if (_configService.Current.EnableBackupBeforeConversion)
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
                    if (perMod.TryGetValue(modName, out var stats) && stats != null && stats.ComparedFiles > 0 && stats.CurrentBytes > stats.OriginalBytes)
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
        return await _penumbraIpc.ScanModTexturesAsync().ConfigureAwait(false);
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
        try { _cts.Cancel(); } catch { }
        try { _cts.Dispose(); } catch { }
    }
}