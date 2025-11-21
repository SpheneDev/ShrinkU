using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Helpers;
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
    private readonly ModStateService _modStateService;

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
    private CancellationTokenSource? _backupRefreshCts;
    private bool _subscriptionsAttached = false;

public TextureConversionService(ILogger logger, PenumbraIpc penumbraIpc, TextureBackupService backupService, ShrinkUConfigService configService, ModStateService modStateService)
{
    _logger = logger;
    _penumbraIpc = penumbraIpc;
    _backupService = backupService;
    _configService = configService;
    _modStateService = modStateService;

        _conversionProgress.ProgressChanged += (_, e) => OnConversionProgress?.Invoke(e);
        _backupProgress.ProgressChanged += (_, e) => OnBackupProgress?.Invoke(e);

        // Forward Penumbra change broadcasts to UI consumers, keep delegate refs for unsubscription
        _onModAdded = dir => {
            OnPenumbraModAdded?.Invoke(dir);
            _ = Task.Run(async () => { try { await UpdateAllModMetadataAsync().ConfigureAwait(false); } catch { } });
        };
        _onModDeleted = dir => {
            OnPenumbraModDeleted?.Invoke(dir);
            try { _modStateService.RemoveEntry(dir); } catch { }
            _ = Task.Run(async () => { try { await UpdateAllModUsedTextureFilesAsync().ConfigureAwait(false); } catch { } });
        };
        _onEnabledChanged = enabled =>
        {
            OnPenumbraEnabledChanged?.Invoke(enabled);
            if (enabled)
            {
                _ = Task.Run(async () =>
                {
                    try { await UpdateAllModTextureCountsAsync().ConfigureAwait(false); } catch { }
                    try { await UpdateAllModUsedTextureFilesAsync().ConfigureAwait(false); } catch { }
                });
            }
        };
        _onModSettingChanged = (change, collectionId, modDir, inherited) =>
        {
            _lastModSettingChangedAt = DateTime.UtcNow;
            OnPenumbraModSettingChanged?.Invoke(change, collectionId, modDir, inherited);
            // When a mod setting changes (enable/disable, priority, etc.),
            // attempt an automatic conversion run if in Automatic mode.
            TryScheduleAutomaticConversion("mod-setting-changed");
            _ = Task.Run(async () =>
            {
                try
                {
                    var coll = await GetCurrentCollectionAsync().ConfigureAwait(false);
                    if (coll.HasValue)
                    {
                        var states = await GetAllModEnabledStatesAsync(coll.Value.Id).ConfigureAwait(false);
                        if (states.TryGetValue(modDir, out var st))
                        {
                            _modStateService.UpdateEnabledState(modDir, st.Enabled, st.Priority);
                        }
                    }
                }
                catch { }
                try { await UpdateAllModUsedTextureFilesAsync().ConfigureAwait(false); } catch { }
            });
        };
        _onModsChanged = () =>
        {
            OnPenumbraModsChanged?.Invoke();
            TryScheduleAutomaticConversion("mods-changed");
            try
            {
                _backupRefreshCts?.Cancel();
                _backupRefreshCts?.Dispose();
                _backupRefreshCts = new CancellationTokenSource();
                var token = _backupRefreshCts.Token;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested) return;
                        await _backupService.RefreshAllBackupStateAsync().ConfigureAwait(false);
                    }
                    catch (TaskCanceledException) { }
                    catch { }
                });
            }
            catch { }
            try { _ = UpdateAllModTextureCountsAsync(); } catch { }
            try { _ = UpdateAllModUsedTextureFilesAsync(); } catch { }
        };

        _onPlayerResourcesChanged = () =>
        {
            OnPlayerResourcesChanged?.Invoke();
            TryScheduleAutomaticConversion("player-resources-changed");
            try { _ = UpdateAllModUsedTextureFilesAsync(); } catch { }
        };

        
    }

    private void AttachPenumbraSubscriptions()
    {
        if (_subscriptionsAttached) return;
        _penumbraIpc.ModAdded += _onModAdded;
        _penumbraIpc.ModDeleted += _onModDeleted;
        _penumbraIpc.PenumbraEnabledChanged += _onEnabledChanged;
        _penumbraIpc.ModSettingChanged += _onModSettingChanged;
        _penumbraIpc.ModsChanged += _onModsChanged;
        _penumbraIpc.PlayerResourcesChanged += _onPlayerResourcesChanged;
        _subscriptionsAttached = true;
    }

    private void DetachPenumbraSubscriptions()
    {
        if (!_subscriptionsAttached) return;
        try { _penumbraIpc.ModAdded -= _onModAdded; } catch { }
        try { _penumbraIpc.ModDeleted -= _onModDeleted; } catch { }
        try { _penumbraIpc.PenumbraEnabledChanged -= _onEnabledChanged; } catch { }
        try { _penumbraIpc.ModSettingChanged -= _onModSettingChanged; } catch { }
        try { _penumbraIpc.ModsChanged -= _onModsChanged; } catch { }
        try { _penumbraIpc.PlayerResourcesChanged -= _onPlayerResourcesChanged; } catch { }
        _subscriptionsAttached = false;
    }

    public void SetEnabled(bool enabled)
    {
        if (enabled)
        {
            AttachPenumbraSubscriptions();
            StartAutoConversionWatcher();
        }
        else
        {
            DetachPenumbraSubscriptions();
            StopAutoConversionWatcher();
            try { _backupRefreshCts?.Cancel(); } catch { }
            try { _backupRefreshCts?.Dispose(); } catch { }
            _backupRefreshCts = null;
        }
    }

    public static string NormalizeLeafKey(string mod)
    {
        if (string.IsNullOrWhiteSpace(mod)) return string.Empty;
        var leaf = mod.Replace('/', System.IO.Path.DirectorySeparatorChar).Replace('\\', System.IO.Path.DirectorySeparatorChar).TrimEnd(System.IO.Path.DirectorySeparatorChar);
        var segs = leaf.Split(System.IO.Path.DirectorySeparatorChar);
        return segs.Length > 0 ? segs[^1] : leaf;
    }

    public static string ComputeRelativePathFromAbs(string root, string abs)
    {
        if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(abs)) return string.Empty;
        try { return System.IO.Path.GetRelativePath(root, abs).Replace('\\', '/'); } catch { return string.Empty; }
    }

    public event Action<(int processed, int total, int etaSeconds)>? OnStartupProgress;

    public async Task RunInitialParallelUpdateAsync(int maxThreads, CancellationToken token)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var mods = await GetAllModFoldersAsync().ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogDebug("Initial update step: GetAllModFolders count={count} elapsedMs={ms}", mods.Count, (int)sw.ElapsedMilliseconds); } catch { }
            var total = mods.Count;
            var start = DateTime.UtcNow;
            sw.Restart();
            var names = await GetModDisplayNamesAsync().ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogDebug("Initial update step: GetModDisplayNames count={count} elapsedMs={ms}", names.Count, (int)sw.ElapsedMilliseconds); } catch { }
            sw.Restart();
            var tags = await GetModTagsAsync().ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogDebug("Initial update step: GetModTags mods={mods} elapsedMs={ms}", tags.Count, (int)sw.ElapsedMilliseconds); } catch { }
            sw.Restart();
            var groupedTextures = await GetGroupedCandidateTexturesAsync().ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogDebug("Initial update step: GroupedTextureScan mods={mods} elapsedMs={ms}", groupedTextures.Count, (int)sw.ElapsedMilliseconds); } catch { }
            Guid? collId = null;
            sw.Restart();
            try { var coll = await GetCurrentCollectionAsync().ConfigureAwait(false); collId = coll?.Id; } catch { }
            sw.Stop();
            try { _logger.LogDebug("Initial update step: GetCurrentCollection elapsedMs={ms}", (int)sw.ElapsedMilliseconds); } catch { }
            Dictionary<string, (bool Enabled, int Priority, bool Inherited, bool Temporary)> states = new(StringComparer.OrdinalIgnoreCase);
            if (collId.HasValue)
            {
                sw.Restart();
                try { states = await GetAllModEnabledStatesAsync(collId.Value).ConfigureAwait(false); } catch { }
                sw.Stop();
                try { _logger.LogDebug("Initial update step: GetAllModEnabledStates count={count} elapsedMs={ms}", states.Count, (int)sw.ElapsedMilliseconds); } catch { }
            }
            var snap = _modStateService.Snapshot();
            var root = _penumbraIpc.ModDirectory ?? string.Empty;

            Func<string, int> prio = m =>
            {
                var key = NormalizeLeafKey(m);
                var p = 0;
                if (snap.TryGetValue(key, out var e) && e != null && e.TotalTextures > 0) p += 2;
                if (states.TryGetValue(m, out var st) && st.Enabled) p += 3;
                return p;
            };
            var ordered = mods.OrderByDescending(prio).ThenBy(m => m, StringComparer.OrdinalIgnoreCase).ToList();

            using var sem = new SemaphoreSlim(Math.Max(1, maxThreads));
            int processed = 0;
            var tasks = new List<Task>(ordered.Count);
            foreach (var mod in ordered)
            {
                await sem.WaitAsync(token).ConfigureAwait(false);
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var key = NormalizeLeafKey(mod);
                        int fileCount = 0;
                        try
                        {
                            if (groupedTextures.TryGetValue(key, out var list) && list != null)
                                fileCount = list.Count;
                        }
                        catch { fileCount = 0; }
                        _modStateService.UpdateTextureCount(key, fileCount);

                        var display = names.TryGetValue(key, out var dn) ? (dn ?? string.Empty) : string.Empty;
                        var tagList = tags.TryGetValue(key, out var tl) ? (tl ?? new List<string>()) : new List<string>();
                        _modStateService.UpdateDisplayAndTags(key, display, tagList);

                        string abs = string.Empty;
                        try
                        {
                            var defaultAbs = string.IsNullOrWhiteSpace(root) ? string.Empty : System.IO.Path.Combine(root, key);
                            abs = (!string.IsNullOrWhiteSpace(defaultAbs) && Directory.Exists(defaultAbs)) ? defaultAbs : (_backupService.GetModAbsolutePath(key) ?? string.Empty);
                        }
                        catch { abs = _backupService.GetModAbsolutePath(key) ?? string.Empty; }
                        string rel = string.Empty;
                        try
                        {
                            var (ec, fullPath, _, _) = _penumbraIpc.GetModPath(key);
                            var p = (fullPath ?? string.Empty).Replace('\\', '/');
                            try { _logger.LogDebug("PenumbraRelativePath Startup: mod={mod} ec={ec} path={path}", key, ec, p); } catch { }
                            if (ec == Penumbra.Api.Enums.PenumbraApiEc.Success && !string.IsNullOrWhiteSpace(p))
                                rel = p;
                            else
                                rel = ComputeRelativePathFromAbs(root, abs);
                        }
                        catch { rel = ComputeRelativePathFromAbs(root, abs); }
                        var existing = snap.TryGetValue(key, out var e) && e != null ? e : null;
                        var ver = existing?.CurrentVersion ?? string.Empty;
                        var auth = existing?.CurrentAuthor ?? string.Empty;
                        _modStateService.UpdateCurrentModInfo(key, abs, rel, ver, auth);

                        if (states.TryGetValue(mod, out var st))
                            _modStateService.UpdateEnabledState(key, st.Enabled, st.Priority);
                    }
                    catch { }
                    finally
                    {
                        var done = Interlocked.Increment(ref processed);
                        var elapsed = DateTime.UtcNow - start;
                        var remaining = Math.Max(0, total - done);
                        var eta = done > 0 ? (int)Math.Round(elapsed.TotalSeconds / done * remaining) : 0;
                        try { OnStartupProgress?.Invoke((done, total, eta)); } catch { }
                        try { sem.Release(); } catch { }
                    }
                }, token));
            }
                sw.Restart();
                try { await Task.WhenAll(tasks).ConfigureAwait(false); } catch { }
                sw.Stop();
                try { _logger.LogDebug("Initial update step: Per-mod tasks elapsedMs={ms}", (int)sw.ElapsedMilliseconds); } catch { }
                try { _modStateService.Save(); } catch { }
                try { _modStateService.RecomputeInstalledButNotConverted(); } catch { }
                try
                {
                    var elapsedTotal = DateTime.UtcNow - start;
                    var avgPerModMs = total > 0 ? (int)Math.Round(elapsedTotal.TotalMilliseconds / total) : 0;
                    var mem = GC.GetTotalMemory(false);
                    var gc0 = GC.CollectionCount(0);
                    var gc1 = GC.CollectionCount(1);
                    var gc2 = GC.CollectionCount(2);
                    _logger.LogDebug("Initial update completed: mods={mods}, elapsedMs={elapsed}, avgPerModMs={avg}, memKB={mem}, gc0={gc0}, gc1={gc1}, gc2={gc2}", total, (int)Math.Round(elapsedTotal.TotalMilliseconds), avgPerModMs, (int)(mem / 1024), gc0, gc1, gc2);
                }
                catch { }
        }
        catch { }
    }

    public async Task RunStartupBenchmarkAsync(int maxThreads, CancellationToken token)
    {
        try
        {
            var proc = System.Diagnostics.Process.GetCurrentProcess();
            var cpuStart = proc.TotalProcessorTime;
            var memStart = GC.GetTotalMemory(false);
            var swTotal = System.Diagnostics.Stopwatch.StartNew();

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var mods = await GetAllModFoldersAsync().ConfigureAwait(false);
            sw.Stop();
            var modsMs = (int)sw.ElapsedMilliseconds;

            sw.Restart();
            var names = await GetModDisplayNamesAsync().ConfigureAwait(false);
            sw.Stop();
            var namesMs = (int)sw.ElapsedMilliseconds;

            sw.Restart();
            var tags = await GetModTagsAsync().ConfigureAwait(false);
            sw.Stop();
            var tagsMs = (int)sw.ElapsedMilliseconds;

            sw.Restart();
            var grouped = await GetGroupedCandidateTexturesAsync().ConfigureAwait(false);
            sw.Stop();
            var groupedMs = (int)sw.ElapsedMilliseconds;

            sw.Restart();
            await RunInitialParallelUpdateAsync(maxThreads, token).ConfigureAwait(false);
            sw.Stop();
            var initialMs = (int)sw.ElapsedMilliseconds;

            sw.Restart();
            await UpdateAllModUsedTextureFilesAsync().ConfigureAwait(false);
            sw.Stop();
            var usedMs = (int)sw.ElapsedMilliseconds;

            sw.Restart();
            _modStateService.Save();
            sw.Stop();
            var saveMs = (int)sw.ElapsedMilliseconds;

            swTotal.Stop();
            var cpuEnd = proc.TotalProcessorTime;
            var memEnd = GC.GetTotalMemory(false);
            var cpuMs = (int)Math.Round((cpuEnd - cpuStart).TotalMilliseconds);
            var totalMs = (int)swTotal.ElapsedMilliseconds;
            var gc0 = GC.CollectionCount(0);
            var gc1 = GC.CollectionCount(1);
            var gc2 = GC.CollectionCount(2);

            try
            {
                _logger.LogDebug(
                    "Startup benchmark: modsMs={modsMs}, namesMs={namesMs}, tagsMs={tagsMs}, groupedMs={groupedMs}, initialMs={initialMs}, usedMs={usedMs}, saveMs={saveMs}, totalMs={totalMs}, cpuMs={cpuMs}, memDeltaKB={memDelta}, gc0={gc0}, gc1={gc1}, gc2={gc2}",
                    modsMs, namesMs, tagsMs, groupedMs, initialMs, usedMs, saveMs, totalMs, cpuMs, (int)((memEnd - memStart) / 1024), gc0, gc1, gc2);
            }
            catch { }
        }
        catch { }
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

        PerfStep traceTotal = default;
        try
        {
            IsConverting = true;
            // Reset cancellation state for this run
            _cts.Dispose();
            _cts = new CancellationTokenSource();
            var token = _cts.Token;

            traceTotal = PerfTrace.Step(_logger, "StartConversion total");
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

                if (_configService.Current.EnableFullModBackupBeforeConversion)
                {
                    bool hasPmp = false;
                    try { hasPmp = await _backupService.HasPmpBackupForModAsync(modName).ConfigureAwait(false); } catch { }
                    if (!hasPmp)
                    {
                        var tracePmp = PerfTrace.Step(_logger, $"Ensure PMP {modName}");
                        await _backupService.CreateFullModBackupAsync(modName, _backupProgress, token).ConfigureAwait(false);
                        tracePmp.Dispose();
                    }
                }

                var mustEnsureBackup = _configService.Current.AutomaticHandledBySphene
                    || _configService.Current.EnableBackupBeforeConversion
                    || _configService.Current.EnableFullModBackupBeforeConversion;
                if (mustEnsureBackup)
                {
                    bool hasBackup = false;
                    try { hasBackup = await _backupService.HasBackupForModAsync(modName).ConfigureAwait(false); } catch { }
                    if (!hasBackup)
                    {
                        var traceBackup = PerfTrace.Step(_logger, $"Ensure Backup {modName}");
                        await _backupService.BackupAsync(modTextures, _backupProgress, token).ConfigureAwait(false);
                        traceBackup.Dispose();
                    }
                }

                var isLastPlannedMod = currentModIndex == totalMods;
                var redrawAfter = isLastPlannedMod || _cancelRequested;

                // Convert textures for the current mod; support cancellation between files.
                var traceConvert = PerfTrace.Step(_logger, $"Convert {modName}");
                await _penumbraIpc.ConvertTextureFilesAsync(_logger, modTextures, _conversionProgress, token, redrawAfter).ConfigureAwait(false);
                traceConvert.Dispose();

                if (_cancelRequested || token.IsCancellationRequested)
                {
                    break;
                }

                // After conversion: evaluate per-mod savings and auto-restore if conversion made it larger.
                try
                {
                    var traceSavings = PerfTrace.Step(_logger, $"Savings {modName}");
                    try { _modStateService.UpdateInstalledButNotConverted(modName, false); } catch { }
                    var stats = await _backupService.ComputeSavingsForModAsync(modName).ConfigureAwait(false);
                    if (stats != null && stats.ComparedFiles > 0)
                    {
                        try { _modStateService.UpdateSavings(modName, stats.OriginalBytes, stats.CurrentBytes, stats.ComparedFiles); } catch { }
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
                    traceSavings.Dispose();
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
            traceTotal.Dispose();
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
        var traceScan = PerfTrace.Step(_logger, "AutoCandidates ScanModTextures");
        var all = await _penumbraIpc.ScanModTexturesAsync().ConfigureAwait(false);
        traceScan.Dispose();
        if (all.Count == 0)
            return all;

        var traceUsed = PerfTrace.Step(_logger, "AutoCandidates UsedPaths");
        var used = await _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().ConfigureAwait(false);
        traceUsed.Dispose();
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
            var traceKeys = PerfTrace.Step(_logger, "AutoCandidates BackedKeysPerMod");
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
            traceKeys.Dispose();
        }
        catch { }

        var pmpConvertedRelByMod = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var tracePmpRel = PerfTrace.Step(_logger, "AutoCandidates PmpConvertedRel");
            var tasksPmp = modsToCheck
                .Select(m => _backupService.GetPmpConvertedRelPathsForModAsync(m)
                    .ContinueWith(t => (Mod: m, Rel: t.IsCompletedSuccessfully ? t.Result : new HashSet<string>(StringComparer.OrdinalIgnoreCase)), TaskScheduler.Default))
                .ToArray();
            await Task.WhenAll(tasksPmp).ConfigureAwait(false);
            foreach (var t in tasksPmp)
            {
                var (mod, rel) = t.Result;
                pmpConvertedRelByMod[mod] = rel ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }
            tracePmpRel.Dispose();
        }
        catch { }

        var result = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in all)
        {
            var file = kv.Key;
            try
            {
                var rel = !string.IsNullOrWhiteSpace(root) ? Path.GetRelativePath(root, file) : file;
                var parts = rel.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var modName = parts.Length > 1 ? parts[0] : string.Empty;
                
                if (string.IsNullOrWhiteSpace(modName) || !modsToCheck.Contains(modName))
                    continue;

                if (!string.IsNullOrWhiteSpace(_penumbraIpc.ModDirectory))
                {
                    try
                    {
                        var modRoot = Path.Combine(_penumbraIpc.ModDirectory!, modName);
                        var relToMod = Path.GetRelativePath(modRoot, file).Replace('\\', '/');
                        if (pmpConvertedRelByMod.TryGetValue(modName, out var relSet) && relSet != null && relSet.Contains(relToMod))
                            continue;
                    }
                    catch { }
                }

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

    public async Task UpdateAllModTextureCountsAsync()
    {
        try
        {
            var grouped = await GetGroupedCandidateTexturesAsync().ConfigureAwait(false);
            var mods = await GetAllModFoldersAsync().ConfigureAwait(false);
            foreach (var mod in mods)
            {
                try
                {
                    var leaf = mod.Replace('/', System.IO.Path.DirectorySeparatorChar).Replace('\\', System.IO.Path.DirectorySeparatorChar).TrimEnd(System.IO.Path.DirectorySeparatorChar);
                    var segs = leaf.Split(System.IO.Path.DirectorySeparatorChar);
                    var key = segs.Length > 0 ? segs[^1] : mod;
                    int count = 0;
                    if (grouped.TryGetValue(key, out var list) && list != null)
                        count = list.Count;
                    _modStateService.UpdateTextureCount(key, count);
                }
                catch { }
            }
        }
        catch { }
    }

    public async Task UpdateAllModMetadataAsync()
    {
        try
        {
            var names = await GetModDisplayNamesAsync().ConfigureAwait(false);
            var tags = await GetModTagsAsync().ConfigureAwait(false);
            var allMods = await GetAllModFoldersAsync().ConfigureAwait(false);
            Guid? collId = null;
            try { var coll = await GetCurrentCollectionAsync().ConfigureAwait(false); collId = coll?.Id; } catch { }
            Dictionary<string, (bool Enabled, int Priority, bool Inherited, bool Temporary)> states = new(StringComparer.OrdinalIgnoreCase);
            if (collId.HasValue)
            {
                try { states = await GetAllModEnabledStatesAsync(collId.Value).ConfigureAwait(false); } catch { }
            }

            var snap = _modStateService.Snapshot();
            foreach (var mod in allMods)
            {
                try
                {
                    var leafNorm = mod.Replace('/', System.IO.Path.DirectorySeparatorChar).Replace('\\', System.IO.Path.DirectorySeparatorChar).TrimEnd(System.IO.Path.DirectorySeparatorChar);
                    var segs = leafNorm.Split(System.IO.Path.DirectorySeparatorChar);
                    var key = segs.Length > 0 ? segs[^1] : mod;
                    var display = names.TryGetValue(key, out var dn) ? (dn ?? string.Empty) : string.Empty;
                    var tagList = tags.TryGetValue(key, out var tl) ? (tl ?? new List<string>()) : new List<string>();
                    _modStateService.UpdateDisplayAndTags(key, display, tagList);

                    var abs = _backupService.GetModAbsolutePath(key) ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(abs) || !Directory.Exists(abs))
                    {
                        try { _logger.LogDebug("Mod absolute path not found for {mod}", key); } catch { }
                    }
                    string rel = string.Empty;
                    try
                    {
                        var root = _penumbraIpc.ModDirectory ?? string.Empty;
                        if (!string.IsNullOrWhiteSpace(root) && !string.IsNullOrWhiteSpace(abs))
                            rel = Path.GetRelativePath(root, abs).Replace('\\', '/');
                    }
                    catch { rel = string.Empty; }
                    if (string.IsNullOrWhiteSpace(rel))
                    {
                        try { _logger.LogDebug("Relative path not computed for {mod}", key); } catch { }
                    }
                    var existing = snap.TryGetValue(key, out var e) && e != null ? e : null;
                    var ver = existing?.CurrentVersion ?? string.Empty;
                    var auth = existing?.CurrentAuthor ?? string.Empty;
                    _modStateService.UpdateCurrentModInfo(key, abs, rel, ver, auth);

                    if (states.TryGetValue(mod, out var st))
                        _modStateService.UpdateEnabledState(key, st.Enabled, st.Priority);
                }
                catch { }
            }
        }
        catch { }
    }

    public async Task UpdateModTextureFilesAsync(string mod)
    {
        try
        {
            var files = await GetModTextureFilesAsync(mod).ConfigureAwait(false);
            _modStateService.UpdateTextureFiles(mod, files);
        }
        catch { }
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

    public async Task UpdateAllModUsedTextureFilesAsync()
    {
        try
        {
            var used = await GetUsedModTexturePathsAsync().ConfigureAwait(false);
            var byMod = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            foreach (var u in used)
            {
                try
                {
                    var path = u ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(path)) continue;
                    var rel = !string.IsNullOrWhiteSpace(root) ? System.IO.Path.GetRelativePath(root, path) : path;
                    var parts = rel.Split(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar);
                    var modName = parts.Length > 1 ? parts[0] : string.Empty;
                    if (string.IsNullOrWhiteSpace(modName)) continue;
                    if (!byMod.TryGetValue(modName, out var list)) byMod[modName] = list = new List<string>();
                    list.Add(path.Replace('/', '\\'));
                }
                catch { }
            }
            var snap = _modStateService.Snapshot();
            foreach (var kv in snap)
            {
                try
                {
                    var key = kv.Key;
                    var list = byMod.TryGetValue(key, out var l) ? l : new List<string>();
                    _modStateService.UpdateUsedTextureFiles(key, list);
                }
                catch { }
            }
        }
        catch { }
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

    public async Task<string> GetModDisplayNameAsync(string mod)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            return mod ?? string.Empty;
        }
        return await _penumbraIpc.GetModDisplayNameAsync(mod).ConfigureAwait(false);
    }

    public async Task<List<string>> GetModTagsAsync(string mod)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            return new List<string>();
        }
        return await _penumbraIpc.GetModTagsAsync(mod).ConfigureAwait(false);
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
        DetachPenumbraSubscriptions();
        StopAutoConversionWatcher();
        try { _cts.Cancel(); } catch { }
        try { _cts.Dispose(); } catch { }
    }
}
