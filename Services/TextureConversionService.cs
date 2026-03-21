using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Helpers;
using System;
using Penumbra.Api.Enums;
using System.Collections.Concurrent;
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
    private DateTime _lastChangeTriggerUtc = DateTime.MinValue;

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
    public event Action<string>? OnPenumbraModPathsChanged;
    private DateTime _lastModSettingChangedAt = DateTime.MinValue;
    private DateTime _lastAutoAttemptUtc = DateTime.MinValue;
    private Dictionary<string, string[]>? _lastAutoCandidates;
    private CancellationTokenSource? _autoPollCts;
    private Task? _autoPollTask;
    private CancellationTokenSource? _pathSyncCts;
    private Task? _pathSyncTask;
    private readonly SemaphoreSlim _pathSyncSemaphore = new(1, 1);
    private DateTime _lastPathSyncUtc = DateTime.MinValue;
    private string _lastPathCategoryFingerprint = string.Empty;
    private int _pathSyncPending;
    private string _pathSyncPendingReason = string.Empty;
    private bool _pathSyncPendingPeriodic;
    private long _pathSyncQueuedCount;
    private long _pathSyncRunCount;
    private long _pathSyncAppliedCount;
    private long _pathSyncSkippedCount;
    private long _pathSyncErrorCount;
    private long _pathSyncLastElapsedMs;
    private DateTime _pathSyncLastRunUtc = DateTime.MinValue;
    private string _pathSyncLastReason = string.Empty;
    private bool _pathSyncLastPeriodic;
    private bool _pathSyncLastFromPendingQueue;
    private CancellationTokenSource? _backupRefreshCts;
    private bool _subscriptionsAttached = false;
    private string _lastChangedModDir = string.Empty;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, DateTime> _recentChangedMods = new(System.StringComparer.OrdinalIgnoreCase);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, CancellationTokenSource> _modSettingRefreshDebounceByMod = new(System.StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _modSettingRefreshSemaphore = new(1, 1);
    private readonly ConcurrentDictionary<string, string> _trackedModSettingState = new(StringComparer.OrdinalIgnoreCase);
    private DateTime _autoPollCooldownUntilUtc = DateTime.MinValue;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _modFileRefreshDebounceByMod = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _modFileRefreshSemaphore = new(1, 1);
    private const int LargeStartupModThreshold = 400;

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
            _lastChangedModDir = dir ?? string.Empty;
            _lastChangeTriggerUtc = DateTime.UtcNow;
            try { if (!string.IsNullOrWhiteSpace(dir)) _recentChangedMods[dir!] = DateTime.UtcNow; } catch { }
            _ = Task.Run(async () => { try { if (!string.IsNullOrWhiteSpace(dir)) await UpdateModMetadataForModAsync(dir!).ConfigureAwait(false); } catch { } });
            ScheduleModFileRefresh(dir, "mod-added");
        };
        _onModDeleted = dir => {
            OnPenumbraModDeleted?.Invoke(dir);
            try
            {
                // Only remove state if the mod truly no longer exists in Penumbra
                var exists = false;
                try { exists = _penumbraIpc.ModExists(dir); } catch { exists = true; }
                if (!exists)
                    _modStateService.RemoveEntry(dir);
                else
                    _logger.LogDebug("Skip RemoveEntry: mod still exists after ModDeleted broadcast {dir}", dir);
            }
            catch { }
            _lastChangedModDir = dir ?? string.Empty;
            _lastChangeTriggerUtc = DateTime.UtcNow;
            try { if (!string.IsNullOrWhiteSpace(dir)) _recentChangedMods[dir!] = DateTime.UtcNow; } catch { }
        };
        _onModPathChanged = (modDir, newPath) =>
        {
            try
            {
                _lastChangedModDir = modDir ?? string.Empty;
                _lastChangeTriggerUtc = DateTime.UtcNow;
                try { if (!string.IsNullOrWhiteSpace(modDir)) _recentChangedMods[modDir!] = DateTime.UtcNow; } catch { }
                _ = Task.Run(async () => { try { if (!string.IsNullOrWhiteSpace(modDir)) await UpdateModMetadataForModAsync(modDir, newPath).ConfigureAwait(false); } catch { } });
                ScheduleModFileRefresh(modDir, "mod-path-changed");
                _ = Task.Run(async () => { try { await ReconcileModPathsAndCategoriesAsync("mod-path-changed", false).ConfigureAwait(false); } catch { } });
            }
            catch { }
        };
        _onModMoved = (oldDir, newDir) =>
        {
            try
            {
                _ = Task.Run(() =>
                {
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(oldDir) && !string.IsNullOrWhiteSpace(newDir)
                            && !string.Equals(oldDir, newDir, StringComparison.OrdinalIgnoreCase))
                        {
                            _modStateService.MoveEntry(oldDir, newDir);
                        }
                        var names = _penumbraIpc.GetModList();
                        var tuple = _penumbraIpc.GetModPath(newDir);
                        var disp = names.TryGetValue(newDir, out var dn) ? (dn ?? string.Empty) : string.Empty;
                        var (folder, leaf) = SplitFolderAndLeaf(tuple.FullPath ?? string.Empty, disp);
                        var existing = _modStateService.Get(newDir);
                        _modStateService.UpdateCurrentModInfo(newDir, existing.ModAbsolutePath, folder, existing.CurrentVersion, existing.CurrentAuthor, string.IsNullOrWhiteSpace(leaf) ? (existing.RelativeModName ?? string.Empty) : leaf);
                        _modStateService.Save();
                    }
                    catch { }
                });
                ScheduleModFileRefresh(newDir, "mod-moved");
            }
            catch { }
        };
        _onEnabledChanged = enabled =>
        {
            OnPenumbraEnabledChanged?.Invoke(enabled);
            if (enabled)
            {
                _lastChangedModDir = string.Empty;
            }
        };
        _onModSettingChanged = (change, collectionId, modDir, inherited) =>
        {
            if (change == ModSettingChange.TemporarySetting)
            {
                _ = Task.Run(async () =>
                {
                    if (!await ShouldProcessTemporarySettingChangeAsync(modDir).ConfigureAwait(false))
                    {
                        _logger.LogDebug("Ignored TemporarySetting event for mod {mod}: effective state unchanged", modDir);
                        return;
                    }
                    ProcessModSettingChanged(change, collectionId, modDir, inherited);
                });
                return;
            }
            ProcessModSettingChanged(change, collectionId, modDir, inherited);
        };
        _onModsChanged = () =>
        {
            OnPenumbraModsChanged?.Invoke();
            TryScheduleAutomaticConversion("mods-changed");
            _lastChangeTriggerUtc = DateTime.UtcNow;
            _ = Task.Run(async () => { try { await ReconcileModPathsAndCategoriesAsync("mods-changed", false).ConfigureAwait(false); } catch { } });
        };

        _onPlayerResourcesChanged = () =>
        {
            OnPlayerResourcesChanged?.Invoke();
            TryScheduleAutomaticConversion("player-resources-changed");
            _lastChangeTriggerUtc = DateTime.UtcNow;
        };

        
    }

    private void AttachPenumbraSubscriptions()
    {
        if (_subscriptionsAttached) return;
        _penumbraIpc.ModAdded += _onModAdded;
        _penumbraIpc.ModDeleted += _onModDeleted;
        _penumbraIpc.ModPathChanged += _onModPathChanged;
        _penumbraIpc.ModMoved += _onModMoved;
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
        try { _penumbraIpc.ModPathChanged -= _onModPathChanged; } catch { }
        try { _penumbraIpc.ModMoved -= _onModMoved; } catch { }
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
            StartPathSyncWatcher();
            _ = Task.Run(async () => { try { await PrimeTrackedModSettingStateAsync().ConfigureAwait(false); } catch { } });
        }
        else
        {
            DetachPenumbraSubscriptions();
            StopAutoConversionWatcher();
            StopPathSyncWatcher();
            try { _backupRefreshCts?.Cancel(); } catch { }
            try { _backupRefreshCts?.Dispose(); } catch { }
            _backupRefreshCts = null;
            _trackedModSettingState.Clear();
        }
    }

    private void ProcessModSettingChanged(ModSettingChange change, Guid collectionId, string modDir, bool inherited)
    {
        _lastModSettingChangedAt = DateTime.UtcNow;
        OnPenumbraModSettingChanged?.Invoke(change, collectionId, modDir, inherited);
        TryScheduleAutomaticConversion("mod-setting-changed");
        ScheduleModSettingRefresh(modDir);
    }

    private static string BuildTrackedStateKey(Guid collectionId, (bool Enabled, int Priority, bool Inherited, bool Temporary) st)
    {
        return string.Concat(
            collectionId.ToString("N"), "|",
            st.Enabled ? "1" : "0", "|",
            st.Priority.ToString(System.Globalization.CultureInfo.InvariantCulture), "|",
            st.Inherited ? "1" : "0", "|",
            st.Temporary ? "1" : "0");
    }

    private void UpdateTrackedState(string modDir, Guid collectionId, (bool Enabled, int Priority, bool Inherited, bool Temporary) st)
    {
        if (string.IsNullOrWhiteSpace(modDir))
            return;
        _trackedModSettingState[modDir.Trim()] = BuildTrackedStateKey(collectionId, st);
    }

    private async Task<bool> ShouldProcessTemporarySettingChangeAsync(string modDir)
    {
        if (string.IsNullOrWhiteSpace(modDir))
            return false;
        try
        {
            var coll = await GetCurrentCollectionAsync().ConfigureAwait(false);
            if (!coll.HasValue)
                return true;
            var states = await GetAllModEnabledStatesAsync(coll.Value.Id).ConfigureAwait(false);
            if (!states.TryGetValue(modDir, out var st))
                return true;
            var key = BuildTrackedStateKey(coll.Value.Id, st);
            var modKey = modDir.Trim();
            if (!_trackedModSettingState.TryGetValue(modKey, out var prev))
            {
                _trackedModSettingState[modKey] = key;
                return true;
            }
            if (string.Equals(prev, key, StringComparison.Ordinal))
                return false;
            _trackedModSettingState[modKey] = key;
            return true;
        }
        catch
        {
            return true;
        }
    }

    private async Task PrimeTrackedModSettingStateAsync()
    {
        try
        {
            var coll = await GetCurrentCollectionAsync().ConfigureAwait(false);
            if (!coll.HasValue)
                return;
            var states = await GetAllModEnabledStatesAsync(coll.Value.Id).ConfigureAwait(false);
            foreach (var kv in states)
            {
                if (string.IsNullOrWhiteSpace(kv.Key))
                    continue;
                UpdateTrackedState(kv.Key, coll.Value.Id, kv.Value);
            }
            _logger.LogDebug("Primed tracked mod setting state for {count} mods", states.Count);
        }
        catch { }
    }

    public static string NormalizeLeafKey(string mod)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return string.Empty;

        var normalized = mod.Replace('/', System.IO.Path.DirectorySeparatorChar)
            .Replace('\\', System.IO.Path.DirectorySeparatorChar)
            .Trim()
            .TrimEnd(System.IO.Path.DirectorySeparatorChar);

        if (normalized.Length == 0)
            return string.Empty;

        var lastSep = normalized.LastIndexOf(System.IO.Path.DirectorySeparatorChar);
        if (lastSep < 0)
            return normalized;

        if (lastSep >= normalized.Length - 1)
            return string.Empty;

        return normalized.Substring(lastSep + 1).Trim();
    }

    public static string ComputeRelativePathFromAbs(string root, string abs)
    {
        if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(abs)) return string.Empty;
        try { return System.IO.Path.GetRelativePath(root, abs).Replace('\\', '/'); } catch { return string.Empty; }
    }

    private static (string folder, string leaf) SplitFolderAndLeaf(string relFull, string dispName)
    {
        var p = (relFull ?? string.Empty).Replace('\\', '/').TrimEnd('/');
        var d = (dispName ?? string.Empty).Replace('\\', '/').TrimEnd('/');
        if (string.IsNullOrWhiteSpace(p)) return (string.Empty, string.Empty);
        var pSegs = p.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (!string.IsNullOrWhiteSpace(d))
        {
            var dSegs = d.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (pSegs.Length >= dSegs.Length)
            {
                bool match = true;
                for (int i = 0; i < dSegs.Length; i++)
                {
                    if (!string.Equals(pSegs[pSegs.Length - dSegs.Length + i], dSegs[i], StringComparison.OrdinalIgnoreCase))
                    { match = false; break; }
                }
                if (match)
                {
                    var folder = string.Join('/', pSegs.Take(pSegs.Length - dSegs.Length));
                    return (folder, d);
                }
            }
        }
        var idx = p.LastIndexOf('/');
        if (idx >= 0) return (p.Substring(0, idx), p.Substring(idx + 1));
        return (string.Empty, p);
    }

    private async Task<List<(string Prefix, string ModDir)>> BuildUsedPathModPrefixesAsync()
    {
        var rulesByPrefix = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        void AddRule(string? prefixRaw, string? modDirRaw)
        {
            var prefix = (prefixRaw ?? string.Empty).Replace('\\', '/').Trim().Trim('/');
            var modDir = modDirRaw ?? string.Empty;
            if (string.IsNullOrWhiteSpace(prefix) || string.IsNullOrWhiteSpace(modDir))
                return;
            rulesByPrefix[prefix] = modDir;
        }

        try
        {
            var modPaths = await _penumbraIpc.GetModPathsAsync().ConfigureAwait(false);
            foreach (var kv in modPaths)
            {
                var modDir = kv.Key ?? string.Empty;
                AddRule(kv.Value, modDir);
                AddRule(modDir, modDir);
            }
        }
        catch { }

        if (rulesByPrefix.Count == 0)
        {
            try
            {
                var modList = _penumbraIpc.GetModList();
                foreach (var modDir in modList.Keys)
                    AddRule(modDir, modDir);
            }
            catch { }
        }

        if (rulesByPrefix.Count == 0)
        {
            try
            {
                var snap = _modStateService.Snapshot();
                foreach (var kv in snap)
                {
                    var modDir = kv.Key ?? string.Empty;
                    var e = kv.Value;
                    var rel = string.Empty;
                    var folder = e?.PenumbraRelativePath ?? string.Empty;
                    var leaf = e?.RelativeModName ?? string.Empty;
                    if (!string.IsNullOrWhiteSpace(folder) && !string.IsNullOrWhiteSpace(leaf))
                        rel = string.Concat(folder, "/", leaf);
                    else if (!string.IsNullOrWhiteSpace(leaf))
                        rel = leaf;
                    else if (!string.IsNullOrWhiteSpace(folder))
                        rel = folder;
                    AddRule(rel, modDir);
                    AddRule(modDir, modDir);
                }
            }
            catch { }
        }

        return rulesByPrefix
            .OrderByDescending(kv => kv.Key.Length)
            .Select(kv => (kv.Key, kv.Value))
            .ToList();
    }

    private static string ResolveModDirFromUsedPath(string? usedPath, string root, IReadOnlyList<(string Prefix, string ModDir)> prefixRules)
    {
        var input = (usedPath ?? string.Empty).Replace('\\', '/').Trim();
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;

        string rel = input;
        if (!string.IsNullOrWhiteSpace(root))
        {
            try { rel = Path.GetRelativePath(root, input).Replace('\\', '/'); }
            catch { rel = input; }
        }
        rel = rel.Trim().TrimStart('/');
        if (string.IsNullOrWhiteSpace(rel))
            return string.Empty;

        for (int i = 0; i < prefixRules.Count; i++)
        {
            var rule = prefixRules[i];
            if (string.Equals(rel, rule.Prefix, StringComparison.OrdinalIgnoreCase)
                || rel.StartsWith(rule.Prefix + "/", StringComparison.OrdinalIgnoreCase))
                return rule.ModDir;
        }

        var slash = rel.IndexOf('/');
        if (slash > 0)
            return rel.Substring(0, slash);
        return string.Empty;
    }

    public event Action<(int processed, int total, int etaSeconds)>? OnStartupProgress;

    public async Task RunInitialParallelUpdateAsync(int maxThreads, CancellationToken token)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var modsRaw = await GetAllModFoldersAsync().ConfigureAwait(false);
            var mods = modsRaw.Where(m => NormalizeLeafKey(m).Length > 0).ToList();
            sw.Stop();
            try { _logger.LogTrace("Initial update step: GetAllModFolders count={count} elapsedMs={ms}", mods.Count, (int)sw.ElapsedMilliseconds); } catch { }
            var total = mods.Count;
            var start = DateTime.UtcNow;
            sw.Restart();
            var names = await GetModDisplayNamesAsync().ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogTrace("Initial update step: GetModDisplayNames count={count} elapsedMs={ms}", names.Count, (int)sw.ElapsedMilliseconds); } catch { }
            sw.Restart();
            var tags = await GetModTagsAsync().ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogTrace("Initial update step: GetModTags mods={mods} elapsedMs={ms}", tags.Count, (int)sw.ElapsedMilliseconds); } catch { }
            sw.Restart();
            var groupedTextures = await GetGroupedCandidateTexturesAsync(false).ConfigureAwait(false);
            sw.Stop();
            try { _logger.LogTrace("Initial update step: GroupedTextureScan mods={mods} elapsedMs={ms}", groupedTextures.Count, (int)sw.ElapsedMilliseconds); } catch { }
            Guid? collId = null;
            sw.Restart();
            try { var coll = await GetCurrentCollectionAsync().ConfigureAwait(false); collId = coll?.Id; } catch { }
            sw.Stop();
            try { _logger.LogTrace("Initial update step: GetCurrentCollection elapsedMs={ms}", (int)sw.ElapsedMilliseconds); } catch { }
            Dictionary<string, (bool Enabled, int Priority, bool Inherited, bool Temporary)> states = new(StringComparer.OrdinalIgnoreCase);
            if (collId.HasValue)
            {
                sw.Restart();
                try { states = await GetAllModEnabledStatesAsync(collId.Value).ConfigureAwait(false); } catch { }
                sw.Stop();
                try { _logger.LogTrace("Initial update step: GetAllModEnabledStates count={count} elapsedMs={ms}", states.Count, (int)sw.ElapsedMilliseconds); } catch { }
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
                        if (key.Length == 0)
                            return;
                        int fileCount = 0;
                        try
                        {
                            if (groupedTextures.TryGetValue(key, out var list) && list != null)
                                fileCount = list.Count;
                        }
                        catch { fileCount = 0; }

                        bool skipUpdate = false;
                        int finalCount = fileCount;
                        if (fileCount == 0)
                        {
                            var existingState = _modStateService.Get(key);
                            if (existingState != null && existingState.TotalTextures > 0)
                            {
                                // Verify with direct filesystem scan
                                var directFiles = await GetModTextureFilesAsync(key).ConfigureAwait(false);
                                if (directFiles.Count > 0)
                                {
                                    finalCount = directFiles.Count;
                                    try { _logger.LogTrace("Startup: Correction for {mod}: Penumbra=0, FS={count}", key, finalCount); } catch { }
                                }
                                else
                                {
                                    skipUpdate = true;
                                    try { _logger.LogTrace("Startup: Skip UpdateTextureCount for {mod}: new=0, old={old} (transient protection)", key, existingState.TotalTextures); } catch { }
                                }
                            }
                        }

                        if (!skipUpdate)
                            _modStateService.UpdateTextureCount(key, finalCount);

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
                        string relFolder = string.Empty;
                        string relLeaf = string.Empty;
                        try
                        {
                            var (ec, fullPath, _, _) = _penumbraIpc.GetModPath(key);
                            var p = (fullPath ?? string.Empty).Replace('\\', '/');
                            var relFull = string.Empty;
                            if (ec == Penumbra.Api.Enums.PenumbraApiEc.Success && !string.IsNullOrWhiteSpace(p))
                                relFull = p;
                            else
                                relFull = ComputeRelativePathFromAbs(root, abs);
                            relFull = (relFull ?? string.Empty).Replace('\\', '/').TrimEnd('/');
                            if (!string.IsNullOrWhiteSpace(relFull))
                            {
                                var dispName = names.TryGetValue(key, out var dispTmp) ? (dispTmp ?? string.Empty) : string.Empty;
                                if (!string.IsNullOrWhiteSpace(dispName))
                                {
                                    var pSegs = relFull.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                                    var dSegs = dispName.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                                    if (pSegs.Length >= dSegs.Length)
                                    {
                                        bool tailMatches = true;
                                        for (int i = 0; i < dSegs.Length; i++)
                                        {
                                            var ps = pSegs[pSegs.Length - dSegs.Length + i];
                                            var ds = dSegs[i];
                                            if (!string.Equals(ps, ds, StringComparison.OrdinalIgnoreCase))
                                            {
                                                tailMatches = false;
                                                break;
                                            }
                                        }
                                        if (tailMatches)
                                        {
                                            relFolder = string.Join('/', pSegs.Take(pSegs.Length - dSegs.Length));
                                            relLeaf = dispName;
                                        }
                                    }
                                }
                                if (string.IsNullOrWhiteSpace(relLeaf))
                                {
                                    var idx = relFull.LastIndexOf('/');
                                    if (idx >= 0)
                                    {
                                        relFolder = relFull.Substring(0, idx);
                                        relLeaf = relFull.Substring(idx + 1);
                                    }
                                    else
                                    {
                                        relFolder = string.Empty;
                                        relLeaf = relFull;
                                    }
                                }
                            }
                        }
                        catch
                        {
                            var relFull = ComputeRelativePathFromAbs(root, abs);
                            relFull = (relFull ?? string.Empty).Replace('\\', '/').TrimEnd('/');
                            if (!string.IsNullOrWhiteSpace(relFull))
                            {
                                var dispName = names.TryGetValue(key, out var dispTmp2) ? (dispTmp2 ?? string.Empty) : string.Empty;
                                if (!string.IsNullOrWhiteSpace(dispName))
                                {
                                    var pSegs = relFull.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                                    var dSegs = dispName.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                                    if (pSegs.Length >= dSegs.Length)
                                    {
                                        bool tailMatches = true;
                                        for (int i = 0; i < dSegs.Length; i++)
                                        {
                                            var ps = pSegs[pSegs.Length - dSegs.Length + i];
                                            var ds = dSegs[i];
                                            if (!string.Equals(ps, ds, StringComparison.OrdinalIgnoreCase))
                                            {
                                                tailMatches = false;
                                                break;
                                            }
                                        }
                                        if (tailMatches)
                                        {
                                            relFolder = string.Join('/', pSegs.Take(pSegs.Length - dSegs.Length));
                                            relLeaf = dispName;
                                        }
                                    }
                                }
                                if (string.IsNullOrWhiteSpace(relLeaf))
                                {
                                    var idx = relFull.LastIndexOf('/');
                                    if (idx >= 0)
                                    {
                                        relFolder = relFull.Substring(0, idx);
                                        relLeaf = relFull.Substring(idx + 1);
                                    }
                                    else
                                    {
                                        relFolder = string.Empty;
                                        relLeaf = relFull;
                                    }
                                }
                            }
                        }
                        var existing = snap.TryGetValue(key, out var e) && e != null ? e : null;
                        var ver = existing?.CurrentVersion ?? string.Empty;
                        var auth = existing?.CurrentAuthor ?? string.Empty;
                        try
                        {
                            var meta = await _penumbraIpc.GetModMetadataAsync(key).ConfigureAwait(false);
                            if (meta != null)
                            {
                                if (!string.IsNullOrWhiteSpace(meta.Version))
                                    ver = meta.Version!;
                                if (!string.IsNullOrWhiteSpace(meta.Author))
                                    auth = meta.Author!;
                            }
                        }
                        catch { }
                        _modStateService.UpdateCurrentModInfo(key, abs, relFolder, ver, auth, relLeaf);

                        if (states.TryGetValue(mod, out var st))
                            _modStateService.UpdateEnabledState(key, st.Enabled, st.Priority);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Initial update failed for mod {mod}", mod);
                    }
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
                try { _logger.LogTrace("Initial update step: Per-mod tasks elapsedMs={ms}", (int)sw.ElapsedMilliseconds); } catch { }
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
                    _logger.LogInformation("Initial update completed: mods={mods}, elapsedMs={elapsed}, avgPerModMs={avg}", total, (int)Math.Round(elapsedTotal.TotalMilliseconds), avgPerModMs);
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
            var grouped = await GetGroupedCandidateTexturesAsync(false).ConfigureAwait(false);
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
            if (string.Equals(reason, "auto-poll", StringComparison.OrdinalIgnoreCase))
            {
                if (now < _autoPollCooldownUntilUtc)
                {
                    try { _logger.LogDebug("Auto-poll skipped due to mode toggle cooldown"); } catch { }
                    return;
                }
                if (_lastChangeTriggerUtc == DateTime.MinValue || (now - _lastChangeTriggerUtc) > TimeSpan.FromMinutes(5))
                {
                    try { _logger.LogDebug("Auto-poll skipped: no recent mod change"); } catch { }
                    return;
                }
            }
            _lastAutoAttemptUtc = now;
            if (string.Equals(reason, "auto-poll", StringComparison.OrdinalIgnoreCase))
            {
                if (_lastChangeTriggerUtc == DateTime.MinValue || (now - _lastChangeTriggerUtc) > TimeSpan.FromMinutes(5))
                {
                    try { _logger.LogDebug("Auto-poll skipped: no recent mod change"); } catch { }
                    return;
                }
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    var needing = await GetUsedModsNeedingProcessingAsync().ConfigureAwait(false);
                    if (needing.Count == 0)
                    {
                        try { _logger.LogDebug("Auto trigger: no used mods need processing ({reason})", reason); } catch { }
                        return;
                    }
                    var candidates = await GetAutomaticCandidateTexturesAsync().ConfigureAwait(false);
                    if (candidates == null || candidates.Count == 0)
                    {
                        try { _logger.LogDebug("No automatic candidates found on trigger: {reason}", reason); } catch { }
                        return;
                    }

                    // Limit to recently changed mods when available to avoid mass updates
                    try
                    {
                        var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
                        var now = DateTime.UtcNow;
                        var recent = new System.Collections.Generic.HashSet<string>(System.StringComparer.OrdinalIgnoreCase);
                        foreach (var kv in _recentChangedMods)
                        {
                            try
                            {
                                if ((now - kv.Value) <= System.TimeSpan.FromMinutes(2))
                                    recent.Add(kv.Key);
                            }
                            catch { }
                        }
                        if (recent.Count > 0)
                        {
                            var root = _penumbraIpc.ModDirectory ?? string.Empty;
                            var filtered = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
                            foreach (var kv in candidates)
                            {
                                try
                                {
                                    var modName = ResolveModDirFromUsedPath(kv.Key, root, usedPathPrefixes);
                                    if (recent.Contains(modName))
                                        filtered[kv.Key] = kv.Value;
                                }
                                catch { }
                            }
                            if (filtered.Count > 0)
                                candidates = filtered;
                        }
                    }
                    catch { }

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

    private void StartPathSyncWatcher()
    {
        try
        {
            StopPathSyncWatcher();
            _pathSyncCts = new CancellationTokenSource();
            var token = _pathSyncCts.Token;
            _pathSyncTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            break;
                        await ReconcileModPathsAndCategoriesAsync("periodic-path-sync", true).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex)
                    {
                        try { _logger.LogDebug(ex, "Path-sync watcher iteration failed"); } catch { }
                    }
                }
            }, token);
            _ = Task.Run(async () => { try { await ReconcileModPathsAndCategoriesAsync("startup-path-sync", true).ConfigureAwait(false); } catch { } });
            try { _logger.LogDebug("Path-sync watcher started"); } catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogDebug(ex, "Failed to start path-sync watcher"); } catch { }
        }
    }

    private void StopPathSyncWatcher()
    {
        try { _pathSyncCts?.Cancel(); } catch { }
        try { _pathSyncCts?.Dispose(); } catch { }
        _pathSyncCts = null;
        _pathSyncTask = null;
        try { _logger.LogDebug("Path-sync watcher stopped"); } catch { }
    }

    private async Task ReconcileModPathsAndCategoriesAsync(string reason, bool periodic)
    {
        if (!_penumbraIpc.APIAvailable)
            return;
        if (periodic && (DateTime.UtcNow - _lastPathSyncUtc) < TimeSpan.FromSeconds(20))
            return;
        var fromPendingQueue = string.Equals(reason, "path-sync-pending", StringComparison.OrdinalIgnoreCase);
        if (!await _pathSyncSemaphore.WaitAsync(0).ConfigureAwait(false))
        {
            _pathSyncPendingReason = string.IsNullOrWhiteSpace(reason) ? "path-sync-pending" : reason;
            _pathSyncPendingPeriodic = _pathSyncPendingPeriodic || periodic;
            Interlocked.Exchange(ref _pathSyncPending, 1);
            Interlocked.Increment(ref _pathSyncQueuedCount);
            return;
        }
        var syncReason = string.IsNullOrWhiteSpace(reason) ? "path-sync" : reason;
        var changed = false;
        var runSw = System.Diagnostics.Stopwatch.StartNew();
        Interlocked.Increment(ref _pathSyncRunCount);
        try
        {
            var paths = await _penumbraIpc.GetModPathsAsync().ConfigureAwait(false);
            var names = _penumbraIpc.GetModList();
            if (paths.Count == 0 && names.Count == 0)
            {
                _lastPathSyncUtc = DateTime.UtcNow;
                Interlocked.Increment(ref _pathSyncSkippedCount);
                try { _logger.LogDebug("Path/category sync skipped: reason={reason} empty-penumbra-set", syncReason); } catch { }
                return;
            }
            var fp = BuildPathCategoryFingerprint(paths, names);
            if (periodic && !string.IsNullOrWhiteSpace(_lastPathCategoryFingerprint) && string.Equals(_lastPathCategoryFingerprint, fp, StringComparison.Ordinal))
            {
                _lastPathSyncUtc = DateTime.UtcNow;
                Interlocked.Increment(ref _pathSyncSkippedCount);
                try { _logger.LogDebug("Path/category sync skipped: reason={reason} unchanged", syncReason); } catch { }
                return;
            }
            await UpdateAllModPathsAsync(paths, names).ConfigureAwait(false);
            changed = !string.IsNullOrWhiteSpace(_lastPathCategoryFingerprint) && !string.Equals(_lastPathCategoryFingerprint, fp, StringComparison.Ordinal);
            if (string.IsNullOrWhiteSpace(_lastPathCategoryFingerprint))
                changed = true;
            _lastPathCategoryFingerprint = fp;
            _lastPathSyncUtc = DateTime.UtcNow;
            try { _logger.LogDebug("Path/category sync completed: reason={reason}", syncReason); } catch { }
            if (changed)
            {
                Interlocked.Increment(ref _pathSyncAppliedCount);
                try { OnPenumbraModPathsChanged?.Invoke(syncReason); } catch { }
                if (!string.Equals(syncReason, "mod-path-changed", StringComparison.OrdinalIgnoreCase))
                {
                    try { OnPenumbraModsChanged?.Invoke(); } catch { }
                }
            }
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _pathSyncErrorCount);
            try { _logger.LogDebug(ex, "Path/category sync failed: reason={reason}", syncReason); } catch { }
        }
        finally
        {
            runSw.Stop();
            _pathSyncLastElapsedMs = (long)Math.Round(runSw.Elapsed.TotalMilliseconds);
            _pathSyncLastRunUtc = DateTime.UtcNow;
            _pathSyncLastReason = syncReason;
            _pathSyncLastPeriodic = periodic;
            _pathSyncLastFromPendingQueue = fromPendingQueue;
            _pathSyncSemaphore.Release();
            if (Interlocked.Exchange(ref _pathSyncPending, 0) == 1)
            {
                var pendingReason = _pathSyncPendingReason;
                var pendingPeriodic = _pathSyncPendingPeriodic;
                _pathSyncPendingReason = string.Empty;
                _pathSyncPendingPeriodic = false;
                _ = Task.Run(async () =>
                {
                    try { await ReconcileModPathsAndCategoriesAsync(string.IsNullOrWhiteSpace(pendingReason) ? "path-sync-pending" : pendingReason, pendingPeriodic).ConfigureAwait(false); } catch { }
                });
            }
        }
    }

    public Task ForcePathAndCategorySyncAsync(string reason)
    {
        return ReconcileModPathsAndCategoriesAsync(string.IsNullOrWhiteSpace(reason) ? "manual-path-sync" : reason, false);
    }

    public PathSyncDebugStats GetPathSyncDebugStats()
    {
        return new PathSyncDebugStats
        {
            LastSyncUtc = _pathSyncLastRunUtc,
            LastReason = _pathSyncLastReason,
            LastElapsedMs = _pathSyncLastElapsedMs,
            LastPeriodic = _pathSyncLastPeriodic,
            LastSyncFromPendingQueue = _pathSyncLastFromPendingQueue,
            QueuedCount = _pathSyncQueuedCount,
            RunCount = _pathSyncRunCount,
            AppliedCount = _pathSyncAppliedCount,
            SkippedCount = _pathSyncSkippedCount,
            ErrorCount = _pathSyncErrorCount,
            PendingNow = _pathSyncPending != 0,
            PendingReason = _pathSyncPendingReason,
        };
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
            var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
            var byMod = new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in textures)
            {
                var source = kvp.Key;
                var modName = ResolveModDirFromUsedPath(source, root, usedPathPrefixes);
                if (string.IsNullOrWhiteSpace(modName))
                    modName = "<unknown>";

                if (!byMod.TryGetValue(modName, out var modDict))
                {
                    modDict = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
                    byMod[modName] = modDict;
                }
                modDict[source] = kvp.Value;
            }
            // Compute planned mods excluding those marked as excluded
            var excludedSet = _configService.Current.ExcludedMods ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var plannedMods = byMod
                .Where(kv => !excludedSet.Contains(kv.Key))
                .ToList();

            var excludedInInput = byMod.Keys.Where(m => excludedSet.Contains(m)).ToList();
            try { _logger.LogDebug("Conversion planning: inputMods={input}, plannedMods={planned}, excluded={excluded}", byMod.Count, plannedMods.Count, excludedInInput.Count); } catch { }

            var totalMods = plannedMods.Count;
            var currentModIndex = 0;
            foreach (var kv in plannedMods)
            {
                var modName = kv.Key;
                var modTextures = kv.Value;
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

                var isLastPlannedMod = currentModIndex == totalMods;
                var redrawAfter = isLastPlannedMod || _cancelRequested;

                var traceConvert = PerfTrace.Step(_logger, $"Convert {modName}");
                await _penumbraIpc.ConvertTextureFilesAsync(_logger, modTextures, _conversionProgress, token, redrawAfter).ConfigureAwait(false);
                traceConvert.Dispose();

                if (_cancelRequested || token.IsCancellationRequested)
                {
                    try { _logger.LogDebug("Conversion cancelled after mod {mod}", modName); } catch { }
                    break;
                }

                try
                {
                    var traceSavings = PerfTrace.Step(_logger, $"Savings {modName}");
                    try { _modStateService.UpdateInstalledButNotConverted(modName, false); } catch { }
                    var stats = await _backupService.ComputeSavingsForModAsync(modName).ConfigureAwait(false);
                    if (stats != null && stats.ComparedFiles > 0)
                    {
                        try { _modStateService.UpdateSavings(modName, stats.OriginalBytes, stats.CurrentBytes, stats.ComparedFiles); } catch { }
                        try { _modStateService.SetLastConvertUtc(modName, DateTime.UtcNow); } catch { }
                        try { _modStateService.Save(); } catch { }
                        if (stats.CurrentBytes > stats.OriginalBytes)
                        {
                            _configService.Current.InefficientMods ??= new List<string>();
                            if (!_configService.Current.InefficientMods.Any(m => string.Equals(m, modName, StringComparison.OrdinalIgnoreCase)))
                            {
                                _configService.Current.InefficientMods.Add(modName);
                                _configService.Save();
                                _logger.LogDebug("Marked mod {modName} as inefficient (larger after conversion)", modName);
                            }

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
            return new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        }
        // Build candidates only from currently-used textures for efficiency
        var traceUsed = PerfTrace.Step(_logger, "AutoCandidates UsedPaths");
        var used = await _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().ConfigureAwait(false);
        traceUsed.Dispose();
        if (used.Count == 0)
            return new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

        var root = _penumbraIpc.ModDirectory ?? string.Empty;
        var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
        var modsToCheck = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in used)
        {
            try
            {
                var modName = ResolveModDirFromUsedPath(path, root, usedPathPrefixes);
                if (!string.IsNullOrWhiteSpace(modName))
                    modsToCheck.Add(modName);
            }
            catch { }
        }

        // Skip mods that already have backup and recorded conversion in mod_state
        var snap = _modStateService.Snapshot();
        var modsToScan = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var m in modsToCheck)
        {
            try
            {
                if (snap.TryGetValue(m, out var ms) && ms != null)
                {
                    var hasBackup = ms.HasTextureBackup || ms.HasPmpBackup;
                    var converted = ms.ComparedFiles > 0 && !ms.InstalledButNotConverted;
                    var totalTexturesKnown = ms.TotalTextures > 0;
                    // Consider fully processed when backup exists and conversion stats are present,
                    // and either all textures compared or the mod is marked not-installed-but-not-converted
                    if (hasBackup && converted && (!totalTexturesKnown || ms.ComparedFiles >= ms.TotalTextures))
                        continue;
                }
            }
            catch { }
            modsToScan.Add(m);
        }

        // Query backed keys per mod to exclude already-backed files while allowing new ones.
        var backedKeysPerMod = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var traceKeys = PerfTrace.Step(_logger, "AutoCandidates BackedKeysPerMod");
            var tasks = modsToScan
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
            var tasksPmp = modsToScan
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
        foreach (var file in used)
        {
            try
            {
                var modName = ResolveModDirFromUsedPath(file, root, usedPathPrefixes);
                if (string.IsNullOrWhiteSpace(modName) || !modsToScan.Contains(modName))
                    continue;
                if (_configService.Current.ExcludedMods != null && _configService.Current.ExcludedMods.Contains(modName))
                    continue;

                try
                {
                    var modRoot = string.IsNullOrWhiteSpace(_penumbraIpc.ModDirectory) ? string.Empty : Path.Combine(_penumbraIpc.ModDirectory!, modName);
                    if (!string.IsNullOrWhiteSpace(modRoot))
                    {
                        var relToMod = Path.GetRelativePath(modRoot, file).Replace('\\', '/');
                        if (pmpConvertedRelByMod.TryGetValue(modName, out var relSet) && relSet != null && relSet.Contains(relToMod))
                            continue;
                    }
                }
                catch { }

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

            result[file] = Array.Empty<string>();
        }

        return result;
    }

    public async Task<HashSet<string>> GetUsedModsProcessedAsync()
    {
        var processed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var used = await _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().ConfigureAwait(false);
            var mods = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
            foreach (var path in used)
            {
                try
                {
                    var mod = ResolveModDirFromUsedPath(path, root, usedPathPrefixes);
                    if (!string.IsNullOrWhiteSpace(mod)) mods.Add(mod);
                }
                catch { }
            }
            var snap = _modStateService.Snapshot();
            foreach (var m in mods)
            {
                if (snap.TryGetValue(m, out var ms) && ms != null)
                {
                    var hasBackup = ms.HasTextureBackup || ms.HasPmpBackup;
                    var converted = ms.ComparedFiles > 0 && !ms.InstalledButNotConverted;
                    var totalKnown = ms.TotalTextures > 0;
                    if (hasBackup && converted && (!totalKnown || ms.ComparedFiles >= ms.TotalTextures))
                        processed.Add(m);
                }
            }
        }
        catch { }
        return processed;
    }

    public async Task<HashSet<string>> GetUsedModsNeedingProcessingAsync()
    {
        var needing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var usedProcessed = await GetUsedModsProcessedAsync().ConfigureAwait(false);
            var used = await _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().ConfigureAwait(false);
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
            foreach (var path in used)
            {
                try
                {
                    var mod = ResolveModDirFromUsedPath(path, root, usedPathPrefixes);
                    if (!string.IsNullOrWhiteSpace(mod) && !usedProcessed.Contains(mod)) needing.Add(mod);
                }
                catch { }
            }
        }
        catch { }
        return needing;
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

    public Task<Dictionary<string, List<string>>> GetGroupedCandidateTexturesAsync()
    {
        return GetGroupedCandidateTexturesAsync(false);
    }

    public async Task<Dictionary<string, List<string>>> GetGroupedCandidateTexturesAsync(bool forceFullScan)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            _logger.LogDebug("Penumbra API not available; grouped auto-scan aborted");
            return new Dictionary<string, List<string>>();
        }

        var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var mods = await GetAllModFoldersAsync().ConfigureAwait(false);
        var snap = _modStateService.Snapshot();
        var root = _penumbraIpc.ModDirectory ?? string.Empty;
        var preferGroupedSource = forceFullScan || mods.Count >= LargeStartupModThreshold;
        Dictionary<string, List<string>>? groupedSource = null;
        if (preferGroupedSource)
        {
            try { groupedSource = await _penumbraIpc.ScanModTexturesGroupedAsync().ConfigureAwait(false); }
            catch { groupedSource = null; }
        }
        foreach (var mod in mods)
        {
            var key = NormalizeLeafKey(mod);
            if (string.IsNullOrWhiteSpace(key)) continue;
            snap.TryGetValue(key, out var entry);

            var modPath = GetModAbsolutePathForScan(entry, key, root);
            DateTime lastWriteUtc = DateTime.MinValue;
            if (!string.IsNullOrWhiteSpace(modPath) && Directory.Exists(modPath))
            {
                try { lastWriteUtc = Directory.GetLastWriteTimeUtc(modPath); } catch { }
            }

            var hasPersisted = (entry?.TotalTextures ?? 0) > 0;
            var needsScan = forceFullScan
                            || entry == null
                            || entry.NeedsRescan
                            || entry.LastKnownWriteUtc == DateTime.MinValue
                            || (lastWriteUtc != DateTime.MinValue && lastWriteUtc > entry.LastKnownWriteUtc)
                            || !hasPersisted;

            if (!needsScan)
            {
                var persisted = _modStateService.ReadDetailTextures(key);
                if (persisted.Count > 0)
                    result[key] = persisted;
                continue;
            }

            List<string> files = new List<string>();
            if (groupedSource != null && groupedSource.TryGetValue(key, out var groupedFiles) && groupedFiles != null)
            {
                files = groupedFiles;
            }
            else if (!string.IsNullOrWhiteSpace(modPath) && Directory.Exists(modPath))
            {
                files = await GetModTextureFilesByPathAsync(modPath).ConfigureAwait(false);
            }
            else
            {
                files = await GetModTextureFilesAsync(key).ConfigureAwait(false);
            }

            if (files.Count > 0)
            {
                result[key] = files;
                _modStateService.UpdateTextureFiles(key, files, lastWriteUtc);
            }
            else if (hasPersisted)
            {
                var persisted = _modStateService.ReadDetailTextures(key);
                if (persisted.Count > 0)
                    result[key] = persisted;
            }
        }

        return result;
    }

    public async Task<Dictionary<string, string>> GetModDisplayNamesAsync()
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (_penumbraIpc.APIAvailable)
        {
            try { dict = await _penumbraIpc.GetModDisplayNamesAsync().ConfigureAwait(false); }
            catch { }
        }
        var snap = _modStateService.Snapshot();
        foreach (var kv in snap)
        {
            if (string.IsNullOrWhiteSpace(kv.Key))
                continue;
            if (!dict.ContainsKey(kv.Key))
            {
                var dn = kv.Value?.DisplayName;
                if (!string.IsNullOrWhiteSpace(dn))
                    dict[kv.Key] = dn;
                else
                    dict[kv.Key] = kv.Key;
            }
        }
        return dict;
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
            return GetModTextureFilesByPathAsync(modPath);
        }
        catch { }
        return Task.FromResult(files);
    }

    private Task<List<string>> GetModTextureFilesByPathAsync(string modPath)
    {
        var files = new List<string>();
        try
        {
            if (string.IsNullOrWhiteSpace(modPath) || !Directory.Exists(modPath))
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

    private string GetModAbsolutePathForScan(ModStateEntry? entry, string key, string root)
    {
        if (!string.IsNullOrWhiteSpace(entry?.ModAbsolutePath) && Directory.Exists(entry.ModAbsolutePath))
            return entry.ModAbsolutePath;
        if (!string.IsNullOrWhiteSpace(entry?.PenumbraRelativePath))
        {
            var rel = entry.PenumbraRelativePath.Replace('/', Path.DirectorySeparatorChar).TrimStart(Path.DirectorySeparatorChar);
            var candidate = Path.Combine(root, rel);
            if (Directory.Exists(candidate)) return candidate;
        }
        if (!string.IsNullOrWhiteSpace(root))
        {
            var candidate = Path.Combine(root, key);
            if (Directory.Exists(candidate)) return candidate;
        }
        return string.Empty;
    }

    public async Task<List<string>> GetAllModFoldersAsync()
    {
        var list = new List<string>();
        if (_penumbraIpc.APIAvailable)
        {
            try { list = await _penumbraIpc.GetAllModFoldersAsync().ConfigureAwait(false); }
            catch { }
        }
        var cached = _modStateService.Snapshot().Keys;
        var set = new HashSet<string>(list, StringComparer.OrdinalIgnoreCase);
        foreach (var c in cached)
            if (!string.IsNullOrWhiteSpace(c)) set.Add(c);
        return set.ToList();
    }

    public async Task UpdateAllModTextureCountsAsync()
    {
        try
        {
            var grouped = await GetGroupedCandidateTexturesAsync(false).ConfigureAwait(false);
            var mods = await GetAllModFoldersAsync().ConfigureAwait(false);
            if (mods.Count > 0 && grouped.Count == 0)
            {
                try { _logger.LogDebug("Skip UpdateAllModTextureCounts: grouped=0 while mods>0 (transient)"); } catch { }
                return;
            }
            foreach (var kv in grouped)
            {
                try
                {
                    var key = kv.Key;
                    var list = kv.Value ?? new List<string>();
                    
                    int finalCount = list.Count;
                    if (finalCount == 0)
                    {
                        var existing = _modStateService.Get(key);
                        if (existing != null && existing.TotalTextures > 0)
                        {
                            var directFiles = await GetModTextureFilesAsync(key).ConfigureAwait(false);
                            if (directFiles.Count > 0)
                            {
                                finalCount = directFiles.Count;
                                try { _logger.LogDebug("Correction: Penumbra scan said 0 for {mod}, but FS found {count}", key, finalCount); } catch { }
                            }
                            else
                            {
                                try { _logger.LogDebug("Skip UpdateTextureCount for {mod}: new=0, old={old} (transient protection)", key, existing.TotalTextures); } catch { }
                                continue;
                            }
                        }
                    }

                    _modStateService.UpdateTextureCount(key, finalCount);
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
                    string relFolder = string.Empty, relLeaf = string.Empty;
                    try
                    {
                        var rf = (rel ?? string.Empty).Replace('\\', '/').TrimEnd('/');
                        if (!string.IsNullOrWhiteSpace(rf))
                        {
                            var idx = rf.LastIndexOf('/');
                            if (idx >= 0)
                            {
                                relFolder = rf.Substring(0, idx);
                                relLeaf = rf.Substring(idx + 1);
                            }
                            else
                            {
                                relFolder = string.Empty;
                                relLeaf = rf;
                            }
                        }
                    }
                    catch { relFolder = string.Empty; relLeaf = string.Empty; }
                    _modStateService.UpdateCurrentModInfo(key, abs, relFolder, ver, auth, relLeaf);

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
            var path = _backupService.GetModAbsolutePath(mod) ?? string.Empty;
            var existing = _modStateService.Get(mod);
            DateTime lastWriteUtc = DateTime.MinValue;
            if (!string.IsNullOrWhiteSpace(path) && Directory.Exists(path))
            {
                try { lastWriteUtc = Directory.GetLastWriteTimeUtc(path); } catch { }
            }
            if (existing != null
                && !existing.NeedsRescan
                && existing.LastKnownWriteUtc != DateTime.MinValue
                && lastWriteUtc != DateTime.MinValue
                && lastWriteUtc <= existing.LastKnownWriteUtc)
            {
                return;
            }
            var files = !string.IsNullOrWhiteSpace(path) && Directory.Exists(path)
                ? await GetModTextureFilesByPathAsync(path).ConfigureAwait(false)
                : await GetModTextureFilesAsync(mod).ConfigureAwait(false);
            if (files.Count == 0)
            {
                if (existing != null && existing.TotalTextures > 0)
                {
                    try { _logger.LogDebug("Skip UpdateTextureFiles for {mod}: new=0, old={old} (transient protection)", mod, existing.TotalTextures); } catch { }
                    return;
                }
            }
            _modStateService.UpdateTextureFiles(mod, files, lastWriteUtc);
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
            var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
            foreach (var u in used)
            {
                try
                {
                    var path = u ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(path)) continue;
                    var modName = ResolveModDirFromUsedPath(path, root, usedPathPrefixes);
                    if (string.IsNullOrWhiteSpace(modName)) continue;
                    if (!byMod.TryGetValue(modName, out var list)) byMod[modName] = list = new List<string>();
                    list.Add(path.Replace('/', '\\'));
                }
                catch { }
            }
            if (byMod.Count == 0)
            {
                try
                {
                    var snap = _modStateService.Snapshot();
                    foreach (var kv in snap)
                    {
                        var mod = kv.Key;
                        if (string.IsNullOrWhiteSpace(mod) || string.Equals(mod, "mod_state", StringComparison.OrdinalIgnoreCase))
                            continue;
                        var e = kv.Value;
                        if (e == null)
                            continue;
                        if (e.UsedTextureCount > 0 || (e.UsedTextureFiles != null && e.UsedTextureFiles.Count > 0))
                            _modStateService.UpdateUsedTextureFiles(mod, Array.Empty<string>());
                    }
                }
                catch { }
                try { _logger.LogDebug("Skip UpdateAllModUsedTextureFiles: byMod=0 (transient)"); } catch { }
                return;
            }
            foreach (var kv in byMod)
            {
                try
                {
                    var key = kv.Key;
                    var list = kv.Value ?? new List<string>();
                    _modStateService.UpdateUsedTextureFiles(key, list);
                }
                catch { }
            }
            try
            {
                var snap = _modStateService.Snapshot();
                foreach (var kv in snap)
                {
                    var mod = kv.Key;
                    if (string.IsNullOrWhiteSpace(mod) || string.Equals(mod, "mod_state", StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (byMod.ContainsKey(mod))
                        continue;
                    var e = kv.Value;
                    if (e == null)
                        continue;
                    if (e.UsedTextureCount > 0 || (e.UsedTextureFiles != null && e.UsedTextureFiles.Count > 0))
                        _modStateService.UpdateUsedTextureFiles(mod, Array.Empty<string>());
                }
            }
            catch { }
        }
        catch { }
    }

    public async Task<Dictionary<string, List<string>>> GetModTagsAsync()
    {
        var dict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        if (_penumbraIpc.APIAvailable)
        {
            try { dict = await _penumbraIpc.GetModTagsAsync().ConfigureAwait(false); }
            catch { }
        }
        var snap = _modStateService.Snapshot();
        foreach (var kv in snap)
        {
            if (string.IsNullOrWhiteSpace(kv.Key))
                continue;
            if (!dict.ContainsKey(kv.Key))
            {
                var t = kv.Value?.Tags;
                dict[kv.Key] = t != null ? new List<string>(t) : new List<string>();
            }
        }
        return dict;
    }

    public async Task UpdateAllModPathsAsync()
    {
        try
        {
            var paths = await _penumbraIpc.GetModPathsAsync().ConfigureAwait(false);
            var names = _penumbraIpc.GetModList();
            await UpdateAllModPathsAsync(paths, names).ConfigureAwait(false);
            _lastPathCategoryFingerprint = BuildPathCategoryFingerprint(paths, names);
        }
        catch { }
    }

    private async Task UpdateAllModPathsAsync(Dictionary<string, string> paths, Dictionary<string, string> names)
    {
        try
        {
            var snap = _modStateService.Snapshot();
            var modKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var k in paths.Keys)
            {
                if (!string.IsNullOrWhiteSpace(k))
                    modKeys.Add(k);
            }
            foreach (var k in names.Keys)
            {
                if (!string.IsNullOrWhiteSpace(k))
                    modKeys.Add(k);
            }
            foreach (var k in snap.Keys)
            {
                if (!string.IsNullOrWhiteSpace(k) && !string.Equals(k, "mod_state", StringComparison.OrdinalIgnoreCase))
                    modKeys.Add(k);
            }
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(root))
            {
                var penumbraAbsByKey = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var mod in modKeys)
                {
                    try
                    {
                        var candidate = Path.Combine(root, mod);
                        if (Directory.Exists(candidate))
                            penumbraAbsByKey[mod] = Path.GetFullPath(candidate);
                    }
                    catch { }
                }

                var penumbraKeysSet = new HashSet<string>(modKeys, StringComparer.OrdinalIgnoreCase);
                foreach (var kv in snap)
                {
                    var oldKey = kv.Key;
                    var entry = kv.Value;
                    if (string.IsNullOrWhiteSpace(oldKey) || string.Equals(oldKey, "mod_state", StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (penumbraKeysSet.Contains(oldKey))
                        continue;
                    var oldAbs = entry?.ModAbsolutePath ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(oldAbs))
                        continue;
                    try { oldAbs = Path.GetFullPath(oldAbs); } catch { }
                    foreach (var pair in penumbraAbsByKey)
                    {
                        if (string.Equals(pair.Value, oldAbs, StringComparison.OrdinalIgnoreCase))
                        {
                            _modStateService.MoveEntry(oldKey, pair.Key);
                            break;
                        }
                    }
                }
                snap = _modStateService.Snapshot();
            }
            foreach (var mod in modKeys)
            {
                try
                {
                    snap.TryGetValue(mod, out var e);
                    var pf = paths.TryGetValue(mod, out var val) ? (val ?? string.Empty) : string.Empty;
                    var disp = names.TryGetValue(mod, out var dn) ? (dn ?? string.Empty) : (e?.RelativeModName ?? string.Empty);
                    var (folder, leaf) = SplitFolderAndLeaf(pf, disp);
                    var abs = e?.ModAbsolutePath ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(abs) && !string.IsNullOrWhiteSpace(root))
                    {
                        try
                        {
                            var candidate = Path.Combine(root, mod);
                            if (Directory.Exists(candidate))
                                abs = candidate;
                        }
                        catch { }
                    }
                    if (string.IsNullOrWhiteSpace(pf))
                    {
                        var rel = ComputeRelativePathFromAbs(root, abs);
                        (folder, leaf) = SplitFolderAndLeaf(rel, disp);
                    }
                    var version = e?.CurrentVersion ?? string.Empty;
                    var author = e?.CurrentAuthor ?? string.Empty;
                    if (paths.ContainsKey(mod) || names.ContainsKey(mod))
                    {
                        try
                        {
                            var meta = await _penumbraIpc.GetModMetadataAsync(mod).ConfigureAwait(false);
                            if (meta != null)
                            {
                                if (!string.IsNullOrWhiteSpace(meta.Version))
                                    version = meta.Version!;
                                if (!string.IsNullOrWhiteSpace(meta.Author))
                                    author = meta.Author!;
                            }
                        }
                        catch { }
                    }
                    _modStateService.UpdateCurrentModInfo(mod, abs, folder, version, author, leaf);
                }
                catch { }
            }
            _modStateService.Save();
        }
        catch { }
    }

    private static string BuildPathCategoryFingerprint(Dictionary<string, string> paths, Dictionary<string, string> names)
    {
        try
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("P:");
            foreach (var kv in paths.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
            {
                var key = kv.Key ?? string.Empty;
                if (string.IsNullOrWhiteSpace(key))
                    continue;
                var val = (kv.Value ?? string.Empty).Replace('\\', '/').TrimEnd('/');
                sb.Append(key).Append('=').Append(val).Append(';');
            }
            sb.Append("|N:");
            foreach (var kv in names.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
            {
                var key = kv.Key ?? string.Empty;
                if (string.IsNullOrWhiteSpace(key))
                    continue;
                sb.Append(key).Append('=').Append(kv.Value ?? string.Empty).Append(';');
            }
            return sb.ToString();
        }
        catch
        {
            return $"p={paths.Count};n={names.Count}";
        }
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

    public async Task<PenumbraIpc.ModMetadata?> GetModMetadataAsync(string mod)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            return null;
        }
        return await _penumbraIpc.GetModMetadataAsync(mod).ConfigureAwait(false);
    }

    public bool IsPenumbraApiAvailable()
    {
        return _penumbraIpc.APIAvailable;
    }

    public string GetPenumbraModDirectory()
    {
        return _penumbraIpc.ModDirectory ?? string.Empty;
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
            var needingMods = await GetUsedModsNeedingProcessingAsync().ConfigureAwait(false);
            if (needingMods.Count == 0)
            {
                try { _logger.LogDebug("Automatic conversion skipped: all used mods already processed"); } catch { }
                return;
            }
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
    private readonly Action<string, string>? _onModPathChanged;
    private readonly Action<string, string>? _onModMoved;
    private readonly Action<bool>? _onEnabledChanged;
    private readonly Action<ModSettingChange, Guid, string, bool>? _onModSettingChanged;
    private readonly Action? _onModsChanged;
    private readonly Action? _onPlayerResourcesChanged;

    public void Dispose()
    {
        DetachPenumbraSubscriptions();
        StopAutoConversionWatcher();
        StopPathSyncWatcher();
        foreach (var kvp in _modSettingRefreshDebounceByMod)
        {
            try { kvp.Value.Cancel(); } catch { }
            try { kvp.Value.Dispose(); } catch { }
        }
        _modSettingRefreshDebounceByMod.Clear();
        foreach (var kvp in _modFileRefreshDebounceByMod)
        {
            try { kvp.Value.Cancel(); } catch { }
            try { kvp.Value.Dispose(); } catch { }
        }
        _modFileRefreshDebounceByMod.Clear();
        try { _modSettingRefreshSemaphore.Dispose(); } catch { }
        try { _modFileRefreshSemaphore.Dispose(); } catch { }
        try { _pathSyncSemaphore.Dispose(); } catch { }
        try { _cts.Cancel(); } catch { }
        try { _cts.Dispose(); } catch { }
    }

    private void ScheduleModSettingRefresh(string modDir)
    {
        if (string.IsNullOrWhiteSpace(modDir))
        {
            return;
        }

        var key = modDir.Trim();
        var cts = new CancellationTokenSource();
        var previous = _modSettingRefreshDebounceByMod.AddOrUpdate(key, cts, (_, old) =>
        {
            try { old.Cancel(); } catch { }
            try { old.Dispose(); } catch { }
            return cts;
        });

        if (!ReferenceEquals(previous, cts))
        {
            try { previous.Cancel(); } catch { }
            try { previous.Dispose(); } catch { }
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(350, cts.Token).ConfigureAwait(false);
                if (cts.Token.IsCancellationRequested)
                {
                    return;
                }

                await RefreshModSettingStateAsync(key).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
            catch { }
            finally
            {
                if (_modSettingRefreshDebounceByMod.TryGetValue(key, out var active) && ReferenceEquals(active, cts))
                {
                    _modSettingRefreshDebounceByMod.TryRemove(key, out _);
                }
                try { cts.Dispose(); } catch { }
            }
        });
    }

    private void ScheduleModFileRefresh(string? modDir, string reason)
    {
        if (string.IsNullOrWhiteSpace(modDir))
            return;

        var key = modDir.Trim();
        var cts = new CancellationTokenSource();
        var previous = _modFileRefreshDebounceByMod.AddOrUpdate(key, cts, (_, old) =>
        {
            try { old.Cancel(); } catch { }
            try { old.Dispose(); } catch { }
            return cts;
        });
        if (!ReferenceEquals(previous, cts))
        {
            try { previous.Cancel(); } catch { }
            try { previous.Dispose(); } catch { }
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(450, cts.Token).ConfigureAwait(false);
                if (cts.Token.IsCancellationRequested)
                    return;
                await RefreshModFileStateAsync(key, reason).ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }
            catch { }
            finally
            {
                if (_modFileRefreshDebounceByMod.TryGetValue(key, out var active) && ReferenceEquals(active, cts))
                    _modFileRefreshDebounceByMod.TryRemove(key, out _);
                try { cts.Dispose(); } catch { }
            }
        });
    }

    private async Task RefreshModFileStateAsync(string modDir, string reason)
    {
        await _modFileRefreshSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await UpdateModTextureFilesAsync(modDir).ConfigureAwait(false);
            _lastChangedModDir = modDir;
            _lastChangeTriggerUtc = DateTime.UtcNow;
            try { _recentChangedMods[modDir] = DateTime.UtcNow; } catch { }
            try { _logger.LogDebug("Targeted mod file-size refresh completed: mod={mod} reason={reason}", modDir, reason); } catch { }
        }
        finally
        {
            _modFileRefreshSemaphore.Release();
        }
    }

    private async Task RefreshModSettingStateAsync(string modDir)
    {
        await _modSettingRefreshSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            _modStateService.BeginBatch();
            try
            {
                var coll = await GetCurrentCollectionAsync().ConfigureAwait(false);
                if (coll.HasValue)
                {
                    var states = await GetAllModEnabledStatesAsync(coll.Value.Id).ConfigureAwait(false);
                    if (states.TryGetValue(modDir, out var st))
                    {
                        _modStateService.UpdateEnabledState(modDir, st.Enabled, st.Priority);
                        UpdateTrackedState(modDir, coll.Value.Id, st);
                    }
                }
            }
            catch { }

            try { await UpdateUsedTextureFilesForModAsync(modDir).ConfigureAwait(false); } catch { }
            try { await UpdateModMetadataForModAsync(modDir).ConfigureAwait(false); } catch { }
            _lastChangedModDir = modDir;
            _lastChangeTriggerUtc = DateTime.UtcNow;
            try { _recentChangedMods[modDir] = DateTime.UtcNow; } catch { }
        }
        finally
        {
            try { _modStateService.EndBatch(); } catch { }
            _modSettingRefreshSemaphore.Release();
        }
    }
    private async Task UpdateModMetadataForModAsync(string modDir, string? preferredPath = null)
    {
        try
        {
            var display = await GetModDisplayNameAsync(modDir).ConfigureAwait(false);
            var tags = await GetModTagsAsync(modDir).ConfigureAwait(false);
            var pathForState = preferredPath ?? string.Empty;
            if (string.IsNullOrWhiteSpace(pathForState))
            {
                var tuple = _penumbraIpc.GetModPath(modDir);
                pathForState = tuple.FullPath ?? string.Empty;
            }
            var (folder, leaf) = SplitFolderAndLeaf(pathForState, display);
            var existing = _modStateService.Get(modDir);
            var version = existing.CurrentVersion ?? string.Empty;
            var author = existing.CurrentAuthor ?? string.Empty;
            try
            {
                var meta = await _penumbraIpc.GetModMetadataAsync(modDir).ConfigureAwait(false);
                if (meta != null)
                {
                    if (!string.IsNullOrWhiteSpace(meta.Version))
                        version = meta.Version!;
                    if (!string.IsNullOrWhiteSpace(meta.Author))
                        author = meta.Author!;
                }
            }
            catch { }
            _modStateService.BeginBatch();
            _modStateService.UpdateDisplayAndTags(modDir, display, tags);
            var absolutePath = !string.IsNullOrWhiteSpace(pathForState) ? pathForState : existing.ModAbsolutePath ?? string.Empty;
            _modStateService.UpdateCurrentModInfo(modDir, absolutePath, folder, version, author, leaf);
            var coll = await GetCurrentCollectionAsync().ConfigureAwait(false);
            if (coll.HasValue)
            {
                var states = await GetAllModEnabledStatesAsync(coll.Value.Id).ConfigureAwait(false);
                if (states.TryGetValue(modDir, out var st))
                    _modStateService.UpdateEnabledState(modDir, st.Enabled, st.Priority);
            }
            _modStateService.EndBatch();
        }
        catch { }
    }

    private async Task UpdateUsedTextureFilesForModAsync(string modDir)
    {
        try
        {
            var used = await GetUsedModTexturePathsAsync().ConfigureAwait(false);
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            var usedPathPrefixes = await BuildUsedPathModPrefixesAsync().ConfigureAwait(false);
            var list = new List<string>();
            foreach (var u in used)
            {
                try
                {
                    var p = u ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(p)) continue;
                    var key = ResolveModDirFromUsedPath(p, root, usedPathPrefixes);
                    if (string.Equals(key, modDir, StringComparison.OrdinalIgnoreCase))
                        list.Add(p.Replace('/', '\\'));
                }
                catch { }
            }
            _modStateService.UpdateUsedTextureFiles(modDir, list);
        }
        catch { }
    }
    public void OnProcessingModeChanged(TextureProcessingMode mode)
    {
        try
        {
            if (mode == TextureProcessingMode.Automatic)
            {
                _autoPollCooldownUntilUtc = DateTime.UtcNow.AddMinutes(5);
                _lastAutoAttemptUtc = DateTime.UtcNow;
                _logger.LogDebug("Processing mode changed to Automatic: applying auto-poll cooldown");
            }
            else
            {
                _autoPollCooldownUntilUtc = DateTime.MinValue;
            }
        }
        catch { }
    }
}

public sealed class PathSyncDebugStats
{
    public DateTime LastSyncUtc { get; set; } = DateTime.MinValue;
    public string LastReason { get; set; } = string.Empty;
    public long LastElapsedMs { get; set; } = 0;
    public bool LastPeriodic { get; set; } = false;
    public bool LastSyncFromPendingQueue { get; set; } = false;
    public long QueuedCount { get; set; } = 0;
    public long RunCount { get; set; } = 0;
    public long AppliedCount { get; set; } = 0;
    public long SkippedCount { get; set; } = 0;
    public long ErrorCount { get; set; } = 0;
    public bool PendingNow { get; set; } = false;
    public string PendingReason { get; set; } = string.Empty;
}
