using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Services;
using System.Numerics;
using System.Collections.Concurrent;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Windows.Forms;
using System.IO;
using System.Linq;
using System.Diagnostics;

namespace ShrinkU.UI;

public sealed partial class ConversionUI : Window, IDisposable
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly TextureConversionService _conversionService;
    private readonly TextureBackupService _backupService;
    private readonly Action? _openSettings;

    private string _scanFilter = string.Empty;
    private float _leftPanelWidthRatio = 0.45f;
    private float _leftPanelWidthPx = 0f;
    private bool _leftWidthInitialized = false;
    private bool _leftWidthDirty = false;
    private float _scannedFirstColWidth = 28f;
    private float _scannedFileColWidth = 0f;
    private float _scannedSizeColWidth = 85f;
    private float _scannedCompressedColWidth = 85f;
    private float _scannedActionColWidth = 60f;
    private readonly Vector4 _compressedTextColor = new Vector4(0.60f, 0.95f, 0.65f, 1f);
    private ScanSortKind _scanSortKind = ScanSortKind.ModName;
    private bool _scanSortAsc = true;
    private bool _initialScanQueued = false;
    private bool _filterPenumbraUsedOnly = false;
    private bool _filterNonConvertibleMods = true;
    private bool _filterInefficientMods = false;
    private HashSet<string> _penumbraUsedFiles = new(StringComparer.OrdinalIgnoreCase);
    private bool _loadingPenumbraUsed = false;

    // Folder structure and collection state
    private Dictionary<string, string> _modPaths = new(StringComparer.OrdinalIgnoreCase);
    private string _modPathsSig = string.Empty;
    private bool _loadingModPaths = false;
    private Dictionary<Guid, string> _collections = new();
    private Guid? _selectedCollectionId = null;
    private Dictionary<string, (bool Enabled, int Priority, bool Inherited, bool Temporary)> _modEnabledStates
        = new(StringComparer.OrdinalIgnoreCase);
    // Debounce heavy scans to avoid repeated disk IO during rapid Penumbra changes
    private volatile bool _scanInProgress = false;
    private DateTime _lastScanAt = DateTime.MinValue;
    private readonly object _modsChangedLock = new();
    private CancellationTokenSource? _modsChangedDebounceCts;
    // Debounce token for enabled-state reloads on ModSettingChanged
    private CancellationTokenSource? _enabledStatesDebounceCts;

    private string _currentTexture = string.Empty;
    private int _convertedCount = 0;
    private int _backupIndex = 0;
    private int _backupTotal = 0;
    private bool _running = false;
    private string _currentModName = string.Empty;
    private int _currentModIndex = 0;
    private int _totalMods = 0;
    private int _currentModTotalFiles = 0;
    // Tag filtering state
    private Dictionary<string, IReadOnlyList<string>> _modTags = new(StringComparer.OrdinalIgnoreCase);
    private string _excludedTagsInput = string.Empty;
    private HashSet<string> _excludedTagsNormalized = new(StringComparer.OrdinalIgnoreCase);
    // Expanded state tracking for table view
    private HashSet<string> _expandedMods = new(StringComparer.OrdinalIgnoreCase);
    private HashSet<string> _selectedEmptyMods = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _selectedCountByMod = new(StringComparer.OrdinalIgnoreCase);
    private bool _footerTotalsDirty = true;
    private string _footerTotalsSignature = string.Empty;
    private long _footerTotalUncompressed = 0;
    private long _footerTotalCompressed = 0;
    private long _footerTotalSaved = 0;
    private int _perModSavingsRevision = 0;
    private int _cachedTotalRows = 0;
    private string _rowsSig = string.Empty;
    private List<FlatRow> _flatRows = new();
    private string _flatRowsSig = string.Empty;
    private string _folderSizeCacheSig = string.Empty;
    private Dictionary<string, (long orig, long comp)> _folderSizeCache = new(StringComparer.OrdinalIgnoreCase);
    private string _folderCountsCacheSig = string.Empty;
    private Dictionary<string, (int modsTotal, int modsConverted, int texturesTotal, int texturesConverted)> _folderCountsCache = new(StringComparer.OrdinalIgnoreCase);
    
    
    private HashSet<string> _expandedFolders = new(StringComparer.OrdinalIgnoreCase);

    // Cached backup storage info for statistics
    private Task<(long totalSize, int fileCount)>? _backupStorageInfoTask = null;
    private DateTime _lastBackupStorageInfoUpdate = DateTime.MinValue;
    private (long totalSize, int fileCount) _cachedBackupStorageInfo = (0L, 0);

    // Cached savings info comparing backups vs current Penumbra files
    private Task<TextureBackupService.BackupSavingsStats>? _savingsInfoTask = null;
    private DateTime _lastSavingsInfoUpdate = DateTime.MinValue;
    private TextureBackupService.BackupSavingsStats _cachedSavingsInfo = new();
    private Task<Dictionary<string, TextureBackupService.ModSavingsStats>>? _perModSavingsTask = null;
    private Dictionary<string, TextureBackupService.ModSavingsStats> _cachedPerModSavings = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, Dictionary<string, long>> _cachedPerModOriginalSizes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Task<Dictionary<string, long>>> _perModOriginalSizesTasks = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, string[]> _texturesToConvert = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<string>> _scannedByMod = new(StringComparer.OrdinalIgnoreCase);
    private Dictionary<string, string> _modDisplayNames = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _selectedTextures = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _modsWithBackupCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _modsWithTexBackupCache = new(StringComparer.OrdinalIgnoreCase);
    private DateTime _disableActionsUntilUtc = DateTime.MinValue;
    private readonly Dictionary<string, string> _fileOwnerMod = new(StringComparer.OrdinalIgnoreCase);
    private void ClearModCaches(string mod)
    {
        _cacheService.ClearModCaches(mod);
        try { _cachedPerModSavings.Remove(mod); } catch { }
        try { _cachedPerModOriginalSizes.Remove(mod); } catch { }
    }

    private void ClearAllCaches()
    {
        _cacheService.ClearAll();
        try { _cachedPerModSavings.Clear(); } catch { }
        try { _cachedPerModOriginalSizes.Clear(); } catch { }
        _needsUIRefresh = true;

        try
        {
            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
            _perModSavingsTask.ContinueWith(ps =>
            {
                if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                {
                    _cachedPerModSavings = ps.Result;
                    _needsUIRefresh = true;
                }
            }, TaskScheduler.Default);
        }
        catch { }
    }

    private void OnModeChanged()
    {
        _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(1);
        _needsUIRefresh = true;
        try
        {
            if (_perModSavingsTask == null || _perModSavingsTask.IsCompleted)
            {
                _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                _perModSavingsTask.ContinueWith(ps =>
                {
                    if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                    {
                        _cachedPerModSavings = ps.Result;
                        _needsUIRefresh = true;
                    }
                }, TaskScheduler.Default);
            }
        }
        catch { }
    }
    private bool ActionsDisabled()
    {
        return _running || _conversionService.IsConverting || DateTime.UtcNow < _disableActionsUntilUtc;
    }
    private Dictionary<string, string[]> BuildToConvert(IEnumerable<string> files)
    {
        var dict = new Dictionary<string, string[]>(StringComparer.Ordinal);
        foreach (var f in files)
        {
            if (string.IsNullOrWhiteSpace(f)) continue;
            dict[f] = Array.Empty<string>();
        }
        return dict;
    }
    private readonly ConcurrentDictionary<string, byte> _modsBackupCheckInFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _modsWithPmpCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, byte> _modsPmpCheckInFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, (string version, string author, DateTime createdUtc, string pmpFileName)> _modsPmpMetaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, (string version, string author, DateTime createdUtc, string zipFileName)> _modsZipMetaCache = new(StringComparer.OrdinalIgnoreCase);
    // Cache file sizes to avoid per-frame disk I/O in UI rendering
    private readonly ConcurrentDictionary<string, long> _fileSizeCache = new(StringComparer.OrdinalIgnoreCase);
    private Task? _fileSizeWarmupTask = null;
    private readonly ModStateService _modStateService;
    private readonly ConversionCacheService _cacheService;
    // Queue for marshalling UI state updates onto the main thread during Draw
    private readonly ConcurrentQueue<Action> _uiThreadActions = new();
    // Cancellation for restore operations
    private CancellationTokenSource? _restoreCancellationTokenSource = null;
    // Per-mod restore progress state
    private string _currentRestoreMod = string.Empty;
    private int _currentRestoreModIndex = 0;
    private volatile bool _orphanScanInFlight = false;
    private DateTime _lastOrphanScanUtc = DateTime.MinValue;
    private List<TextureBackupService.OrphanBackupInfo> _orphaned = new();
    private int _currentRestoreModTotal = 0;
    private bool _restoreAfterCancel = false;
    private string _cancelTargetMod = string.Empty;
    private string _statusMessage = string.Empty;
    private DateTime _statusMessageAt = DateTime.MinValue;
    private bool _statusPersistent = false;
    
    // UI refresh flag to force ImGui redraw after async data updates
    private volatile bool _needsUIRefresh = false;
    // Snapshot of known Penumbra mod folders to guard heavy scans
    private HashSet<string> _knownModFolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    // Debounce sequence to ensure trailing-edge execution for mod add/delete
    private int _modsChangedDebounceSeq = 0;
    // Timestamp of last heavy scan to rate-limit repeated rescans
    private DateTime _lastHeavyScanAt = DateTime.MinValue;
    // Poller to keep Used-Only list in sync even when no redraw occurs
    private CancellationTokenSource? _usedResourcesPollCts;
    private Task? _usedResourcesPollTask;
    private volatile bool _usedWatcherActive = false;

    // Stored delegate references for clean unsubscription on dispose
    private readonly Action<(string, int)> _onConversionProgress;
    private readonly Action<(string, int, int)> _onBackupProgress;
    private readonly Action _onConversionCompleted;
    private readonly Action<(string modName, int current, int total, int fileTotal)> _onModProgress;
    private readonly Action _onPenumbraModsChanged;
    private readonly Action<string> _onPenumbraModAdded;
    private readonly Action<string> _onPenumbraModDeleted;
    private readonly Action<Penumbra.Api.Enums.ModSettingChange, Guid, string, bool> _onPenumbraModSettingChanged;
    private readonly Action<bool> _onPenumbraEnabledChanged;
    private readonly Action<string> _onExternalTexturesChanged;
    private readonly Action _onExcludedTagsUpdated;
    private readonly Action _onPlayerResourcesChanged;
    private bool _reinstallInProgress = false;
    private string _reinstallMod = string.Empty;
    // Track last external conversion/restore notification to surface UI indicator
    private DateTime _lastExternalChangeAt = DateTime.MinValue;
    private string _lastExternalChangeReason = string.Empty;

public ConversionUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, TextureBackupService backupService, Action? openSettings = null, ModStateService? modStateService = null, ConversionCacheService? cacheService = null)
        : base("ShrinkU###ShrinkUConversionUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _backupService = backupService;
        _openSettings = openSettings;
        _modStateService = modStateService ?? new ModStateService(_logger, _configService);
        _cacheService = cacheService ?? new ConversionCacheService(_logger, _configService, _backupService, _modStateService);

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(820, 400),
            MaximumSize = new Vector2(1920, 1080),
        };

        _onConversionProgress = e => { _currentTexture = e.Item1; _convertedCount = e.Item2; };
        _conversionService.OnConversionProgress += _onConversionProgress;

        _onBackupProgress = e => 
        { 
            _currentTexture = e.Item1; 
            _backupIndex = e.Item2; 
            _backupTotal = e.Item3; 
            try
            {
                var path = e.Item1;
                if (!string.IsNullOrWhiteSpace(path))
                {
                    var ext = Path.GetExtension(path);
                    if (string.Equals(ext, ".pmp", StringComparison.OrdinalIgnoreCase))
                    {
                        var modDir = Path.GetDirectoryName(path);
                        var mod = !string.IsNullOrWhiteSpace(modDir) ? Path.GetFileName(modDir) : string.Empty;
                        if (!string.IsNullOrWhiteSpace(mod))
                        {
                            _modsWithBackupCache[mod] = true;
                            _modsWithPmpCache[mod] = true;
                            RefreshModState(mod, "backup-progress-pmp");
                            TriggerMetricsRefresh();
                            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                            _perModSavingsTask.ContinueWith(ps =>
                            {
                                if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                                {
                                    _cachedPerModSavings = ps.Result;
                                    _needsUIRefresh = true;
                                }
                            }, TaskScheduler.Default);
                            // Warm original sizes cache for this mod from PMP so tooltips reflect immediately
                            _perModOriginalSizesTasks[mod] = _backupService.GetLatestOriginalSizesForModAsync(mod);
                            _perModOriginalSizesTasks[mod].ContinueWith(t =>
                            {
                                if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                                {
                                    _cachedPerModOriginalSizes[mod] = t.Result;
                                    _needsUIRefresh = true;
                                }
                                _perModOriginalSizesTasks.Remove(mod);
                            }, TaskScheduler.Default);
                        }
                    }
                }
            }
            catch { }
        };
        _conversionService.OnBackupProgress += _onBackupProgress;

        _onConversionCompleted = () =>
        {
            // If user cancelled conversion, immediately restore the current mod with validation
            if (_restoreAfterCancel && !string.IsNullOrEmpty(_cancelTargetMod))
            {
                var target = _cancelTargetMod;
                _logger.LogDebug("Cancel detected; attempting restore of current mod: {mod}", target);
                // Gate restore in automatic mode if the target mod contains currently used textures
                var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                var targetHasUsed = _scannedByMod.TryGetValue(target, out var targetFiles) && targetFiles.Any(f => _penumbraUsedFiles.Contains(f));
                if (automaticMode && targetHasUsed)
                {
                    _logger.LogDebug("Automatic mode active; skipping restore-after-cancel for currently used mod: {mod}", target);
                    _running = false;
                    return;
                }
                var progress = new Progress<(string, int, int)>(e =>
                {
                    _currentTexture = e.Item1;
                    _backupIndex = e.Item2;
                    _backupTotal = e.Item3;
                    _currentRestoreModIndex = e.Item2;
                    _currentRestoreModTotal = e.Item3;
                });
                try { _restoreCancellationTokenSource?.Dispose(); } catch { }
                _restoreCancellationTokenSource = new CancellationTokenSource();
                var restoreToken = _restoreCancellationTokenSource.Token;

                _running = true;
                // Clear stale conversion/restore progress before starting a restore
                ResetConversionProgress();
                ResetRestoreProgress();
                _currentRestoreMod = target;
                _currentRestoreModIndex = 0;
                _currentRestoreModTotal = 0;

                _ = _backupService.RestoreLatestForModAsync(target, progress, restoreToken)
                    .ContinueWith(t =>
                    {
                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                        _logger.LogDebug(success
                            ? "Restore after cancel succeeded for {mod}"
                            : "Restore after cancel failed for {mod}", target);
                        if (success)
                        {
                            try { _backupService.RedrawPlayer(); } catch { }
                            try
                            {
                                if (_configService.Current.ExternalConvertedMods.Remove(target))
                                    _configService.Save();
                            }
                            catch { }
                        }
                        RefreshScanResults(true, success ? "restore-after-cancel-success" : "restore-after-cancel-fail");
                        TriggerMetricsRefresh();
                        // Recompute per-mod savings after restore to refresh Uncompressed/Compressed sizes
                        _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                        _perModSavingsTask.ContinueWith(ps =>
                        {
                            if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                            {
                                _cachedPerModSavings = ps.Result;
                                _needsUIRefresh = true;
                            }
                        }, TaskScheduler.Default);
                        _ = _backupService.HasBackupForModAsync(target).ContinueWith(bt =>
                        {
                            if (bt.Status == TaskStatus.RanToCompletion)
                            {
                                bool any = bt.Result;
                                try { any = any || _backupService.HasPmpBackupForModAsync(target).GetAwaiter().GetResult(); } catch { }
                                _cacheService.SetModHasBackup(target, any);
                            }
                        });
                        _running = false;
                        try { _restoreCancellationTokenSource?.Dispose(); } catch { }
                        _restoreCancellationTokenSource = null;
                        _currentRestoreMod = string.Empty;
                        _currentRestoreModIndex = 0;
                        _currentRestoreModTotal = 0;
                        _cancelTargetMod = string.Empty;
                        _restoreAfterCancel = false;
                    }, TaskScheduler.Default);
                return;
            }

            // Normal completion path: recompute savings and refresh UI
            _cacheService.ClearAll();
            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
            _perModSavingsTask.ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                {
                    _cachedPerModSavings = t.Result;
                    _needsUIRefresh = true;
                }
            }, TaskScheduler.Default);
            _logger.LogDebug("Heavy scan triggered: conversion completed");
            RefreshScanResults(true, "conversion-completed");
            _running = false;
            // Clear progress so UI returns to Waiting...
            ResetConversionProgress();
            ResetRestoreProgress();
        };
        _conversionService.OnConversionCompleted += _onConversionCompleted;

        _onModProgress = e => { _currentModName = e.modName; _currentModIndex = e.current; _totalMods = e.total; _currentModTotalFiles = e.fileTotal; RefreshModState(e.modName, "conversion-progress"); };
        _conversionService.OnModProgress += _onModProgress;

        // Generic ModsChanged only marks UI for refresh; heavy scan is driven by add/delete.
        _onPenumbraModsChanged = () =>
        {
            _modsWithBackupCache.Clear();
            _modsPmpMetaCache.Clear();
            _modsZipMetaCache.Clear();
            _modsWithPmpCache.Clear();
            _modsPmpCheckInFlight.Clear();
            _logger.LogDebug("Penumbra mods changed (DIAG-v3); refreshing UI state");
            _needsUIRefresh = true;
            if (!_orphanScanInFlight)
            {
                _orphanScanInFlight = true;
                _ = Task.Run(() =>
                {
                    try
                    {
                        var orphans = _backupService.FindOrphanedBackupsAsync().GetAwaiter().GetResult();
                        _uiThreadActions.Enqueue(() =>
                        {
                            _orphaned = orphans ?? new List<TextureBackupService.OrphanBackupInfo>();
                            _lastOrphanScanUtc = DateTime.UtcNow;
                            _needsUIRefresh = true;
                        });
                    }
                    finally
                    {
                        _orphanScanInFlight = false;
                    }
                });
            }
            // Refresh hierarchical mod paths so folder moves/creations reflect in the UI
            if (!_loadingModPaths)
            {
                _loadingModPaths = true;
                _ = _conversionService.GetModPathsAsync().ContinueWith(t =>
                {
                    if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                    {
                        _modPaths = t.Result;
                        try
                        {
                            var sb = new System.Text.StringBuilder();
                            foreach (var kv in _modPaths.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                                sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
                            _modPathsSig = sb.ToString();
                        }
                        catch { _modPathsSig = _modPaths.Count.ToString(); }
                    }
                    _loadingModPaths = false;
                    _needsUIRefresh = true;
                });
            }

            // If Used-Only filter is active, reload the currently used textures
            if (_filterPenumbraUsedOnly && !_loadingPenumbraUsed)
            {
                _loadingPenumbraUsed = true;
                _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                {
                    try
                    {
                        if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                        {
                            _penumbraUsedFiles = t.Result;
                            _logger.LogDebug("Penumbra used set reloaded after ModsChanged: {count} files", _penumbraUsedFiles.Count);
                            _uiThreadActions.Enqueue(() =>
                            {
                                try { RefreshScanResults(false, "penumbra-used-mods-changed"); } catch { }
                                _needsUIRefresh = true;
                            });
                        }
                    }
                    finally
                    {
                        _loadingPenumbraUsed = false;
                    }
                }, TaskScheduler.Default);
            }

            lock (_modsChangedLock)
            {
                try { _modsChangedDebounceCts?.Cancel(); } catch { }
                try { _modsChangedDebounceCts?.Dispose(); } catch { }
                _modsChangedDebounceCts = new CancellationTokenSource();
                var token = _modsChangedDebounceCts.Token;
                _modsChangedDebounceSeq++;
                var mySeq = _modsChangedDebounceSeq;
                Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(1200, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            return;
                        lock (_modsChangedLock)
                        {
                            if (mySeq != _modsChangedDebounceSeq)
                                return;
                        }
                        var currentFolders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);
                        bool modsChanged;
                        lock (_modsChangedLock)
                        {
                            var snapshot = _knownModFolders;
                            modsChanged = currentFolders.Count != snapshot.Count
                                || currentFolders.Any(f => !snapshot.Contains(f));
                        }
                        if (!modsChanged)
                        {
                            _uiThreadActions.Enqueue(() => { _needsUIRefresh = true; });
                            return;
                        }
                        if (_scanInProgress)
                            return;
                        _scanInProgress = true;
                        await Task.Delay(300, token).ConfigureAwait(false);
                        RefreshScanResults(true, "penumbra-mods-changed");
                    }
                    catch (TaskCanceledException) { }
                    catch { }
                });
            }
        };
        _conversionService.OnPenumbraModsChanged += _onPenumbraModsChanged;

        // React to external texture changes (e.g., conversions/restores initiated by Sphene)
        _onExternalTexturesChanged = reason =>
        {
            _logger.LogDebug("External texture change: {reason}; refreshing UI and scan state", reason);
            try { _lastExternalChangeReason = reason ?? string.Empty; _lastExternalChangeAt = DateTime.UtcNow; } catch { }
            // Ensure backup states and metrics reflect latest changes from Sphene
            try { _modsWithBackupCache.Clear(); } catch { }
            try { _modsWithTexBackupCache.Clear(); } catch { }
            try { _modsPmpMetaCache.Clear(); } catch { }
            try { _modsZipMetaCache.Clear(); } catch { }
            try { _modsBackupCheckInFlight.Clear(); } catch { }
            try { _modsWithPmpCache.Clear(); } catch { }
            try { _modsPmpCheckInFlight.Clear(); } catch { }
            try { _cachedPerModSavings.Clear(); } catch { }
            try { _cachedPerModOriginalSizes.Clear(); } catch { }
            try { TriggerMetricsRefresh(); } catch { }

            // Persist per-mod external-change markers so indicators survive restarts
            try
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var candidates = await _conversionService.GetAutomaticCandidateTexturesAsync().ConfigureAwait(false);
                        var now = DateTime.UtcNow;
                        if (candidates != null && candidates.Count > 0)
                        {
                            foreach (var mod in candidates.Keys)
                            {
                                try
                                {
                                    _configService.Current.ExternalConvertedMods[mod] = new ShrinkU.Configuration.ExternalChangeMarker
                                    {
                                        Reason = string.IsNullOrWhiteSpace(reason) ? "external" : reason,
                                        AtUtc = now,
                                    };
                                }
                                catch { }
                            }
                            try { _configService.Save(); } catch { }
                        }
                    }
                    catch { }
                });
            }
            catch { }

            // If Penumbra Used Only is enabled, reload the used textures set so filter applies immediately
            if (_filterPenumbraUsedOnly)
            {
                try { _penumbraUsedFiles.Clear(); } catch { }
                if (!_loadingPenumbraUsed)
                {
                    _loadingPenumbraUsed = true;
                    _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                    {
                        try
                        {
                            if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                            {
                                _penumbraUsedFiles = t.Result;
                                _logger.LogDebug("Reloaded {count} used textures from Penumbra after external change", _penumbraUsedFiles.Count);
                                _uiThreadActions.Enqueue(() =>
                                {
                                    try { RefreshScanResults(false, $"external-{reason}-penumbra-used-reloaded"); } catch { }
                                    _needsUIRefresh = true;
                                });
                            }
                        }
                        finally
                        {
                            _loadingPenumbraUsed = false;
                        }
                    }, TaskScheduler.Default);
                }
            }

            if (_scanInProgress)
            {
                _needsUIRefresh = true;
                return;
            }
            _scanInProgress = true;
            RefreshScanResults(true, $"external-{reason}");
            _needsUIRefresh = true;
        };
        _conversionService.OnExternalTexturesChanged += _onExternalTexturesChanged;

        // React to player redraws/resources changes to keep Used-Only filter in sync
        _onPlayerResourcesChanged = () =>
        {
            try
            {
                if (!_filterPenumbraUsedOnly)
                {
                    _needsUIRefresh = true;
                    return;
                }
                if (_loadingPenumbraUsed)
                {
                    _needsUIRefresh = true;
                    return;
                }
                _loadingPenumbraUsed = true;
                _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                {
                    try
                    {
                        if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                        {
                            _penumbraUsedFiles = t.Result;
                            _logger.LogDebug("Reloaded {count} used textures after player redraw/resources changed", _penumbraUsedFiles.Count);
                            _uiThreadActions.Enqueue(() =>
                            {
                                try { RefreshScanResults(false, "player-resources-changed"); } catch { }
                                _needsUIRefresh = true;
                            });
                        }
                    }
                    finally
                    {
                        _loadingPenumbraUsed = false;
                    }
                }, TaskScheduler.Default);
            }
            catch { }
        };
        _conversionService.OnPlayerResourcesChanged += _onPlayerResourcesChanged;

        // Heavy refresh only on actual mod add/delete, coalesced with trailing edge
        _onPenumbraModAdded = dir =>
        {
            lock (_modsChangedLock)
            {
                try { _modsChangedDebounceCts?.Cancel(); } catch { }
                try { _modsChangedDebounceCts?.Dispose(); } catch { }
                _modsChangedDebounceCts = new CancellationTokenSource();
                var token = _modsChangedDebounceCts.Token;
                // Increment debounce sequence to invalidate prior scheduled tasks
                _modsChangedDebounceSeq++;
                var mySeq = _modsChangedDebounceSeq;
                Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(1000, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            return;
                        // Ensure only the latest scheduled task runs (trailing edge)
                        lock (_modsChangedLock)
                        {
                            if (mySeq != _modsChangedDebounceSeq)
                                return;
                        }
                        // Check whether the actual set of mods has changed before heavy scan.
                        var currentFolders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);
                        // Compare against snapshot under lock to avoid races.
                        bool modsChanged;
                        lock (_modsChangedLock)
                        {
                            var snapshot = _knownModFolders;
                            modsChanged = currentFolders.Count != snapshot.Count
                                || currentFolders.Any(f => !snapshot.Contains(f));
                        }
                        if (!modsChanged)
                        {
                            // No actual mod set change; just mark UI refresh.
                            _uiThreadActions.Enqueue(() =>
                            {
                                _modsWithBackupCache.Clear();
                                _modsPmpMetaCache.Clear();
                                _modsZipMetaCache.Clear();
                                _modsWithPmpCache.Clear();
                                _modsPmpCheckInFlight.Clear();
                                _logger.LogDebug("Suppressed heavy scan: mod folders unchanged after ModAdded");
                                _needsUIRefresh = true;
                                RefreshModState(dir, "mod-added-targeted");
                            });
                            return;
                        }
                        if (_scanInProgress)
                            return;
                        _scanInProgress = true;
                        _logger.LogDebug("Heavy scan triggered: ModAdded detected changes");
                        await Task.Delay(500, token).ConfigureAwait(false);
                        RefreshScanResults(true, "mod-added");
                    }
                    catch (TaskCanceledException) { }
                    catch { }
                });
            }
        };
        _conversionService.OnPenumbraModAdded += _onPenumbraModAdded;

        _onPenumbraModDeleted = modDir =>
        {
            lock (_modsChangedLock)
            {
                                ClearModCaches(modDir);
                try { _modsChangedDebounceCts?.Cancel(); } catch { }
                try { _modsChangedDebounceCts?.Dispose(); } catch { }
                _modsChangedDebounceCts = new CancellationTokenSource();
                var token = _modsChangedDebounceCts.Token;
                // Increment debounce sequence to invalidate prior scheduled tasks
                _modsChangedDebounceSeq++;
                var mySeq = _modsChangedDebounceSeq;
                Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(1200, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            return;
                        // Ensure only the latest scheduled task runs (trailing edge)
                        lock (_modsChangedLock)
                        {
                            if (mySeq != _modsChangedDebounceSeq)
                                return;
                        }
                        // Check whether the actual set of mods has changed before heavy scan.
                        var currentFolders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);
                        bool modsChanged;
                        lock (_modsChangedLock)
                        {
                            var snapshot = _knownModFolders;
                            modsChanged = currentFolders.Count != snapshot.Count
                                || currentFolders.Any(f => !snapshot.Contains(f));
                        }
                        if (!modsChanged)
                        {
                            _uiThreadActions.Enqueue(() =>
                            {
                                _modsWithBackupCache.Clear();
                                _modsPmpMetaCache.Clear();
                                _modsZipMetaCache.Clear();
                                _modsWithPmpCache.Clear();
                                _modsPmpCheckInFlight.Clear();
                                _logger.LogDebug("Suppressed heavy scan: mod folders unchanged after ModDeleted");
                                _needsUIRefresh = true;
                                ClearModCaches(modDir);
                                RefreshModState(modDir, "mod-deleted-targeted");
                            });
                            return;
                        }
                        if (_scanInProgress)
                            return;
                        _scanInProgress = true;
                        _logger.LogDebug("Heavy scan triggered: ModDeleted detected changes");
                        await Task.Delay(500, token).ConfigureAwait(false);
                        RefreshScanResults(true, "mod-deleted");
                    }
                    catch (TaskCanceledException) { }
                    catch { }
                });
            }
        };
        _conversionService.OnPenumbraModDeleted += _onPenumbraModDeleted;

        // Lightweight handling for mod setting changes: only affect current collection and debounce enabled-state reloads.
        _onPenumbraModSettingChanged = (change, collectionId, modDir, inherited) =>
        {
            // Ignore changes not belonging to the currently selected collection.
            if (!_selectedCollectionId.HasValue || collectionId != _selectedCollectionId.Value)
            {
                return;
            }

            // Debounce enabled-state reloads to coalesce cascaded events during a single toggle.
            lock (_modsChangedLock)
            {
                try { _enabledStatesDebounceCts?.Cancel(); } catch { }
                try { _enabledStatesDebounceCts?.Dispose(); } catch { }
                _enabledStatesDebounceCts = new CancellationTokenSource();
                var token = _enabledStatesDebounceCts.Token;
                Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(350, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            return;
                        var states = await _conversionService.GetAllModEnabledStatesAsync(_selectedCollectionId.Value).ConfigureAwait(false);
                        _uiThreadActions.Enqueue(() =>
                        {
                            if (states != null)
                                _modEnabledStates = states;
                            _needsUIRefresh = true;
                        });
                    }
                    catch (TaskCanceledException) { }
                    catch { }
                });
            }

            // If Used-Only filter is active, reload currently used textures to reflect setting changes immediately.
            if (_filterPenumbraUsedOnly && !_loadingPenumbraUsed)
            {
                _loadingPenumbraUsed = true;
                _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                {
                    try
                    {
                        if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                        {
                            _penumbraUsedFiles = t.Result;
                            _logger.LogDebug("Penumbra used set reloaded after ModSettingChanged: {count} files", _penumbraUsedFiles.Count);
                            _uiThreadActions.Enqueue(() =>
                            {
                                try { RefreshScanResults(false, "penumbra-used-setting-changed"); } catch { }
                                _needsUIRefresh = true;
                            });
                        }
                    }
                    finally
                    {
                        _loadingPenumbraUsed = false;
                    }
                }, TaskScheduler.Default);
            }
        };
        _conversionService.OnPenumbraModSettingChanged += _onPenumbraModSettingChanged;

        // React when Penumbra is enabled/disabled to keep filter and UI consistent
        _onPenumbraEnabledChanged = enabled =>
        {
            try
            {
                if (!enabled)
                {
                    // Penumbra disabled: clear used-only set and refresh UI
                    try { _penumbraUsedFiles.Clear(); } catch { }
                    _uiThreadActions.Enqueue(() =>
                    {
                        try { RefreshScanResults(false, "penumbra-disabled"); } catch { }
                        _needsUIRefresh = true;
                    });
                    return;
                }

                // Penumbra enabled: if filter active, reload used-only set
                if (_filterPenumbraUsedOnly && !_loadingPenumbraUsed)
                {
                    _loadingPenumbraUsed = true;
                    _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                    {
                        try
                        {
                            if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                            {
                                _penumbraUsedFiles = t.Result;
                                _logger.LogDebug("Reloaded {count} used textures after Penumbra enabled", _penumbraUsedFiles.Count);
                                _uiThreadActions.Enqueue(() =>
                                {
                                    try { RefreshScanResults(false, "penumbra-enabled-used-reloaded"); } catch { }
                                    _needsUIRefresh = true;
                                });
                            }
                        }
                        finally
                        {
                            _loadingPenumbraUsed = false;
                        }
                    }, TaskScheduler.Default);
                }
                else
                {
                    _uiThreadActions.Enqueue(() => { _needsUIRefresh = true; });
                }
            }
            catch { }
        };
        _conversionService.OnPenumbraEnabledChanged += _onPenumbraEnabledChanged;

        // Initialize left panel width from config if present
        if (_configService.Current.LeftPanelWidthPx > 0f)
        {
            _leftPanelWidthPx = _configService.Current.LeftPanelWidthPx;
            _leftWidthInitialized = true;
        }

        // Initialize excluded tags state from config
        var savedTags = _configService.Current.ExcludedModTags ?? new List<string>();
        _excludedTagsInput = string.Join(", ", savedTags);
        _excludedTagsNormalized = new HashSet<string>(savedTags.Select(NormalizeTag).Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);

        // Subscribe to excluded tags updates to refresh UI immediately
        _onExcludedTagsUpdated = () =>
        {
            try
            {
                var tags = _configService.Current.ExcludedModTags ?? new List<string>();
                _excludedTagsInput = string.Join(", ", tags);
                _excludedTagsNormalized = new HashSet<string>(tags.Select(NormalizeTag).Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);
                _needsUIRefresh = true;
                RefreshScanResults(false, "excluded-tags-updated");
            }
            catch { }
        };
        _configService.OnExcludedTagsUpdated += _onExcludedTagsUpdated;

        // Initialize persisted UI settings from config
        _filterPenumbraUsedOnly = _configService.Current.FilterPenumbraUsedOnly;
        _filterNonConvertibleMods = _configService.Current.FilterNonConvertibleMods;
        _filterInefficientMods = _configService.Current.HideInefficientMods;
        _scanSortAsc = true;
        _scanSortKind = ScanSortKind.ModName;

        // Initialize first column width from config (default 30px on first open)
        _scannedFirstColWidth = _configService.Current.ScannedFilesFirstColWidth > 0f
            ? _configService.Current.ScannedFilesFirstColWidth
            : 28f;
        _scannedSizeColWidth = _configService.Current.ScannedFilesSizeColWidth > 0f
            ? _configService.Current.ScannedFilesSizeColWidth
            : 85f;
        _scannedActionColWidth = _configService.Current.ScannedFilesActionColWidth > 0f
            ? _configService.Current.ScannedFilesActionColWidth
            : 120f;

        // If Penumbra Used Only is enabled from config, preload used files on first open
        if (_filterPenumbraUsedOnly && _penumbraUsedFiles.Count == 0 && !_loadingPenumbraUsed)
        {
            _logger.LogDebug("Preloading Penumbra used textures set on initial open");
            _loadingPenumbraUsed = true;
            _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                {
                    _penumbraUsedFiles = t.Result;
                    _logger.LogDebug("Loaded {count} currently used textures from Penumbra (initial)", _penumbraUsedFiles.Count);
                    // Trigger a scan refresh to apply filter immediately
                    try { RefreshScanResults(false, "penumbra-used-prefetch"); } catch { }
                }
                _loadingPenumbraUsed = false;
            });
        }

        // Defer initial scan until first draw to avoid UI hitch on window open
    }

    // Helper to show a tooltip when the last item is hovered (also when disabled)
    private static void ShowTooltip(string text)
    {
        if (!ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            return;
        ImGui.BeginTooltip();
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.TooltipText);
        ImGui.TextUnformatted(text);
        ImGui.PopStyleColor();
        ImGui.EndTooltip();
    }

    public override void Draw()
    {
        // Gate usage until first-run setup is completed
        if (!_configService.Current.FirstRunCompleted)
        {
            ImGui.TextWrapped("Please complete the First Start Setup to use ShrinkU.");
            ImGui.Spacing();
            ImGui.TextWrapped("Open the setup guide and select a backup folder.");
            return;
        }
        // Apply any background-computed UI state updates on the main thread
        while (_uiThreadActions.TryDequeue(out var action))
        {
            try { action(); } catch { }
        }
        // Status message will be rendered in the progress area instead of here

        // Check if UI needs refresh due to async data updates
        if (_needsUIRefresh)
        {
            _needsUIRefresh = false;
            // Avoid forcing focus to prevent hitch; normal redraw will occur
        }
        // Trigger initial scan lazily on first draw
        QueueInitialScan();

        // Ensure Used-Only watcher is running/stopped according to current filter state
        if (_filterPenumbraUsedOnly && !_usedWatcherActive)
        {
            StartUsedResourcesWatcher();
        }
        else if (!_filterPenumbraUsedOnly && _usedWatcherActive)
        {
            StopUsedResourcesWatcher();
        }

        // Preload mod paths lazily
        if (!_loadingModPaths && _modPaths.Count == 0)
        {
            _loadingModPaths = true;
            _ = _conversionService.GetModPathsAsync().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                {
                    _modPaths = t.Result;
                    try
                    {
                        var sb = new System.Text.StringBuilder();
                        foreach (var kv in _modPaths.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                            sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
                        _modPathsSig = sb.ToString();
                    }
                    catch { _modPathsSig = _modPaths.Count.ToString(); }
                }
                _loadingModPaths = false;
            });
        }
        // Subtle accent header bar for branding
        var headerStart = ImGui.GetCursorScreenPos();
        var headerWidth = Math.Max(1f, ImGui.GetContentRegionAvail().X);
        var headerEnd = new Vector2(headerStart.X + headerWidth, headerStart.Y + 2f);
        ImGui.GetWindowDrawList().AddRectFilled(headerStart, headerEnd, ShrinkUColors.ToImGuiColor(ShrinkUColors.Accent));
        ImGui.Dummy(new Vector2(0, 6f));

        var avail = ImGui.GetContentRegionAvail();
        var totalWidth = Math.Max(520f, avail.X);
        if (!_leftWidthInitialized)
        {
            _leftPanelWidthPx = Math.Max(360f, totalWidth * _leftPanelWidthRatio);
            _leftWidthInitialized = true;
        }
        var leftWidth = Math.Clamp(_leftPanelWidthPx, 360f, Math.Max(360f, totalWidth - 360f));

        ImGui.BeginChild("LeftPanel", new Vector2(leftWidth, 0), true);
        DrawSettings();
        ImGui.Separator();
        DrawActions();
        ImGui.Separator();
        DrawProgress();
        ImGui.EndChild();

        ImGui.SameLine();
        DrawSplitter(totalWidth, ref leftWidth);
        ImGui.BeginChild("RightPanel", new Vector2(0, 0), true);
        DrawOverview();
        ImGui.EndChild();
    }

    public void Dispose()
    {
        // Unsubscribe from all service events
        try { _conversionService.OnConversionProgress -= _onConversionProgress; } catch { }
        try { _conversionService.OnBackupProgress -= _onBackupProgress; } catch { }
        try { _conversionService.OnConversionCompleted -= _onConversionCompleted; } catch { }
        try { _conversionService.OnModProgress -= _onModProgress; } catch { }
        try { _conversionService.OnPenumbraModsChanged -= _onPenumbraModsChanged; } catch { }
        try { _conversionService.OnPenumbraModAdded -= _onPenumbraModAdded; } catch { }
        try { _conversionService.OnPenumbraModDeleted -= _onPenumbraModDeleted; } catch { }
        try { _conversionService.OnExternalTexturesChanged -= _onExternalTexturesChanged; } catch { }
        try { _conversionService.OnPenumbraModSettingChanged -= _onPenumbraModSettingChanged; } catch { }
        try { _conversionService.OnPenumbraEnabledChanged -= _onPenumbraEnabledChanged; } catch { }
        try { _conversionService.OnPlayerResourcesChanged -= _onPlayerResourcesChanged; } catch { }
        try { _configService.OnExcludedTagsUpdated -= _onExcludedTagsUpdated; } catch { }

        // Cancel and dispose any outstanding debounce or restore operations
        try { _modsChangedDebounceCts?.Cancel(); } catch { }
        try { _modsChangedDebounceCts?.Dispose(); } catch { }
        _modsChangedDebounceCts = null;
        try { _enabledStatesDebounceCts?.Cancel(); } catch { }
        try { _enabledStatesDebounceCts?.Dispose(); } catch { }
        _enabledStatesDebounceCts = null;
        try { _restoreCancellationTokenSource?.Cancel(); } catch { }
        try { _restoreCancellationTokenSource?.Dispose(); } catch { }
        _restoreCancellationTokenSource = null;

        // Stop Used-Only watcher
        StopUsedResourcesWatcher();

        // Drain any queued UI actions
        try { while (_uiThreadActions.TryDequeue(out _)) { } } catch { }

        try { _logger.LogDebug("ConversionUI disposed: DIAG-v3"); } catch { }
    }

    // Start background watcher to refresh used-only set even without redraw events
    private void StartUsedResourcesWatcher()
    {
        try
        {
            StopUsedResourcesWatcher();
            _usedResourcesPollCts = new CancellationTokenSource();
            var token = _usedResourcesPollCts.Token;
            _usedWatcherActive = true;
            _usedResourcesPollTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(1500, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            break;

                        if (!_filterPenumbraUsedOnly || _loadingPenumbraUsed)
                            continue;

                        _loadingPenumbraUsed = true;
                        var result = await _conversionService.GetUsedModTexturePathsAsync().ConfigureAwait(false);
                        try
                        {
                            // Compare current vs new set; if changed, update and refresh UI
                            bool changed = result.Count != _penumbraUsedFiles.Count
                                || result.Any(p => !_penumbraUsedFiles.Contains(p))
                                || _penumbraUsedFiles.Any(p => !result.Contains(p));
                            if (changed)
                            {
                                _penumbraUsedFiles = result;
                                _logger.LogDebug("Used-Only watcher updated: {count} files", _penumbraUsedFiles.Count);
                                _uiThreadActions.Enqueue(() =>
                                {
                                    try { RefreshScanResults(false, "used-only-watcher-update"); } catch { }
                                    _needsUIRefresh = true;
                                });
                            }
                        }
                        finally
                        {
                            _loadingPenumbraUsed = false;
                        }
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex)
                    {
                        try { _logger.LogDebug(ex, "Used-Only watcher iteration failed"); } catch { }
                    }
                }
            }, token);
            try { _logger.LogDebug("Used-Only watcher started"); } catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogDebug(ex, "Failed to start Used-Only watcher"); } catch { }
        }
    }

    private void StopUsedResourcesWatcher()
    {
        try { _usedResourcesPollCts?.Cancel(); } catch { }
        try { _usedResourcesPollCts?.Dispose(); } catch { }
        _usedResourcesPollCts = null;
        _usedResourcesPollTask = null;
        _usedWatcherActive = false;
        try { _logger.LogDebug("Used-Only watcher stopped"); } catch { }
    }

    private void DrawSettings()
    {
        DrawSettings_ViewImpl();
    }

    

    private int _zebraRowIndex = 0;
    private readonly Vector4 _zebraEvenColor = new(0.16f, 0.16f, 0.16f, 0.10f);
    private readonly Vector4 _zebraOddColor  = new(0.16f, 0.16f, 0.16f, 0.30f);

private void DrawCategoryTableNode(TableCatNode node, Dictionary<string, List<string>> visibleByMod, ref int idx, string pathPrefix, int depth = 0, int clipStart = 0, int clipEnd = int.MaxValue)
    {
        DrawCategoryTableNode_ViewImpl(node, visibleByMod, ref idx, pathPrefix, depth, clipStart, clipEnd);
        return;
    }

    private void DrawActions()
    {
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Actions");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);
        ImGui.BeginDisabled(ActionsDisabled());
        if (ImGui.Button("Re-Scan All Mod Textures"))
        {
            RefreshScanResults(true, "manual-rescan");
            _needsUIRefresh = true;
        }
        ShowTooltip("Scan all mods for candidate textures and refresh the table.");
        ImGui.EndDisabled();

        if (!(_running || _conversionService.IsConverting))
        {
            // Aggregate mod statistics
            int totalMods = _scannedByMod.Count;
            int restorableModsCount = 0;
            int convertedModsCount = 0;
            int convertibleModsCount = 0;
            foreach (var mod in _scannedByMod.Keys)
            {
                var hasBackup = GetOrQueryModBackup(mod);
                if (hasBackup)
                    restorableModsCount++;

                var remaining = _scannedByMod[mod].Count;
                if (remaining == 0)
                    convertedModsCount++;
                else if (remaining > 0)
                {
                    // Exclude mods marked by tags from convertible count
                    if (!IsModExcludedByTags(mod))
                        convertibleModsCount++;
                }
            }

            // Event-driven metrics update: only when focused or idle
            var shouldUpdateMetrics = ImGui.IsWindowFocused(ImGuiFocusedFlags.RootAndChildWindows) || !_running;
            if (shouldUpdateMetrics)
            {
                if (_backupStorageInfoTask == null)
                {
                    _backupStorageInfoTask = ComputeBackupStorageInfoAsync();
                }
                if (_backupStorageInfoTask != null && _backupStorageInfoTask.IsCompleted)
                {
                    _cachedBackupStorageInfo = _backupStorageInfoTask.Result;
                }

                // Update per-mod savings asynchronously
                if (_perModSavingsTask == null)
                {
                    _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                }
                if (_perModSavingsTask != null && _perModSavingsTask.IsCompleted)
                {
                    _cachedPerModSavings = _perModSavingsTask.Result ?? new Dictionary<string, TextureBackupService.ModSavingsStats>(StringComparer.OrdinalIgnoreCase);
                    _perModSavingsRevision++;
                    _footerTotalsDirty = true;
                }
            }

            // Stats removed from action area; table now contains key information

        }
        else
        {
            // Show detailed per-mod restore progress while running
            var displayMod = _currentRestoreMod;
            if (!string.IsNullOrEmpty(displayMod) && _modDisplayNames.TryGetValue(displayMod, out var dn))
                displayMod = dn;

            if (!string.IsNullOrEmpty(displayMod))
            {
                ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Restoring");
                ImGui.Text($"Mod: {displayMod} ({_currentRestoreModIndex}/{_currentRestoreModTotal})");
            }
            if (!string.IsNullOrEmpty(_currentTexture))
            {
                ImGui.Text($"File: {_currentTexture}");
            }

            // Disable cancel when conversion was initiated externally (Sphene automatic)
            ImGui.BeginDisabled(!_running);
            if (ImGui.Button("Cancel"))
            {
                // Mark that we want to restore the currently converting mod after cancellation completes
                _restoreAfterCancel = true;
                _cancelTargetMod = _currentModName;
                _conversionService.Cancel();
                // If a restore is already in flight, cancel it too
                _restoreCancellationTokenSource?.Cancel();
                _logger.LogDebug("Cancel pressed; will restore current mod {mod} after conversion stops", _cancelTargetMod);
            }
            ImGui.EndDisabled();
            ShowTooltip("Cancel the current conversion or restore operation.");
        }
    }

    private void QueueInitialScan()
    {
        if (_initialScanQueued)
            return;
        _initialScanQueued = true;
        Task.Run(async () =>
        {
            try
            {
                await Task.Delay(500).ConfigureAwait(false);
                var grouped = await _conversionService.GetGroupedCandidateTexturesAsync().ConfigureAwait(false);
                var names = await _conversionService.GetModDisplayNamesAsync().ConfigureAwait(false);
                var tags = await _conversionService.GetModTagsAsync().ConfigureAwait(false);
                var folders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);

                _uiThreadActions.Enqueue(() =>
                {
                    _scannedByMod.Clear();
                    _selectedTextures.Clear();
                    _texturesToConvert.Clear();
                    _fileOwnerMod.Clear();
                    if (grouped != null)
                    {
                        foreach (var mod in grouped)
                        {
                            _scannedByMod[mod.Key] = mod.Value;
                            foreach (var file in mod.Value)
                            {
                                _texturesToConvert[file] = Array.Empty<string>();
                                _fileOwnerMod[file] = mod.Key;
                            }
                        }
                    }
                    if (names != null)
                        _modDisplayNames = names;
                    if (tags != null)
                    {
                        _modTags = tags.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            // Persistently expand known tags list with newly discovered tags
                            var existingKnown = _configService.Current.KnownModTags ?? new List<string>();
                            var normalizedExisting = new HashSet<string>(existingKnown.Select(NormalizeTag).Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);
                            foreach (var kv in _modTags)
                            {
                                var tagList = kv.Value;
                                if (tagList == null) continue;
                                foreach (var t in tagList)
                                {
                                    var nt = NormalizeTag(t);
                                    if (nt.Length == 0) continue;
                                    if (!normalizedExisting.Contains(nt))
                                    {
                                        existingKnown.Add(nt);
                                        normalizedExisting.Add(nt);
                                    }
                                }
                            }
                            _configService.Current.KnownModTags = existingKnown;
                            _configService.Save();
                        }
                        catch { }
                    }
                    if (folders != null)
                    {
                        foreach (var folder in folders)
                        {
                            if (!_scannedByMod.ContainsKey(folder))
                                _scannedByMod[folder] = new List<string>();
                        }
                        // Update known mod folders snapshot
                        _knownModFolders = new HashSet<string>(folders, StringComparer.OrdinalIgnoreCase);
                    }
                    _logger.LogDebug("Initial scan loaded {mods} mods and {files} textures", _scannedByMod.Count, _texturesToConvert.Count);
                    _needsUIRefresh = true;
                    // Warm up file size cache on background thread after UI state is applied
                    Task.Run(() => { try { WarmupFileSizeCache(); } catch { } });
                });
            }
            catch { }
        });
    }

    // Refresh scan results to reflect latest restore/conversion state
    private void RefreshScanResults(bool force = false, string origin = "unknown")
    {
        Task.Run(async () =>
        {
            try
            {
                _logger.LogDebug("Scan requested: origin={origin} force={force}", origin, force);
                var names = await _conversionService.GetModDisplayNamesAsync().ConfigureAwait(false);
                var tags = await _conversionService.GetModTagsAsync().ConfigureAwait(false);
                var folders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);
                Dictionary<string, string>? paths = null;
                paths = await _conversionService.GetModPathsAsync().ConfigureAwait(false);

                // Determine if heavy grouped scan can be skipped based on folder snapshot.
                bool skipHeavy;
                lock (_modsChangedLock)
                {
                    var snapshot = _knownModFolders;
                    skipHeavy = !force && folders != null && snapshot.Count == folders.Count && !folders.Any(f => !snapshot.Contains(f));
                    _logger.LogDebug("Folder snapshot: prev={prev} curr={curr}", snapshot.Count, folders?.Count ?? 0);
                }

                // Global heavy-scan rate limiter: suppress repeated heavy scans within 2 seconds
                var now = DateTime.UtcNow;
                var since = now - _lastHeavyScanAt;
                var allowHeavy = !skipHeavy || force;
                _logger.LogDebug("Scan decision: origin={origin} skipHeavy={skip} sinceMs={ms} allowHeavyPreLimit={allow}", origin, skipHeavy, (int)since.TotalMilliseconds, allowHeavy);
                if (allowHeavy && since < TimeSpan.FromSeconds(2))
                {
                    _logger.LogDebug("Suppressed heavy scan: rate limit active ({ms} ms since last heavy)", (int)since.TotalMilliseconds);
                    allowHeavy = false;
                }

                Dictionary<string, List<string>>? grouped = null;
                if (allowHeavy)
                {
                    grouped = await _conversionService.GetGroupedCandidateTexturesAsync().ConfigureAwait(false);
                    _lastHeavyScanAt = DateTime.UtcNow;
                    _logger.LogDebug("Heavy scan executed: origin={origin} groupedMods={mods}", origin, grouped?.Count ?? 0);
                }

                _uiThreadActions.Enqueue(() =>
                {
                    if (grouped != null)
                    {
                        _scannedByMod.Clear();
                        _selectedTextures.Clear();
                        _texturesToConvert.Clear();
                        _fileSizeCache.Clear();
                        _fileOwnerMod.Clear();
                        foreach (var mod in grouped)
                        {
                            _scannedByMod[mod.Key] = mod.Value;
                            foreach (var file in mod.Value)
                            {
                                _texturesToConvert[file] = Array.Empty<string>();
                                _fileOwnerMod[file] = mod.Key;
                            }
                        }
                    }
                    else
                    {
                        // Preserve existing scan results when skipping heavy scan; only ensure folder keys exist
                        if (folders != null)
                        {
                            foreach (var folder in folders)
                            {
                                if (!_scannedByMod.ContainsKey(folder))
                                    _scannedByMod[folder] = new List<string>();
                            }
                        }
                        _logger.LogDebug("UI state updated (DIAG-v3): origin={origin} heavy scan skipped; preserving previous results", origin);
                    }

                    if (grouped != null)
                        _logger.LogDebug("Refreshed scan (DIAG-v3): origin={origin} mods={mods} textures={files}", origin, _scannedByMod.Count, _texturesToConvert.Count);

                    if (names != null)
                        _modDisplayNames = names;
                    if (tags != null)
                    {
                        _modTags = tags.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.OrdinalIgnoreCase);
                        try
                        {
                            // Persistently expand known tags list with newly discovered tags
                            var existingKnown = _configService.Current.KnownModTags ?? new List<string>();
                            var normalizedExisting = new HashSet<string>(existingKnown.Select(NormalizeTag).Where(s => s.Length > 0), StringComparer.OrdinalIgnoreCase);
                            foreach (var kv in _modTags)
                            {
                                var tagList = kv.Value;
                                if (tagList == null) continue;
                                foreach (var t in tagList)
                                {
                                    var nt = NormalizeTag(t);
                                    if (nt.Length == 0) continue;
                                    if (!normalizedExisting.Contains(nt))
                                    {
                                        existingKnown.Add(nt);
                                        normalizedExisting.Add(nt);
                                    }
                                }
                            }
                            _configService.Current.KnownModTags = existingKnown;
                            _configService.Save();
                        }
                        catch { }
                    }
                    if (paths != null)
                        _modPaths = paths;
                    if (folders != null)
                    {
                        foreach (var folder in folders)
                        {
                            if (!_scannedByMod.ContainsKey(folder))
                                _scannedByMod[folder] = new List<string>();
                        }
                        // Update known mod folders snapshot
                        _knownModFolders = new HashSet<string>(folders, StringComparer.OrdinalIgnoreCase);
                    }
                    _needsUIRefresh = true;
                    _scanInProgress = false;
                    _lastScanAt = DateTime.UtcNow;
                    // Warm up file size cache on background thread after UI state is applied
                    Task.Run(() => { try { WarmupFileSizeCache(); } catch { } });
                });
            }
            catch
            {
                _uiThreadActions.Enqueue(() => { _scanInProgress = false; _lastScanAt = DateTime.UtcNow; });
            }
        });
    }

    private void DrawProgress()
    {
        DrawProgress_ViewImpl();
    }

    // Reset all conversion-related progress state to avoid stale UI between operations
    private void ResetConversionProgress()
    {
        _currentModName = string.Empty;
        _currentModIndex = 0;
        _totalMods = 0;
        _currentModTotalFiles = 0;
        _convertedCount = 0;
        _currentTexture = string.Empty;
        _backupIndex = 0;
        _backupTotal = 0;
    }

    // Reset restore-related progress state to avoid stale UI between operations
    private void ResetRestoreProgress()
    {
        _currentRestoreMod = string.Empty;
        _currentRestoreModIndex = 0;
        _currentRestoreModTotal = 0;
        _currentTexture = string.Empty;
        _backupIndex = 0;
        _backupTotal = 0;
    }

    private void ResetBothProgress()
    {
        ResetConversionProgress();
        ResetRestoreProgress();
    }

    private void SetStatus(string msg, bool persistent = false)
    {
        _statusMessage = msg;
        _statusMessageAt = DateTime.UtcNow;
        _statusPersistent = persistent;
    }

    private void OpenFolderPicker()
    {
        try
        {
            using var dialog = new FolderBrowserDialog();
            dialog.Description = "Select backup folder for ShrinkU";
            dialog.SelectedPath = _configService.Current.BackupFolderPath;
            dialog.ShowNewFolderButton = true;

            if (dialog.ShowDialog() == DialogResult.OK && !string.IsNullOrWhiteSpace(dialog.SelectedPath))
            {
                _configService.Current.BackupFolderPath = dialog.SelectedPath;
                _configService.Save();
                _logger.LogDebug("Backup folder changed to: {path}", dialog.SelectedPath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open folder picker");
        }
    }

    private void DrawOverview()
    {
        DrawOverview_ViewImpl();
    }

    private void DrawScannedFilesTable()
    {
        DrawScannedFilesTable_ViewImpl();
    }

    // Human-readable size formatting
    private string FormatSize(long bytes)
    {
        if (bytes < 0) return "-";
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double size = bytes;
        int unit = 0;
        while (size >= 1024 && unit < units.Length - 1)
        {
            size /= 1024;
            unit++;
        }
        return unit == 0 ? $"{bytes} {units[unit]}" : $"{size:0.##} {units[unit]}";
    }

    // Safe file size lookup
    private long GetFileSizeSafe(string path)
    {
        try
        {
            var fi = new FileInfo(path);
            return fi.Exists ? fi.Length : -1;
        }
        catch { return -1; }
    }

    // Warm up cache for all known files asynchronously to prevent UI stutter
    private void WarmupFileSizeCache()
    {
        try
        {
            if (_fileSizeWarmupTask != null && !_fileSizeWarmupTask.IsCompleted)
                return;
            var allFiles = _scannedByMod.SelectMany(kv => kv.Value)
                                        .Distinct(StringComparer.OrdinalIgnoreCase)
                                        .ToList();
            _fileSizeWarmupTask = Task.Run(() => _cacheService.WarmupFileSizeCache(allFiles));
        }
        catch { }
    }

    private (string version, string author, DateTime createdUtc, string pmpFileName)? GetOrQueryModPmpMeta(string mod)
    {
        return _cacheService.GetOrQueryModPmpMeta(mod);
    }

    private (string version, string author, DateTime createdUtc, string zipFileName)? GetOrQueryModZipMeta(string mod)
    {
        return _cacheService.GetOrQueryModZipMeta(mod);
    }

    // Get size from cache, computing once if missing
    private long GetCachedOrComputeSize(string path)
    {
        return _cacheService.GetCachedOrComputeSize(path);
    }

    // Draw the given file size right-aligned within the current table column.
    private void DrawRightAlignedSize(long bytes)
    {
        var text = FormatSize(bytes);
        var textSize = ImGui.CalcTextSize(text).X;
        var avail = ImGui.GetContentRegionAvail().X;
        var targetX = ImGui.GetCursorPosX() + Math.Max(0f, avail - textSize);
        ImGui.SetCursorPosX(targetX);
        ImGui.TextUnformatted(text);
    }

    // Draw size right-aligned.
    private void DrawRightAlignedSizeColored(long bytes, Vector4 color)
    {
        var text = FormatSize(bytes);
        var textSize = ImGui.CalcTextSize(text).X;
        var avail = ImGui.GetContentRegionAvail().X;
        var targetX = ImGui.GetCursorPosX() + Math.Max(0f, avail - textSize);
        ImGui.SetCursorPosX(targetX);
        ImGui.PushStyleColor(ImGuiCol.Text, color);
        ImGui.TextUnformatted(text);
        ImGui.PopStyleColor();
    }

    // Draw arbitrary text right-aligned.
    private void DrawRightAlignedTextColored(string text, Vector4 color)
    {
        var textSize = ImGui.CalcTextSize(text).X;
        var avail = ImGui.GetContentRegionAvail().X;
        var targetX = ImGui.GetCursorPosX() + Math.Max(0f, avail - textSize);
        ImGui.SetCursorPosX(targetX);
        ImGui.PushStyleColor(ImGuiCol.Text, color);
        ImGui.TextUnformatted(text);
        ImGui.PopStyleColor();
    }

    // Draw compressed total with reduction percentage compared to uncompressed.
    private void DrawRightAlignedCompressedTotalWithPercent(long compressedBytes, long uncompressedBytes, Vector4 color)
    {
        var sizeText = FormatSize(compressedBytes);
        string text = sizeText;
        if (uncompressedBytes > 0)
        {
            var reduction = MathF.Max(0f, (float)(uncompressedBytes - compressedBytes) / uncompressedBytes * 100f);
            text = string.Concat(sizeText, " (", reduction.ToString("0.00"), "%)");
        }
        var textSize = ImGui.CalcTextSize(text).X;
        var avail = ImGui.GetContentRegionAvail().X;
        var targetX = ImGui.GetCursorPosX() + Math.Max(0f, avail - textSize);
        ImGui.SetCursorPosX(targetX);
        ImGui.PushStyleColor(ImGuiCol.Text, color);
        ImGui.TextUnformatted(text);
        ImGui.PopStyleColor();
    }

    // Get per-mod original total bytes, preferring computed savings; fallback to latest backup manifest.
    private long GetOrQueryModOriginalTotal(string mod)
    {
        try
        {
            if (_cachedPerModSavings.TryGetValue(mod, out var modStats) && modStats != null && modStats.OriginalBytes > 0)
                return modStats.OriginalBytes;

            if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null && map.Count > 0)
                return map.Values.Sum();

            // Only query backup sizes when the mod has a backup; otherwise use current file sizes
            if (GetOrQueryModBackup(mod))
            {
                var sizes = _backupService.GetLatestOriginalSizesForModAsync(mod).GetAwaiter().GetResult();
                if (sizes != null && sizes.Count > 0)
                {
                    _cachedPerModOriginalSizes[mod] = sizes;
                    return sizes.Values.Sum();
                }
            }

            // Fallback for unconverted mods: sum current file sizes
            if (_scannedByMod.TryGetValue(mod, out var files) && files != null)
            {
                long total = 0;
                foreach (var f in files)
                {
                    var sz = GetCachedOrComputeSize(f);
                    if (sz > 0) total += sz;
                }
                return total;
            }
        }
        catch { }
        return 0;
    }

    private bool GetOrQueryModBackup(string mod)
    {
        return _cacheService.GetOrQueryModBackup(mod);
    }

    private bool GetOrQueryModTextureBackup(string mod)
    {
        return _cacheService.GetOrQueryModTextureBackup(mod);
    }

    // Query whether a mod has PMP backup, caching the result
    private bool GetOrQueryModPmp(string mod)
    {
        return _cacheService.GetOrQueryModPmp(mod);
    }

    private void DrawSplitter(float totalWidth, ref float leftWidth)
    {
        DrawSplitter_ViewImpl(totalWidth, ref leftWidth);
    }

    private enum ScanSortKind
    {
        FileName,
        ModName,
    }

    private string ResolveModDisplayName(string folder)
    {
        if (string.IsNullOrEmpty(folder))
            return folder;
        if (_modDisplayNames.TryGetValue(folder, out var name) && !string.IsNullOrWhiteSpace(name))
            return name;
        return folder;
    }

    private void RefreshModState(string mod, string origin)
    {
        Task.Run(async () =>
        {
            try
            {
                var files = await _conversionService.GetModTextureFilesAsync(mod).ConfigureAwait(false);
                var names = await _conversionService.GetModDisplayNamesAsync().ConfigureAwait(false);
                var tags = await _conversionService.GetModTagsAsync().ConfigureAwait(false);
                _uiThreadActions.Enqueue(() =>
                {
                    _logger.LogDebug("Mod scan refreshed: origin={origin} mod={mod} files={count}", origin, mod, files?.Count ?? 0);
                    _scannedByMod[mod] = files ?? new List<string>();
                    foreach (var f in _scannedByMod[mod])
                        _texturesToConvert[f] = Array.Empty<string>();
                    if (names != null) _modDisplayNames = names;
                    if (tags != null) _modTags = tags.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.OrdinalIgnoreCase);
                    _needsUIRefresh = true;
                });
            }
            catch { }
        });
    }

    private static string NormalizeTag(string tag)
    {
        return (tag ?? string.Empty).Trim();
    }

    private bool IsModExcludedByTags(string mod)
    {
        if (_excludedTagsNormalized.Count == 0)
            return false;
        if (string.IsNullOrEmpty(mod))
            return false;
        if (_modTags.TryGetValue(mod, out var tags) && tags != null)
        {
            foreach (var t in tags)
            {
                var nt = NormalizeTag(t);
                if (nt.Length == 0)
                    continue;
                if (_excludedTagsNormalized.Contains(nt))
                    return true;
            }
        }
        return false;
    }

    private bool IsModInefficient(string mod)
    {
        var list = _configService.Current.InefficientMods ?? new List<string>();
        foreach (var m in list)
        {
            if (string.Equals(m, mod, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private List<string> GetRestorableModsForCurrentSelection()
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (mod, count) in _selectedCountByMod)
        {
            if (count > 0 && GetOrQueryModBackup(mod))
                result.Add(mod);
        }
        foreach (var m in _selectedEmptyMods)
        {
            if (GetOrQueryModBackup(m))
                result.Add(m);
        }
        return result.ToList();
    }

    private (int convertableMods, int restorableMods) GetSelectedModStates()
    {
        var selectedMods = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (mod, count) in _selectedCountByMod)
        {
            if (count > 0) selectedMods.Add(mod);
        }
        foreach (var m in _selectedEmptyMods) selectedMods.Add(m);
        
        int convertableSelected = 0;
        int restorableSelected = 0;
        
        foreach (var mod in selectedMods)
        {
            var hasBackup = GetOrQueryModBackup(mod);
            if (hasBackup)
                restorableSelected++;
            else
            {
                if (!IsModExcludedByTags(mod))
                    convertableSelected++;
            }
        }
        
        return (convertableSelected, restorableSelected);
    }

    private Dictionary<string, string[]> GetConvertableTextures()
    {
        // Always work with selected textures only
        var filteredTextures = new Dictionary<string, string[]>(StringComparer.Ordinal);

        foreach (var texture in _selectedTextures)
        {
            if (_fileOwnerMod.TryGetValue(texture, out var ownerMod))
            {
                if (!GetOrQueryModBackup(ownerMod) && !IsModExcludedByTags(ownerMod))
                    filteredTextures[texture] = Array.Empty<string>();
            }
        }

        return filteredTextures;
    }

    // Compute backup storage info by enumerating files under the configured backup folder
    private Task<(long totalSize, int fileCount)> ComputeBackupStorageInfoAsync()
    {
        return Task.Run<(long, int)>(() =>
        {
            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (string.IsNullOrEmpty(backupDirectory) || !Directory.Exists(backupDirectory))
                {
                    return (0L, 0);
                }

                long totalSize = 0L;
                int fileCount = 0;
                var files = Directory.GetFiles(backupDirectory, "*", SearchOption.AllDirectories);
                foreach (var file in files)
                {
                    try
                    {
                        var fi = new FileInfo(file);
                        totalSize += fi.Length;
                        fileCount++;
                    }
                    catch
                    {
                        // Ignore file access errors
                    }
                }
                return (totalSize, fileCount);
            }
            catch
            {
                return (0L, 0);
            }
        });
    }

    // Queue recomputation of backup storage and savings metrics off the UI thread
    private void TriggerMetricsRefresh()
    {
        if (_backupStorageInfoTask == null || _backupStorageInfoTask.IsCompleted)
            _backupStorageInfoTask = ComputeBackupStorageInfoAsync();
        if (_savingsInfoTask == null || _savingsInfoTask.IsCompleted)
            _savingsInfoTask = _backupService.ComputeSavingsAsync();
        if (_perModSavingsTask == null || _perModSavingsTask.IsCompleted)
        {
            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
            _perModSavingsTask.ContinueWith(ps =>
            {
                if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                {
                    _cachedPerModSavings = ps.Result;
                    _needsUIRefresh = true;
                }
            }, TaskScheduler.Default);
        }
    }
    private void DrawFolderFlatRow(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        DrawFolderFlatRow_ViewImpl(row, visibleByMod);
    }

    private void DrawModFlatRow(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        DrawModFlatRow_ViewImpl(row, visibleByMod);
    }

    private void DrawFileFlatRow(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        DrawFileFlatRow_ViewImpl(row, visibleByMod);
    }
}
