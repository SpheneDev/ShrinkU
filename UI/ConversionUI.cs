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

public sealed class ConversionUI : Window, IDisposable
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
    
    private void TryStartPmpRestoreNewest(string mod, string refreshReason, bool removeCacheBefore, bool setCompletionStatus, bool resetConversionAfter, bool resetRestoreAfter, bool closePopup)
    {
        if (_configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic)
        {
            SetStatus("Automatic mode active: restoring is disabled.");
            return;
        }
        List<string>? pmpFiles = null;
        try { pmpFiles = _backupService.GetPmpBackupsForModAsync(mod).GetAwaiter().GetResult(); } catch { }
        if (pmpFiles == null || pmpFiles.Count == 0)
        {
            SetStatus($"No .pmp backups found for {mod}");
            return;
        }
        var latest = pmpFiles.OrderByDescending(f => f).First();
        var display = Path.GetFileName(latest);

        if (removeCacheBefore)
        {
            ClearModCaches(mod);
        }

        _running = true;
        ResetBothProgress();
        _currentRestoreMod = mod;
        SetStatus($"PMP restore requested for {mod}: {display}");
        var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
        _ = _backupService.RestorePmpAsync(mod, latest, progress, CancellationToken.None)
            .ContinueWith(t =>
            {
                var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                try { _backupService.RedrawPlayer(); } catch { }
                if (setCompletionStatus)
                {
                    _uiThreadActions.Enqueue(() => { SetStatus(success ? $"PMP restore completed for {mod}: {display}" : $"PMP restore failed for {mod}: {display}"); });
                }
                RefreshScanResults(true, refreshReason);
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
                _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                {
                    if (bt.Status == TaskStatus.RanToCompletion)
                    {
                        bool any = bt.Result;
                        try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                        _modsWithBackupCache[mod] = any;
                        // Refresh PMP-specific caches after restore
                        try { bool _r; _modsWithPmpCache.TryRemove(mod, out _r); } catch { }
                        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
                        try { var hasPmpNow = _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); _modsWithPmpCache[mod] = hasPmpNow; } catch { }
                    }
                });
                _running = false;
                if (resetConversionAfter) ResetConversionProgress();
                if (resetRestoreAfter) ResetRestoreProgress();
                if (closePopup) ImGui.CloseCurrentPopup();
            });
    }
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
    private void ClearModCaches(string mod)
    {
        try { _modsWithBackupCache.TryRemove(mod, out _); } catch { }
        try { _modsWithTexBackupCache.TryRemove(mod, out _); } catch { }
        try { bool _rp; _modsWithPmpCache.TryRemove(mod, out _rp); } catch { }
        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
        try { (string version, string author, DateTime createdUtc, string zipFileName) _rz; _modsZipMetaCache.TryRemove(mod, out _rz); } catch { }
        try { _cachedPerModSavings.Remove(mod); } catch { }
        try { _cachedPerModOriginalSizes.Remove(mod); } catch { }
    }

    private void ClearAllCaches()
    {
        try { _modsWithBackupCache.Clear(); } catch { }
        try { _modsWithTexBackupCache.Clear(); } catch { }
        try { _modsWithPmpCache.Clear(); } catch { }
        try { _modsPmpMetaCache.Clear(); } catch { }
        try { _modsZipMetaCache.Clear(); } catch { }
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

public ConversionUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, TextureBackupService backupService, Action? openSettings = null, ModStateService? modStateService = null)
        : base("ShrinkU###ShrinkUConversionUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _backupService = backupService;
        _openSettings = openSettings;
        _modStateService = modStateService ?? new ModStateService(_logger, _configService);

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
                                _modsWithBackupCache[target] = any;
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
            _modsWithBackupCache.Clear();
            _modsPmpMetaCache.Clear();
            _modsZipMetaCache.Clear();
            _modsWithPmpCache.Clear();
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
                        _modPaths = t.Result;
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
                    _modPaths = t.Result;
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
        // Render general settings directly without tabs. Extras were moved to SettingsUI.
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Texture Settings");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);
        var mode = _configService.Current.TextureProcessingMode;
        var controller = _configService.Current.AutomaticControllerName ?? string.Empty;
        var spheneIntegrated = !string.IsNullOrWhiteSpace(controller) || _configService.Current.AutomaticHandledBySphene;
        var modeDisplay = mode == TextureProcessingMode.Automatic && spheneIntegrated
            ? "Automatic (handled by Sphene)"
            : mode.ToString();
        if (ImGui.BeginCombo("Mode", modeDisplay))
        {
            if (ImGui.Selectable("Manual", mode == TextureProcessingMode.Manual))
            {
                _configService.Current.TextureProcessingMode = TextureProcessingMode.Manual;
                _configService.Save();
                try { OnModeChanged(); TriggerMetricsRefresh(); } catch { }
            }

            if (spheneIntegrated)
            {
                var isAuto = mode == TextureProcessingMode.Automatic;
                if (ImGui.Selectable("Automatic (handled by Sphene)", isAuto))
                {
                    _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                    _configService.Current.AutomaticHandledBySphene = true;
                    _configService.Current.AutomaticControllerName = string.IsNullOrWhiteSpace(controller) ? "Sphene" : controller;
                    _configService.Save();
                    try { OnModeChanged(); TriggerMetricsRefresh(); } catch { }
                }
            }
            else
            {
                if (ImGui.Selectable("Automatic", mode == TextureProcessingMode.Automatic))
                {
                    _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                    _configService.Current.AutomaticHandledBySphene = false;
                    _configService.Current.AutomaticControllerName = string.Empty;
                    _configService.Save();
                    try { OnModeChanged(); TriggerMetricsRefresh(); } catch { }
                }
            }
            ImGui.EndCombo();
        }
        ShowTooltip("Choose how textures are processed. When integrated with Sphene, only the Sphene-handled automatic mode is available.");

        //bool backup = _configService.Current.EnableBackupBeforeConversion;
        //if (ImGui.Checkbox("Enable backup before conversion", ref backup))
        //{
        //    _configService.Current.EnableBackupBeforeConversion = backup;
        //    _configService.Save();
        //}
        //ShowTooltip("Create a backup before converting textures.");
        //bool zip = _configService.Current.EnableZipCompressionForBackups;
        //if (ImGui.Checkbox("ZIP backups by default", ref zip))
        //{
        //    _configService.Current.EnableZipCompressionForBackups = zip;
        //    _configService.Save();
        //}
        //ShowTooltip("Compress backups into ZIP archives by default.");
        //bool deleteOriginals = _configService.Current.DeleteOriginalBackupsAfterCompression;
        //if (ImGui.Checkbox("Delete originals after ZIP", ref deleteOriginals))
        //{
        //    _configService.Current.DeleteOriginalBackupsAfterCompression = deleteOriginals;
        //    _configService.Save();
        //}
        //ShowTooltip("Remove original backup files after ZIP compression.");
        bool autoRestore = _configService.Current.AutoRestoreInefficientMods;
        if (ImGui.Checkbox("Auto-restore backups for inefficient mods", ref autoRestore))
        {
            _configService.Current.AutoRestoreInefficientMods = autoRestore;
            _configService.Save();
        }
        ShowTooltip("Automatically restore the latest backup when a mod becomes larger after conversion.");


        ImGui.Text("Backup Folder:");
        ImGui.SameLine();
        ImGui.TextWrapped(_configService.Current.BackupFolderPath);
        if (ImGui.Button("Browse..."))
        {
            OpenFolderPicker();
        }
        ShowTooltip("Choose a folder to store texture backups.");
        ImGui.SameLine();
        if (ImGui.Button("Open Folder"))
        {
            try
            {
                var path = _configService.Current.BackupFolderPath;
                if (!string.IsNullOrWhiteSpace(path))
                {
                    try { Directory.CreateDirectory(path); } catch { }
                    try
                    {
                        Process.Start(new ProcessStartInfo("explorer.exe", path) { UseShellExecute = true });
                    }
                    catch { }
                }
            }
            catch { }
        }
        ShowTooltip("Open the current backup folder in Explorer.");
        ImGui.SameLine();
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        var openSettingsClicked = ImGui.Button("Settings");
        ImGui.PopStyleColor(4);
        ShowTooltip("Open ShrinkU Settings window.");
        if (openSettingsClicked)
        {
            try { _openSettings?.Invoke(); } catch { }
        }
        try
        {
            if (!_configService.Current.EnableBackupBeforeConversion && !_configService.Current.EnableFullModBackupBeforeConversion)
            {
                _configService.Current.EnableBackupBeforeConversion = true;
                _configService.Current.EnableFullModBackupBeforeConversion = false;
                _configService.Save();
            }
        }
        catch { }
    }

    // Nested folder tree for Table View
    private sealed class TableCatNode
    {
        public string Name { get; }
        public Dictionary<string, TableCatNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<string> Mods { get; } = new();
        public TableCatNode(string name) => Name = name;
    }

    private enum FlatRowKind { Folder, Mod, File }
    private sealed class FlatRow
    {
        public FlatRowKind Kind;
        public TableCatNode Node = null!;
        public string FolderPath = string.Empty;
        public string Mod = string.Empty;
        public string File = string.Empty;
        public int Depth;
    }

    private int _zebraRowIndex = 0;
    private readonly Vector4 _zebraEvenColor = new(0.16f, 0.16f, 0.16f, 0.10f);
    private readonly Vector4 _zebraOddColor  = new(0.16f, 0.16f, 0.16f, 0.30f);

    private TableCatNode BuildTableCategoryTree(IEnumerable<string> mods)
    {
        var root = new TableCatNode("/");
        foreach (var mod in mods)
        {
            if (!_modPaths.TryGetValue(mod, out var fullPath) || string.IsNullOrWhiteSpace(fullPath))
            {
                if (!root.Children.TryGetValue("(Uncategorized)", out var unc))
                    root.Children["(Uncategorized)"] = unc = new TableCatNode("(Uncategorized)");
                unc.Mods.Add(mod);
                continue;
            }

            var norm = fullPath.Replace('\\', '/');
            var parts = norm.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length <= 1)
            {
                if (!root.Children.TryGetValue("(Uncategorized)", out var unc2))
                    root.Children["(Uncategorized)"] = unc2 = new TableCatNode("(Uncategorized)");
                unc2.Mods.Add(mod);
                continue;
            }

            var cursor = root;
            for (var i = 0; i < parts.Length - 1; i++)
            {
                var seg = parts[i];
                if (!cursor.Children.TryGetValue(seg, out var next))
                {
                    next = new TableCatNode(seg);
                    cursor.Children[seg] = next;
                }
                cursor = next;
            }
            cursor.Mods.Add(mod);
        }
        return root;
    }

    private IEnumerable<KeyValuePair<string, TableCatNode>> OrderedChildrenPairs(TableCatNode node)
    {
        return node.Children
            .OrderBy(kv => kv.Key.Equals("(Uncategorized)", StringComparison.OrdinalIgnoreCase) ? 1 : 0)
            .ThenBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase);
    }

    private void BuildFlatRows(TableCatNode node, Dictionary<string, List<string>> visibleByMod, string pathPrefix, int depth)
    {
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            _flatRows.Add(new FlatRow { Kind = FlatRowKind.Folder, Node = child, FolderPath = fullPath, Depth = depth });
            var catOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
            if (!catOpen)
                continue;
            foreach (var mod in child.Mods)
            {
                if (!visibleByMod.ContainsKey(mod))
                    continue;
                _flatRows.Add(new FlatRow { Kind = FlatRowKind.Mod, Node = child, Mod = mod, Depth = depth + 1 });
                if (_configService.Current.ShowModFilesInOverview && _expandedMods.Contains(mod))
                {
                    var files = visibleByMod[mod];
                    if (files != null)
                    {
                        for (int i = 0; i < files.Count; i++)
                        {
                            _flatRows.Add(new FlatRow { Kind = FlatRowKind.File, Node = child, Mod = mod, File = files[i], Depth = depth + 2 });
                        }
                    }
                }
            }
            if (catOpen)
                BuildFlatRows(child, visibleByMod, fullPath, depth + 1);
        }
    }

    private void BuildFolderCountsCache(TableCatNode node, Dictionary<string, List<string>> visibleByMod, string pathPrefix)
    {
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            int modsTotal = 0, modsConverted = 0, texturesTotal = 0, texturesConverted = 0;
            // Sum across this folder's subtree using lightweight per-mod stats
            var stack = new Stack<TableCatNode>();
            stack.Push(child);
            while (stack.Count > 0)
            {
                var cur = stack.Pop();
                modsTotal += cur.Mods.Count;
                foreach (var m in cur.Mods)
                {
                    if (_scannedByMod.TryGetValue(m, out var files) && files != null)
                        texturesTotal += files.Count;
                    if (_cachedPerModSavings.TryGetValue(m, out var s) && s != null && s.ComparedFiles > 0)
                        texturesConverted += s.ComparedFiles;
                    else
                    {
                        var snap = _modStateService.Snapshot();
                        if (snap.TryGetValue(m, out var st) && st != null && st.ComparedFiles > 0)
                            texturesConverted += st.ComparedFiles;
                    }
                    if (GetOrQueryModBackup(m))
                        modsConverted++;
                }
                foreach (var ch in cur.Children.Values)
                    stack.Push(ch);
            }
            _folderCountsCache[fullPath] = (modsTotal, modsConverted, texturesTotal, Math.Min(texturesConverted, texturesTotal));
            BuildFolderCountsCache(child, visibleByMod, fullPath);
        }
    }

    private int CountModsRecursive(TableCatNode node)
    {
        var count = node.Mods.Count;
        foreach (var child in node.Children.Values)
            count += CountModsRecursive(child);
        return count;
    }

    // Count converted mods (with backup) recursively within a folder node
    private int CountConvertedModsRecursive(TableCatNode node)
    {
        var count = 0;
        foreach (var mod in node.Mods)
        {
            var hasBackup = GetOrQueryModBackup(mod);
            if (hasBackup)
                count++;
        }
        foreach (var child in node.Children.Values)
            count += CountConvertedModsRecursive(child);
        return count;
    }

    // Count total convertable textures recursively within a folder node
    private int CountTexturesRecursive(TableCatNode node)
    {
        var count = 0;
        foreach (var mod in node.Mods)
        {
            if (_scannedByMod.TryGetValue(mod, out var files) && files != null)
                count += files.Count;
        }
        foreach (var child in node.Children.Values)
            count += CountTexturesRecursive(child);
        return count;
    }

    // Count converted textures recursively within a folder node (based on backups/original-size map)
    private int CountConvertedTexturesRecursive(TableCatNode node)
    {
        var count = 0;
        foreach (var mod in node.Mods)
        {
            var hasBackup = GetOrQueryModBackup(mod);
            if (!hasBackup)
                continue;
            if (_scannedByMod.TryGetValue(mod, out var files) && files != null && files.Count > 0)
            {
                // Ensure original-size map is hydrated
                _ = GetOrQueryModOriginalTotal(mod);
                if (_modPaths.TryGetValue(mod, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
                {
                    if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null && map.Count > 0)
                    {
                        foreach (var f in files)
                        {
                            try
                            {
                                var rel = Path.GetRelativePath(modRoot, f).Replace('\\', '/');
                                if (map.ContainsKey(rel)) count++;
                            }
                            catch { }
                        }
                    }
                    else if (_cachedPerModSavings.TryGetValue(mod, out var s) && s != null && s.ComparedFiles > 0)
                    {
                        // Fallback to stats when map not available
                        count += Math.Min(s.ComparedFiles, files.Count);
                    }
                }
            }
        }
        foreach (var child in node.Children.Values)
            count += CountConvertedTexturesRecursive(child);
        return count;
    }

    // Collect all visible files under a folder node (recursive over children) for selection/size aggregation
    private List<string> CollectFilesRecursive(TableCatNode node, Dictionary<string, List<string>> visibleByMod)
    {
        var files = new List<string>();
        foreach (var mod in node.Mods)
        {
            if (visibleByMod.TryGetValue(mod, out var modFiles) && modFiles != null && modFiles.Count > 0)
            {
                for (int i = 0; i < modFiles.Count; i++)
                    files.Add(modFiles[i]);
            }
        }
        foreach (var child in node.Children.Values)
        {
            var sub = CollectFilesRecursive(child, visibleByMod);
            if (sub != null && sub.Count > 0)
            {
                for (int i = 0; i < sub.Count; i++)
                    files.Add(sub[i]);
            }
        }
        return files;
    }

    private bool HasSelectableFiles(TableCatNode node, Dictionary<string, List<string>> visibleByMod)
    {
        foreach (var mod in node.Mods)
        {
            if (visibleByMod.TryGetValue(mod, out var modFiles) && modFiles != null && modFiles.Count > 0)
                return true;
        }
        foreach (var child in node.Children.Values)
        {
            if (HasSelectableFiles(child, visibleByMod))
                return true;
        }
        return false;
    }

    private int CountVisibleRows(TableCatNode node, Dictionary<string, List<string>> visibleByMod, string pathPrefix, int depth = 0)
    {
        int count = 0;
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            count++;
            var catOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
            if (catOpen)
            {
                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.ContainsKey(mod))
                        continue;
                    count++;
                    if (_configService.Current.ShowModFilesInOverview && _expandedMods.Contains(mod))
                    {
                        var files = visibleByMod[mod];
                        if (files != null)
                            count += files.Count;
                    }
                }
                count += CountVisibleRows(child, visibleByMod, fullPath, depth + 1);
            }
        }
        return count;
    }

    // Collect all folder paths in the category tree for Expand All
    private void CollectFolderPaths(TableCatNode node, string pathPrefix, List<string> result)
    {
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            result.Add(fullPath);
            CollectFolderPaths(child, fullPath, result);
            // Include leaf folder that contains mods even without children
            foreach (var m in child.Mods)
            {
                // no-op for mods; path added above is sufficient for folder expansion
                break;
            }
        }
    }

private void DrawCategoryTableNode(TableCatNode node, Dictionary<string, List<string>> visibleByMod, ref int idx, string pathPrefix, int depth = 0, int clipStart = 0, int clipEnd = int.MaxValue)
    {
        const float indentStep = 16f;
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            bool drawFolderRow = idx >= clipStart && idx < clipEnd;
            if (drawFolderRow)
            {
                ImGui.TableNextRow();
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                ImGui.TableSetColumnIndex(0);
            }
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            List<string>? folderFiles = null;
            bool hasSelectable = false;
            if (drawFolderRow)
            {
                hasSelectable = HasSelectableFiles(child, visibleByMod);
            }
            bool folderSelected = false;
            if (drawFolderRow)
            {
                folderFiles = CollectFilesRecursive(child, visibleByMod);
                var hasFiles = folderFiles != null && folderFiles.Count > 0;
                folderSelected = (hasFiles && folderFiles.All(f => _selectedTextures.Contains(f)))
                    || (!hasFiles && child.Mods.Count > 0 && child.Mods.All(m => _selectedEmptyMods.Contains(m)));
            }
            if (drawFolderRow) ImGui.BeginDisabled(!hasSelectable && child.Mods.Count == 0);
            if (drawFolderRow && ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
            {
                if (folderSelected)
                {
                    folderFiles = CollectFilesRecursive(child, visibleByMod);
                    var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                    foreach (var mod in child.Mods)
                    {
                        if (!visibleByMod.TryGetValue(mod, out var files))
                            continue;
                        var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                        var totalAll = _scannedByMod.TryGetValue(mod, out var allModFiles2) && allModFiles2 != null ? allModFiles2.Count : files.Count;
                        var convertedAll = 0;
                        var hasBackupM = GetOrQueryModBackup(mod);
                        if (hasBackupM && totalAll > 0)
                        {
                            if (_modPaths.TryGetValue(mod, out var modRoot2) && !string.IsNullOrWhiteSpace(modRoot2))
                            {
                                if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map2) && map2 != null && map2.Count > 0 && allModFiles2 != null)
                                {
                                    foreach (var f in allModFiles2)
                                    {
                                        try
                                        {
                                            var rel = Path.GetRelativePath(modRoot2, f).Replace('\\', '/');
                                            if (map2.ContainsKey(rel)) convertedAll++;
                                        }
                                        catch { }
                                    }
                                }
                            }
                            if (convertedAll == 0 && _cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
                        }
                        var disableCheckbox = automaticMode && !isOrphan && (convertedAll >= totalAll);
                        if (totalAll == 0)
                        {
                            _selectedEmptyMods.Add(mod);
                            continue;
                        }
                        if (!disableCheckbox)
                            foreach (var f in files) _selectedTextures.Add(f);
                    }
                }
                else
                {
                    folderFiles = CollectFilesRecursive(child, visibleByMod);
                    foreach (var f in folderFiles) _selectedTextures.Remove(f);
                    foreach (var mod in child.Mods) _selectedEmptyMods.Remove(mod);
                }
            }
            if (drawFolderRow) ImGui.EndDisabled();
            if (drawFolderRow && !hasSelectable && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                ImGui.SetTooltip("No selectable files in this folder (filtered or excluded).");
            else if (drawFolderRow)
                ShowTooltip("Select or deselect all files in this folder.");
            idx++;

            // Folder label and tree toggle in File column
            var catDefaultOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
            bool catOpen;
            if (drawFolderRow)
            {
                ImGui.TableSetColumnIndex(1);
                ImGui.SetCursorPosX(ImGui.GetCursorPosX() + depth * indentStep);
                ImGui.SetNextItemOpen(catDefaultOpen, ImGuiCond.Always);
                catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
                var catToggled = ImGui.IsItemToggledOpen();
                if (catToggled)
                {
                    if (catOpen) _expandedFolders.Add(fullPath);
                    else _expandedFolders.Remove(fullPath);
                }
                ImGui.SameLine();
                var folderColor = new Vector4(0.70f, 0.80f, 1.00f, 1f);
                ImGui.PushFont(UiBuilder.IconFont);
                ImGui.TextColored(folderColor, (catOpen ? FontAwesomeIcon.FolderOpen : FontAwesomeIcon.Folder).ToIconString());
                ImGui.PopFont();
                ImGui.SameLine();
                var totalModsInFolder = CountModsRecursive(child);
                var convertedModsInFolder = CountConvertedModsRecursive(child);
                var totalTexturesInFolder = CountTexturesRecursive(child);
                var convertedTexturesInFolder = CountConvertedTexturesRecursive(child);
                ImGui.TextColored(folderColor, $"{name} (mods {convertedModsInFolder}/{totalModsInFolder}, textures {convertedTexturesInFolder}/{totalTexturesInFolder})");
                ImGui.TableSetColumnIndex(3);
                long folderOriginalBytes = 0;
                foreach (var m in child.Mods)
                {
                    var modOrig = GetOrQueryModOriginalTotal(m);
                    if (modOrig > 0)
                        folderOriginalBytes += modOrig;
                }
                if (folderOriginalBytes > 0)
                    DrawRightAlignedSize(folderOriginalBytes);
                else
                    ImGui.TextUnformatted("");
                ImGui.TableSetColumnIndex(2);
                long folderCompressedBytes = 0;
                foreach (var m in child.Mods)
                {
                    var hasBackupM = GetOrQueryModBackup(m);
                    if (!hasBackupM) continue;
                    if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null && stats.CurrentBytes > 0)
                        folderCompressedBytes += stats.CurrentBytes;
                }
                if (folderCompressedBytes > 0)
                {
                    var color = folderCompressedBytes > folderOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
                    DrawRightAlignedSizeColored(folderCompressedBytes, color);
                }
                else
                {
                    DrawRightAlignedTextColored("-", _compressedTextColor);
                }
            }
            else
            {
                catOpen = catDefaultOpen;
            }

            if (catOpen)
            {
                DrawCategoryTableNode(child, visibleByMod, ref idx, fullPath, depth + 1, clipStart, clipEnd);

                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.TryGetValue(mod, out var files))
                        continue;

                    bool drawModRow = idx >= clipStart && idx < clipEnd;
                    if (drawModRow)
                    {
                        ImGui.TableNextRow();
                        ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                    }
                    var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                    var hasBackup = GetOrQueryModBackup(mod);
                    var excluded = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
                    var isNonConvertible = files.Count == 0;

                    if (drawModRow)
                    {
                        ImGui.TableSetColumnIndex(1);
                        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (depth + 1) * indentStep);
                    }
                    var nodeFlags = ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.FramePadding;
                    if (!_configService.Current.ShowModFilesInOverview)
                        nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
                    int totalAll = 0;
                    int convertedAll = 0;
                    string header;
                    if (drawModRow)
                    {
                        totalAll = _scannedByMod.TryGetValue(mod, out var allModFiles2) && allModFiles2 != null ? allModFiles2.Count : files.Count;
                        if (hasBackup && totalAll > 0)
                        {
                            if (_cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
                            else
                            {
                                var snap = _modStateService.Snapshot();
                                if (snap.TryGetValue(mod, out var ms) && ms != null && ms.ComparedFiles > 0)
                                    convertedAll = Math.Min(ms.ComparedFiles, totalAll);
                            }
                        }
                        header = hasBackup
                            ? $"{ResolveModDisplayName(mod)} ({convertedAll}/{totalAll})"
                            : $"{ResolveModDisplayName(mod)} ({totalAll})";
                    }
                    else
                    {
                        header = ResolveModDisplayName(mod);
                    }
                    bool open = _expandedMods.Contains(mod);
                    if (drawModRow)
                    {
                        ImGui.SetNextItemOpen(open, ImGuiCond.Always);
                        open = ImGui.TreeNodeEx($"##mod-{mod}", nodeFlags);
                    }
                    var headerHoveredTree = ImGui.IsItemHovered();
                    if (drawModRow && ImGui.BeginPopupContextItem($"modctx-{mod}"))
                    {
                        if (ImGui.MenuItem("Open in Penumbra"))
                        {
                            var display = ResolveModDisplayName(mod);
                            _conversionService.OpenModInPenumbra(mod, display);
                        }
                        if (ImGui.MenuItem("Backup Mod (PMP)"))
                        {
                            _running = true;
                            ResetBothProgress();
                            SetStatus($"Creating full mod backup (PMP) for {mod}");
                            var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                            _ = _backupService.CreateFullModBackupAsync(mod, progress, CancellationToken.None)
                                .ContinueWith(t =>
                                {
                                    var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                    _uiThreadActions.Enqueue(() => { SetStatus(success ? $"Backup completed for {mod}" : $"Backup failed for {mod}"); });
                                    try { RefreshModState(mod, "manual-pmp-backup"); } catch { }
                                    TriggerMetricsRefresh();
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                        {
                                            bool any = bt.Result;
                                            try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                            _modsWithBackupCache[mod] = any;
                                            _modsWithPmpCache[mod] = true;
                                        }
                                    });
                                    _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                                }, TaskScheduler.Default);
                        }
                        if (ImGui.MenuItem("Open Backup Folder"))
                        {
                            try
                            {
                                var path = _configService.Current.BackupFolderPath;
                                if (!string.IsNullOrWhiteSpace(path))
                                {
                                    var modDir = Path.Combine(path, mod);
                                    try { Directory.CreateDirectory(modDir); } catch { }
                                    try { Process.Start(new ProcessStartInfo("explorer.exe", modDir) { UseShellExecute = true }); } catch { }
                                }
                            }
                            catch { }
                        }
                        ImGui.EndPopup();
                    }
                    if (drawModRow)
                    {
                        ImGui.SameLine();
                        ImGui.AlignTextToFramePadding();
                        ImGui.PushFont(UiBuilder.IconFont);
                    }
                    if (_modEnabledStates.TryGetValue(mod, out var stIcon))
                    {
                        // Use white when disabled, green when enabled
                        var iconColor = stIcon.Enabled ? new Vector4(0.40f, 0.85f, 0.40f, 1f) : new Vector4(1f, 1f, 1f, 1f);
                        if (drawModRow) ImGui.TextColored(iconColor, FontAwesomeIcon.Cube.ToIconString());
                    }
                    else
                    {
                        if (drawModRow) ImGui.TextUnformatted(FontAwesomeIcon.Cube.ToIconString());
                    }
                    if (drawModRow) ImGui.PopFont();
                    if (drawModRow)
                    {
                        ImGui.SameLine();
                        ImGui.TextUnformatted(header);
                    }

                    // Draw checkbox after computing convertibility (auto-mode disables when fully converted or no files)
                    if (drawModRow) ImGui.TableSetColumnIndex(0);
                    bool modSelected = isNonConvertible
                        ? _selectedEmptyMods.Contains(mod)
                        : files.All(f => _selectedTextures.Contains(f));
                    var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                    var disableCheckbox = false;
                    if (drawModRow)
                        disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
                    if (drawModRow) ImGui.BeginDisabled(disableCheckbox);
                    if (drawModRow && ImGui.Checkbox($"##modsel-{mod}", ref modSelected))
                    {
                        if (isNonConvertible)
                        {
                            if (modSelected) _selectedEmptyMods.Add(mod); else _selectedEmptyMods.Remove(mod);
                        }
                        else
                        {
                            if (modSelected)
                                foreach (var f in files) _selectedTextures.Add(f);
                            else
                                foreach (var f in files) _selectedTextures.Remove(f);
                        }
                    }
                    if (drawModRow) ImGui.EndDisabled();
                    if (drawModRow && disableCheckbox && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        ImGui.SetTooltip(automaticMode ? "Automatic mode: mod cannot be selected for conversion." : "Mod excluded by tags");
                    else if (drawModRow)
                        ShowTooltip("Toggle selection for all files in this mod.");
                    // Rich mod tooltip with helpful information (triggered when hovering header)
                    if (drawModRow && headerHoveredTree)
                    {
                        bool hasPmp = GetOrQueryModPmp(mod);
                        long origBytesTip = GetOrQueryModOriginalTotal(mod);
                        long compBytesTip = 0;
                        if (!isOrphan && _cachedPerModSavings.TryGetValue(mod, out var tipStats) && tipStats != null)
                            compBytesTip = Math.Max(0, tipStats.CurrentBytes);

                        float reductionPctTip = 0f;
                        if (origBytesTip > 0 && compBytesTip > 0)
                            reductionPctTip = MathF.Max(0f, (float)(origBytesTip - compBytesTip) / origBytesTip * 100f);

                        ImGui.BeginTooltip();
                        ImGui.TextUnformatted($"{ResolveModDisplayName(mod)}");
                        ImGui.Separator();
                        ImGui.TextUnformatted($"Textures: {(hasBackup ? convertedAll : 0)}/{totalAll} converted");
                        ImGui.TextUnformatted($"Uncompressed: {FormatSize(origBytesTip)}");
                        var compText = compBytesTip > 0 ? $"{FormatSize(compBytesTip)} ({reductionPctTip:0.00}% reduction)" : "-";
                        ImGui.TextUnformatted($"Compressed: {compText}");
                        var texturesEnabled = _configService.Current.EnableBackupBeforeConversion;
                        var pmpEnabled = _configService.Current.EnableFullModBackupBeforeConversion;
                        string backupText = (hasBackup && hasPmp && texturesEnabled && pmpEnabled) ? "Tex, PMP" : (hasPmp ? "PMP" : (hasBackup ? "Textures" : "None"));
                        if (hasPmp)
                        {
                            var meta = GetOrQueryModPmpMeta(mod);
                            if (meta.HasValue)
                            {
                                var (v, a, created, fileName) = meta.Value;
                                ImGui.TextUnformatted($"Backups: {backupText}");
                                if (!string.IsNullOrWhiteSpace(v)) ImGui.TextUnformatted($"Version: {v}");
                                if (!string.IsNullOrWhiteSpace(a)) ImGui.TextUnformatted($"Author: {a}");
                            }
                            else
                            {
                                ImGui.TextUnformatted($"Backups: {backupText}");
                            }
                        }
                        else
                        {
                            ImGui.TextUnformatted($"Backups: {backupText}");
                            if (hasBackup)
                            {
                                var zmeta = GetOrQueryModZipMeta(mod);
                                if (zmeta.HasValue)
                                {
                                    var (v2, a2, created2, fileName2) = zmeta.Value;
                                    if (!string.IsNullOrWhiteSpace(v2)) ImGui.TextUnformatted($"Version: {v2}");
                                    if (!string.IsNullOrWhiteSpace(a2)) ImGui.TextUnformatted($"Author: {a2}");
                                }
                                else
                                {
                                    var live = _backupService.GetLiveModMeta(mod);
                                    if (!string.IsNullOrWhiteSpace(live.version)) ImGui.TextUnformatted($"Version: {live.version}");
                                    if (!string.IsNullOrWhiteSpace(live.author)) ImGui.TextUnformatted($"Author: {live.author}");
                                }
                            }
                        }
                        var enabledState = _modEnabledStates.TryGetValue(mod, out var st) ? (st.Enabled ? "Enabled" : "Disabled") : "Disabled";
                        ImGui.TextUnformatted($"State: {enabledState}");
                        ImGui.EndTooltip();
                    }
                    // If an external change was detected recently or persisted, show a small plug indicator
                    var hasPersistent = _configService.Current.ExternalConvertedMods.ContainsKey(mod);
                    var showExternal = ((DateTime.UtcNow - _lastExternalChangeAt).TotalSeconds < 30 && !string.IsNullOrEmpty(_lastExternalChangeReason)) || hasPersistent;
                    if (showExternal && drawModRow)
                    {
                        ImGui.SameLine();
                        ImGui.PushFont(UiBuilder.IconFont);
                        ImGui.TextColored(new Vector4(0.70f, 0.85f, 1.00f, 1f), FontAwesomeIcon.Plug.ToIconString());
                        ImGui.PopFont();
                        if (ImGui.IsItemHovered())
                        {
                            string reason = _lastExternalChangeReason;
                            if (hasPersistent)
                            {
                                try
                                {
                                    if (_configService.Current.ExternalConvertedMods.TryGetValue(mod, out var marker) && marker != null)
                                        reason = marker.Reason ?? reason;
                                }
                                catch { }
                            }
                            var tip = !string.IsNullOrWhiteSpace(reason) && reason.Equals("ipc-auto-conversion-complete", StringComparison.OrdinalIgnoreCase)
                                ? "Automatic via Sphene"
                                : string.IsNullOrWhiteSpace(reason) ? "External conversion detected" : "External conversion detected: " + reason;
                            ImGui.SetTooltip(tip);
                        }
                    }
                    if (drawModRow) ImGui.SameLine();
                    if (drawModRow && IsModInefficient(mod))
                    {
                        ImGui.PushFont(UiBuilder.IconFont);
                        ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
                        ImGui.PopFont();
                        if (ImGui.IsItemHovered())
                            ImGui.SetTooltip("This mod becomes larger after conversion");
                        ImGui.SameLine();
                    }
                    if (drawModRow && hasBackup && _cachedPerModSavings.TryGetValue(mod, out var noteStats1) && noteStats1 != null && noteStats1.OriginalBytes > 0 && noteStats1.CurrentBytes > noteStats1.OriginalBytes)
                    {
                        ImGui.PushFont(UiBuilder.IconFont);
                        ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
                        ImGui.PopFont();
                        if (ImGui.IsItemHovered())
                            ImGui.SetTooltip("This mod is smaller when not converted");
                        ImGui.SameLine();
                    }
                    // Uncompressed and Compressed columns for mod row in folder view
                    if (drawModRow) ImGui.TableSetColumnIndex(3);
                    _cachedPerModSavings.TryGetValue(mod, out var modStats);
                    var stateSnap = _modStateService.Snapshot();
                    stateSnap.TryGetValue(mod, out var modState);
                    long modOriginalBytes = modState != null && modState.ComparedFiles > 0 ? modState.OriginalBytes : GetOrQueryModOriginalTotal(mod);
                    var hideStatsForNoTextures = totalAll == 0;
                    if (drawModRow && hideStatsForNoTextures)
                        ImGui.TextUnformatted("");
                    else if (drawModRow)
                        DrawRightAlignedSize(modOriginalBytes);

                    if (drawModRow) ImGui.TableSetColumnIndex(2);
                    long modCurrentBytes = 0;
                    var hideCompressed = modState != null && modState.InstalledButNotConverted;
                    if (!isOrphan && hasBackup && modState != null && modState.CurrentBytes > 0 && !hideCompressed)
                        modCurrentBytes = modState.CurrentBytes;
                    if (drawModRow && hideStatsForNoTextures)
                        ImGui.TextUnformatted("");
                    else if (drawModRow && modCurrentBytes <= 0)
                        DrawRightAlignedTextColored("-", _compressedTextColor);
                    else if (drawModRow)
                    {
                        var color = modCurrentBytes > modOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
                        DrawRightAlignedSizeColored(modCurrentBytes, color);
                    }

                    // Action column
                    if (drawModRow) ImGui.TableSetColumnIndex(4);
                    if (drawModRow) ImGui.BeginDisabled(ActionsDisabled());
                    if (hasBackup || files.Count > 0 || isOrphan)
                    {
                        var hasTexBackup = GetOrQueryModTextureBackup(mod);
                        var hasPmpBackup = GetOrQueryModPmp(mod);
                        bool doInstall = isOrphan;
                        bool doRestore = false;
                        bool doReinstall = false;
                        bool doConvert = false;
                        if (doInstall)
                        {
                            doRestore = false;
                            doConvert = false;
                        }
                        else
                        {
                            var anyBackup = hasBackup || hasTexBackup || hasPmpBackup;
                            if (isNonConvertible)
                            {
                                doConvert = false;
                                if (hasPmpBackup && anyBackup)
                                {
                                    doReinstall = true;
                                    doRestore = false;
                                }
                                else
                                {
                                    doRestore = anyBackup;
                                }
                            }
                            else
                            {
                                doRestore = anyBackup;
                                doConvert = !anyBackup;
                                if (modState != null && modState.InstalledButNotConverted)
                                {
                                    doConvert = true;
                                    doRestore = false;
                                }
                            }
                        }
                        var actionLabel = doInstall
                            ? $"Install##install-{mod}"
                            : (doReinstall ? $"Reinstall##reinstall-{mod}" : (doRestore ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}"));

                        if (drawModRow)
                        {
                            if (doInstall)
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                            else if (doRestore)
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                            else if (doReinstall)
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ReinstallButton);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ReinstallButtonHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ReinstallButtonActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                            else
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ConvertButton);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ConvertButtonHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ConvertButtonActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                        }

                        var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                        bool modHasUsed = false;
                        try { modHasUsed = files.Any(f => _penumbraUsedFiles.Contains(f)); } catch { }
                        var restoreDisabledByAuto = autoMode && (doRestore || doReinstall);
                        bool canInstall = true;
                        if (doInstall)
                        {
                            try
                            {
                                var info = _orphaned.FirstOrDefault(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                                canInstall = info != null && !string.IsNullOrWhiteSpace(info.LatestPmpPath);
                            }
                            catch { canInstall = false; }
                        }
                        if (drawModRow) ImGui.BeginDisabled(excluded || (!isOrphan && restoreDisabledByAuto) || (isOrphan && !canInstall) || ActionsDisabled());
                        if (drawModRow && ImGui.Button(actionLabel, new Vector2(60, 0)))
                        {
                            if (doInstall)
                            {
                                _running = true;
                                ResetBothProgress();
                                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                                _ = _backupService.ReinstallModFromLatestPmpAsync(mod, progress, CancellationToken.None)
                                    .ContinueWith(t =>
                                    {
                                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                        _uiThreadActions.Enqueue(() =>
                                        {
                                            SetStatus(success ? $"Install completed for {mod}" : $"Install failed for {mod}");
                                            _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                            _needsUIRefresh = true;
                                            ClearModCaches(mod);
                                        });
                                        try { _conversionService.OpenModInPenumbra(mod, null); } catch { }
                                        try
                                        {
                                            var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                                            if (autoMode)
                                            {
                                                _ = _conversionService.StartAutomaticConversionForModWithDelayAsync(mod, 2000);
                                            }
                                        }
                                        catch { }
                                        try { _configService.Current.ExternalConvertedMods.Remove(mod); _configService.Save(); } catch { }
                                        try { RefreshModState(mod, "orphan-install"); } catch { }
                                        try { TriggerMetricsRefresh(); } catch { }
                                        _running = false;
                                        _needsUIRefresh = true;
                                    }, TaskScheduler.Default);
                            }
                            else if (doReinstall)
                            {
                                _running = true;
                                ResetBothProgress();
                                _reinstallInProgress = true;
                                _reinstallMod = mod;
                                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                                _ = _backupService.ReinstallModFromLatestPmpAsync(mod, progress, CancellationToken.None)
                                    .ContinueWith(t =>
                                    {
                                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                        _uiThreadActions.Enqueue(() =>
                                        {
                                            SetStatus(success ? $"Reinstall completed for {mod}" : $"Reinstall failed for {mod}");
                                            _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                            _needsUIRefresh = true;
                                            ClearModCaches(mod);
                                            RefreshModState(mod, "reinstall-completed");
                                            Task.Run(async () =>
                                            {
                                                try
                                                {
                                                    await Task.Delay(1200).ConfigureAwait(false);
                                                    _uiThreadActions.Enqueue(() =>
                                                    {
                                                        RefreshScanResults(true, "reinstall-completed");
                                                    });
                                                }
                                                catch { }
                                            });
                                        });
                                        
                                        Task.Run(async () =>
                                        {
                                            try
                                            {
                                                await Task.Delay(1200).ConfigureAwait(false);
                                                _conversionService.OpenModInPenumbra(mod, null);
                                            }
                                            catch { }
                                        });
                                        try { RefreshModState(mod, "reinstall-folder-view"); } catch { }
                                        try { TriggerMetricsRefresh(); } catch { }
                                        _reinstallInProgress = false;
                                        _reinstallMod = string.Empty;
                                        ResetBothProgress();
                                        _running = false;
                                    }, TaskScheduler.Default);
                            }
                            else if (doRestore)
                            {
                                // Prefer PMP restore when available if configured; otherwise show context menu
                                var hasPmpForClick = GetOrQueryModPmp(mod);
                                if (hasPmpForClick && _configService.Current.PreferPmpRestoreWhenAvailable)
                                {
                                    TryStartPmpRestoreNewest(mod, "restore-pmp-folder-view", true, false, false, true, false);
                                }
                                else if (hasPmpForClick)
                                {
                                    ImGui.OpenPopup($"restorectx-{mod}");
                                }
                                else
                                {
                                    _running = true;
                                    _modsWithBackupCache.TryRemove(mod, out _);
                                    // Reset progress state for restore and initialize current restore mod
                                    ResetBothProgress();
                                    _currentRestoreMod = mod;
                                    var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                                            _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                                .ContinueWith(t => {
                                                    var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                                    try { _backupService.RedrawPlayer(); } catch { }
                                                    _logger.LogDebug("Restore completed (mod button in folder view)");
                                                    RefreshModState(mod, "restore-folder-view");
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
                                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                                    {
                                                        if (bt.Status == TaskStatus.RanToCompletion)
                                                        {
                                                            bool any = bt.Result;
                                                            try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                                            _modsWithBackupCache[mod] = any;
                                                        }
                                                    });
                                                    _running = false;
                                                    // Clear restore progress after completion
                                                    ResetRestoreProgress();
                                                });
                                }
                            }
                            else
                            {
                            var allFilesForMod = files;
                            try
                            {
                                if (_configService.Current.IncludeHiddenModTexturesOnConvert && _scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
                                    allFilesForMod = all;
                            }
                            catch { }
                            var toConvert = BuildToConvert(allFilesForMod);
                            // Reset progress state for a fresh conversion
                            ResetConversionProgress();
                            ResetRestoreProgress();
                            _ = _conversionService.StartConversionAsync(toConvert)
                                .ContinueWith(_ =>
                                {
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                        {
                                            bool any = bt.Result;
                                            try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                            _modsWithBackupCache[mod] = any;
                                        }
                                    });
                                    _running = false;
                                });
                            }
                        }
                        // Tooltip for action button (Install/Reinstall/Restore/Convert)
                        if (drawModRow) ImGui.PopStyleColor(1);
                        if (drawModRow && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        {
                            if (excluded)
                                ImGui.SetTooltip("Mod excluded by tags");
                            else if (restoreDisabledByAuto)
                                ImGui.SetTooltip("Automatic mode active: restoring is disabled.");
                            else
                            {
                                if (doInstall)
                                    ShowTooltip("Install mod from PMP backup.");
                                else if (doReinstall)
                                    ShowTooltip("Reinstall mod from PMP backup.");
                                else if (doRestore)
                                    ShowTooltip("Restore backups for this mod.");
                                else
                                {
                                    var msg = _configService.Current.IncludeHiddenModTexturesOnConvert
                                        ? "Convert all textures for this mod."
                                        : "Convert all visible textures for this mod.";
                                    ShowTooltip(msg);
                                }
                            }
                        }
                        // Enhance Restore button when PMP is available: context menu
                        var hasPmp = GetOrQueryModPmp(mod);
                        if (drawModRow && doRestore && hasPmp && !automaticMode)
                        {
                            // Right-click opens named popup; left-click also opens it above
                            if (ImGui.BeginPopupContextItem($"restorectx-{mod}"))
                            {
                                ImGui.OpenPopup($"restorectx-{mod}");
                                ImGui.EndPopup();
                            }
                            // Unified context menu popup
                            if (ImGui.BeginPopup($"restorectx-{mod}"))
                            {
                                var hasTexBk = GetOrQueryModTextureBackup(mod);
                                ImGui.BeginDisabled(!hasTexBk);
                                if (ImGui.MenuItem("Restore textures"))
                                {
                                    _running = true;
                                    _modsWithBackupCache.TryRemove(mod, out _);
                                    ResetConversionProgress();
                                    ResetRestoreProgress();
                                    _currentRestoreMod = mod;
                                    SetStatus($"Restore requested for {mod}");
                                    var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                                    _logger.LogInformation("Restore requested for {mod}", mod);
                                    _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                        .ContinueWith(t => {
                                            var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                            try { _backupService.RedrawPlayer(); } catch { }
                                            _logger.LogDebug(success
                                                ? "Restore completed successfully (context menu, folder view)"
                                                : "Restore failed or aborted (context menu, folder view)");
                                            _logger.LogInformation(success
                                                ? "Restore completed for {mod}"
                                                : "Restore failed for {mod}", mod);
                                            _uiThreadActions.Enqueue(() => { SetStatus(success ? $"Restore completed for {mod}" : $"Restore failed for {mod}"); });
                                            RefreshModState(mod, "restore-folder-view-context");
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
                                            _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                            {
                                                if (bt.Status == TaskStatus.RanToCompletion)
                                                {
                                                    bool any = bt.Result;
                                                    try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                                    _modsWithBackupCache[mod] = any;
                                                }
                                            });
                                            _running = false;
                                            ImGui.CloseCurrentPopup();
                                        });
                                }
                                ImGui.EndDisabled();
                                if (!hasTexBk) ShowTooltip("No texture backups available for this mod.");
                                // restore PMP if available
                                if (ImGui.MenuItem("Restore PMP"))
                                {
                                    TryStartPmpRestoreNewest(mod, "pmp-restore-folder-view-context-newest", false, true, false, false, true);
                                }
                                
                                ImGui.EndPopup();
                            }
                            
                        }
                        if (drawModRow) ImGui.PopStyleColor(3);
                        if (drawModRow) ImGui.EndDisabled();

                    }
                    // Always show Backup button, even when mod has no convertible textures
                    if (drawModRow && isNonConvertible && !hasBackup)
                    {
                        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                        var backupLabel = $"Backup##backup-{mod}";
                        ImGui.BeginDisabled(isOrphan || ActionsDisabled());
                        if (ImGui.Button(backupLabel, new Vector2(60, 0)))
                        {
                        _running = true;
                        ResetBothProgress();
                        SetStatus($"Creating full mod backup (PMP) for {mod}");
                        var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                        _ = _backupService.CreateFullModBackupAsync(mod, progress, CancellationToken.None)
                            .ContinueWith(t =>
                            {
                                var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                _uiThreadActions.Enqueue(() => { SetStatus(success ? $"Backup completed for {mod}" : $"Backup failed for {mod}"); });
                                try { RefreshModState(mod, "manual-pmp-backup-button"); } catch { }
                                TriggerMetricsRefresh();
                                _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                {
                                    if (bt.Status == TaskStatus.RanToCompletion)
                                    {
                                        bool any = bt.Result;
                                        try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                        _modsWithBackupCache[mod] = any;
                                        _modsWithPmpCache[mod] = true;
                                    }
                                });
                                _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                            }, TaskScheduler.Default);
                    }
                        ImGui.EndDisabled();
                        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                            ShowTooltip("Create a full mod backup (PMP).");
                        ImGui.PopStyleColor(4);
                    }
                    if (drawModRow) ImGui.EndDisabled();
                    idx++;

                    if (open)
                    {
                        if (_configService.Current.ShowModFilesInOverview)
                        {
                            foreach (var file in files)
                            {
                                bool drawFileRow = idx >= clipStart && idx < clipEnd;
                                if (drawFileRow)
                                {
                                    ImGui.TableNextRow();
                                    ImGui.TableSetColumnIndex(0);
                                }

                                if (drawFileRow)
                                {
                                    ImGui.TableSetColumnIndex(1);
                                    ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (depth + 2) * indentStep);
                                    var baseName = Path.GetFileName(file);
                                    ImGui.TextUnformatted(baseName);
                                    if (ImGui.IsItemHovered())
                                        ImGui.SetTooltip(file);
                                }

                                if (drawFileRow) ImGui.TableSetColumnIndex(2);
                                var hasBackupForMod = GetOrQueryModBackup(mod);
                                var fileSize = GetCachedOrComputeSize(file);
                                if (drawFileRow && !hasBackupForMod)
                                {
                                    DrawRightAlignedTextColored("-", _compressedTextColor);
                                }
                                else if (drawFileRow)
                                {
                                    if (fileSize > 0)
                                        DrawRightAlignedSizeColored(fileSize, _compressedTextColor);
                                    else
                                        ImGui.TextUnformatted("");
                                }

                                if (drawFileRow) ImGui.TableSetColumnIndex(3);
                                if (!hasBackupForMod && drawFileRow)
                                {
                                    if (fileSize > 0)
                                        DrawRightAlignedSize(fileSize);
                                    else
                                        ImGui.TextUnformatted("");
                                }
                                else if (drawFileRow)
                                {
                                    long originalSize = 0;
                                    if (_modPaths.TryGetValue(mod, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
                                    {
                                        try
                                        {
                                            var rel = Path.GetRelativePath(modRoot, file).Replace('\\', '/');
                                            if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null)
                                            {
                                                map.TryGetValue(rel, out originalSize);
                                            }
                                        }
                                        catch { }
                                    }
                                    if (originalSize > 0)
                                        DrawRightAlignedSize(originalSize);
                                    else
                                        ImGui.TextUnformatted("");
                                }

                                if (drawFileRow) ImGui.TableSetColumnIndex(4);
                                idx++;
                            }
                        }
                    }
                }
            }
        }

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
                    if (grouped != null)
                    {
                        foreach (var mod in grouped)
                        {
                            _scannedByMod[mod.Key] = mod.Value;
                            foreach (var file in mod.Value)
                                _texturesToConvert[file] = Array.Empty<string>();
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
                        foreach (var mod in grouped)
                        {
                            _scannedByMod[mod.Key] = mod.Value;
                            foreach (var file in mod.Value)
                                _texturesToConvert[file] = Array.Empty<string>();
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
        // Expire old status messages and decide whether to show
        if (!string.IsNullOrEmpty(_statusMessage))
        {
            var recent = (DateTime.UtcNow - _statusMessageAt).TotalSeconds <= 10.0;
            if (!_statusPersistent && !recent)
            {
                _statusMessage = string.Empty;
                _statusPersistent = false;
                _statusMessageAt = DateTime.MinValue;
            }
        }
        var showStatus = !string.IsNullOrEmpty(_statusMessage);
        if (_reinstallInProgress)
        {
            var displayMod = _reinstallMod;
            if (!string.IsNullOrEmpty(displayMod) && _modDisplayNames.TryGetValue(displayMod, out var dn))
                displayMod = dn;
            var avail0 = ImGui.GetContentRegionAvail();
            var barSize0 = new Vector2(Math.Max(120f, avail0.X), 0);
            ImGui.Text($"Reinstalling: {displayMod}");
            ImGui.PushStyleColor(ImGuiCol.PlotHistogram, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.FrameBg, ShrinkUColors.WithAlpha(ShrinkUColors.Accent, 0.15f));
            ImGui.ProgressBar(_backupTotal > 0 ? (float)_backupIndex / _backupTotal : 0f, barSize0, _backupTotal > 0 ? $"{_backupIndex}/{_backupTotal}" : string.Empty);
            ImGui.PopStyleColor(2);
            ImGui.Text($"Current File: {_currentTexture}");
            return;
        }
        // Show progress whenever a conversion or restore is active, even if triggered externally
        if (!(_running || _conversionService.IsConverting || _backupTotal > 0 || _convertedCount > 0))
        {
            if (showStatus)
            {
                ImGui.TextWrapped(_statusMessage);
            }
            else
            {
                ImGui.Text("Waiting...");
            }
            return;
        }
        var avail = ImGui.GetContentRegionAvail();
        var barSize = new Vector2(Math.Max(120f, avail.X), 0);

        // If a restore is in progress, show restore-oriented progress bars
        var isRestoring = !string.IsNullOrEmpty(_currentRestoreMod) || (_restoreCancellationTokenSource != null && _backupTotal > 0);
        if (isRestoring)
        {
            if (showStatus)
            {
                ImGui.TextWrapped(_statusMessage);
                ImGui.Dummy(new Vector2(0, 4f));
            }
            var displayMod = _currentRestoreMod;
            if (!string.IsNullOrEmpty(displayMod) && _modDisplayNames.TryGetValue(displayMod, out var dn))
                displayMod = dn;

            ImGui.Text($"Restoring: {displayMod}");
            ImGui.PushStyleColor(ImGuiCol.PlotHistogram, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.FrameBg, ShrinkUColors.WithAlpha(ShrinkUColors.Accent, 0.15f));
            var restoreFraction = _currentRestoreModTotal > 0 ? (float)_currentRestoreModIndex / _currentRestoreModTotal : 0f;
            ImGui.ProgressBar(restoreFraction, barSize, _currentRestoreModTotal > 0 ? $"{_currentRestoreModIndex}/{_currentRestoreModTotal}" : string.Empty);

            ImGui.Text($"Current File: {_currentTexture}");
            if (_backupTotal > 0)
            {
                ImGui.ProgressBar(_backupTotal > 0 ? (float)_backupIndex / _backupTotal : 0f, barSize, _backupTotal > 0 ? $"{_backupIndex}/{_backupTotal}" : string.Empty);
            }
            ImGui.PopStyleColor(2);
            return;
        }

        // Overall mods progress: completed mods plus fraction of current mod
        if (showStatus)
        {
            ImGui.TextWrapped(_statusMessage);
            ImGui.Dummy(new Vector2(0, 4f));
        }
        if (_totalMods > 0)
        {
            var completedMods = Math.Max(_currentModIndex - 1, 0);
            var currentModFraction = _currentModTotalFiles > 0 ? (float)_convertedCount / _currentModTotalFiles : 0f;
            var overallFraction = Math.Clamp(((float)completedMods + currentModFraction) / _totalMods, 0f, 1f);
            ImGui.Text($"Overall Mods: {_currentModIndex}/{_totalMods}");
            ImGui.PushStyleColor(ImGuiCol.PlotHistogram, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.FrameBg, ShrinkUColors.WithAlpha(ShrinkUColors.Accent, 0.15f));
            ImGui.ProgressBar(overallFraction, barSize, string.Empty);
            ImGui.PopStyleColor(2);
        }

        // Current mod progress bar
        ImGui.Text($"Current Mod: {_currentModName}");
        var modFraction = _currentModTotalFiles > 0 ? (float)_convertedCount / _currentModTotalFiles : 0f;
        ImGui.PushStyleColor(ImGuiCol.PlotHistogram, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.FrameBg, ShrinkUColors.WithAlpha(ShrinkUColors.Accent, 0.15f));
        ImGui.ProgressBar(modFraction, barSize, _currentModTotalFiles > 0 ? $"{_convertedCount}/{_currentModTotalFiles}" : string.Empty);
        ImGui.PopStyleColor(2);

        // Current file name
        ImGui.Text($"Current File: {_currentTexture}");
        if (_backupTotal > 0)
        {
            ImGui.Text($"Backup: {_backupIndex}/{_backupTotal}");
        }
        ImGui.Text($"Converted (mod): {_convertedCount}");
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
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Scanned Files Overview");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);
        ImGui.SetNextItemWidth(186f);
        ImGui.InputTextWithHint("##scanFilter", "Filter by file or mod", ref _scanFilter, 128);
        ShowTooltip("Filter results by file name or mod name.");
        

        // Place sort controls below the search text field for better horizontal space
        
        ImGui.SameLine();
        var h = ImGui.GetFrameHeight();
        var w = h + ImGui.GetStyle().ItemInnerSpacing.X * 2f;
        ImGui.PushFont(UiBuilder.IconFont);
        if (_filterPenumbraUsedOnly)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        }
        ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
        var usedOnlyClicked = ImGui.Button(FontAwesomeIcon.Eye.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterPenumbraUsedOnly)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (usedOnlyClicked)
        {
            _filterPenumbraUsedOnly = !_filterPenumbraUsedOnly;
            _configService.Current.FilterPenumbraUsedOnly = _filterPenumbraUsedOnly;
            _configService.Save();
            if (_filterPenumbraUsedOnly && _penumbraUsedFiles.Count == 0 && !_loadingPenumbraUsed)
            {
                _loadingPenumbraUsed = true;
                _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                {
                    if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                    {
                        _penumbraUsedFiles = t.Result;
                        _logger.LogDebug("Loaded {count} currently used textures from Penumbra", _penumbraUsedFiles.Count);
                    }
                    _loadingPenumbraUsed = false;
                });
            }
        }
        ShowTooltip("Show only textures currently used by Penumbra.");

        ImGui.SameLine();
        ImGui.PushFont(UiBuilder.IconFont);
        if (_filterNonConvertibleMods)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        }
        ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
        var hideNonConvertibleClicked = ImGui.Button(FontAwesomeIcon.Ban.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterNonConvertibleMods)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (hideNonConvertibleClicked)
        {
            _filterNonConvertibleMods = !_filterNonConvertibleMods;
            _configService.Current.FilterNonConvertibleMods = _filterNonConvertibleMods;
            _configService.Save();
        }
        ShowTooltip("Hide mods without convertible textures.");

        ImGui.SameLine();
        ImGui.PushFont(UiBuilder.IconFont);
        if (_filterInefficientMods)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        }
        ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
        var hideInefficientClicked = ImGui.Button(FontAwesomeIcon.ExclamationTriangle.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterInefficientMods)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (hideInefficientClicked)
        {
            _filterInefficientMods = !_filterInefficientMods;
            _configService.Current.HideInefficientMods = _filterInefficientMods;
            _configService.Save();
        }
        ShowTooltip("Hide mods marked as inefficient (larger when converted).");

        if (!_selectedCollectionId.HasValue)
        {
            _ = _conversionService.GetCurrentCollectionAsync().ContinueWith(cc =>
            {
                if (cc.Status == TaskStatus.RanToCompletion && cc.Result != null)
                {
                    _selectedCollectionId = cc.Result?.Id;
                    if (_selectedCollectionId.HasValue)
                    {
                        _ = _conversionService.GetAllModEnabledStatesAsync(_selectedCollectionId.Value).ContinueWith(es =>
                        {
                            if (es.Status == TaskStatus.RanToCompletion && es.Result != null)
                                _modEnabledStates = es.Result;
                        });
                    }
                }
            });
        }
        if (_scannedByMod.Count == 0)
        {
            ImGui.Text("No scan results yet. Use 'Scan All Mod Textures'.");
            return;
        }

        DrawScannedFilesTable();
        return;

    }

    private void DrawScannedFilesTable()
    {
        // Build visible list grouped by mod
        var visibleByMod = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (mod, files) in _scannedByMod)
        {
            // Skip mods excluded by tags entirely from the table
            if (IsModExcludedByTags(mod))
                continue;
            var displayName = ResolveModDisplayName(mod);
            var filtered = string.IsNullOrEmpty(_scanFilter)
                ? files
                : files.Where(f => Path.GetFileName(f).IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                || mod.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                || displayName.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0).ToList();
            if (_filterPenumbraUsedOnly && _penumbraUsedFiles.Count > 0)
                 filtered = filtered.Where(f => _penumbraUsedFiles.Contains(f)).ToList();

            var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
            if (_filterNonConvertibleMods && files.Count == 0 && !isOrphan)
                continue;

            // Skip mods marked as inefficient when the filter is enabled
            if (_filterInefficientMods && IsModInefficient(mod))
                continue;

            // Include mods even if no filtered files, when filter matches the mod, or filter is empty
            var modMatchesFilter = string.IsNullOrEmpty(_scanFilter)
                                   || displayName.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                   || mod.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0;
            // In used-only mode, require at least one used file; otherwise include when either files or mod match
            var include = _filterPenumbraUsedOnly ? filtered.Count > 0 : (filtered.Count > 0 || modMatchesFilter);
            if (include)
                visibleByMod[mod] = filtered;
        }

        foreach (var o in _orphaned)
        {
            var name = o.ModFolderName;
            if (!string.IsNullOrWhiteSpace(name) && !visibleByMod.ContainsKey(name))
                visibleByMod[name] = new List<string>();
        }

        // Sort mods or files based on selection
        var mods = visibleByMod.Keys.ToList();
        if (_scanSortKind == ScanSortKind.ModName)
            mods = (_scanSortAsc ? mods.OrderBy(m => ResolveModDisplayName(m)) : mods.OrderByDescending(m => ResolveModDisplayName(m))).ToList();
        else
        {
            // Sort files within mods by file name
            foreach (var k in mods.ToList())
            {
                var sorted = _scanSortAsc
                    ? visibleByMod[k].OrderBy(f => Path.GetFileName(f)).ToList()
                    : visibleByMod[k].OrderByDescending(f => Path.GetFileName(f)).ToList();
                visibleByMod[k] = sorted;
            }
        }

        // Build nested folder tree
        TableCatNode? root = null;
        if (_modPaths.Count > 0)
        {
            root = BuildTableCategoryTree(mods);
        }

        // Bulk actions for current filtered view
        var selectAllClicked = ImGui.Button("Select All");
        ShowTooltip("Select all visible entries in the current view.");
        if (selectAllClicked)
        {
            foreach (var kv in visibleByMod)
            {
                var mod = kv.Key;
                var hasBackup = GetOrQueryModBackup(mod);
                var excluded = !hasBackup && IsModExcludedByTags(mod);
                foreach (var f in kv.Value)
                {
                    if (!excluded || hasBackup)
                        _selectedTextures.Add(f);
                }
            }

        }
        ImGui.SameLine();
        var clearAllClicked = ImGui.Button("Clear All");
        ShowTooltip("Clear all current selections.");
        if (clearAllClicked)
        {
            foreach (var kv in visibleByMod)
                foreach (var f in kv.Value)
                    _selectedTextures.Remove(f);
        }

        // Expand / Collapse controls above the table
        ImGui.SameLine();
        var expandAllClicked = ImGui.Button("Expand All");
        ShowTooltip("Expand all mods and folders in the current view.");
        if (expandAllClicked)
        {
            foreach (var m in visibleByMod.Keys)
                _expandedMods.Add(m);
            if (root != null)
            {
                var allFolders = new List<string>();
                CollectFolderPaths(root, string.Empty, allFolders);
                foreach (var fp in allFolders)
                    _expandedFolders.Add(fp);
            }
        }
        ImGui.SameLine();
        var collapseAllClicked = ImGui.Button("Collapse All");
        ShowTooltip("Collapse all mods and folders in the current view.");
        if (collapseAllClicked)
        {
            _expandedMods.Clear();
            _expandedFolders.Clear();
        }

        // Indicator when Penumbra Used-Only filter is active
        if (_filterPenumbraUsedOnly)
        {
            ImGui.SameLine();
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.InfoCircle.ToIconString());
            var iconHovered = ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled);
            ImGui.PopFont();
            if (iconHovered)
                ImGui.SetTooltip("Only mods currently used by Penumbra are shown. Disable Used-Only to see all mods.");

            ImGui.SameLine();
            ImGui.TextUnformatted("Penumbra Used-Only active");
            ShowTooltip("Only mods currently used by Penumbra are shown. Disable Used-Only to see all mods.");
        }

        // Reserve space for action buttons at the bottom by constraining the table height
        float availY = ImGui.GetContentRegionAvail().Y;
        float frameH = ImGui.GetFrameHeight();
        float reserveH = (frameH * 2) + ImGui.GetStyle().ItemSpacing.Y * 6;
        float childH = MathF.Max(150f, availY - reserveH);
        ImGui.BeginChild("ScannedFilesTableRegion", new Vector2(0, childH), false, ImGuiWindowFlags.None);

        var flags = ImGuiTableFlags.BordersOuter | ImGuiTableFlags.BordersV | ImGuiTableFlags.Resizable | ImGuiTableFlags.ScrollY | ImGuiTableFlags.RowBg;
        if (ImGui.BeginTable("ScannedFilesTable", 5, flags))
        {
            ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, _scannedFirstColWidth);
            ImGui.TableSetupColumn("Mod", ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed, _scannedCompressedColWidth);
            ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, _scannedActionColWidth);
            ImGui.TableSetupScrollFreeze(0, 1);
            ImGui.TableHeadersRow();
            // Reset zebra row index at the start of table drawing
            _zebraRowIndex = 0;

            int idx = 0;
            if (root != null)
            {
                var sig = string.Concat(_expandedFolders.Count.ToString(), "|", _expandedMods.Count.ToString(), "|", visibleByMod.Count.ToString(), "|", (_configService.Current.ShowModFilesInOverview ? "1" : "0"), "|", _scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0");
                if (!string.Equals(sig, _flatRowsSig, StringComparison.Ordinal))
                {
                    _flatRows.Clear();
                    BuildFlatRows(root, visibleByMod, string.Empty, 0);
                    _cachedTotalRows = _flatRows.Count;
                    _flatRowsSig = sig;
                    _folderSizeCache.Clear();
                    _folderSizeCacheSig = _flatRowsSig;
                    _folderCountsCache.Clear();
                    _folderCountsCacheSig = _flatRowsSig;
                    BuildFolderCountsCache(root, visibleByMod, string.Empty);
                }
                var clipper = ImGui.ImGuiListClipper();
                clipper.Begin(_cachedTotalRows);
                while (clipper.Step())
                {
                    for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; i++)
                    {
                        var row = _flatRows[i];
                        ImGui.TableNextRow();
                        ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                        if (row.Kind == FlatRowKind.Folder)
                        {
                            DrawFolderFlatRow(row, visibleByMod);
                        }
                        else if (row.Kind == FlatRowKind.Mod)
                        {
                            DrawModFlatRow(row, visibleByMod);
                        }
                        else
                        {
                            DrawFileFlatRow(row, visibleByMod);
                        }
                    }
                }
                clipper.End();
            }

            // Persist first column width changes once the user finishes resizing
            ImGui.TableSetColumnIndex(0);
            var currentFirstWidth = ImGui.GetColumnWidth();
            if (MathF.Abs(currentFirstWidth - _scannedFirstColWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _scannedFirstColWidth = currentFirstWidth;
                _configService.Current.ScannedFilesFirstColWidth = currentFirstWidth;
                _configService.Save();
                _logger.LogDebug($"Saved first column width: {currentFirstWidth}px");
            }

            ImGui.TableSetColumnIndex(1);
            var currentFileWidth = ImGui.GetColumnWidth();
            _scannedFileColWidth = currentFileWidth;

            // Track Compressed column width every frame (session only)
            ImGui.TableSetColumnIndex(2);
            var prevCompressedWidth = _scannedCompressedColWidth;
            var currentCompressedWidth = ImGui.GetColumnWidth();
            _scannedCompressedColWidth = currentCompressedWidth;
            // Session-only tracking for compressed column; do not persist

            // Track Uncompressed column width every frame; persist on release
            ImGui.TableSetColumnIndex(3);
            var prevUncompressedWidth = _scannedSizeColWidth;
            var currentUncompressedWidth = ImGui.GetColumnWidth();
            _scannedSizeColWidth = currentUncompressedWidth;
            if (MathF.Abs(currentUncompressedWidth - prevUncompressedWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _configService.Current.ScannedFilesSizeColWidth = currentUncompressedWidth;
                _configService.Save();
                _logger.LogDebug($"Saved size column width: {currentUncompressedWidth}px");
            }

            // Track Action column width every frame; persist on release
            ImGui.TableSetColumnIndex(4);
            var prevActionWidth = _scannedActionColWidth;
            var currentActionWidth = ImGui.GetColumnWidth();
            _scannedActionColWidth = currentActionWidth;
            if (MathF.Abs(currentActionWidth - prevActionWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _configService.Current.ScannedFilesActionColWidth = currentActionWidth;
                _configService.Save();
                _logger.LogDebug($"Saved action column width: {currentActionWidth}px");
            }

            ImGui.EndTable();
        }
        ImGui.EndChild();

        ImGui.Separator();

        // Totals footer
        {
            bool showFiles = _configService.Current.ShowModFilesInOverview;
            int visibleModsWithTextures = 0;
            foreach (var m in mods)
            {
                if (visibleByMod.TryGetValue(m, out var files) && files != null && files.Count > 0)
                    visibleModsWithTextures++;
            }
            var sig = string.Concat(showFiles ? "1" : "0", "|", visibleModsWithTextures.ToString(), "|", _perModSavingsRevision.ToString(), "|", _scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0");

            if (_footerTotalsDirty || !string.Equals(sig, _footerTotalsSignature, StringComparison.Ordinal))
            {
                long totalUncompressedCalc = 0;
                long totalCompressedCalc = 0;
                long savedBytesCalc = 0;
                foreach (var m in mods)
                {
                    if (!visibleByMod.TryGetValue(m, out var files) || files == null || files.Count == 0)
                        continue;

                    long modOrig = 0;
                    long modCur = 0;

                    if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null)
                    {
                        if (stats.OriginalBytes > 0) modOrig = stats.OriginalBytes;
                        if (stats.CurrentBytes > 0) modCur = stats.CurrentBytes;
                    }
                    if (modOrig <= 0)
                    {
                        var snap = _modStateService.Snapshot();
                        if (snap.TryGetValue(m, out var st) && st != null && st.ComparedFiles > 0)
                            modOrig = st.OriginalBytes;
                        else
                            modOrig = GetOrQueryModOriginalTotal(m);
                    }

                    if (modOrig > 0) totalUncompressedCalc += modOrig;
                    if (modCur > 0)
                    {
                        totalCompressedCalc += modCur;
                        if (modOrig > 0) savedBytesCalc += Math.Max(0, modOrig - modCur);
                    }
                }
                _footerTotalUncompressed = totalUncompressedCalc;
                _footerTotalCompressed = totalCompressedCalc;
                _footerTotalSaved = savedBytesCalc;
                _footerTotalsSignature = sig;
                _footerTotalsDirty = false;
            }

            long totalUncompressed = _footerTotalUncompressed;
            long totalCompressed = _footerTotalCompressed;
            long savedBytes = _footerTotalSaved;

            var footerFlags = ImGuiTableFlags.BordersOuter | ImGuiTableFlags.BordersV | ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingFixedFit;
            if (ImGui.BeginTable("ScannedFilesTotals", 5, footerFlags))
            {
                ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, _scannedFirstColWidth);
                ImGui.TableSetupColumn("Mod", ImGuiTableColumnFlags.WidthFixed, _scannedFileColWidth);
                ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed, _scannedCompressedColWidth);
                ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
                ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, _scannedActionColWidth);

                ImGui.TableNextRow();
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                ImGui.TableSetColumnIndex(1);
                var reduction = totalUncompressed > 0
                    ? MathF.Max(0f, (float)savedBytes / totalUncompressed * 100f)
                    : 0f;
                ImGui.TextUnformatted($"Total saved ({reduction.ToString("0.00")}%)");
                ImGui.TableSetColumnIndex(2);
                if (totalCompressed > 0)
                {
                    var color = (totalUncompressed > 0 && totalCompressed > totalUncompressed)
                        ? ShrinkUColors.WarningLight
                        : _compressedTextColor;
                    DrawRightAlignedSizeColored(totalCompressed, color);
                }
                else
                    DrawRightAlignedTextColored("-", _compressedTextColor);
                ImGui.TableSetColumnIndex(3);
                if (totalUncompressed > 0)
                    DrawRightAlignedSize(totalUncompressed);
                else
                    ImGui.TextUnformatted("");
                ImGui.TableSetColumnIndex(4);
                ImGui.TextUnformatted("");

                ImGui.EndTable();
            }
        }

        // Global action buttons below the table
        ImGui.Spacing();
        var (convertableMods, restorableMods) = GetSelectedModStates();
        bool hasConvertableMods = convertableMods > 0;
        bool hasRestorableMods = restorableMods > 0;
        bool hasOnlyRestorableMods = hasRestorableMods && !hasConvertableMods;

                ImGui.BeginDisabled(ActionsDisabled() || hasOnlyRestorableMods || !hasConvertableMods);
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        if (ImGui.Button("Backup and Convert"))
        {
            _running = true;
            var toConvert = GetConvertableTextures();
            ResetBothProgress();
            var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
            var selectedEmptyMods = _selectedEmptyMods.Where(m => !GetOrQueryModBackup(m)).ToList();
            var backupTasks = new List<Task<bool>>();
            foreach (var m in selectedEmptyMods)
            {
                try { backupTasks.Add(_backupService.CreateFullModBackupAsync(m, progress, CancellationToken.None)); } catch { }
            }
            if (toConvert.Count == 0 && backupTasks.Count == 0)
            {
                SetStatus("Nothing selected to backup or convert.");
                _running = false;
                ResetBothProgress();
            }
            else
            {
                if (backupTasks.Count > 0)
                {
                    var modsQueue = selectedEmptyMods.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                    _ = Task.Run(async () =>
                    {
                        foreach (var m in modsQueue)
                        {
                            try { await _backupService.CreateFullModBackupAsync(m, progress, CancellationToken.None).ConfigureAwait(false); } catch { }
                            await Task.Yield();
                        }
                        _uiThreadActions.Enqueue(() => { TriggerMetricsRefresh(); if (toConvert.Count == 0) { _running = false; ResetBothProgress(); } });
                    });
                }
                if (toConvert.Count > 0)
                {
                    _ = _conversionService.StartConversionAsync(toConvert)
                        .ContinueWith(_ => { _running = false; }, TaskScheduler.Default);
                }
        }
        }
        ImGui.EndDisabled();
        ImGui.PopStyleColor(4);

        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (hasOnlyRestorableMods)
                ImGui.SetTooltip("Only mods with backups are selected. Use 'Restore Backups' instead.");
            else if (!hasConvertableMods)
                ImGui.SetTooltip("No selected textures available to convert.");
        }

        ImGui.SameLine();

        var restorableModsForAction = GetRestorableModsForCurrentSelection();
        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var restorableFiltered = automaticMode
            ? restorableModsForAction.Where(m =>
                !_scannedByMod.TryGetValue(m, out var files) || !files.Any(f => _penumbraUsedFiles.Contains(f)))
                .ToList()
            : restorableModsForAction;
        bool canRestore = restorableFiltered.Count > 0;
        bool someSkippedByAuto = automaticMode && restorableFiltered.Count < restorableModsForAction.Count;

                ImGui.BeginDisabled(ActionsDisabled() || !canRestore);
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        if (ImGui.Button("Restore Backups"))
        {
            _running = true;
            // Reset progress state before bulk restore
            ResetBothProgress();
            var progress = new Progress<(string, int, int)>(e =>
            {
                _currentTexture = e.Item1;
                _backupIndex = e.Item2;
                _backupTotal = e.Item3;
                _currentRestoreModIndex = e.Item2;
                _currentRestoreModTotal = e.Item3;
            });
            _restoreCancellationTokenSource?.Dispose();
            _restoreCancellationTokenSource = new CancellationTokenSource();
            var restoreToken = _restoreCancellationTokenSource.Token;

            _ = Task.Run(async () =>
            {
                foreach (var mod in restorableFiltered)
                {
                    try
                    {
                        _currentRestoreMod = mod;
                        _currentRestoreModIndex = 0;
                        _currentRestoreModTotal = 0;
                        var preferPmp = _configService.Current.PreferPmpRestoreWhenAvailable;
                        var hasPmp = false;
                        try { hasPmp = _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                        if (preferPmp && hasPmp)
                        {
                            var latestPmp = _backupService.GetPmpBackupsForModAsync(mod).GetAwaiter().GetResult().FirstOrDefault();
                            if (!string.IsNullOrEmpty(latestPmp))
                            {
                                await _backupService.RestorePmpAsync(mod, latestPmp, progress, restoreToken);
                            }
                            else
                            {
                                await _backupService.RestoreLatestForModAsync(mod, progress, restoreToken);
                            }
                        }
                        else
                        {
                            await _backupService.RestoreLatestForModAsync(mod, progress, restoreToken);
                        }
                    }
                    catch { }
                }
            }).ContinueWith(_ =>
            {
                try { _backupService.RedrawPlayer(); } catch { }
                _logger.LogDebug("Restore completed (bulk action)");
                foreach (var m in restorableFiltered)
                {
                    try { RefreshModState(m, "restore-bulk"); } catch { }
                }
                TriggerMetricsRefresh();
                // Recompute per-mod savings after bulk restore to refresh Uncompressed/Compressed sizes
                _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                _perModSavingsTask.ContinueWith(ps =>
                {
                                                    if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                                                    {
                                                        _cachedPerModSavings = ps.Result;
                                                        _perModSavingsRevision++;
                                                        _footerTotalsDirty = true;
                                                        _needsUIRefresh = true;
                                                    }
                }, TaskScheduler.Default);
                // Re-check backup availability for each restored mod and update cache
                foreach (var m in restorableFiltered)
                {
                    try { bool _r; _modsWithPmpCache.TryRemove(m, out _r); } catch { }
                    try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(m, out _rm); } catch { }
                    _ = _backupService.HasBackupForModAsync(m).ContinueWith(bt =>
                    {
                        if (bt.Status == TaskStatus.RanToCompletion)
                        {
                            bool any = bt.Result;
                            try { any = any || _backupService.HasPmpBackupForModAsync(m).GetAwaiter().GetResult(); } catch { }
                            _modsWithBackupCache[m] = any;
                            // Reprime PMP cache with current status
                            try { var hasPmpNow = _backupService.HasPmpBackupForModAsync(m).GetAwaiter().GetResult(); _modsWithPmpCache[m] = hasPmpNow; } catch { }
                        }
                    });
                }
                // Clear persistent external conversion markers for all restored mods
                try
                {
                    int removed = 0;
                    foreach (var m in restorableFiltered)
                    {
                        if (_configService.Current.ExternalConvertedMods.Remove(m))
                            removed++;
                    }
                    if (removed > 0)
                        _configService.Save();
                }
                catch { }
                _running = false;
                _restoreCancellationTokenSource?.Dispose();
                _restoreCancellationTokenSource = null;
                _currentRestoreMod = string.Empty;
                _currentRestoreModIndex = 0;
                _currentRestoreModTotal = 0;
            });
        }
        ImGui.PopStyleColor(4);
        ImGui.EndDisabled();

        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (!canRestore)
                ImGui.SetTooltip("No selected mods have backups to restore.");
            else if (someSkippedByAuto)
                ImGui.SetTooltip("Automatic mode: skipping mods with currently used textures.");
        }
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
            _fileSizeWarmupTask = Task.Run(() =>
            {
                foreach (var f in allFiles)
                {
                    var size = GetFileSizeSafe(f);
                    _fileSizeCache[f] = size;
                }
            });
        }
        catch
        {
            // Swallow exceptions; caching is best-effort
        }
    }

    private (string version, string author, DateTime createdUtc, string pmpFileName)? GetOrQueryModPmpMeta(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod)) return null;
            if (_modsPmpMetaCache.TryGetValue(mod, out var v)) return v;
            var meta = _backupService.GetLatestPmpManifestForModAsync(mod).GetAwaiter().GetResult();
            if (meta.HasValue)
            {
                _modsPmpMetaCache[mod] = meta.Value;
                return meta.Value;
            }
            return null;
        }
        catch { return null; }
    }

    private (string version, string author, DateTime createdUtc, string zipFileName)? GetOrQueryModZipMeta(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod)) return null;
            if (_modsZipMetaCache.TryGetValue(mod, out var v)) return v;
            var meta = _backupService.GetLatestZipMetaForModAsync(mod).GetAwaiter().GetResult();
            if (meta.HasValue)
            {
                _modsZipMetaCache[mod] = meta.Value;
                return meta.Value;
            }
            return null;
        }
        catch { return null; }
    }

    // Get size from cache, computing once if missing
    private long GetCachedOrComputeSize(string path)
    {
        if (_fileSizeCache.TryGetValue(path, out var size))
            return size;
        size = GetFileSizeSafe(path);
        _fileSizeCache[path] = size;
        return size;
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

    // Removed mod-level size aggregation to focus only on file sizes per request

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
        try
        {
            var snap = _modStateService.Snapshot();
            if (snap.TryGetValue(mod, out var e))
                return e.HasTextureBackup || e.HasPmpBackup;
        }
        catch { }
        return false;
    }

    private bool GetOrQueryModTextureBackup(string mod)
    {
        try
        {
            var snap = _modStateService.Snapshot();
            if (snap.TryGetValue(mod, out var e))
                return e.HasTextureBackup;
        }
        catch { }
        return false;
    }

    // Query whether a mod has PMP backup, caching the result
    private bool GetOrQueryModPmp(string mod)
    {
        try
        {
            var snap = _modStateService.Snapshot();
            if (snap.TryGetValue(mod, out var e))
                return e.HasPmpBackup;
        }
        catch { }
        return false;
    }

    private void DrawSplitter(float totalWidth, ref float leftWidth)
    {
        var height = ImGui.GetContentRegionAvail().Y;
        ImGui.InvisibleButton("##splitter", new Vector2(4f, height));
        if (ImGui.IsItemActive())
        {
            var delta = ImGui.GetIO().MouseDelta.X;
            leftWidth = Math.Max(360f, Math.Min(totalWidth - 360f, leftWidth + delta));
            _leftPanelWidthPx = leftWidth;
            // Keep ratio for backwards compatibility, but drive layout by absolute px
            _leftPanelWidthRatio = Math.Clamp(leftWidth / totalWidth, 0.25f, 0.75f);
            _leftWidthDirty = true;
        }
        else if (_leftWidthDirty)
        {
            // Persist to config on drag release
            _configService.Current.LeftPanelWidthPx = _leftPanelWidthPx;
            _configService.Save();
            _leftWidthDirty = false;
        }
        // Visual handle
        var drawList = ImGui.GetWindowDrawList();
        var min = ImGui.GetItemRectMin();
        var max = ImGui.GetItemRectMax();
        drawList.AddRectFilled(min, max, ImGui.GetColorU32(new Vector4(0.3f, 0.3f, 0.3f, 0.6f)));
        ImGui.SameLine();
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
        // Always work with selected textures only
        if (_selectedTextures.Count > 0)
        {
            foreach (var texture in _selectedTextures)
            {
                string? ownerMod = null;
                foreach (var (mod, files) in _scannedByMod)
                {
                    if (files.Contains(texture))
                    {
                        ownerMod = mod;
                        break;
                    }
                }
                if (ownerMod != null && GetOrQueryModBackup(ownerMod))
                    result.Add(ownerMod);
            }
        }
        return result.ToList();
    }

    private (int convertableMods, int restorableMods) GetSelectedModStates()
    {
        // Always check only selected textures
        var selectedMods = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var texture in _selectedTextures)
        {
            foreach (var (mod, files) in _scannedByMod)
            {
                if (files.Contains(texture)) { selectedMods.Add(mod); break; }
            }
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
            string? ownerMod = null;
            foreach (var (mod, files) in _scannedByMod)
            {
                if (files.Contains(texture))
                {
                    ownerMod = mod;
                    break;
                }
            }

            // Only include if the mod doesn't have a backup (is convertable) and not excluded by tags
            if (ownerMod != null && !GetOrQueryModBackup(ownerMod) && !IsModExcludedByTags(ownerMod))
            {
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
        ImGui.TableSetColumnIndex(0);
        var fullPath = row.FolderPath;
        var child = row.Node;
        var folderFiles = CollectFilesRecursive(child, visibleByMod);
        bool folderSelected = (folderFiles.Count > 0 && folderFiles.All(f => _selectedTextures.Contains(f)))
            || (folderFiles.Count == 0 && child.Mods.Count > 0 && child.Mods.All(m => _selectedEmptyMods.Contains(m)));
        ImGui.BeginDisabled(folderFiles.Count == 0 && child.Mods.Count == 0);
        if (ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
        {
            if (folderSelected)
            {
                var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.TryGetValue(mod, out var files))
                        continue;
                    var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                    var hasBackup = GetOrQueryModBackup(mod);
                    var excluded = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
                    var convertedAll = 0;
                    var totalAll = files.Count;
                    var disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
                    if (!disableCheckbox)
                    {
                        foreach (var f in files)
                            _selectedTextures.Add(f);
                    }
                    else if (!isOrphan)
                    {
                        _selectedEmptyMods.Add(mod);
                    }
                }
            }
            else
            {
                foreach (var f in folderFiles) _selectedTextures.Remove(f);
                foreach (var mod in child.Mods) _selectedEmptyMods.Remove(mod);
            }
        }
        ImGui.EndDisabled();
        if (folderFiles.Count == 0 && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip("No selectable files in this folder (filtered or excluded).");
        else
            ShowTooltip("Select or deselect all files in this folder.");

        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var catDefaultOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
        ImGui.SetNextItemOpen(catDefaultOpen, ImGuiCond.Always);
        var catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
        var catToggled = ImGui.IsItemToggledOpen();
        if (catToggled)
        {
            if (catOpen) _expandedFolders.Add(fullPath);
            else _expandedFolders.Remove(fullPath);
        }
        ImGui.SameLine();
        var folderColor = new Vector4(0.70f, 0.80f, 1.00f, 1f);
        ImGui.PushFont(UiBuilder.IconFont);
        ImGui.TextColored(folderColor, (catOpen ? FontAwesomeIcon.FolderOpen : FontAwesomeIcon.Folder).ToIconString());
        ImGui.PopFont();
        ImGui.SameLine();
        if (!_folderCountsCache.TryGetValue(fullPath, out var cvals))
            cvals = (0, 0, 0, 0);
        ImGui.TextColored(folderColor, $"{child.Name} (mods {cvals.modsConverted}/{cvals.modsTotal}, textures {cvals.texturesConverted}/{cvals.texturesTotal})");

        ImGui.TableSetColumnIndex(3);
        if (!string.Equals(_folderSizeCacheSig, _flatRowsSig, StringComparison.Ordinal))
        {
            _folderSizeCache.Clear();
            _folderSizeCacheSig = _flatRowsSig;
        }
        if (!_folderSizeCache.TryGetValue(fullPath, out var cached))
        {
            long orig = 0;
            long comp = 0;
            foreach (var m in child.Mods)
            {
                var modOrig = GetOrQueryModOriginalTotal(m);
                if (modOrig > 0)
                    orig += modOrig;
                var hasBackupM = GetOrQueryModBackup(m);
                if (!hasBackupM) continue;
                if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null && stats.CurrentBytes > 0)
                    comp += stats.CurrentBytes;
            }
            cached = (orig, comp);
            _folderSizeCache[fullPath] = cached;
        }
        if (cached.orig > 0)
            DrawRightAlignedSize(cached.orig);
        else
            ImGui.TextUnformatted("");

        ImGui.TableSetColumnIndex(2);
        if (cached.comp > 0)
        {
            var color = cached.comp > cached.orig ? ShrinkUColors.WarningLight : _compressedTextColor;
            DrawRightAlignedSizeColored(cached.comp, color);
        }
        else
        {
            DrawRightAlignedTextColored("-", _compressedTextColor);
        }
    }

    private void DrawModFlatRow(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        var mod = row.Mod;
        var files = visibleByMod.TryGetValue(mod, out var list) ? list : new List<string>();
        var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
        var hasBackup = GetOrQueryModBackup(mod);
        var excluded = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
        var isNonConvertible = files.Count == 0;

        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var nodeFlags = ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.FramePadding;
        if (!_configService.Current.ShowModFilesInOverview)
            nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
        int totalAll = files.Count;
        int convertedAll = 0;
        if (hasBackup && totalAll > 0)
        {
            if (_cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
            else
            {
                var snap = _modStateService.Snapshot();
                if (snap.TryGetValue(mod, out var ms) && ms != null && ms.ComparedFiles > 0)
                    convertedAll = Math.Min(ms.ComparedFiles, totalAll);
            }
        }
        var header = hasBackup ? $"{ResolveModDisplayName(mod)} ({convertedAll}/{totalAll})" : $"{ResolveModDisplayName(mod)} ({totalAll})";
        var open = _expandedMods.Contains(mod);
        ImGui.SetNextItemOpen(open, ImGuiCond.Always);
        open = ImGui.TreeNodeEx($"##mod-{mod}", nodeFlags);
        var headerHoveredTree = ImGui.IsItemHovered();
        if (ImGui.BeginPopupContextItem($"modctx-{mod}"))
        {
            if (ImGui.MenuItem("Open in Penumbra"))
            {
                try { _conversionService.OpenModInPenumbra(mod, null); } catch { }
            }
            var hasTexBk = GetOrQueryModTextureBackup(mod);
            ImGui.BeginDisabled(!hasTexBk);
            if (ImGui.MenuItem("Restore textures"))
            {
                _running = true;
                _modsWithBackupCache.TryRemove(mod, out _);
                ResetBothProgress();
                _currentRestoreMod = mod;
                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                    .ContinueWith(t => {
                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                        try { _backupService.RedrawPlayer(); } catch { }
                        RefreshModState(mod, "restore-flat-context");
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
                        _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                        {
                            if (bt.Status == TaskStatus.RanToCompletion)
                            {
                                bool any = bt.Result;
                                try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                _modsWithBackupCache[mod] = any;
                            }
                        });
                        _running = false;
                        ResetRestoreProgress();
                        ImGui.CloseCurrentPopup();
                    });
            }
            ImGui.EndDisabled();
            var hasPmp = GetOrQueryModPmp(mod);
            ImGui.BeginDisabled(!hasPmp);
            if (ImGui.MenuItem("Restore PMP"))
            {
                TryStartPmpRestoreNewest(mod, "pmp-restore-flat-context-newest", false, true, false, false, true);
            }
            ImGui.EndDisabled();
            if (ImGui.MenuItem("Open Backup Folder"))
            {
                try
                {
                    var path = _configService.Current.BackupFolderPath;
                    if (!string.IsNullOrWhiteSpace(path))
                    {
                        var modDir = Path.Combine(path, mod);
                        try { Directory.CreateDirectory(modDir); } catch { }
                        try { Process.Start(new ProcessStartInfo("explorer.exe", modDir) { UseShellExecute = true }); } catch { }
                    }
                }
                catch { }
            }
            ImGui.EndPopup();
        }
        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.PushFont(UiBuilder.IconFont);
        if (_modEnabledStates.TryGetValue(mod, out var stIcon))
        {
            var iconColor = stIcon.Enabled ? new Vector4(0.40f, 0.85f, 0.40f, 1f) : new Vector4(1f, 1f, 1f, 1f);
            ImGui.TextColored(iconColor, FontAwesomeIcon.Cube.ToIconString());
        }
        else
        {
            ImGui.TextUnformatted(FontAwesomeIcon.Cube.ToIconString());
        }
        ImGui.PopFont();
        ImGui.SameLine();
        ImGui.TextUnformatted(header);

        ImGui.TableSetColumnIndex(0);
        bool modSelected = isNonConvertible ? _selectedEmptyMods.Contains(mod) : files.All(f => _selectedTextures.Contains(f));
        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
        ImGui.BeginDisabled(disableCheckbox);
        if (ImGui.Checkbox($"##modsel-{mod}", ref modSelected))
        {
            if (modSelected)
            {
                foreach (var f in files) _selectedTextures.Add(f);
                if (isNonConvertible && !isOrphan) _selectedEmptyMods.Add(mod);
            }
            else
            {
                foreach (var f in files) _selectedTextures.Remove(f);
                _selectedEmptyMods.Remove(mod);
            }
        }
        ImGui.EndDisabled();
        if (disableCheckbox && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip(automaticMode ? "Automatic mode: mod cannot be selected for conversion." : "Mod excluded by tags");
        else
            ShowTooltip("Toggle selection for all files in this mod.");
        if (headerHoveredTree)
        {
            bool hasPmp = GetOrQueryModPmp(mod);
            long origBytesTip = GetOrQueryModOriginalTotal(mod);
            long compBytesTip = 0;
            if (!isOrphan && _cachedPerModSavings.TryGetValue(mod, out var tipStats) && tipStats != null)
                compBytesTip = Math.Max(0, tipStats.CurrentBytes);

            float reductionPctTip = 0f;
            if (origBytesTip > 0 && compBytesTip > 0)
                reductionPctTip = MathF.Max(0f, (float)(origBytesTip - compBytesTip) / origBytesTip * 100f);

            ImGui.BeginTooltip();
            ImGui.TextUnformatted($"{ResolveModDisplayName(mod)}");
            ImGui.Separator();
            ImGui.TextUnformatted($"Textures: {(hasBackup ? convertedAll : 0)}/{totalAll} converted");
            ImGui.TextUnformatted($"Uncompressed: {FormatSize(origBytesTip)}");
            var compText = compBytesTip > 0 ? $"{FormatSize(compBytesTip)} ({reductionPctTip:0.00}% reduction)" : "-";
            ImGui.TextUnformatted($"Compressed: {compText}");
            var texturesEnabled = _configService.Current.EnableBackupBeforeConversion;
            var pmpEnabled = _configService.Current.EnableFullModBackupBeforeConversion;
            string backupText = (hasBackup && hasPmp && texturesEnabled && pmpEnabled) ? "Tex, PMP" : (hasPmp ? "PMP" : (hasBackup ? "Textures" : "None"));
            if (hasPmp)
            {
                var meta = GetOrQueryModPmpMeta(mod);
                if (meta.HasValue)
                {
                    var (v, a, created, fileName) = meta.Value;
                    ImGui.TextUnformatted($"Backups: {backupText}");
                    if (!string.IsNullOrWhiteSpace(v)) ImGui.TextUnformatted($"Version: {v}");
                    if (!string.IsNullOrWhiteSpace(a)) ImGui.TextUnformatted($"Author: {a}");
                }
                else
                {
                    ImGui.TextUnformatted($"Backups: {backupText}");
                }
            }
            else
            {
                ImGui.TextUnformatted($"Backups: {backupText}");
                if (hasBackup)
                {
                    var zmeta = GetOrQueryModZipMeta(mod);
                    if (zmeta.HasValue)
                    {
                        var (v2, a2, created2, fileName2) = zmeta.Value;
                        if (!string.IsNullOrWhiteSpace(v2)) ImGui.TextUnformatted($"Version: {v2}");
                        if (!string.IsNullOrWhiteSpace(a2)) ImGui.TextUnformatted($"Author: {a2}");
                    }
                    else
                    {
                        var live = _backupService.GetLiveModMeta(mod);
                        if (!string.IsNullOrWhiteSpace(live.version)) ImGui.TextUnformatted($"Version: {live.version}");
                        if (!string.IsNullOrWhiteSpace(live.author)) ImGui.TextUnformatted($"Author: {live.author}");
                    }
                }
            }
            var enabledState = _modEnabledStates.TryGetValue(mod, out var st) ? (st.Enabled ? "Enabled" : "Disabled") : "Disabled";
            ImGui.TextUnformatted($"State: {enabledState}");
            ImGui.EndTooltip();
        }
        var hasPersistent = _configService.Current.ExternalConvertedMods.ContainsKey(mod);
        var showExternal = ((DateTime.UtcNow - _lastExternalChangeAt).TotalSeconds < 30 && !string.IsNullOrEmpty(_lastExternalChangeReason)) || hasPersistent;
        if (showExternal)
        {
            ImGui.SameLine();
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(new Vector4(0.70f, 0.85f, 1.00f, 1f), FontAwesomeIcon.Plug.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
            {
                string reason = _lastExternalChangeReason;
                if (hasPersistent)
                {
                    try
                    {
                        if (_configService.Current.ExternalConvertedMods.TryGetValue(mod, out var marker) && marker != null)
                            reason = marker.Reason ?? reason;
                    }
                    catch { }
                }
                var tip = !string.IsNullOrWhiteSpace(reason) && reason.Equals("ipc-auto-conversion-complete", StringComparison.OrdinalIgnoreCase)
                    ? "Automatic via Sphene"
                    : string.IsNullOrWhiteSpace(reason) ? "External conversion detected" : "External conversion detected: " + reason;
                ImGui.SetTooltip(tip);
            }
        }
        ImGui.SameLine();
        if (IsModInefficient(mod))
        {
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("This mod becomes larger after conversion");
            ImGui.SameLine();
        }
        if (hasBackup && _cachedPerModSavings.TryGetValue(mod, out var noteStats1) && noteStats1 != null && noteStats1.OriginalBytes > 0 && noteStats1.CurrentBytes > noteStats1.OriginalBytes)
        {
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("This mod is smaller when not converted");
            ImGui.SameLine();
        }

        ImGui.TableSetColumnIndex(3);
        _cachedPerModSavings.TryGetValue(mod, out var modStats);
        var stateSnap = _modStateService.Snapshot();
        stateSnap.TryGetValue(mod, out var modState);
        long modOriginalBytes = modState != null && modState.ComparedFiles > 0 ? modState.OriginalBytes : GetOrQueryModOriginalTotal(mod);
        var hideStatsForNoTextures = totalAll == 0;
        if (hideStatsForNoTextures)
            ImGui.TextUnformatted("");
        else
            DrawRightAlignedSize(modOriginalBytes);

        ImGui.TableSetColumnIndex(2);
        long modCurrentBytes = 0;
        var hideCompressed = modState != null && modState.InstalledButNotConverted;
        if (!isOrphan && hasBackup && modState != null && modState.CurrentBytes > 0 && !hideCompressed)
            modCurrentBytes = modState.CurrentBytes;
        if (hideStatsForNoTextures)
            ImGui.TextUnformatted("");
        else if (modCurrentBytes <= 0)
            DrawRightAlignedTextColored("-", _compressedTextColor);
        else
        {
            var color = modCurrentBytes > modOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
            DrawRightAlignedSizeColored(modCurrentBytes, color);
        }

        ImGui.TableSetColumnIndex(4);
        ImGui.BeginDisabled(ActionsDisabled());
        if (hasBackup || files.Count > 0 || isOrphan)
        {
            var hasTexBackup = GetOrQueryModTextureBackup(mod);
            var hasPmpBackup = GetOrQueryModPmp(mod);
            bool doInstall = isOrphan;
            bool doRestore = false;
            bool doReinstall = false;
            bool doConvert = false;
            var anyBackup = hasBackup || hasTexBackup || hasPmpBackup;
            if (doInstall)
            {
                doRestore = false;
                doConvert = false;
            }
            else if (isNonConvertible)
            {
                doConvert = false;
                if (hasPmpBackup && anyBackup)
                {
                    doReinstall = true;
                    doRestore = false;
                }
                else
                {
                    doRestore = anyBackup;
                }
            }
            else
            {
                doRestore = anyBackup;
                doConvert = !anyBackup;
                if (modState != null && modState.InstalledButNotConverted)
                {
                    doConvert = true;
                    doRestore = false;
                }
            }

            var actionLabel = doInstall
                ? $"Install##install-{mod}"
                : (doReinstall ? $"Reinstall##reinstall-{mod}" : (doRestore ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}"));

            if (doInstall)
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
            }
            else if (doRestore)
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
            }
            else if (doReinstall)
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ReinstallButton);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ReinstallButtonHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ReinstallButtonActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
            }
            else
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ConvertButton);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ConvertButtonHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ConvertButtonActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
            }

            var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
            bool canInstall = true;
            if (doInstall)
            {
                try
                {
                    var info = _orphaned.FirstOrDefault(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                    canInstall = info != null && !string.IsNullOrWhiteSpace(info.LatestPmpPath);
                }
                catch { canInstall = false; }
            }
            var restoreDisabledByAuto = autoMode && (doRestore || doReinstall);
            ImGui.BeginDisabled(excluded || (!isOrphan && restoreDisabledByAuto) || (isOrphan && !canInstall) || ActionsDisabled());
            if (ImGui.Button(actionLabel, new Vector2(60, 0)))
            {
                if (doInstall)
                {
                    _running = true;
                    ResetBothProgress();
                    var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                    _ = _backupService.ReinstallModFromLatestPmpAsync(mod, progress, CancellationToken.None)
                        .ContinueWith(t =>
                        {
                            var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                            _uiThreadActions.Enqueue(() =>
                            {
                                SetStatus(success ? $"Install completed for {mod}" : $"Install failed for {mod}");
                                _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                _needsUIRefresh = true;
                                ClearModCaches(mod);
                            });
                            try { _conversionService.OpenModInPenumbra(mod, null); } catch { }
                            try
                            {
                                var am = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                                if (am)
                                    _ = _conversionService.StartAutomaticConversionForModWithDelayAsync(mod, 2000);
                            }
                            catch { }
                            try { _configService.Current.ExternalConvertedMods.Remove(mod); _configService.Save(); } catch { }
                            try { RefreshModState(mod, "orphan-install"); } catch { }
                            try { TriggerMetricsRefresh(); } catch { }
                            _running = false;
                            _needsUIRefresh = true;
                        }, TaskScheduler.Default);
                }
                else if (doReinstall)
                {
                    _running = true;
                    ResetBothProgress();
                    _reinstallInProgress = true;
                    _reinstallMod = mod;
                    var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                    _ = _backupService.ReinstallModFromLatestPmpAsync(mod, progress, CancellationToken.None)
                        .ContinueWith(t =>
                        {
                            var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                            _uiThreadActions.Enqueue(() =>
                            {
                                SetStatus(success ? $"Reinstall completed for {mod}" : $"Reinstall failed for {mod}");
                                _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                _needsUIRefresh = true;
                                ClearModCaches(mod);
                                RefreshModState(mod, "reinstall-completed");
                                Task.Run(async () =>
                                {
                                    try
                                    {
                                        await Task.Delay(1200).ConfigureAwait(false);
                                        _uiThreadActions.Enqueue(() => { RefreshScanResults(true, "reinstall-completed"); });
                                    }
                                    catch { }
                                });
                            });
                            Task.Run(async () =>
                            {
                                try
                                {
                                    await Task.Delay(1200).ConfigureAwait(false);
                                    _conversionService.OpenModInPenumbra(mod, null);
                                }
                                catch { }
                            });
                            try { RefreshModState(mod, "reinstall-folder-view"); } catch { }
                            try { TriggerMetricsRefresh(); } catch { }
                            _reinstallInProgress = false;
                            _reinstallMod = string.Empty;
                            ResetBothProgress();
                            _running = false;
                        }, TaskScheduler.Default);
                }
                else if (doRestore)
                {
                    var hasPmpForClick = GetOrQueryModPmp(mod);
                    if (hasPmpForClick && _configService.Current.PreferPmpRestoreWhenAvailable)
                    {
                        TryStartPmpRestoreNewest(mod, "restore-pmp-flat-view", true, false, false, true, false);
                    }
                    else if (hasPmpForClick)
                    {
                        ImGui.OpenPopup($"restorectx-{mod}");
                    }
                    else
                    {
                        _running = true;
                        _modsWithBackupCache.TryRemove(mod, out _);
                        ResetBothProgress();
                        _currentRestoreMod = mod;
                        var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                        _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                            .ContinueWith(t =>
                            {
                                var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                try { _backupService.RedrawPlayer(); } catch { }
                                RefreshModState(mod, "restore-flat-view");
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
                                _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                {
                                    if (bt.Status == TaskStatus.RanToCompletion)
                                    {
                                        bool any = bt.Result;
                                        try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                        _modsWithBackupCache[mod] = any;
                                    }
                                });
                                _running = false;
                                ResetRestoreProgress();
                            });
                    }
                }
                else
                {
                    var allFilesForMod = files;
                    try
                    {
                        if (_configService.Current.IncludeHiddenModTexturesOnConvert && _scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
                            allFilesForMod = all;
                    }
                    catch { }
                    var toConvert = BuildToConvert(allFilesForMod);
                    ResetConversionProgress();
                    ResetRestoreProgress();
                    _ = _conversionService.StartConversionAsync(toConvert)
                        .ContinueWith(_ =>
                        {
                            _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                            {
                                if (bt.Status == TaskStatus.RanToCompletion)
                                {
                                    bool any = bt.Result;
                                    try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                    _modsWithBackupCache[mod] = any;
                                }
                            });
                            _running = false;
                        });
                }
            }
            if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            {
                if (excluded)
                    ImGui.SetTooltip("Mod excluded by tags");
                else if (restoreDisabledByAuto)
                    ImGui.SetTooltip("Automatic mode active: restoring is disabled.");
                else
                {
                    if (doInstall)
                        ShowTooltip("Install mod from PMP backup.");
                    else if (doReinstall)
                        ShowTooltip("Reinstall mod from PMP backup.");
                    else if (doRestore)
                        ShowTooltip("Restore backups for this mod.");
                    else
                    {
                        var msg = _configService.Current.IncludeHiddenModTexturesOnConvert
                            ? "Convert all textures for this mod."
                            : "Convert all visible textures for this mod.";
                        ShowTooltip(msg);
                    }
                }
            }
            ImGui.PopStyleColor(4);
            ImGui.EndDisabled();
        }
        ImGui.EndDisabled();
    }

    private void DrawFileFlatRow(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        var mod = row.Mod;
        var file = row.File;
        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var baseName = Path.GetFileName(file);
        ImGui.TextUnformatted(baseName);
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip(file);

        ImGui.TableSetColumnIndex(2);
        var hasBackupForMod = GetOrQueryModBackup(mod);
        var fileSize = GetCachedOrComputeSize(file);
        if (!hasBackupForMod)
        {
            DrawRightAlignedTextColored("-", _compressedTextColor);
        }
        else
        {
            if (fileSize > 0)
                DrawRightAlignedSizeColored(fileSize, _compressedTextColor);
            else
                ImGui.TextUnformatted("");
        }

        ImGui.TableSetColumnIndex(3);
        if (!hasBackupForMod)
        {
            if (fileSize > 0)
                DrawRightAlignedSize(fileSize);
            else
                ImGui.TextUnformatted("");
        }
        else
        {
            long originalSize = 0;
            if (_modPaths.TryGetValue(mod, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
            {
                try
                {
                    var rel = Path.GetRelativePath(modRoot, file).Replace('\\', '/');
                    if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null)
                    {
                        map.TryGetValue(rel, out originalSize);
                    }
                }
                catch { }
            }
            if (originalSize > 0)
                DrawRightAlignedSize(originalSize);
            else
                ImGui.TextUnformatted("");
        }

        ImGui.TableSetColumnIndex(4);
    }
}