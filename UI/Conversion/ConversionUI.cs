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
    private float _leftPanelWidthPx = 0f;
    private bool _leftWidthInitialized = false;
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
    private Dictionary<string, string> _modPathsStable = new(StringComparer.OrdinalIgnoreCase);
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
    private DateTime _bulkStartedAt = DateTime.MinValue;
    private readonly HashSet<string> _bulkBackedUpMods = new(StringComparer.OrdinalIgnoreCase);
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
    private readonly ConcurrentDictionary<string, DateTime> _lastModRefreshAt = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _modsTouchedLastRun = new(StringComparer.OrdinalIgnoreCase);
    private DateTime _disableActionsUntilUtc = DateTime.MinValue;
    private readonly Dictionary<string, string> _fileOwnerMod = new(StringComparer.OrdinalIgnoreCase);

    private struct ModCapabilities
    {
        public bool CanConvert;
        public bool CanBackup;
        public bool CanRestore;
        public bool CanReinstall;
        public bool HasAnyBackup;
        public bool Excluded;
        public int TotalTextures;
        public int VisibleFileCount;
    }

    private ModCapabilities EvaluateModCapabilities(string mod)
    {
        var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
        var snap = _modStateSnapshot ?? _modStateService.Snapshot();
        int totalTextures = GetTotalTexturesForMod(mod, _visibleByMod.TryGetValue(mod, out var vis) ? vis : null);
        var hasTexBackup = GetOrQueryModTextureBackup(mod);
        var hasPmpBackup = GetOrQueryModPmp(mod);
        var hasModBackup = GetOrQueryModBackup(mod);
        var hasAnyBackup = hasTexBackup || hasPmpBackup || hasModBackup;
        var excluded = !hasModBackup && !isOrphan && IsModExcludedByTags(mod);
        var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var canConvert = totalTextures > 0 && !hasAnyBackup && !excluded;
        var canBackup = totalTextures == 0 && !hasAnyBackup;
        var canRestore = hasAnyBackup;
        if (autoMode && !isOrphan)
        {
            if (snap.TryGetValue(mod, out var ams) && ams != null && ams.UsedTextureFiles != null && ams.UsedTextureFiles.Count > 0)
                canRestore = false;
        }
        var canReinstall = isOrphan && hasPmpBackup;
        int visibleCount = _visibleByMod.TryGetValue(mod, out var vlist) && vlist != null ? vlist.Count : 0;
        return new ModCapabilities
        {
            CanConvert = canConvert,
            CanBackup = canBackup,
            CanRestore = canRestore,
            CanReinstall = canReinstall,
            HasAnyBackup = hasAnyBackup,
            Excluded = excluded,
            TotalTextures = totalTextures,
            VisibleFileCount = visibleCount,
        };
    }
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
        RequestUiRefresh("clear-all-caches");

        try
        {
            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
            _perModSavingsTask.ContinueWith(ps =>
            {
                if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                {
                    _cachedPerModSavings = ps.Result;
                    RequestUiRefresh("per-mod-savings-updated-clear-all-caches");
                }
            }, TaskScheduler.Default);
        }
        catch { }
    }

    

    private void OnModeChanged()
    {
        _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(1);
        RequestUiRefresh("mode-changed");
        _footerTotalsDirty = true;
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
    private readonly DebugTraceService? _debugTrace;
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
    private int _orphanRevision = 0;
    private int _currentRestoreModTotal = 0;
    private int _restoreModsTotal = 0;
    private int _restoreModsDone = 0;
    private bool _restoreAfterCancel = false;
    private string _cancelTargetMod = string.Empty;
    private string _statusMessage = string.Empty;
    private DateTime _statusMessageAt = DateTime.MinValue;
    private bool _statusPersistent = false;
    
    // UI refresh flag to force ImGui redraw after async data updates
    private volatile bool _needsUIRefresh = false;
    private volatile bool _startupRefreshInProgress = false;
    private int _startupTotalMods = 0;
    private int _startupProcessedMods = 0;
    private int _startupEtaSeconds = 0;
    private readonly System.Collections.Generic.List<string> _startupDependencyErrors = new System.Collections.Generic.List<string>();
    private DateTime _startupRefreshStartedAt = DateTime.MinValue;
    
    // Snapshot of known Penumbra mod folders to guard heavy scans
    private HashSet<string> _knownModFolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    // Debounce sequence to ensure trailing-edge execution for mod add/delete
    private int _modsChangedDebounceSeq = 0;
    // Timestamp of last heavy scan to rate-limit repeated rescans
    private DateTime _lastHeavyScanAt = DateTime.MinValue;
    
    private void RequestUiRefresh(string reason)
    {
        try
        {
            if (_configService.Current.DebugTraceUiRefresh)
            {
                _logger.LogDebug("UI refresh requested: {reason}", reason);
                _debugTrace?.AddUi($"refresh requested: {reason} thread={System.Environment.CurrentManagedThreadId}");
            }
        }
        catch { }
        _needsUIRefresh = true;
    }
    private void TraceAction(string action, string function, string path = "")
    {
        try
        {
            if (!_configService.Current.DebugTraceActions)
                return;
            var msg = string.IsNullOrWhiteSpace(path)
                ? $"action={action} func={function}"
                : $"action={action} func={function} path={path}";
            _debugTrace?.AddAction(msg);
        }
        catch { }
    }
    private void ReloadPenumbraUsedFiles(string reason)
    {
        try
        {
            if (!_filterPenumbraUsedOnly)
                return;
            if (_loadingPenumbraUsed)
                return;
            TraceAction(reason, nameof(ReloadPenumbraUsedFiles));
            _loadingPenumbraUsed = true;
            _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
            {
                try
                {
                    if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                    {
                        var result = t.Result;
                        bool changed = result.Count != _penumbraUsedFiles.Count
                            || result.Any(p => !_penumbraUsedFiles.Contains(p))
                            || _penumbraUsedFiles.Any(p => !result.Contains(p));
                        if (changed)
                        {
                            _penumbraUsedFiles = result;
                            if (_configService.Current.DebugTraceActions)
                            {
                                foreach (var p in result)
                                    TraceAction(reason, "used-file", p);
                            }
                            _uiThreadActions.Enqueue(() =>
                            {
                                try { RefreshScanResults(false, reason); } catch { }
                                _needsUIRefresh = true;
                            });
                        }
                    }
                }
                finally
                {
                    _loadingPenumbraUsed = false;
                }
            }, TaskScheduler.Default);
        }
        catch { }
    }
    private void SubscribeStartupProgress(ShrinkU.Services.TextureConversionService conversion)
    {
        try
        {
            conversion.OnStartupProgress += e =>
            {
                _startupProcessedMods = e.processed;
                _startupTotalMods = e.total;
                _startupEtaSeconds = e.etaSeconds;
                _needsUIRefresh = true;
            };
        }
        catch { }
    }
    // Poller to keep Used-Only list in sync even when no redraw occurs
    private CancellationTokenSource? _usedResourcesPollCts;
    private Task? _usedResourcesPollTask;
    private volatile bool _usedWatcherActive = false;
    private int _usedWatcherIntervalMs = 3000;
    private volatile bool _windowFocused = false;
    private CancellationTokenSource? _modStatePollCts;
    private Task? _modStatePollTask;
    private volatile bool _modStateWatcherActive = false;
    private int _modStateWatcherIntervalMs = 3000;

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
    private readonly Action<string> _onModStateEntryChanged;
    
    // Track last external conversion/restore notification to surface UI indicator
    private DateTime _lastExternalChangeAt = DateTime.MinValue;
    private string _lastExternalChangeReason = string.Empty;

public ConversionUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, TextureBackupService backupService, Action? openSettings = null, ModStateService? modStateService = null, ConversionCacheService? cacheService = null, DebugTraceService? debugTrace = null)
        : base("ShrinkU###ShrinkUConversionUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _backupService = backupService;
        _openSettings = openSettings;
        _modStateService = modStateService ?? new ModStateService(_logger, _configService);
        try { _modStateService.OnStateSaved += () => { _needsUIRefresh = true; }; }
        catch (Exception ex) { _logger.LogError(ex, "Subscribe OnStateSaved failed"); }
        _onModStateEntryChanged = m =>
        {
            try { RefreshModState(m, "mod-state-changed"); }
            catch (Exception ex) { _logger.LogError(ex, "RefreshModState failed for {mod}", m); }
            try { _uiThreadActions.Enqueue(() => { _modStateSnapshot = _modStateService.Snapshot(); _footerTotalsDirty = true; _needsUIRefresh = true; }); }
            catch (Exception ex) { _logger.LogError(ex, "Snapshot refresh failed for {mod}", m); }
        };
        try { _modStateService.OnEntryChanged += _onModStateEntryChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Subscribe OnEntryChanged failed"); }
        _cacheService = cacheService ?? new ConversionCacheService(_logger, _configService, _backupService, _modStateService);
        _debugTrace = debugTrace;

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
                            _bulkBackedUpMods.Add(mod);
                            RefreshModState(mod, "backup-progress-pmp");
                            _ = _backupService.ComputeSavingsForModAsync(mod).ContinueWith(ps =>
                            {
                                if (ps.Status == TaskStatus.RanToCompletion)
                                {
                                    try { _cachedPerModSavings[mod] = ps.Result; } catch { }
                                    _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                                }
                            }, TaskScheduler.Default);
                            // Warm original sizes cache for this mod from PMP so tooltips reflect immediately
                            _perModOriginalSizesTasks[mod] = _backupService.GetLatestOriginalSizesForModAsync(mod);
                            _perModOriginalSizesTasks[mod].ContinueWith(t =>
                            {
                                _uiThreadActions.Enqueue(() =>
                                {
                                    if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                                    {
                                        _cachedPerModOriginalSizes[mod] = t.Result;
                                        _needsUIRefresh = true;
                                    }
                                    _perModOriginalSizesTasks.Remove(mod);
                                });
                            }, TaskScheduler.Default);
                        }
                    }
                    else
                    {
                        var modDir2 = Path.GetDirectoryName(path);
                        var mod2 = !string.IsNullOrWhiteSpace(modDir2) ? Path.GetFileName(modDir2) : string.Empty;
                        if (!string.IsNullOrWhiteSpace(mod2))
                            _bulkBackedUpMods.Add(mod2);
                    }
                }
            }
            catch (Exception ex) { _logger.LogError(ex, "ExcludedTagsUpdated handler failed"); }
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
                    _uiThreadActions.Enqueue(() => { _running = false; });
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
                        _ = _backupService.ComputeSavingsForModAsync(target).ContinueWith(ps =>
                        {
                            if (ps.Status == TaskStatus.RanToCompletion)
                            {
                                try { _cachedPerModSavings[target] = ps.Result; } catch { }
                                _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
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
                        _uiThreadActions.Enqueue(() =>
                        {
                            _running = false;
                            try { _restoreCancellationTokenSource?.Dispose(); } catch { }
                            _restoreCancellationTokenSource = null;
                            _currentRestoreMod = string.Empty;
                            _currentRestoreModIndex = 0;
                            _currentRestoreModTotal = 0;
                            _cancelTargetMod = string.Empty;
                            _restoreAfterCancel = false;
                        });
                    }, TaskScheduler.Default);
                return;
            }

            // Normal completion path: refresh UI without global savings scan
            _cacheService.ClearAll();
            if (_filterPenumbraUsedOnly)
            {
                try { ReloadPenumbraUsedFiles("conversion-completed"); } catch { }
            }
            _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
            _ = Task.Run(async () =>
            {
                var mods = _modsTouchedLastRun.Keys.ToList();
                foreach (var m in mods)
                {
                    try
                    {
                        var stats = await _backupService.ComputeSavingsForModAsync(m).ConfigureAwait(false);
                        _uiThreadActions.Enqueue(() =>
                        {
                            try { _cachedPerModSavings[m] = stats; } catch { }
                            _perModSavingsRevision++;
                            _footerTotalsDirty = true;
                            _needsUIRefresh = true;
                        });
                    }
                    catch { }
                    await Task.Yield();
                }
            });
            _modsTouchedLastRun.Clear();
            _uiThreadActions.Enqueue(() =>
            {
                _running = false;
                ResetConversionProgress();
                ResetRestoreProgress();
            });

            try
            {
                var threads = Math.Max(1, _configService.Current.MaxStartupThreads);
                _uiThreadActions.Enqueue(() => { SetStartupRefreshInProgress(true); });
                _ = Task.Run(async () =>
                {
                    try { await _conversionService.RunInitialParallelUpdateAsync(threads, CancellationToken.None).ConfigureAwait(false); } catch { }
                    _uiThreadActions.Enqueue(() => { SetStartupRefreshInProgress(false); _needsUIRefresh = true; });
                });
            }
            catch { }
        };
        _conversionService.OnConversionCompleted += _onConversionCompleted;

        _onModProgress = e => { if (e.current == 1) { try { _modsTouchedLastRun.Clear(); } catch { } } _currentModName = e.modName; _currentModIndex = e.current; _totalMods = e.total; _currentModTotalFiles = e.fileTotal; try { _modsTouchedLastRun[e.modName] = true; } catch { } RefreshModState(e.modName, "conversion-progress"); try { _uiThreadActions.Enqueue(() => { _footerTotalsDirty = true; }); } catch { } };
        _conversionService.OnModProgress += _onModProgress;

        // Generic ModsChanged only marks UI for refresh; heavy scan is driven by add/delete.
        _onPenumbraModsChanged = () =>
        {
            _logger.LogDebug("Penumbra mods changed (DIAG-v3); refreshing UI state");
            RequestUiRefresh("penumbra-mods-changed");
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
                            _orphanRevision++;
                            _lastOrphanScanUtc = DateTime.UtcNow;
                            RequestUiRefresh("orphaned-backups-refreshed");
                        });
                    }
                    finally
                    {
                        _orphanScanInFlight = false;
                    }
                });
            }
            // Refresh hierarchical mod paths from mod_state snapshot
            if (!_loadingModPaths)
            {
                _loadingModPaths = true;
                _uiThreadActions.Enqueue(() =>
                {
                var snap = _modStateService.Snapshot();
                var paths = snap.ToDictionary(
                    kv => kv.Key,
                    kv => {
                        var folder = kv.Value?.PenumbraRelativePath ?? string.Empty;
                        var leaf = kv.Value?.RelativeModName ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(folder)) return leaf ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(leaf)) return folder ?? string.Empty;
                        return string.Concat(folder, "/", leaf);
                    },
                    StringComparer.OrdinalIgnoreCase);
                    try { paths.Remove("mod_state"); } catch { }
                    foreach (var kvp in paths)
                    {
                        var key = kvp.Key;
                        var val = kvp.Value ?? string.Empty;
                        if (!string.IsNullOrWhiteSpace(val))
                            _modPathsStable[key] = val;
                        else if (_modPathsStable.TryGetValue(key, out var prev) && !string.IsNullOrWhiteSpace(prev))
                            _modPathsStable[key] = prev;
                    }
                    _modPaths = new Dictionary<string, string>(_modPathsStable, StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        var sb = new System.Text.StringBuilder();
                        foreach (var kv in _modPaths.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                            sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
                        _modPathsSig = sb.ToString();
                    }
                    catch { _modPathsSig = _modPaths.Count.ToString(); }
                    _loadingModPaths = false;
                    RequestUiRefresh("mod-paths-reloaded");
                });
            }

            if (_filterPenumbraUsedOnly) ReloadPenumbraUsedFiles("mods-changed");

            _uiThreadActions.Enqueue(() => { _needsUIRefresh = true; RequestUiRefresh("mods-changed-light"); });
        };
        _conversionService.OnPenumbraModsChanged += _onPenumbraModsChanged;

        // React to external texture changes (e.g., conversions/restores initiated by Sphene)
        _onExternalTexturesChanged = reason =>
        {
            _logger.LogDebug("External texture change: {reason}; refreshing UI and scan state", reason);
            try { _lastExternalChangeReason = reason ?? string.Empty; _lastExternalChangeAt = DateTime.UtcNow; } catch { }
            try { ClearAllCaches(); } catch { }
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
                            var modsToMark = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            foreach (var file in candidates.Keys)
                            {
                                try
                                {
                                    string? mod = null;
                                    if (!_fileOwnerMod.TryGetValue(file, out var modTmp) || string.IsNullOrWhiteSpace(modTmp))
                                    {
                                        foreach (var kvp in _modPaths)
                                        {
                                            var abs = kvp.Value ?? string.Empty;
                                            if (string.IsNullOrWhiteSpace(abs)) continue;
                                            if (file.StartsWith(abs, StringComparison.OrdinalIgnoreCase)) { mod = kvp.Key; break; }
                                        }
                                    }
                                    else
                                    {
                                        mod = modTmp;
                                    }
                                    if (!string.IsNullOrWhiteSpace(mod)) modsToMark.Add(mod);
                                }
                                catch { }
                            }
                            foreach (var mod in modsToMark)
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

            if (_filterPenumbraUsedOnly) ReloadPenumbraUsedFiles($"external-{reason}");

            if (_scanInProgress)
            {
                RequestUiRefresh("scan-in-progress");
                return;
            }
            _scanInProgress = true;
            try { RefreshScanResults(true, $"external-{reason}"); }
            catch (Exception ex) { _logger.LogError(ex, "RefreshScanResults failed for external change {reason}", reason); }
            RequestUiRefresh("external-change-refresh");
        };
        _conversionService.OnExternalTexturesChanged += _onExternalTexturesChanged;

        // React to player redraws/resources changes to keep Used-Only filter in sync
        _onPlayerResourcesChanged = () =>
        {
            try
            {
                if (!_filterPenumbraUsedOnly)
                {
                    RequestUiRefresh("player-resources-changed-no-used-filter");
                    return;
                }
                ReloadPenumbraUsedFiles("player-resources-changed");
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
                                ClearModCaches(dir);
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
                    catch (Exception ex) { _logger.LogError(ex, "ModAdded debounce task failed"); }
                });
            }
        };
        _conversionService.OnPenumbraModAdded += _onPenumbraModAdded;

        _onPenumbraModDeleted = modDir =>
        {
            lock (_modsChangedLock)
            {
                _uiThreadActions.Enqueue(() => { ClearModCaches(modDir); });
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
                        await Task.Delay(350, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            return;
                        lock (_modsChangedLock)
                        {
                            if (mySeq != _modsChangedDebounceSeq)
                                return;
                        }
                        _uiThreadActions.Enqueue(() =>
                        {
                            try
                            {
                                var snap = _modStateService.Snapshot();
                                _knownModFolders = new HashSet<string>(snap.Keys, StringComparer.OrdinalIgnoreCase);
                                _modPaths = snap.ToDictionary(kv => kv.Key, kv => kv.Value?.PenumbraRelativePath ?? string.Empty, StringComparer.OrdinalIgnoreCase);
                                ClearModCaches(modDir);
                                _needsUIRefresh = true;
                            }
                            catch { }
                            RequestUiRefresh("mod-deleted-light");
                        });

                        try
                        {
                            var orphans = await _backupService.FindOrphanedBackupsAsync().ConfigureAwait(false);
                            _uiThreadActions.Enqueue(() =>
                            {
                                _orphaned = orphans ?? new List<TextureBackupService.OrphanBackupInfo>();
                                _orphanRevision++;
                                _lastOrphanScanUtc = DateTime.UtcNow;
                                _needsUIRefresh = true;
                                RequestUiRefresh("mod-deleted-orphan-refresh");
                            });
                        }
                        catch { }
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex) { _logger.LogError(ex, "ModDeleted debounce task failed"); }
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
                    catch (Exception ex) { _logger.LogError(ex, "ModSettingChanged debounce task failed"); }
                });
            }

            ReloadPenumbraUsedFiles("mod-setting");
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

                ReloadPenumbraUsedFiles("penumbra-enabled");
            }
            catch (Exception ex) { _logger.LogError(ex, "PenumbraEnabledChanged handler failed"); }
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

        SubscribeStartupProgress(conversionService);
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
        // Keep startup flag controlled by Plugin; do not auto-clear here
        if (_startupDependencyErrors.Count > 0)
        {
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.WarningLight);
            ImGui.TextWrapped("Dependency checks failed. Some features are disabled.");
            ImGui.PopStyleColor();
            foreach (var e in _startupDependencyErrors)
                ImGui.BulletText(e);
            ImGui.Separator();
        }
        // Apply any background-computed UI state updates on the main thread
        while (_uiThreadActions.TryDequeue(out var action))
        {
            try { action(); }
            catch (Exception ex) { _logger.LogError(ex, "UI thread action failed"); }
        }
        var showStartupProgress = (_startupTotalMods > 0) && (_startupProcessedMods < _startupTotalMods || _startupEtaSeconds > 0);
        if (showStartupProgress)
        {
            try
            {
                var eta = _startupEtaSeconds;
                if (eta < 0) eta = 0;
                var m = eta / 60;
                var s = eta % 60;
                var etaStr = $"{m}:{s:D2}";
                ImGui.TextColored(ShrinkUColors.Accent, $"Initializing: {_startupProcessedMods}/{_startupTotalMods} • ETA {etaStr}");
                var width = ImGui.GetContentRegionAvail().X;
                float frac = (_startupTotalMods > 0) ? (float)_startupProcessedMods / _startupTotalMods : 0f;
                ImGui.ProgressBar(frac, new System.Numerics.Vector2(width, 0), "");
                ImGui.Separator();
            }
            catch { }
        }
        // Status message will be rendered in the progress area instead of here

            // Check if UI needs refresh due to async data updates
            if (_needsUIRefresh)
            {
                _needsUIRefresh = false;
                try
                {
                    if (_configService.Current.DebugTraceUiRefresh)
                        _logger.LogDebug("UI refresh applied: thread={thread}", System.Environment.CurrentManagedThreadId);
                }
                catch { }
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

        if (!_modStateWatcherActive)
        {
            StartModStateWatcher();
        }

        // Preload mod paths lazily from mod_state snapshot
        if (!_loadingModPaths && _modPaths.Count == 0)
        {
            _loadingModPaths = true;
            var snap = _modStateService.Snapshot();
            var paths2 = snap.ToDictionary(
                kv => kv.Key,
                kv => {
                    var folder = kv.Value?.PenumbraRelativePath ?? string.Empty;
                    var leaf = kv.Value?.RelativeModName ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(folder)) return leaf ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(leaf)) return folder ?? string.Empty;
                    return string.Concat(folder, "/", leaf);
                },
                StringComparer.OrdinalIgnoreCase);
            try { paths2.Remove("mod_state"); } catch { }
            foreach (var kvp in paths2)
            {
                var key = kvp.Key;
                var val = kvp.Value ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(val))
                    _modPathsStable[key] = val;
            }
            _modPaths = new Dictionary<string, string>(_modPathsStable, StringComparer.OrdinalIgnoreCase);
            try
            {
                var sb = new System.Text.StringBuilder();
                foreach (var kv in _modPaths.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                    sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
                _modPathsSig = sb.ToString();
            }
            catch { _modPathsSig = _modPaths.Count.ToString(); }
            _loadingModPaths = false;
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
            _leftPanelWidthPx = 300f;
            _leftWidthInitialized = true;
        }
        var leftWidth = 300f;

        ImGui.BeginChild("LeftPanel", new Vector2(leftWidth, 0), true);
        DrawSettings();
        ImGui.Separator();
        DrawActions();
        ImGui.Separator();
        DrawProgress();
        ImGui.EndChild();

        _windowFocused = ImGui.IsWindowFocused(ImGuiFocusedFlags.RootAndChildWindows);

        ImGui.SameLine();
        DrawSplitter(totalWidth, ref leftWidth);
        ImGui.BeginChild("RightPanel", new Vector2(0, 0), true);
        DrawOverview();
        ImGui.EndChild();
    }

    public void Dispose()
    {
        // Unsubscribe from all service events
        try { _conversionService.OnConversionProgress -= _onConversionProgress; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnConversionProgress failed"); }
        try { _conversionService.OnBackupProgress -= _onBackupProgress; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnBackupProgress failed"); }
        try { _conversionService.OnConversionCompleted -= _onConversionCompleted; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnConversionCompleted failed"); }
        try { _conversionService.OnModProgress -= _onModProgress; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnModProgress failed"); }
        try { _conversionService.OnPenumbraModsChanged -= _onPenumbraModsChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnPenumbraModsChanged failed"); }
        try { _conversionService.OnPenumbraModAdded -= _onPenumbraModAdded; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnPenumbraModAdded failed"); }
        try { _conversionService.OnPenumbraModDeleted -= _onPenumbraModDeleted; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnPenumbraModDeleted failed"); }
        try { _conversionService.OnExternalTexturesChanged -= _onExternalTexturesChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnExternalTexturesChanged failed"); }
        try { _conversionService.OnPenumbraModSettingChanged -= _onPenumbraModSettingChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnPenumbraModSettingChanged failed"); }
        try { _conversionService.OnPenumbraEnabledChanged -= _onPenumbraEnabledChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnPenumbraEnabledChanged failed"); }
        try { _conversionService.OnPlayerResourcesChanged -= _onPlayerResourcesChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnPlayerResourcesChanged failed"); }
        try { _configService.OnExcludedTagsUpdated -= _onExcludedTagsUpdated; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnExcludedTagsUpdated failed"); }
        try { _modStateService.OnEntryChanged -= _onModStateEntryChanged; }
        catch (Exception ex) { _logger.LogError(ex, "Unsubscribe OnEntryChanged failed"); }

        // Cancel and dispose any outstanding debounce or restore operations
        try { _modsChangedDebounceCts?.Cancel(); }
        catch (Exception ex) { _logger.LogError(ex, "Cancel modsChangedDebounceCts failed"); }
        try { _modsChangedDebounceCts?.Dispose(); }
        catch (Exception ex) { _logger.LogError(ex, "Dispose modsChangedDebounceCts failed"); }
        _modsChangedDebounceCts = null;
        try { _enabledStatesDebounceCts?.Cancel(); }
        catch (Exception ex) { _logger.LogError(ex, "Cancel enabledStatesDebounceCts failed"); }
        try { _enabledStatesDebounceCts?.Dispose(); }
        catch (Exception ex) { _logger.LogError(ex, "Dispose enabledStatesDebounceCts failed"); }
        _enabledStatesDebounceCts = null;
        try { _restoreCancellationTokenSource?.Cancel(); }
        catch (Exception ex) { _logger.LogError(ex, "Cancel restoreCancellationTokenSource failed"); }
        try { _restoreCancellationTokenSource?.Dispose(); }
        catch (Exception ex) { _logger.LogError(ex, "Dispose restoreCancellationTokenSource failed"); }
        _restoreCancellationTokenSource = null;

        // Stop Used-Only watcher
        StopUsedResourcesWatcher();

        StopModStateWatcher();

        // Drain any queued UI actions
        try { while (_uiThreadActions.TryDequeue(out _)) { } }
        catch (Exception ex) { _logger.LogError(ex, "Drain UI thread actions failed"); }

        try { _logger.LogDebug("ConversionUI disposed: DIAG-v3"); }
        catch (Exception ex) { _logger.LogError(ex, "Log dispose message failed"); }
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
                        await Task.Delay(_usedWatcherIntervalMs, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            break;

                        if (!IsOpen || !_filterPenumbraUsedOnly || _loadingPenumbraUsed || !_windowFocused)
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
                                _usedWatcherIntervalMs = 3000;
                            }
                            else
                            {
                                var next = _usedWatcherIntervalMs * 2;
                                _usedWatcherIntervalMs = next > 15000 ? 15000 : next;
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
        _usedWatcherIntervalMs = 3000;
        try { _logger.LogDebug("Used-Only watcher stopped"); } catch { }
    }

    private void StartModStateWatcher()
    {
        try
        {
            StopModStateWatcher();
            _modStatePollCts = new CancellationTokenSource();
            var token = _modStatePollCts.Token;
            _modStateWatcherActive = true;
            _modStatePollTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(_modStateWatcherIntervalMs, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            break;

                        if (!IsOpen || !_windowFocused)
                            continue;

                        var changed = false;
                        try { changed = _modStateService.ReloadIfChanged(); } catch { }
                        if (changed)
                        {
                            _uiThreadActions.Enqueue(() =>
                            {
                                try { _modStateSnapshot = _modStateService.Snapshot(); } catch { }
                                _needsUIRefresh = true;
                            });
                            _modStateWatcherIntervalMs = 3000;
                        }
                        else
                        {
                            var next = _modStateWatcherIntervalMs * 2;
                            _modStateWatcherIntervalMs = next > 15000 ? 15000 : next;
                        }
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex)
                    {
                        try { _logger.LogDebug(ex, "ModState watcher iteration failed"); } catch { }
                    }
                }
            }, token);
            try { _logger.LogDebug("ModState watcher started"); } catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogDebug(ex, "Failed to start ModState watcher"); } catch { }
        }
    }

    private void StopModStateWatcher()
    {
        try { _modStatePollCts?.Cancel(); } catch { }
        try { _modStatePollCts?.Dispose(); } catch { }
        _modStatePollCts = null;
        _modStatePollTask = null;
        _modStateWatcherActive = false;
        _modStateWatcherIntervalMs = 3000;
        try { _logger.LogDebug("ModState watcher stopped"); } catch { }
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
            
        }
        else
        {
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
                var names = await _conversionService.GetModDisplayNamesAsync().ConfigureAwait(false);
                var tags = await _conversionService.GetModTagsAsync().ConfigureAwait(false);
                var folders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);

                _uiThreadActions.Enqueue(() =>
                {
                    _scannedByMod.Clear();
                    _selectedTextures.Clear();
                    _texturesToConvert.Clear();
                    _fileOwnerMod.Clear();
                    if (names != null)
                        _modDisplayNames = names;
                    if (tags != null)
                    {
                        _modTags = tags.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.OrdinalIgnoreCase);
                        try
                        {
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
                            var leaf = TextureConversionService.NormalizeLeafKey(folder);
                            var files = _modStateService.ReadDetailTextures(leaf);
                            _scannedByMod[leaf] = files ?? new List<string>();
                            foreach (var file in (files ?? new List<string>()))
                            {
                                _texturesToConvert[file] = Array.Empty<string>();
                                _fileOwnerMod[file] = leaf;
                            }
                        }
                        _knownModFolders = new HashSet<string>(folders, StringComparer.OrdinalIgnoreCase);
                    }
                    _logger.LogDebug("Initial mods loaded: {mods}", _scannedByMod.Count);
                    _needsUIRefresh = true;
                    Task.Run(async () => { try { await _conversionService.UpdateAllModTextureCountsAsync().ConfigureAwait(false); } catch { } });
                });
            }
            catch (Exception ex) { _logger.LogError(ex, "Startup refresh check failed"); }
        });
    }

    // Refresh scan results to reflect latest restore/conversion state
    private void RefreshScanResults(bool force = false, string origin = "unknown")
    {
        Task.Run(async () =>
        {
            try
            {
                TraceAction(origin, nameof(RefreshScanResults), force ? "force" : "no-force");
                _logger.LogDebug("Scan requested: origin={origin} force={force}", origin, force);
                TraceAction(origin, "GetModDisplayNamesAsync");
                var names = await _conversionService.GetModDisplayNamesAsync().ConfigureAwait(false);
                TraceAction(origin, "GetModTagsAsync");
                var tags = await _conversionService.GetModTagsAsync().ConfigureAwait(false);
                TraceAction(origin, "GetAllModFoldersAsync");
                var folders = await _conversionService.GetAllModFoldersAsync().ConfigureAwait(false);
                Dictionary<string, string>? paths = null;
                var snap = _modStateService.Snapshot();
                paths = snap.ToDictionary(
                    kv => kv.Key,
                    kv => {
                        var folder = kv.Value?.PenumbraRelativePath ?? string.Empty;
                        var leaf = kv.Value?.RelativeModName ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(folder)) return leaf ?? string.Empty;
                        if (string.IsNullOrWhiteSpace(leaf)) return folder ?? string.Empty;
                        return string.Concat(folder, "/", leaf);
                    },
                    StringComparer.OrdinalIgnoreCase);

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
                if (allowHeavy && since < TimeSpan.FromSeconds(3) && !force)
                {
                    _logger.LogDebug("Suppressed heavy scan: rate limit active ({ms} ms since last heavy)", (int)since.TotalMilliseconds);
                    allowHeavy = false;
                }

                Dictionary<string, List<string>>? grouped = null;
                if (allowHeavy)
                {
                    TraceAction(origin, "GetGroupedCandidateTexturesAsync");
                    grouped = await _conversionService.GetGroupedCandidateTexturesAsync().ConfigureAwait(false);
                    _lastHeavyScanAt = DateTime.UtcNow;
                    _logger.LogDebug("Heavy scan executed: origin={origin} groupedMods={mods}", origin, grouped?.Count ?? 0);

                    try
                    {
                        if (grouped != null)
                        {
                            _modStateService.BeginBatch();
                            foreach (var kv in grouped)
                            {
                                var prev = _modStateService.ReadDetailTextures(kv.Key);
                                var prevCount = prev?.Count ?? 0;
                                bool sameCount = prevCount == kv.Value.Count;
                                if (sameCount)
                                {
                                    var prevSet = new HashSet<string>(prev ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                                    var newSet = new HashSet<string>(kv.Value, StringComparer.OrdinalIgnoreCase);
                                    if (prevSet.SetEquals(newSet))
                                        continue;
                                }
                                _modStateService.UpdateTextureFiles(kv.Key, kv.Value);
                            }
                            _modStateService.EndBatch();
                        }
                    }
                    catch { }
                }
                else
                {
                    try
                    {
                        if (folders != null)
                        {
                            var persisted = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                            foreach (var folder in folders)
                            {
                                var leaf = TextureConversionService.NormalizeLeafKey(folder);
                                var list = _modStateService.ReadDetailTextures(leaf);
                                if (list != null && list.Count > 0)
                                    persisted[leaf] = list;
                            }
                            if (persisted.Count > 0)
                                grouped = persisted;
                        }
                    }
                    catch { }
                }

                _uiThreadActions.Enqueue(() =>
                {
                    if (grouped != null && grouped.Count > 0)
                    {
                        _scannedByMod.Clear();
                        _selectedTextures.Clear();
                        _texturesToConvert.Clear();
                        _fileSizeCache.Clear();
                        _fileOwnerMod.Clear();
                        foreach (var mod in grouped)
                        {
                            if (_configService.Current.DebugTraceActions)
                            {
                                TraceAction(origin, "mod", mod.Key);
                                foreach (var file in mod.Value)
                                    TraceAction(origin, "file", file);
                            }
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
                        _logger.LogDebug("UI state updated (DIAG-v3): origin={origin} heavy scan skipped or empty; preserving previous/persisted results", origin);
                    }

                    if (grouped != null && grouped.Count > 0)
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
                    {
                        foreach (var kvp in paths)
                        {
                            var key = kvp.Key;
                            var val = kvp.Value ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(val))
                                _modPathsStable[key] = val;
                        }
                        _modPaths = new Dictionary<string, string>(_modPathsStable, StringComparer.OrdinalIgnoreCase);
                        try { _modPaths.Remove("mod_state"); } catch { }
                        try
                        {
                            var sb = new System.Text.StringBuilder();
                            foreach (var kv in _modPaths.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                                sb.Append(kv.Key).Append('=').Append(kv.Value).Append(';');
                            _modPathsSig = sb.ToString();
                        }
                        catch { _modPathsSig = _modPaths.Count.ToString(); }
                    }
                    if (folders != null)
                    {
                        foreach (var folder in folders)
                        {
                            var leaf = TextureConversionService.NormalizeLeafKey(folder);
                            if (!_scannedByMod.ContainsKey(leaf))
                                _scannedByMod[leaf] = new List<string>();
                        }
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
        _bulkStartedAt = DateTime.MinValue;
        try { _bulkBackedUpMods.Clear(); } catch { }
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
        _restoreModsTotal = 0;
        _restoreModsDone = 0;
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

    private List<string> GetAllFilesForModDisplay(string mod, List<string>? visibleFiles = null)
    {
        if (_scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
            return all;
        var vf = visibleFiles ?? _modStateService.ReadDetailTextures(mod);
        return vf ?? new List<string>();
    }

    private int GetTotalTexturesForMod(string mod, List<string>? filesGuess = null)
    {
        if (_scannedByMod.TryGetValue(mod, out var all) && all != null)
            return all.Count;
        if (_modStateSnapshot != null && _modStateSnapshot.TryGetValue(mod, out var ms) && ms != null && ms.TotalTextures > 0)
            return ms.TotalTextures;
        var det = filesGuess ?? _modStateService.ReadDetailTextures(mod);
        return det?.Count ?? 0;
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

    // Get per-mod original total bytes, preferring ModState; fallback to computed or backup manifest.
    private long GetOrQueryModOriginalTotal(string mod)
    {
        try
        {
            var snap = _modStateSnapshot ?? _modStateService.Snapshot();
            if (snap.TryGetValue(mod, out var e) && e != null && e.OriginalBytes > 0)
                return e.OriginalBytes;

            if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null && map.Count > 0)
                return map.Values.Sum();

            if (GetOrQueryModBackup(mod))
            {
                if (_perModOriginalSizesTasks.TryGetValue(mod, out var task))
                {
                    if (task.Status == TaskStatus.RanToCompletion && task.Result != null && task.Result.Count > 0)
                    {
                        _cachedPerModOriginalSizes[mod] = task.Result;
                        return task.Result.Values.Sum();
                    }
                }
                else
                {
                    _perModOriginalSizesTasks[mod] = _backupService.GetLatestOriginalSizesForModAsync(mod);
                    _perModOriginalSizesTasks[mod].ContinueWith(t =>
                    {
                        _uiThreadActions.Enqueue(() =>
                        {
                            if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                            {
                                _cachedPerModOriginalSizes[mod] = t.Result;
                                _needsUIRefresh = true;
                            }
                        });
                    }, TaskScheduler.Default);
                }
                return 0;
            }

            if (_scannedByMod.TryGetValue(mod, out var files) && files != null && files.Count > 0)
            {
                try
                {
                    _cacheService.WarmupFileSizeCache(files.Take(500));
                }
                catch { }
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
        if (_modStateSnapshot != null && _modStateSnapshot.TryGetValue(folder, out var ms) && ms != null && !string.IsNullOrWhiteSpace(ms.DisplayName))
            return ms.DisplayName.Replace('\\', '/');
        if (_modDisplayNames.TryGetValue(folder, out var name) && !string.IsNullOrWhiteSpace(name))
            return name.Replace('\\', '/');
        return folder;
    }

    private void RefreshModState(string mod, string origin)
    {
        if (_startupRefreshInProgress)
            return;
        try
        {
            if (_lastModRefreshAt.TryGetValue(mod, out var last) && (DateTime.UtcNow - last).TotalMilliseconds < 300)
                return;
            _lastModRefreshAt[mod] = DateTime.UtcNow;
        }
        catch { }
        Task.Run(async () =>
        {
            try
            {
                var files = await _conversionService.GetModTextureFilesAsync(mod).ConfigureAwait(false);
                try { _modStateService.UpdateTextureCount(mod, files?.Count ?? 0); } catch { }
                var display = await _conversionService.GetModDisplayNameAsync(mod).ConfigureAwait(false);
                var tagList = await _conversionService.GetModTagsAsync(mod).ConfigureAwait(false);
                _uiThreadActions.Enqueue(() =>
                {
                    _logger.LogDebug("Mod scan refreshed: origin={origin} mod={mod} files={count}", origin, mod, files?.Count ?? 0);
                    _scannedByMod[mod] = files ?? new List<string>();
                    foreach (var f in _scannedByMod[mod])
                        _texturesToConvert[f] = Array.Empty<string>();
                    _modDisplayNames[mod] = display ?? string.Empty;
                    _modTags[mod] = tagList ?? new List<string>();
                    _modStateSnapshot = _modStateService.Snapshot();
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
        IReadOnlyList<string>? tags = null;
        if (_modStateSnapshot != null && _modStateSnapshot.TryGetValue(mod, out var ms) && ms != null && ms.Tags != null)
            tags = ms.Tags;
        else if (_modTags.TryGetValue(mod, out var t) && t != null)
            tags = t;
        if (tags != null)
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

    public void SetStartupRefreshInProgress(bool value)
    {
        _startupRefreshInProgress = value;
        if (value) _startupRefreshStartedAt = DateTime.UtcNow;
        else _startupRefreshStartedAt = DateTime.MinValue;
    }

    public void TriggerStartupRescan()
    {
        RefreshScanResults(true, "startup");
    }

    public void SetStartupDependencyErrors(System.Collections.Generic.IEnumerable<string> errors)
    {
        _startupDependencyErrors.Clear();
        if (errors == null) return;
        foreach (var e in errors)
        {
            if (!string.IsNullOrWhiteSpace(e))
                _startupDependencyErrors.Add(e);
        }
    }

    public void DisableModStateSaving()
    {
        try { _modStateService.SetSavingEnabled(false); } catch { }
    }

    public void ShutdownBackgroundWork()
    {
        try { _cacheService.Dispose(); } catch { }
    }
}
