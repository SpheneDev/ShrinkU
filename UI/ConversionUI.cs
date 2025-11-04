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
using System.Collections.Concurrent;

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
    private bool _useFolderStructure = false;
    private Dictionary<string, string> _modPaths = new(StringComparer.OrdinalIgnoreCase);
    private bool _loadingModPaths = false;
    private Dictionary<Guid, string> _collections = new();
    private bool _loadingCollections = false;
    private Guid? _selectedCollectionId = null;
    private bool _loadingEnabledStates = false;
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
    private readonly ConcurrentDictionary<string, byte> _modsBackupCheckInFlight = new(StringComparer.OrdinalIgnoreCase);
    // Cache file sizes to avoid per-frame disk I/O in UI rendering
    private readonly ConcurrentDictionary<string, long> _fileSizeCache = new(StringComparer.OrdinalIgnoreCase);
    private Task? _fileSizeWarmupTask = null;
    // Queue for marshalling UI state updates onto the main thread during Draw
    private readonly ConcurrentQueue<Action> _uiThreadActions = new();
    // Cancellation for restore operations
    private CancellationTokenSource? _restoreCancellationTokenSource = null;
    // Per-mod restore progress state
    private string _currentRestoreMod = string.Empty;
    private int _currentRestoreModIndex = 0;
    private int _currentRestoreModTotal = 0;
    private bool _restoreAfterCancel = false;
    private string _cancelTargetMod = string.Empty;
    
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
    // Track last external conversion/restore notification to surface UI indicator
    private DateTime _lastExternalChangeAt = DateTime.MinValue;
    private string _lastExternalChangeReason = string.Empty;

public ConversionUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, TextureBackupService backupService, Action? openSettings = null)
        : base("ShrinkU###ShrinkUConversionUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _backupService = backupService;
        _openSettings = openSettings;

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(520, 300),
            MaximumSize = new Vector2(1920, 1080),
        };

        _onConversionProgress = e => { _currentTexture = e.Item1; _convertedCount = e.Item2; };
        _conversionService.OnConversionProgress += _onConversionProgress;

        _onBackupProgress = e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; };
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
                                _modsWithBackupCache[target] = bt.Result;
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
        };
        _conversionService.OnConversionCompleted += _onConversionCompleted;

        _onModProgress = e => { _currentModName = e.modName; _currentModIndex = e.current; _totalMods = e.total; _currentModTotalFiles = e.fileTotal; };
        _conversionService.OnModProgress += _onModProgress;

        // Generic ModsChanged only marks UI for refresh; heavy scan is driven by add/delete.
        _onPenumbraModsChanged = () =>
        {
            _modsWithBackupCache.Clear();
            _logger.LogDebug("Penumbra mods changed (DIAG-v3); refreshing UI state");
            _needsUIRefresh = true;
            // Refresh hierarchical mod paths so folder moves/creations reflect in the UI
            if (_useFolderStructure && !_loadingModPaths)
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

            // Force a heavy scan to fully refresh candidate textures after mods changed
            if (_scanInProgress)
            {
                _needsUIRefresh = true;
                return;
            }
            _scanInProgress = true;
            RefreshScanResults(true, "penumbra-mods-changed");
            _needsUIRefresh = true;
        };
        _conversionService.OnPenumbraModsChanged += _onPenumbraModsChanged;

        // React to external texture changes (e.g., conversions/restores initiated by Sphene)
        _onExternalTexturesChanged = reason =>
        {
            _logger.LogDebug("External texture change: {reason}; refreshing UI and scan state", reason);
            try { _lastExternalChangeReason = reason ?? string.Empty; _lastExternalChangeAt = DateTime.UtcNow; } catch { }
            // Ensure backup states and metrics reflect latest changes from Sphene
            try { _modsWithBackupCache.Clear(); } catch { }
            try { _modsBackupCheckInFlight.Clear(); } catch { }
            try { TriggerMetricsRefresh(); } catch { }

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
        _onPenumbraModAdded = _ =>
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
                                _logger.LogDebug("Suppressed heavy scan: mod folders unchanged after ModAdded");
                                _needsUIRefresh = true;
                            });
                            return;
                        }
                        if (_scanInProgress)
                            return;
                        _scanInProgress = true;
                        _logger.LogDebug("Heavy scan triggered: ModAdded detected changes");
                        RefreshScanResults(true, "mod-added");
                    }
                    catch (TaskCanceledException) { }
                    catch { }
                });
            }
        };
        _conversionService.OnPenumbraModAdded += _onPenumbraModAdded;

        _onPenumbraModDeleted = _ =>
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
                                _logger.LogDebug("Suppressed heavy scan: mod folders unchanged after ModDeleted");
                                _needsUIRefresh = true;
                            });
                            return;
                        }
                        if (_scanInProgress)
                            return;
                        _scanInProgress = true;
                        _logger.LogDebug("Heavy scan triggered: ModDeleted detected changes");
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
                        _loadingEnabledStates = true;
                        var states = await _conversionService.GetAllModEnabledStatesAsync(_selectedCollectionId.Value).ConfigureAwait(false);
                        _uiThreadActions.Enqueue(() =>
                        {
                            if (states != null)
                                _modEnabledStates = states;
                            _loadingEnabledStates = false;
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
        _useFolderStructure = _configService.Current.UseFolderStructure;
        _filterPenumbraUsedOnly = _configService.Current.FilterPenumbraUsedOnly;
        _filterNonConvertibleMods = _configService.Current.FilterNonConvertibleMods;
        _filterInefficientMods = _configService.Current.HideInefficientMods;
        _scanSortAsc = _configService.Current.ScanSortAsc;
        var sortKey = _configService.Current.ScanSortKey ?? "ModName";
        _scanSortKind = string.Equals(sortKey, "FileName", StringComparison.OrdinalIgnoreCase)
            ? ScanSortKind.FileName
            : ScanSortKind.ModName;

        // Initialize first column width from config (default 30px on first open)
        _scannedFirstColWidth = _configService.Current.ScannedFilesFirstColWidth > 0f
            ? _configService.Current.ScannedFilesFirstColWidth
            : 28f;
        _scannedSizeColWidth = _configService.Current.ScannedFilesSizeColWidth > 0f
            ? _configService.Current.ScannedFilesSizeColWidth
            : 85f;
        _scannedActionColWidth = _configService.Current.ScannedFilesActionColWidth > 0f
            ? _configService.Current.ScannedFilesActionColWidth
            : 60f;

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
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip(text);
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

        // Preload mod paths lazily if folder structure is enabled
        if (_useFolderStructure && !_loadingModPaths && _modPaths.Count == 0)
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

                        if (!_filterPenumbraUsedOnly)
                            continue;
                        if (_loadingPenumbraUsed)
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
        var modeDisplay = mode == TextureProcessingMode.Automatic && !string.IsNullOrWhiteSpace(controller)
            ? $"Automatic (controlled by {controller})"
            : (mode == TextureProcessingMode.Automatic && _configService.Current.AutomaticHandledBySphene ? "Automatic (controlled by Sphene)" : mode.ToString());
        var disableModeCombo = mode == TextureProcessingMode.Automatic && (!string.IsNullOrWhiteSpace(controller) || _configService.Current.AutomaticHandledBySphene);
        if (disableModeCombo) ImGui.BeginDisabled();
        if (ImGui.BeginCombo("Mode", modeDisplay))
        {
            if (ImGui.Selectable("Manual", mode == TextureProcessingMode.Manual))
            {
                _configService.Current.TextureProcessingMode = TextureProcessingMode.Manual;
                _configService.Current.AutomaticHandledBySphene = false;
                _configService.Current.AutomaticControllerName = string.Empty;
                _configService.Save();
            }
            if (ImGui.Selectable("Automatic", mode == TextureProcessingMode.Automatic))
            {
                _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                _configService.Save();
            }
            ImGui.EndCombo();
        }
        if (disableModeCombo)
        {
            ImGui.EndDisabled();
            var ctrlLabel = !string.IsNullOrWhiteSpace(controller) ? controller : "Sphene";
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip($"Controlled by {ctrlLabel}");
        }
        ShowTooltip("Choose how textures are processed.");

        bool backup = _configService.Current.EnableBackupBeforeConversion;
        if (ImGui.Checkbox("Enable backup before conversion", ref backup))
        {
            _configService.Current.EnableBackupBeforeConversion = backup;
            _configService.Save();
        }
        ShowTooltip("Create a backup before converting textures.");
        bool zip = _configService.Current.EnableZipCompressionForBackups;
        if (ImGui.Checkbox("ZIP backups by default", ref zip))
        {
            _configService.Current.EnableZipCompressionForBackups = zip;
            _configService.Save();
        }
        ShowTooltip("Compress backups into ZIP archives by default.");
        bool deleteOriginals = _configService.Current.DeleteOriginalBackupsAfterCompression;
        if (ImGui.Checkbox("Delete originals after ZIP", ref deleteOriginals))
        {
            _configService.Current.DeleteOriginalBackupsAfterCompression = deleteOriginals;
            _configService.Save();
        }
        ShowTooltip("Remove original backup files after ZIP compression.");
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
    }

    // Nested folder tree for Table View
    private sealed class TableCatNode
    {
        public string Name { get; }
        public Dictionary<string, TableCatNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<string> Mods { get; } = new();
        public TableCatNode(string name) => Name = name;
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
                files.AddRange(modFiles);
        }
        foreach (var child in node.Children.Values)
        {
            files.AddRange(CollectFilesRecursive(child, visibleByMod));
        }
        return files;
    }

private void DrawCategoryTableNode(TableCatNode node, Dictionary<string, List<string>> visibleByMod, ref int idx, string pathPrefix, int depth = 0)
    {
        const float indentStep = 16f;
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            ImGui.TableNextRow();
            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
            // Folder row: selection checkbox in first column
            ImGui.TableSetColumnIndex(0);
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            var folderFiles = CollectFilesRecursive(child, visibleByMod);
            bool folderSelected = folderFiles.Count > 0 && folderFiles.All(f => _selectedTextures.Contains(f));
            ImGui.BeginDisabled(folderFiles.Count == 0);
            if (ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
            {
                if (folderSelected)
                    foreach (var f in folderFiles) _selectedTextures.Add(f);
                else
                    foreach (var f in folderFiles) _selectedTextures.Remove(f);
            }
            ImGui.EndDisabled();
            if (folderFiles.Count == 0 && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                ImGui.SetTooltip("No selectable files in this folder (filtered or excluded).");
            else
                ShowTooltip("Select or deselect all files in this folder.");

            // Folder label and tree toggle in File column
            ImGui.TableSetColumnIndex(1);
            // Indent folder rows in the File column based on depth, without affecting other columns.
            ImGui.SetCursorPosX(ImGui.GetCursorPosX() + depth * indentStep);
            var catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
            ImGui.SameLine();
            // Use a distinct color for folder icon and label for better visual separation
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

            // Uncompressed size for folder contents (include converted and unconverted mods)
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

            // Compressed size aggregated preferring per-mod stats; show '-' when none
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

            if (catOpen)
            {
                DrawCategoryTableNode(child, visibleByMod, ref idx, fullPath, depth + 1);

                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.TryGetValue(mod, out var files))
                        continue;

                    ImGui.TableNextRow();
                    ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                    ImGui.TableSetColumnIndex(0);
                    var hasBackup = GetOrQueryModBackup(mod);
                    var excluded = !hasBackup && IsModExcludedByTags(mod);
                    // Always show mods; icon color indicates enabled/disabled state
                    bool modSelected = files.All(f => _selectedTextures.Contains(f));
                    ImGui.BeginDisabled(excluded);
                    if (ImGui.Checkbox($"##modsel-{mod}", ref modSelected))
                    {
                        if (modSelected)
                            foreach (var f in files) _selectedTextures.Add(f);
                        else
                            foreach (var f in files) _selectedTextures.Remove(f);

                    }
                    ImGui.EndDisabled();
                    if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        ImGui.SetTooltip("Mod excluded by tags");
                    else
                        ShowTooltip("Toggle selection for all files in this mod.");

                    ImGui.TableSetColumnIndex(1);
                    ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (depth + 1) * indentStep);
                    var nodeFlags = ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.NoTreePushOnOpen;
                    if (!_configService.Current.ShowModFilesInOverview)
                        nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
                    long modVisibleSize = 0;
                    foreach (var f in files)
                    {
                        var sz = GetCachedOrComputeSize(f);
                        if (sz > 0) modVisibleSize += sz;
                    }
                    // Build header using full mod totals (independent of filters)
                    int totalAll = _scannedByMod.TryGetValue(mod, out var allModFiles2) && allModFiles2 != null ? allModFiles2.Count : files.Count;
                    int convertedAll = 0;
                    if (hasBackup && totalAll > 0)
                    {
                        // Ensure original-size map is hydrated
                        _ = GetOrQueryModOriginalTotal(mod);
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
                        // Fallback to stats count when map not yet available
                        if (convertedAll == 0 && _cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                            convertedAll = Math.Min(s2.ComparedFiles, totalAll);
                    }
                    var header = hasBackup
                        ? $"{ResolveModDisplayName(mod)} ({convertedAll}/{totalAll})"
                        : $"{ResolveModDisplayName(mod)} ({totalAll})";
                    bool open = ImGui.TreeNodeEx($"##mod-{mod}", nodeFlags);
                    if (ImGui.BeginPopupContextItem($"modctx-{mod}"))
                    {
                        if (ImGui.MenuItem("Open in Penumbra"))
                        {
                            var display = ResolveModDisplayName(mod);
                            _conversionService.OpenModInPenumbra(mod, display);
                        }
                        ImGui.EndPopup();
                    }
                    ImGui.SameLine();
                    ImGui.PushFont(UiBuilder.IconFont);
                    if (_modEnabledStates.TryGetValue(mod, out var stIcon))
                    {
                        // Use white when disabled, green when enabled
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
                    // If an external change was detected recently or persisted, show a small plug indicator
                    var hasPersistent = _configService.Current.ExternalConvertedMods.ContainsKey(mod);
                    var showExternal = hasBackup && (((DateTime.UtcNow - _lastExternalChangeAt).TotalSeconds < 30 && !string.IsNullOrEmpty(_lastExternalChangeReason)) || hasPersistent);
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
                    // Uncompressed and Compressed columns for mod row in folder view
                    ImGui.TableSetColumnIndex(3);
                    _cachedPerModSavings.TryGetValue(mod, out var modStats);
                    long modOriginalBytes = 0;
                if (modStats != null && modStats.OriginalBytes > 0)
                {
                    modOriginalBytes = modStats.OriginalBytes;
                }
                else if (!hasBackup && _scannedByMod.TryGetValue(mod, out var allModFiles) && allModFiles != null)
                {
                    foreach (var f in allModFiles)
                    {
                        var sz = GetCachedOrComputeSize(f);
                        if (sz > 0) modOriginalBytes += sz;
                    }
                }
                    DrawRightAlignedSize(modOriginalBytes);

                    ImGui.TableSetColumnIndex(2);
                    var modCurrentBytes = hasBackup ? (modStats?.CurrentBytes ?? 0) : 0;
                    if (modCurrentBytes <= 0)
                    {
                        DrawRightAlignedTextColored("-", _compressedTextColor);
                    }
                    else
                    {
                        var color = modCurrentBytes > modOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
                        DrawRightAlignedSizeColored(modCurrentBytes, color);
                    }

                    // Action column
                    ImGui.TableSetColumnIndex(4);
                    ImGui.BeginDisabled(_running || _conversionService.IsConverting);
                    if (hasBackup || files.Count > 0)
                    {
                        var actionLabel = hasBackup ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}";

                        if (hasBackup)
                        {
                            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                        }
                        else
                        {
                            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ConvertButton);
                            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ConvertButtonHovered);
                            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ConvertButtonActive);
                            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                        }

                        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                        bool modHasUsed = false;
                        try { modHasUsed = files.Any(f => _penumbraUsedFiles.Contains(f)); } catch { }
                        var restoreDisabledByAuto = automaticMode && hasBackup && modHasUsed;
                        ImGui.BeginDisabled(excluded || restoreDisabledByAuto || _conversionService.IsConverting);
                        if (ImGui.Button(actionLabel))
                        {
                            _running = true;
                            if (hasBackup)
                            {
                            _modsWithBackupCache.TryRemove(mod, out _);
                            // Reset progress state for restore and initialize current restore mod
                            ResetConversionProgress();
                            ResetRestoreProgress();
                            _currentRestoreMod = mod;
                            var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                            _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                .ContinueWith(t => {
                                    var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                    try { _backupService.RedrawPlayer(); } catch { }
                                    _logger.LogDebug("Heavy scan triggered: restore completed (mod button in folder view)");
                                    RefreshScanResults(true, "restore-folder-view");
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
                                            _modsWithBackupCache[mod] = bt.Result;
                                    });
                                    if (success)
                                    {
                                        try
                                        {
                                            if (_configService.Current.ExternalConvertedMods.Remove(mod))
                                                _configService.Save();
                                        }
                                        catch { }
                                    }
                                    _running = false;
                                });
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
                            var toConvert = allFilesForMod.ToDictionary(f => f, f => Array.Empty<string>(), StringComparer.Ordinal);
                            // Reset progress state for a fresh conversion
                            ResetConversionProgress();
                            ResetRestoreProgress();
                            _ = _conversionService.StartConversionAsync(toConvert)
                                .ContinueWith(_ =>
                                {
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                            _modsWithBackupCache[mod] = bt.Result;
                                    });
                                    _running = false;
                                });
                            }
                        }
                        // Tooltip for action button (Convert/Restore)
                        ImGui.PopStyleColor(1);
                        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        {
                            if (excluded)
                                ImGui.SetTooltip("Mod excluded by tags");
                            else if (restoreDisabledByAuto)
                                ImGui.SetTooltip("Automatic mode active: restoring currently used textures is disabled.");
                            else
                            {
                                if (hasBackup)
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
                        ImGui.PopStyleColor(3);
                        ImGui.EndDisabled();
                    }
                    ImGui.EndDisabled();

                    if (open)
                    {
                        if (_configService.Current.ShowModFilesInOverview)
                        {
                            foreach (var file in files)
                            {
                                ImGui.TableNextRow();
                                ImGui.TableSetColumnIndex(0);
                                bool selected = _selectedTextures.Contains(file);
                                ImGui.BeginDisabled(excluded);
                                if (ImGui.Checkbox($"##selsub-{idx}", ref selected))
                                {
                                    if (selected) { _selectedTextures.Add(file); }
                                    else _selectedTextures.Remove(file);
                                }
                                ImGui.EndDisabled();
                                if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                                    ImGui.SetTooltip("Mod excluded by tags");
                                else
                                    ShowTooltip("Select or deselect this file.");

                                ImGui.TableSetColumnIndex(1);
                                ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (depth + 2) * indentStep);
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
        ImGui.BeginDisabled(_running || _conversionService.IsConverting);
        if (ImGui.Button("Re-Scan All Mod Textures"))
        {
            _ = _conversionService.GetGroupedCandidateTexturesAsync().ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                {
                    _scannedByMod.Clear();
                    _selectedTextures.Clear();
                    _texturesToConvert.Clear();
                    foreach (var mod in t.Result)
                    {
                        _scannedByMod[mod.Key] = mod.Value;
                        foreach (var file in mod.Value)
                        {
                            _texturesToConvert[file] = Array.Empty<string>();
                        }
                    }
                    _logger.LogDebug("Scanned {mods} mods and {files} textures", _scannedByMod.Count, _texturesToConvert.Count);

                    // Load Penumbra display names for mods
                    _ = _conversionService.GetModDisplayNamesAsync().ContinueWith(dt =>
                    {
                        if (dt.Status == TaskStatus.RanToCompletion && dt.Result != null)
                            _modDisplayNames = dt.Result;
                    });

                    // Ensure all Penumbra mods are present (even if no textures), so UI shows them
                    _ = _conversionService.GetAllModFoldersAsync().ContinueWith(mt =>
                    {
                        if (mt.Status == TaskStatus.RanToCompletion && mt.Result != null)
                        {
                            foreach (var folder in mt.Result)
                            {
                                if (!_scannedByMod.ContainsKey(folder))
                                    _scannedByMod[folder] = new List<string>();
                            }
                        }
                    });
                }
            });
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
                if (_useFolderStructure)
                {
                    paths = await _conversionService.GetModPathsAsync().ConfigureAwait(false);
                }

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
        if (!_running)
        {
            ImGui.Text("Waiting...");
            return;
        }
        var avail = ImGui.GetContentRegionAvail();
        var barSize = new Vector2(Math.Max(120f, avail.X), 0);

        // If a restore is in progress, show restore-oriented progress bars
        var isRestoring = !string.IsNullOrEmpty(_currentRestoreMod) || (_restoreCancellationTokenSource != null && _backupTotal > 0);
        if (isRestoring)
        {
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
        ImGui.SetNextItemWidth(180f);
        ImGui.InputTextWithHint("##scanFilter", "Filter by file or mod", ref _scanFilter, 128);
        ShowTooltip("Filter results by file name or mod name.");
        ImGui.SameLine();
        if (ImGui.Checkbox("Folder Structure", ref _useFolderStructure))
        {
            _configService.Current.UseFolderStructure = _useFolderStructure;
            _configService.Save();
            if (_useFolderStructure && !_loadingModPaths && _modPaths.Count == 0)
            {
                _loadingModPaths = true;
                _ = _conversionService.GetModPathsAsync().ContinueWith(t =>
                {
                    if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                        _modPaths = t.Result;
                    _loadingModPaths = false;
                });
            }
        }
        ShowTooltip("Group mods by their folder hierarchy.");

        // Place sort controls below the search text field for better horizontal space
        ImGui.NewLine();
        ImGui.Text("Sort:");
        ImGui.SameLine();
        if (ImGui.RadioButton("File", _scanSortKind == ScanSortKind.FileName))
        {
            _scanSortKind = ScanSortKind.FileName;
            _configService.Current.ScanSortKey = "FileName";
            _configService.Save();
        }
        ShowTooltip("Sort entries by file name.");
        ImGui.SameLine();
        if (ImGui.RadioButton("Mod", _scanSortKind == ScanSortKind.ModName))
        {
            _scanSortKind = ScanSortKind.ModName;
            _configService.Current.ScanSortKey = "ModName";
            _configService.Save();
        }
        ShowTooltip("Sort entries by mod name.");
        ImGui.SameLine();
        if (ImGui.Checkbox("Asc", ref _scanSortAsc))
        {
            _configService.Current.ScanSortAsc = _scanSortAsc;
            _configService.Save();
        }
        ShowTooltip("Use ascending sort order.");
        ImGui.SameLine();
        if (ImGui.Checkbox("Penumbra Used Only", ref _filterPenumbraUsedOnly))
        {
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
        // Suppress transient loading text to avoid layout flicker near checkboxes
        ImGui.SameLine();
        if (ImGui.Checkbox("Hide non-convertible mods", ref _filterNonConvertibleMods))
        {
            _configService.Current.FilterNonConvertibleMods = _filterNonConvertibleMods;
            _configService.Save();
        }
        ShowTooltip("Hide mods without convertible textures.");

        ImGui.SameLine();
        if (ImGui.Checkbox("Hide mods larger after conversion", ref _filterInefficientMods))
        {
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
                        _loadingEnabledStates = true;
                        _ = _conversionService.GetAllModEnabledStatesAsync(_selectedCollectionId.Value).ContinueWith(es =>
                        {
                            if (es.Status == TaskStatus.RanToCompletion && es.Result != null)
                                _modEnabledStates = es.Result;
                            _loadingEnabledStates = false;
                        });
                    }
                }
            });
        }
        ImGui.Spacing();
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

            // Skip mods that have no convertible textures if the filter is enabled
            if (_filterNonConvertibleMods && files.Count == 0)
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

        // Optional: build nested folder tree when enabled
        TableCatNode? root = null;
        if (_useFolderStructure && _modPaths.Count > 0)
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
            ImGui.TableSetupColumn("File", ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed, _scannedCompressedColWidth);
            ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, _scannedActionColWidth);
            ImGui.TableHeadersRow();
            // Reset zebra row index at the start of table drawing
            _zebraRowIndex = 0;

            int idx = 0;
            if (root != null)
            {
                DrawCategoryTableNode(root, visibleByMod, ref idx, string.Empty, 0);
            }
            else
            {
                foreach (var mod in mods)
                {
                    var files = visibleByMod[mod];

                // Mod row
                ImGui.TableNextRow();
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                ImGui.TableSetColumnIndex(0);
                var hasBackup = GetOrQueryModBackup(mod);
                var excluded = !hasBackup && IsModExcludedByTags(mod);
                bool modSelected = files.All(f => _selectedTextures.Contains(f));
                ImGui.BeginDisabled(excluded);
                if (ImGui.Checkbox($"##modsel-{mod}", ref modSelected))
                {
                    if (modSelected)
                        foreach (var f in files) _selectedTextures.Add(f);
                    else
                        foreach (var f in files) _selectedTextures.Remove(f);

                }
                ImGui.EndDisabled();
                if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                    ImGui.SetTooltip("Mod excluded by tags");
                else
                    ShowTooltip("Toggle selection for all files in this mod.");

                ImGui.TableSetColumnIndex(1);
                var nodeFlags = ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding;
                if (!_configService.Current.ShowModFilesInOverview)
                    nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
                // Compute total size of visible files using cached sizes only
                long modVisibleSize = 0;
                foreach (var f in files)
                {
                    var sz = GetCachedOrComputeSize(f);
                    if (sz > 0) modVisibleSize += sz;
                }
                // Build header using full mod totals (independent of filters)
                int totalAll = _scannedByMod.TryGetValue(mod, out var allModFiles) && allModFiles != null ? allModFiles.Count : files.Count;
                int convertedAll = 0;
                if (hasBackup && totalAll > 0)
                {
                    // Ensure original-size map is hydrated
                    _ = GetOrQueryModOriginalTotal(mod);
                    if (_modPaths.TryGetValue(mod, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
                    {
                        if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null && map.Count > 0 && allModFiles != null)
                        {
                            foreach (var f in allModFiles)
                            {
                                try
                                {
                                    var rel = Path.GetRelativePath(modRoot, f).Replace('\\', '/');
                                    if (map.ContainsKey(rel)) convertedAll++;
                                }
                                catch { }
                            }
                        }
                    }
                    // Fallback to stats count when map not yet available
                    if (convertedAll == 0 && _cachedPerModSavings.TryGetValue(mod, out var s) && s != null && s.ComparedFiles > 0)
                        convertedAll = Math.Min(s.ComparedFiles, totalAll);
                }
                var header = hasBackup
                    ? $"{ResolveModDisplayName(mod)} ({convertedAll}/{totalAll})"
                    : $"{ResolveModDisplayName(mod)} ({totalAll})";
                bool open = ImGui.TreeNodeEx($"##mod-{mod}", nodeFlags);
                if (ImGui.BeginPopupContextItem($"modctx-{mod}"))
                {
                    if (ImGui.MenuItem("Open in Penumbra"))
                    {
                        var display = ResolveModDisplayName(mod);
                        _conversionService.OpenModInPenumbra(mod, display);
                    }
                    ImGui.EndPopup();
                }
                // Show mod icon with enabled state color, same as folder view
                ImGui.SameLine();
                ImGui.PushFont(UiBuilder.IconFont);
                if (_modEnabledStates.TryGetValue(mod, out var stIcon))
                {
                    // Use white when disabled, green when enabled
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
                // If an external change was detected recently or persisted, show a small plug indicator
                var hasPersistent = _configService.Current.ExternalConvertedMods.ContainsKey(mod);
                var showExternal = hasBackup && (((DateTime.UtcNow - _lastExternalChangeAt).TotalSeconds < 30 && !string.IsNullOrEmpty(_lastExternalChangeReason)) || hasPersistent);
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
                if (hasBackup && _cachedPerModSavings.TryGetValue(mod, out var noteStats2) && noteStats2 != null && noteStats2.OriginalBytes > 0 && noteStats2.CurrentBytes > noteStats2.OriginalBytes)
                {
                    ImGui.TextColored(ShrinkUColors.WarningLight, "(This mod is smaller when not converted)");
                }


                // Uncompressed and Compressed columns for mod row
                ImGui.TableSetColumnIndex(3);
                _cachedPerModSavings.TryGetValue(mod, out var modStats);
                long modOriginalBytes = GetOrQueryModOriginalTotal(mod);
                if (modOriginalBytes > 0)
                    DrawRightAlignedSize(modOriginalBytes);
                else
                    DrawRightAlignedSize(modVisibleSize);

                ImGui.TableSetColumnIndex(2);
                var modCurrentBytes = hasBackup ? (modStats?.CurrentBytes ?? 0) : 0; // force 0 when no backup
                if (modCurrentBytes <= 0)
                {
                    // After restore or when no compressed stats exist, show '-'
                    DrawRightAlignedTextColored("-", _compressedTextColor);
                }
                else
                {
                    var color = modCurrentBytes > modOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
                    DrawRightAlignedSizeColored(modCurrentBytes, color);
                }

                // Action column
                ImGui.TableSetColumnIndex(4);
                ImGui.BeginDisabled(_running || _conversionService.IsConverting);
                // hasBackup already computed above
                if (hasBackup || files.Count > 0)
                {
                    var actionLabel = hasBackup ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}";

                    // Distinguish button colors for Convert vs Restore
                    if (hasBackup)
                    {
                        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                    }
                    else
                    {
                        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ConvertButton);
                        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ConvertButtonHovered);
                        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ConvertButtonActive);
                        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                    }

                    ImGui.BeginDisabled(excluded);
                    if (ImGui.Button(actionLabel))
                    {
                        _running = true;
                        if (hasBackup)
                        {
                            _modsWithBackupCache.TryRemove(mod, out _);
                            // Reset progress state for restore and initialize current restore mod
                            ResetConversionProgress();
                            ResetRestoreProgress();
                            _currentRestoreMod = mod;
                            var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                            _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                .ContinueWith(t => {
                                    var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                    try { _backupService.RedrawPlayer(); } catch { }
                                    _logger.LogDebug("Heavy scan triggered: restore completed (mod button in mod-only view, async)");
                                    RefreshScanResults(true, "restore-mod-only-view-async");
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
                                            _modsWithBackupCache[mod] = bt.Result;
                                    });
                                    if (success)
                                    {
                                        try
                                        {
                                            if (_configService.Current.ExternalConvertedMods.Remove(mod))
                                                _configService.Save();
                                        }
                                        catch { }
                                    }
                                    _running = false;
                                });
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
                            var toConvert = allFilesForMod.ToDictionary(f => f, f => Array.Empty<string>(), StringComparer.Ordinal);
                            // Reset progress state for a fresh conversion
                            ResetConversionProgress();
                            ResetRestoreProgress();
                            _ = _conversionService.StartConversionAsync(toConvert)
                                .ContinueWith(_ =>
                                {
                                    // After conversion completes, re-check backup availability and update cache
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                            _modsWithBackupCache[mod] = bt.Result;
                                    });
                                    TriggerMetricsRefresh();
                                    _running = false;
                                });
                        }
                    }
                    ImGui.PopStyleColor(1);
                    if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        ImGui.SetTooltip("Mod excluded by tags");
                    else
                    {
                        if (hasBackup)
                            ShowTooltip("Restore backups for this mod.");
                        else
                        {
                            var msg = _configService.Current.IncludeHiddenModTexturesOnConvert
                                ? "Convert all textures for this mod."
                                : "Convert all visible textures for this mod.";
                            ShowTooltip(msg);
                        }
                    }
                    ImGui.EndDisabled();
                    ImGui.PopStyleColor(3);
                }
                ImGui.EndDisabled();

                if (open)
                {
                    // Warm up original-size map for this mod when expanded
                    if (!_cachedPerModOriginalSizes.ContainsKey(mod) && !_perModOriginalSizesTasks.ContainsKey(mod))
                    {
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

                    if (_configService.Current.ShowModFilesInOverview)
                    {
                        // Child file rows
                        foreach (var file in files)
                        {
                            ImGui.TableNextRow();
                            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                            ImGui.TableSetColumnIndex(0);
                            bool selected = _selectedTextures.Contains(file);
                            ImGui.BeginDisabled(excluded);
                            if (ImGui.Checkbox($"##selsub-{idx}", ref selected))
                            {
                                if (selected) { _selectedTextures.Add(file); }
                                else _selectedTextures.Remove(file);
                            }
                            ImGui.EndDisabled();
                            if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                                ImGui.SetTooltip("Mod excluded by tags");
                            else
                                ShowTooltip("Select or deselect this file.");

                            ImGui.TableSetColumnIndex(1);
                            var baseName = Path.GetFileName(file);
                            ImGui.TextUnformatted(baseName);
                            if (ImGui.IsItemHovered())
                                ImGui.SetTooltip(file);

                            // Compressed column: show '-' when no backup; otherwise current file size
                            ImGui.TableSetColumnIndex(2);
                            if (hasBackup)
                            {
                                var currentSize = GetCachedOrComputeSize(file);
                                DrawRightAlignedSize(currentSize);
                            }
                            else
                            {
                                DrawRightAlignedTextColored("-", _compressedTextColor);
                            }

                            // Uncompressed column: show original size when backup exists; otherwise visible current size
                            ImGui.TableSetColumnIndex(3);
                            long originalSize = 0;
                            if (hasBackup)
                            {
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
                            else
                            {
                                var currentSize = GetCachedOrComputeSize(file);
                                if (currentSize > 0)
                                    DrawRightAlignedSize(currentSize);
                                else
                                    ImGui.TextUnformatted("");
                            }

                            idx++;
                        }
                    }
                    ImGui.TreePop();
                }
                }
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
            long totalUncompressed = 0;
            long totalCompressed = 0;
            long savedBytes = 0;
            bool showFiles = _configService.Current.ShowModFilesInOverview;
            foreach (var m in mods)
            {
                var hasBackupM = GetOrQueryModBackup(m);
                visibleByMod.TryGetValue(m, out var files);
                if (showFiles)
                {
                    long perFileUncompressed = 0;
                    long perFileCompressed = 0;
                    long perFileSaved = 0;
                    bool anyFileOrigShown = false;
                    if (files != null)
                    {
                        foreach (var file in files)
                        {
                            if (hasBackupM)
                            {
                                long orig = 0;
                                if (_modPaths.TryGetValue(m, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
                                {
                                    try
                                    {
                                        var rel = Path.GetRelativePath(modRoot, file).Replace('\\', '/');
                                        if (_cachedPerModOriginalSizes.TryGetValue(m, out var map) && map != null)
                                            map.TryGetValue(rel, out orig);
                                    }
                                    catch { }
                                }
                                if (orig > 0)
                                {
                                    perFileUncompressed += orig;
                                    anyFileOrigShown = true;
                                }
                                var cur = GetCachedOrComputeSize(file);
                                if (cur > 0)
                                {
                                    perFileCompressed += cur;
                                    if (orig > 0)
                                        perFileSaved += Math.Max(0, orig - cur);
                                }
                            }
                            else
                            {
                                var cur = GetCachedOrComputeSize(file);
                                if (cur > 0) perFileUncompressed += cur;
                            }
                        }
                    }

                    if (anyFileOrigShown || !hasBackupM)
                    {
                        // File rows reflect uncompressed values; use them directly
                        totalUncompressed += perFileUncompressed;
                        totalCompressed += perFileCompressed;
                        savedBytes += perFileSaved;
                    }
                    else
                    {
                        long modVisibleSize = 0;
                        if (files != null)
                        {
                            foreach (var f in files)
                            {
                                var s = GetCachedOrComputeSize(f);
                                if (s > 0) modVisibleSize += s;
                            }
                        }
                        var modOrig = GetOrQueryModOriginalTotal(m);
                        if (modOrig > 0) totalUncompressed += modOrig; else totalUncompressed += modVisibleSize;

                        long modCur = 0;
                        if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null)
                        {
                            if (stats.CurrentBytes > 0)
                                modCur = stats.CurrentBytes;
                            if (stats.OriginalBytes > 0 && stats.CurrentBytes > 0)
                                savedBytes += Math.Max(0, stats.OriginalBytes - stats.CurrentBytes);
                        }
                        if (modCur > 0) totalCompressed += modCur;
                    }
                }
                else
                {
                    long modVisibleSize = 0;
                    if (files != null)
                    {
                        foreach (var f in files)
                        {
                            var s = GetCachedOrComputeSize(f);
                            if (s > 0) modVisibleSize += s;
                        }
                    }
                    var modOrig = GetOrQueryModOriginalTotal(m);
                    if (modOrig > 0) totalUncompressed += modOrig; else totalUncompressed += modVisibleSize;

                    long modCur = 0;
                    if (hasBackupM && _cachedPerModSavings.TryGetValue(m, out var stats) && stats != null)
                    {
                        if (stats.CurrentBytes > 0)
                            modCur = stats.CurrentBytes;
                        if (stats.OriginalBytes > 0 && stats.CurrentBytes > 0)
                            savedBytes += Math.Max(0, stats.OriginalBytes - stats.CurrentBytes);
                    }
                    if (modCur > 0) totalCompressed += modCur;
                }
            }

            var footerFlags = ImGuiTableFlags.BordersOuter | ImGuiTableFlags.BordersV | ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingFixedFit;
            if (ImGui.BeginTable("ScannedFilesTotals", 5, footerFlags))
            {
                ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, _scannedFirstColWidth);
                ImGui.TableSetupColumn("File", ImGuiTableColumnFlags.WidthStretch);
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

                ImGui.BeginDisabled(_running || _conversionService.IsConverting || hasOnlyRestorableMods || !hasConvertableMods);
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        if (ImGui.Button("Backup and Convert"))
        {
            _running = true;
            var toConvert = GetConvertableTextures();
            // Reset progress state for a fresh conversion
            ResetConversionProgress();
            ResetRestoreProgress();
            _ = _conversionService.StartConversionAsync(toConvert);
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

                ImGui.BeginDisabled(_running || _conversionService.IsConverting || !canRestore);
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        if (ImGui.Button("Restore Backups"))
        {
            _running = true;
            // Reset progress state before bulk restore
            ResetConversionProgress();
            ResetRestoreProgress();
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
                        await _backupService.RestoreLatestForModAsync(mod, progress, restoreToken);
                    }
                    catch { }
                }
            }).ContinueWith(_ =>
            {
                try { _backupService.RedrawPlayer(); } catch { }
                _logger.LogDebug("Heavy scan triggered: restore completed (bulk action)");
                RefreshScanResults(true, "restore-bulk");
                TriggerMetricsRefresh();
                // Recompute per-mod savings after bulk restore to refresh Uncompressed/Compressed sizes
                _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                _perModSavingsTask.ContinueWith(ps =>
                {
                    if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                    {
                        _cachedPerModSavings = ps.Result;
                        _needsUIRefresh = true;
                    }
                }, TaskScheduler.Default);
                // Re-check backup availability for each restored mod and update cache
                foreach (var m in restorableFiltered)
                {
                    _ = _backupService.HasBackupForModAsync(m).ContinueWith(bt =>
                    {
                        if (bt.Status == TaskStatus.RanToCompletion)
                            _modsWithBackupCache[m] = bt.Result;
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
        if (_modsWithBackupCache.TryGetValue(mod, out var has))
            return has;
        try
        {
            // Perform a synchronous check to avoid race conditions enabling conversion before backup status is known
            var res = _backupService.HasBackupForModAsync(mod).GetAwaiter().GetResult();
            _modsWithBackupCache[mod] = res;
            return res;
        }
        catch { return false; }
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
                string ownerMod = null;
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
            // Find which mod this texture belongs to
            foreach (var (mod, files) in _scannedByMod)
            {
                if (files.Contains(texture))
                {
                    selectedMods.Add(mod);
                    break;
                }
            }
        }
        
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
            // Find which mod this texture belongs to
            string ownerMod = null;
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
    }
}