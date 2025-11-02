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

    private string _scanFilter = string.Empty;
    private float _leftPanelWidthRatio = 0.45f;
    private float _leftPanelWidthPx = 0f;
    private bool _leftWidthInitialized = false;
    private bool _leftWidthDirty = false;
    private float _scannedFirstColWidth = 28f;
    private float _scannedSizeColWidth = 85f;
    private float _scannedActionColWidth = 60f;
    private ScanSortKind _scanSortKind = ScanSortKind.ModName;
    private bool _scanSortAsc = true;
    private bool _initialScanQueued = false;
    private bool _filterPenumbraUsedOnly = false;
    private bool _filterNonConvertibleMods = true;
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
    
    // UI refresh flag to force ImGui redraw after async data updates
    private volatile bool _needsUIRefresh = false;
    // Snapshot of known Penumbra mod folders to guard heavy scans
    private HashSet<string> _knownModFolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    // Debounce sequence to ensure trailing-edge execution for mod add/delete
    private int _modsChangedDebounceSeq = 0;
    // Timestamp of last heavy scan to rate-limit repeated rescans
    private DateTime _lastHeavyScanAt = DateTime.MinValue;

    // Stored delegate references for clean unsubscription on dispose
    private readonly Action<(string, int)> _onConversionProgress;
    private readonly Action<(string, int, int)> _onBackupProgress;
    private readonly Action _onConversionCompleted;
    private readonly Action<(string modName, int current, int total, int fileTotal)> _onModProgress;
    private readonly Action _onPenumbraModsChanged;
    private readonly Action<string> _onPenumbraModAdded;
    private readonly Action<string> _onPenumbraModDeleted;
    private readonly Action<Penumbra.Api.Enums.ModSettingChange, Guid, string, bool> _onPenumbraModSettingChanged;

    public ConversionUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, TextureBackupService backupService)
        : base("ShrinkU###ShrinkUConversionUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _backupService = backupService;

        // Diagnostic marker to confirm updated ConversionUI is loaded and running
        _logger.LogDebug("ConversionUI initialized: DIAG-v2");

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
            // Ensure action buttons reflect backup availability immediately after conversion
            _modsWithBackupCache.Clear();
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
        };
        _conversionService.OnPenumbraModsChanged += _onPenumbraModsChanged;

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
        };
        _conversionService.OnPenumbraModSettingChanged += _onPenumbraModSettingChanged;

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

        // Initialize persisted UI settings from config
        _useFolderStructure = _configService.Current.UseFolderStructure;
        _filterPenumbraUsedOnly = _configService.Current.FilterPenumbraUsedOnly;
        _filterNonConvertibleMods = _configService.Current.FilterNonConvertibleMods;
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
        try { _conversionService.OnPenumbraModSettingChanged -= _onPenumbraModSettingChanged; } catch { }

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

        // Drain any queued UI actions
        try { while (_uiThreadActions.TryDequeue(out _)) { } } catch { }

        try { _logger.LogDebug("ConversionUI disposed: DIAG-v3"); } catch { }
    }

    private void DrawSettings()
    {
        // Render general settings directly without tabs. Extras were moved to SettingsUI.
        ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Texture Settings");
        var mode = _configService.Current.TextureProcessingMode;
        if (ImGui.BeginCombo("Mode", mode.ToString()))
        {
            if (ImGui.Selectable("Manual", mode == TextureProcessingMode.Manual))
            {
                _configService.Current.TextureProcessingMode = TextureProcessingMode.Manual;
                _configService.Save();
            }
            if (ImGui.Selectable("Automatic", mode == TextureProcessingMode.Automatic))
            {
                _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                _configService.Save();
            }
            ImGui.EndCombo();
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
    }

    // Nested folder tree for Table View
    private sealed class TableCatNode
    {
        public string Name { get; }
        public Dictionary<string, TableCatNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<string> Mods { get; } = new();
        public TableCatNode(string name) => Name = name;
    }

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

private void DrawCategoryTableNode(TableCatNode node, Dictionary<string, List<string>> visibleByMod, ref int idx, string pathPrefix, int depth = 0)
    {
        const float indentStep = 16f;
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            ImGui.TableNextRow();
            ImGui.TableSetColumnIndex(1);
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            // Indent folder rows in the File column based on depth, without affecting other columns.
            ImGui.SetCursorPosX(ImGui.GetCursorPosX() + depth * indentStep);
            var catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
            ImGui.SameLine();
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextUnformatted((catOpen ? FontAwesomeIcon.FolderOpen : FontAwesomeIcon.Folder).ToIconString());
            ImGui.PopFont();
            ImGui.SameLine();
            ImGui.TextUnformatted($"{name} ({CountModsRecursive(child)})");

            if (catOpen)
            {
                DrawCategoryTableNode(child, visibleByMod, ref idx, fullPath, depth + 1);

                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.TryGetValue(mod, out var files))
                        continue;

                    ImGui.TableNextRow();
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
                    long modVisibleSize = 0;
                    foreach (var f in files)
                    {
                        var sz = GetCachedOrComputeSize(f);
                        if (sz > 0) modVisibleSize += sz;
                    }
                    var header = $"{ResolveModDisplayName(mod)} ({files.Count})";
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

                    // Removed status label text; icon color indicates enabled state

                    ImGui.TableSetColumnIndex(2);
                    DrawRightAlignedSize(modVisibleSize);

                    ImGui.TableSetColumnIndex(3);
                    ImGui.BeginDisabled(_running);
                    if (hasBackup || files.Count > 0)
                    {
                        var actionLabel = hasBackup ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}";

                        if (hasBackup)
                        {
                            ImGui.PushStyleColor(ImGuiCol.Button, new Vector4(0.90f, 0.65f, 0.25f, 1f));
                            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, new Vector4(0.95f, 0.75f, 0.35f, 1f));
                            ImGui.PushStyleColor(ImGuiCol.ButtonActive, new Vector4(0.85f, 0.55f, 0.20f, 1f));
                        }
                        else
                        {
                            ImGui.PushStyleColor(ImGuiCol.Button, new Vector4(0.25f, 0.80f, 0.35f, 1f));
                            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, new Vector4(0.30f, 0.90f, 0.45f, 1f));
                            ImGui.PushStyleColor(ImGuiCol.ButtonActive, new Vector4(0.20f, 0.70f, 0.30f, 1f));
                        }

                        ImGui.BeginDisabled(excluded);
                        if (ImGui.Button(actionLabel))
                        {
                            _running = true;
                            if (hasBackup)
                            {
                                _modsWithBackupCache.TryRemove(mod, out _);
                                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                                _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                    .ContinueWith(_ => {
                                        try { _backupService.RedrawPlayer(); } catch { }
                                        _logger.LogDebug("Heavy scan triggered: restore completed (mod button in folder view)");
                                        RefreshScanResults(true, "restore-folder-view");
                                        TriggerMetricsRefresh();
                                        _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                        {
                                            if (bt.Status == TaskStatus.RanToCompletion)
                                                _modsWithBackupCache[mod] = bt.Result;
                                        });
                                        _running = false;
                                    });
                            }
                            else
                            {
                                var toConvert = files.ToDictionary(f => f, f => Array.Empty<string>(), StringComparer.Ordinal);
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
                        if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                            ImGui.SetTooltip("Mod excluded by tags");
                        else
                            ShowTooltip(hasBackup ? "Restore backups for this mod." : "Convert all visible textures for this mod.");
                        ImGui.PopStyleColor(3);
                        ImGui.EndDisabled();
                    }
                    ImGui.EndDisabled();

                    if (open)
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
                            var fileSize = GetCachedOrComputeSize(file);
                            DrawRightAlignedSize(fileSize);

                            ImGui.TableSetColumnIndex(3);
                            idx++;
                        }
                    }
                }
            }
        }

    }

    private void DrawActions()
    {
        ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Actions");
        ImGui.BeginDisabled(_running);
        if (ImGui.Button("Scan All Mod Textures"))
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
        // (Buttons moved below the Scanned Files Overview table)

        if (!_running)
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
            }

            // Display stats
            ImGui.Text($"Mods: {totalMods} | Restorable: {restorableModsCount} | Converted: {convertedModsCount} | Convertible: {convertibleModsCount}");
            ImGui.Text($"Textures: {_texturesToConvert.Count} queued | Selected: {_selectedTextures.Count}");

            if (_cachedBackupStorageInfo.fileCount > 0)
            {
                var sizeInMB = _cachedBackupStorageInfo.totalSize / (1024.0 * 1024.0);
                ImGui.Text($"Backups: {_cachedBackupStorageInfo.fileCount} files, {sizeInMB:F2} MB");
            }
            else
            {
                ImGui.Text("Backups: none");
            }

            if (shouldUpdateMetrics)
            {
                if (_savingsInfoTask == null)
                {
                    _savingsInfoTask = _backupService.ComputeSavingsAsync();
                }
                if (_savingsInfoTask != null && _savingsInfoTask.IsCompleted)
                {
                    _cachedSavingsInfo = _savingsInfoTask.Result ?? new TextureBackupService.BackupSavingsStats();
                }
            }

            if (_cachedSavingsInfo.ComparedFiles > 0)
            {
                var originalMB = _cachedSavingsInfo.OriginalTotalBytes / (1024.0 * 1024.0);
                var currentMB = _cachedSavingsInfo.CurrentTotalBytes / (1024.0 * 1024.0);
                var savedMB = Math.Max(0.0, (originalMB - currentMB));
                var pct = _cachedSavingsInfo.OriginalTotalBytes > 0
                    ? (100.0 * Math.Max(0.0, (_cachedSavingsInfo.OriginalTotalBytes - _cachedSavingsInfo.CurrentTotalBytes)) / _cachedSavingsInfo.OriginalTotalBytes)
                    : 0.0;
                ImGui.Text($"Savings: {savedMB:F2} MB ({pct:F1}%) over {_cachedSavingsInfo.ComparedFiles} files");
                if (_cachedSavingsInfo.MissingCurrentFiles > 0)
                {
                    ImGui.Text($"Missing current files: {_cachedSavingsInfo.MissingCurrentFiles}");
                }
            }
            else
            {
                ImGui.Text("Savings: no comparable backups found");
            }
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

            if (ImGui.Button("Cancel"))
            {
                _conversionService.Cancel();
                _restoreCancellationTokenSource?.Cancel();
            }
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
                        _modTags = tags.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.OrdinalIgnoreCase);
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
                    _scannedByMod.Clear();
                    _selectedTextures.Clear();
                    _texturesToConvert.Clear();
                    _fileSizeCache.Clear();
                    if (grouped != null)
                    {
                        foreach (var mod in grouped)
                        {
                            _scannedByMod[mod.Key] = mod.Value;
                            foreach (var file in mod.Value)
                                _texturesToConvert[file] = Array.Empty<string>();
                        }
                    }
                    else
                    {
                        // Ensure known folders exist even when skipping heavy scan
                        if (folders != null)
                        {
                            foreach (var folder in folders)
                            {
                                if (!_scannedByMod.ContainsKey(folder))
                                    _scannedByMod[folder] = new List<string>();
                            }
                        }
                        _logger.LogDebug("UI state updated (DIAG-v3): origin={origin} heavy scan skipped", origin);
                    }

                    if (grouped != null)
                        _logger.LogDebug("Refreshed scan (DIAG-v3): origin={origin} mods={mods} textures={files}", origin, _scannedByMod.Count, _texturesToConvert.Count);

                    if (names != null)
                        _modDisplayNames = names;
                    if (tags != null)
                        _modTags = tags.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.OrdinalIgnoreCase);
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

        // Overall mods progress: completed mods plus fraction of current mod
        if (_totalMods > 0)
        {
            var completedMods = Math.Max(_currentModIndex - 1, 0);
            var currentModFraction = _currentModTotalFiles > 0 ? (float)_convertedCount / _currentModTotalFiles : 0f;
            var overallFraction = Math.Clamp(((float)completedMods + currentModFraction) / _totalMods, 0f, 1f);
            ImGui.Text($"Overall Mods: {_currentModIndex}/{_totalMods}");
            ImGui.ProgressBar(overallFraction, barSize, string.Empty);
        }

        // Current mod progress bar
        ImGui.Text($"Current Mod: {_currentModName}");
        var modFraction = _currentModTotalFiles > 0 ? (float)_convertedCount / _currentModTotalFiles : 0f;
        ImGui.ProgressBar(modFraction, barSize, _currentModTotalFiles > 0 ? $"{_convertedCount}/{_currentModTotalFiles}" : string.Empty);

        // Current file name
        ImGui.Text($"Current File: {_currentTexture}");
        if (_backupTotal > 0)
        {
            ImGui.Text($"Backup: {_backupIndex}/{_backupTotal}");
        }
        ImGui.Text($"Converted (mod): {_convertedCount}");
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
        ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Scanned Files Overview");
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
        if (_loadingPenumbraUsed)
        {
            ImGui.SameLine();
            ImGui.Text("(Loading used...)");
        }
        ImGui.SameLine();
        if (ImGui.Checkbox("Hide non-convertible mods", ref _filterNonConvertibleMods))
        {
            _configService.Current.FilterNonConvertibleMods = _filterNonConvertibleMods;
            _configService.Save();
        }
        ShowTooltip("Hide mods without convertible textures.");

        // Collections dropdown
        if (!_loadingCollections && _collections.Count == 0)
        {
            _loadingCollections = true;
            _ = _conversionService.GetCollectionsAsync().ContinueWith(ct =>
            {
                if (ct.Status == TaskStatus.RanToCompletion && ct.Result != null)
                    _collections = ct.Result;
                _loadingCollections = false;
            });
            _ = _conversionService.GetCurrentCollectionAsync().ContinueWith(cc =>
            {
                if (cc.Status == TaskStatus.RanToCompletion && cc.Result != null)
                {
                    _selectedCollectionId = cc.Result?.Id;
                    // Auto-load enabled states for the current collection so icons/status work without manual switch
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
        ImGui.Text("Collection:");
        ImGui.SameLine();
        var entries = _collections.Select(kv => (kv.Key, kv.Value)).OrderBy(e => e.Value).ToList();
        int selIndex = -1;
        for (int i = 0; i < entries.Count; i++)
        {
            if (_selectedCollectionId.HasValue && entries[i].Key == _selectedCollectionId.Value)
            {
                selIndex = i;
                break;
            }
        }
        var currentLabel = selIndex >= 0 ? entries[selIndex].Value : "(none)";
        ImGui.SetNextItemWidth(200f);
        if (ImGui.BeginCombo("##collectionSelect", currentLabel))
        {
            for (int i = 0; i < entries.Count; i++)
            {
                bool selected = i == selIndex;
                if (ImGui.Selectable(entries[i].Value, selected))
                {
                    _selectedCollectionId = entries[i].Key;
                    // Load enabled states for selected collection
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
                if (selected)
                    ImGui.SetItemDefaultFocus();
            }
            ImGui.EndCombo();
        }
        ShowTooltip("Select the Penumbra collection to evaluate enabled states.");
        if (_loadingCollections || _loadingEnabledStates)
        {
            ImGui.SameLine();
            ImGui.Text("(Loading collections...)");
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
        float reserveH = frameH + ImGui.GetStyle().ItemSpacing.Y * 4; // buttons + spacing
        float childH = MathF.Max(150f, availY - reserveH);
        ImGui.BeginChild("ScannedFilesTableRegion", new Vector2(0, childH), false, ImGuiWindowFlags.None);

        var flags = ImGuiTableFlags.BordersOuter | ImGuiTableFlags.BordersV | ImGuiTableFlags.Resizable | ImGuiTableFlags.ScrollY;
        if (ImGui.BeginTable("ScannedFilesTable", 4, flags))
        {
            ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, _scannedFirstColWidth);
            ImGui.TableSetupColumn("File", ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn("Size", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, _scannedActionColWidth);
            ImGui.TableHeadersRow();

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
                // Compute total size of visible files using cached sizes only
                long modVisibleSize = 0;
                foreach (var f in files)
                {
                    var sz = GetCachedOrComputeSize(f);
                    if (sz > 0) modVisibleSize += sz;
                }
                var header = $"{ResolveModDisplayName(mod)} ({files.Count})";
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

                // Action column button (either Convert or Restore)
                // Size column for mod row
                ImGui.TableSetColumnIndex(2);
                DrawRightAlignedSize(modVisibleSize);
                // Action column
                ImGui.TableSetColumnIndex(3);
                ImGui.BeginDisabled(_running);
                // hasBackup already computed above
                if (hasBackup || files.Count > 0)
                {
                    var actionLabel = hasBackup ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}";

                    // Distinguish button colors for Convert vs Restore
                    if (hasBackup)
                    {
                        ImGui.PushStyleColor(ImGuiCol.Button, new Vector4(0.90f, 0.65f, 0.25f, 1f));
                        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, new Vector4(0.95f, 0.75f, 0.35f, 1f));
                        ImGui.PushStyleColor(ImGuiCol.ButtonActive, new Vector4(0.85f, 0.55f, 0.20f, 1f));
                    }
                    else
                    {
                        ImGui.PushStyleColor(ImGuiCol.Button, new Vector4(0.25f, 0.80f, 0.35f, 1f));
                        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, new Vector4(0.30f, 0.90f, 0.45f, 1f));
                        ImGui.PushStyleColor(ImGuiCol.ButtonActive, new Vector4(0.20f, 0.70f, 0.30f, 1f));
                    }

                    ImGui.BeginDisabled(excluded);
                    if (ImGui.Button(actionLabel))
                    {
                        _running = true;
                        if (hasBackup)
                        {
                            _modsWithBackupCache.TryRemove(mod, out _);
                            var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                            _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                .ContinueWith(_ => {
                                    try { _backupService.RedrawPlayer(); } catch { }
                                    _logger.LogDebug("Heavy scan triggered: restore completed (mod button in mod-only view, async)");
                                    RefreshScanResults(true, "restore-mod-only-view-async");
                                    TriggerMetricsRefresh();
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                            _modsWithBackupCache[mod] = bt.Result;
                                    });
                                    _running = false;
                                });
                        }
                        else
                        {
                            var toConvert = files.ToDictionary(f => f, f => Array.Empty<string>(), StringComparer.Ordinal);
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
                    if (excluded && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        ImGui.SetTooltip("Mod excluded by tags");
                    else
                        ShowTooltip(hasBackup ? "Restore backups for this mod." : "Convert all visible textures for this mod.");
                    ImGui.EndDisabled();
                    ImGui.PopStyleColor(3);
                }
                ImGui.EndDisabled();

                if (open)
                {
                    // Child file rows
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
                        var baseName = Path.GetFileName(file);
                        ImGui.TextUnformatted(baseName);
                        if (ImGui.IsItemHovered())
                            ImGui.SetTooltip(file);

                        // Per-file size
                        ImGui.TableSetColumnIndex(2);
                        var fileSize = GetCachedOrComputeSize(file);
                        DrawRightAlignedSize(fileSize);

                        // Leave action column empty for file rows
                        ImGui.TableSetColumnIndex(3);
                        
                        idx++;
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

            ImGui.TableSetColumnIndex(2);
            var currentSizeWidth = ImGui.GetColumnWidth();
            if (MathF.Abs(currentSizeWidth - _scannedSizeColWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _scannedSizeColWidth = currentSizeWidth;
                _configService.Current.ScannedFilesSizeColWidth = currentSizeWidth;
                _configService.Save();
                _logger.LogDebug($"Saved size column width: {currentSizeWidth}px");
            }

            ImGui.TableSetColumnIndex(3);
            var currentActionWidth = ImGui.GetColumnWidth();
            if (MathF.Abs(currentActionWidth - _scannedActionColWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _scannedActionColWidth = currentActionWidth;
                _configService.Current.ScannedFilesActionColWidth = currentActionWidth;
                _configService.Save();
                _logger.LogDebug($"Saved action column width: {currentActionWidth}px");
            }

            ImGui.EndTable();
        }
        ImGui.EndChild();

        // Global action buttons below the table
        ImGui.Spacing();
        var (convertableMods, restorableMods) = GetSelectedModStates();
        bool hasConvertableMods = convertableMods > 0;
        bool hasRestorableMods = restorableMods > 0;
        bool hasOnlyRestorableMods = hasRestorableMods && !hasConvertableMods;

        ImGui.BeginDisabled(_running || hasOnlyRestorableMods || !hasConvertableMods);
        if (ImGui.Button("Backup and Convert"))
        {
            _running = true;
            var toConvert = GetConvertableTextures();
            _ = _conversionService.StartConversionAsync(toConvert);
        }
        ImGui.EndDisabled();

        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (hasOnlyRestorableMods)
                ImGui.SetTooltip("Only mods with backups are selected. Use 'Restore Backups' instead.");
            else if (!hasConvertableMods)
                ImGui.SetTooltip("No selected textures available to convert.");
        }

        ImGui.SameLine();

        var restorableModsForAction = GetRestorableModsForCurrentSelection();
        bool canRestore = restorableModsForAction.Count > 0;

        ImGui.BeginDisabled(_running || !canRestore);
        if (ImGui.Button("Restore Backups"))
        {
            _running = true;
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
                foreach (var mod in restorableModsForAction)
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
                // Re-check backup availability for each restored mod and update cache
                foreach (var m in restorableModsForAction)
                {
                    _ = _backupService.HasBackupForModAsync(m).ContinueWith(bt =>
                    {
                        if (bt.Status == TaskStatus.RanToCompletion)
                            _modsWithBackupCache[m] = bt.Result;
                    });
                }
                _running = false;
                _restoreCancellationTokenSource?.Dispose();
                _restoreCancellationTokenSource = null;
                _currentRestoreMod = string.Empty;
                _currentRestoreModIndex = 0;
                _currentRestoreModTotal = 0;
            });
        }
        ImGui.EndDisabled();

        if (!canRestore && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            ImGui.SetTooltip("No selected mods have backups to restore.");
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

    // Removed mod-level size aggregation to focus only on file sizes per request

    private bool GetOrQueryModBackup(string mod)
    {
        if (_modsWithBackupCache.TryGetValue(mod, out var has))
            return has;
        if (_modsBackupCheckInFlight.ContainsKey(mod))
            return false;
        _modsBackupCheckInFlight.TryAdd(mod, 1);
        _ = _backupService.HasBackupForModAsync(mod).ContinueWith(t =>
        {
            if (t.Status == TaskStatus.RanToCompletion)
            {
                _modsWithBackupCache[mod] = t.Result;
            }
            _modsBackupCheckInFlight.TryRemove(mod, out _);
        }, TaskScheduler.Default);
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