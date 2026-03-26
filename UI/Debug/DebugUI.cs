using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Helpers;
using ShrinkU.Services;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Diagnostics;
using System.Threading.Tasks;

namespace ShrinkU.UI;

public sealed class DebugUI : Window
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly DebugTraceService _debugTrace;
    private readonly PenumbraFolderWatcherService _penumbraFolderWatcher;
    private readonly BackupFolderWatcherService _backupFolderWatcher;
    private readonly TextureConversionService _conversionService;
    private readonly ModStateService _modStateService;
    private string _actionFilter = string.Empty;
    private bool _penumbraModListLoading = false;
    private Dictionary<string, string> _penumbraModPaths = new(StringComparer.OrdinalIgnoreCase);
    private string _penumbraModSelected = string.Empty;
    private bool _penumbraModDataLoading = false;
    private string _penumbraModDataError = string.Empty;
    private PenumbraModDebugSnapshot? _penumbraModData;
    private int _penumbraModLoadVersion = 0;
    private readonly Queue<SystemPerfSample> _systemPerfSamples = new();
    private DateTime _lastSystemPerfSampleUtc = DateTime.MinValue;
    private TimeSpan _lastProcessCpuTime = TimeSpan.Zero;
    private bool _systemPerfInitialized = false;
    private readonly int _gcGen0Baseline;
    private readonly int _gcGen1Baseline;
    private readonly int _gcGen2Baseline;

    private sealed class SystemPerfSample
    {
        public DateTime AtUtc { get; init; }
        public double CpuPercent { get; init; }
        public long WorkingSetBytes { get; init; }
        public long PrivateBytes { get; init; }
        public long GcHeapBytes { get; init; }
        public int ThreadCount { get; init; }
    }

    private sealed class PenumbraModDebugSnapshot
    {
        public bool ApiAvailable { get; init; }
        public string ModDirectory { get; init; } = string.Empty;
        public string PenumbraRoot { get; init; } = string.Empty;
        public string PenumbraPath { get; init; } = string.Empty;
        public string DisplayName { get; init; } = string.Empty;
        public string MetadataSource { get; init; } = string.Empty;
        public string? Author { get; init; }
        public string? Version { get; init; }
        public string? Description { get; init; }
        public string? Website { get; init; }
        public Guid? CollectionId { get; init; }
        public string CollectionName { get; init; } = string.Empty;
        public bool? Enabled { get; init; }
        public int? Priority { get; init; }
        public bool? Inherited { get; init; }
        public bool? Temporary { get; init; }
        public List<string> Tags { get; init; } = new();
        public DateTime LoadedAtUtc { get; init; }
    }

    public DebugUI(ILogger logger, ShrinkUConfigService configService, DebugTraceService debugTrace, PenumbraFolderWatcherService penumbraFolderWatcher, BackupFolderWatcherService backupFolderWatcher, TextureConversionService conversionService, ModStateService modStateService)
        : base("ShrinkU Debug###ShrinkUDebugUI")
    {
        _logger = logger;
        _configService = configService;
        _debugTrace = debugTrace;
        _penumbraFolderWatcher = penumbraFolderWatcher;
        _backupFolderWatcher = backupFolderWatcher;
        _conversionService = conversionService;
        _modStateService = modStateService;
        _gcGen0Baseline = GC.CollectionCount(0);
        _gcGen1Baseline = GC.CollectionCount(1);
        _gcGen2Baseline = GC.CollectionCount(2);

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(720, 360),
            MaximumSize = new Vector2(1920, 1080),
        };
    }

    public override void Draw()
    {
        UiHeader.DrawAccentHeaderBar();

        if (ImGui.BeginTabBar("DebugTabs"))
        {
            if (ImGui.BeginTabItem("Trace"))
            {
                DrawTraceTab();
                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem("Penumbra Watcher"))
            {
                DrawPenumbraWatcherTab();
                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem("Backup Watcher"))
            {
                DrawBackupWatcherTab();
                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem("Storage"))
            {
                DrawStorageTab();
                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem("System"))
            {
                DrawSystemTab();
                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem("Penumbra Mod Debug"))
            {
                DrawPenumbraModDebugTab();
                ImGui.EndTabItem();
            }
            ImGui.EndTabBar();
        }
    }

    private void DrawTraceTab()
    {
        var avail = ImGui.GetContentRegionAvail();
        var leftWidth = Math.Max(200f, avail.X * 0.5f - 4f);
        var rightWidth = Math.Max(200f, avail.X - leftWidth - 8f);

        ImGui.TextColored(ShrinkUColors.Accent, "Mod-State Updates");
        ImGui.SameLine();
        ImGui.SetCursorPosX(avail.X * 0.5f + 8f);
        ImGui.TextColored(ShrinkUColors.Accent, "UI Refreshes");

        var leftSize = new Vector2(leftWidth, avail.Y - ImGui.GetTextLineHeight() - 12f);
        var rightSize = new Vector2(rightWidth, avail.Y - ImGui.GetTextLineHeight() - 12f);

        ImGui.BeginChild("##modstate-panel", leftSize, true);
        DrawList(_debugTrace.SnapshotModState());
        ImGui.EndChild();

        ImGui.SameLine();

        ImGui.BeginChild("##ui-panel", rightSize, true);
        DrawList(_debugTrace.SnapshotUi());
        ImGui.EndChild();

        ImGui.Spacing();
        if (ImGui.Button("Clear"))
        {
            try { _debugTrace.Clear(); }
            catch (Exception ex) { _logger.LogError(ex, "DebugTrace.Clear failed"); }
        }

        ImGui.Spacing();
        ImGui.TextColored(ShrinkUColors.Accent, "Actions");
        ImGui.SameLine();
        ImGui.SetCursorPosX(96f);
        ImGui.SetNextItemWidth(Math.Max(200f, avail.X - 120f));
        ImGui.InputText("##ActionFilter", ref _actionFilter, 256);
        var actionsSize = new Vector2(avail.X, Math.Max(120f, avail.Y * 0.40f));
        ImGui.BeginChild("##actions-panel", actionsSize, true);
        DrawListFiltered(_debugTrace.SnapshotActions(), _actionFilter);
        ImGui.EndChild();
    }

    private void DrawPenumbraWatcherTab()
    {
        var status = _penumbraFolderWatcher.GetStatus();
        var pathSync = _conversionService.GetPathSyncDebugStats();
        ImGui.TextColored(ShrinkUColors.Accent, "Penumbra Folder Watcher");
        ImGui.Separator();

        ImGui.Text($"Realtime FS Watcher Enabled: {FormatBool(status.RealtimeWatcherEnabled)}");
        ImGui.Text($"Active: {FormatBool(status.WatcherActive)}");
        ImGui.Text($"Root: {SafeText(status.RootPath)}");
        ImGui.Text($"Last Event: {FormatUtc(status.LastEventUtc)}");
        ImGui.Text($"Last Event Kind: {SafeText(status.LastEventKind)}");
        ImGui.TextWrapped($"Last Event Path: {SafeText(status.LastEventPath)}");
        ImGui.Text($"Event Burst Count: {status.EventBurstCount}");
        ImGui.Text($"Last Scan: {FormatUtc(status.LastScanUtc)} ({status.LastScanDurationMs} ms)");
        ImGui.Text($"Directories Scanned: {status.LastScanDirectoryCount}");
        ImGui.Text($"Max Directory Write: {FormatUtc(status.LastScanMaxWriteUtc)}");
        ImGui.Text($"Stored Snapshot: {FormatUtc(status.StoredFingerprintUtc)}");
        ImGui.Text($"Startup Stored Snapshot: {FormatUtc(status.StartupStoredFingerprintUtc)}");
        ImGui.Text($"Startup Stored Root: {SafeText(status.StartupStoredRootPath)}");
        ImGui.Text($"Startup Fingerprint Match: {FormatBool(status.StartupStoredFingerprintMatchesCurrent)}");
        ImGui.Text($"Startup Root Match: {FormatBool(status.StartupStoredRootMatchesCurrent)}");
        ImGui.Text($"Startup Diff: {FormatBool(status.StartupDiffDetected)}");
        ImGui.TextWrapped($"Last Reason: {SafeText(status.LastChangeReason)}");
        ImGui.TextWrapped($"Last Error: {SafeText(status.LastError)}");
        ImGui.Separator();
        ImGui.TextColored(ShrinkUColors.Accent, "Path-Sync Queue Stats");
        ImGui.Text($"Queued While Busy: {pathSync.QueuedCount}");
        ImGui.Text($"Sync Runs: {pathSync.RunCount}");
        ImGui.Text($"Applied Changes: {pathSync.AppliedCount}");
        ImGui.Text($"Skipped Runs: {pathSync.SkippedCount}");
        ImGui.Text($"Errors: {pathSync.ErrorCount}");
        ImGui.Text($"Pending Now: {FormatBool(pathSync.PendingNow)}");
        ImGui.TextWrapped($"Pending Reason: {SafeText(pathSync.PendingReason)}");
        ImGui.Text($"Last Sync UTC: {FormatUtc(pathSync.LastSyncUtc)}");
        ImGui.Text($"Last Sync Reason: {SafeText(pathSync.LastReason)}");
        ImGui.Text($"Last Sync Duration: {pathSync.LastElapsedMs} ms");
        ImGui.Text($"Last Sync Periodic: {FormatBool(pathSync.LastPeriodic)}");
        ImGui.Text($"Last Sync Was Queued: {FormatBool(pathSync.LastSyncFromPendingQueue)}");
        ImGui.Text("Dropped Events: 0");
    }

    private void DrawBackupWatcherTab()
    {
        var status = _backupFolderWatcher.GetStatus();
        ImGui.TextColored(ShrinkUColors.Accent, "Backup Folder Watcher");
        ImGui.Separator();

        ImGui.Text($"Active: {FormatBool(status.WatcherActive)}");
        ImGui.Text($"Root: {SafeText(status.RootPath)}");
        ImGui.Text($"Last Event: {FormatUtc(status.LastEventUtc)}");
        ImGui.Text($"Last Event Kind: {SafeText(status.LastEventKind)}");
        ImGui.TextWrapped($"Last Event Path: {SafeText(status.LastEventPath)}");
        ImGui.Text($"Event Burst Count: {status.EventBurstCount}");
        ImGui.Text($"Last Refresh: {FormatUtc(status.LastRefreshUtc)} ({status.LastRefreshDurationMs} ms)");
        ImGui.Text($"Directories Scanned: {status.LastScanDirectoryCount}");
        ImGui.Text($"Max Directory Write: {FormatUtc(status.LastScanMaxWriteUtc)}");
        ImGui.Text($"Stored Snapshot: {FormatUtc(status.StoredFingerprintUtc)}");
        ImGui.Text($"Snapshot Unchanged: {FormatBool(status.StoredFingerprintMatchesCurrent)}");
        ImGui.TextWrapped($"Last Error: {SafeText(status.LastError)}");
    }

    private void DrawStorageTab()
    {
        var s = _modStateService.GetStorageDebugStats();
        ImGui.TextColored(ShrinkUColors.Accent, "Mod State Storage");
        ImGui.Separator();
        ImGui.Text($"Mode: {SafeText(s.StorageMode)}");
        ImGui.TextWrapped($"Path: {SafeText(s.StatePath)}");
        ImGui.Text($"Save In Progress: {FormatBool(s.SaveInProgress)}");
        ImGui.Text($"Force Full Sync Pending: {FormatBool(s.ForceFullSyncPending)}");
        ImGui.Text($"Pending Dirty Mods: {s.PendingDirtyCount}");
        ImGui.Text($"Pending Deleted Mods: {s.PendingDeletedCount}");
        ImGui.Separator();
        ImGui.TextColored(ShrinkUColors.Accent, "SQLite Save Throughput");
        ImGui.Text($"Saves: {s.SqliteSaveCount}");
        ImGui.Text($"Skipped Saves: {s.SqliteSaveSkippedCount}");
        ImGui.Text($"Full Saves: {s.SqliteFullSaveCount}");
        ImGui.Text($"Incremental Saves: {s.SqliteIncrementalSaveCount}");
        ImGui.Text($"Total Upserts: {s.SqliteUpsertCount}");
        ImGui.Text($"Total Deletes: {s.SqliteDeleteCount}");
        ImGui.Text($"Avg Save Duration: {s.SqliteAverageElapsedMs.ToString("0.0", CultureInfo.InvariantCulture)} ms");
        ImGui.Text($"Max Save Duration: {s.SqliteMaxElapsedMs} ms");
        ImGui.Separator();
        ImGui.TextColored(ShrinkUColors.Accent, "Last Save");
        ImGui.Text($"UTC: {FormatUtc(s.LastSaveUtc)}");
        ImGui.Text($"Duration: {s.LastSaveElapsedMs} ms");
        ImGui.Text($"Full Sync: {FormatBool(s.LastSaveWasFullSync)}");
        ImGui.Text($"Dirty Mods Seen: {s.LastSaveDirtyCount}");
        ImGui.Text($"Deleted Mods Seen: {s.LastSaveDeletedCount}");
        ImGui.Text($"Rows Upserted: {s.LastSaveUpsertedCount}");
        ImGui.Text($"Rows Deleted: {s.LastSaveDeletedRowsCount}");
    }

    private void DrawSystemTab()
    {
        CaptureSystemPerfSample();
        var perfSummary = PerfTrace.GetSummary(TimeSpan.FromSeconds(60), 8);
        if (_systemPerfSamples.Count == 0)
        {
            ImGui.TextUnformatted("No system samples captured yet.");
            return;
        }

        var samples = _systemPerfSamples.ToArray();
        var latest = samples[^1];
        var avgCpu = samples.Average(static x => x.CpuPercent);
        var maxCpu = samples.Max(static x => x.CpuPercent);
        var maxWorkingSet = samples.Max(static x => x.WorkingSetBytes);
        var maxPrivate = samples.Max(static x => x.PrivateBytes);
        var maxGcHeap = samples.Max(static x => x.GcHeapBytes);
        var avgThreads = samples.Average(static x => x.ThreadCount);
        var maxThreads = samples.Max(static x => x.ThreadCount);

        ImGui.TextColored(ShrinkUColors.Accent, "Process Utilization (Sphene host process)");
        ImGui.Separator();
        ImGui.Text($"Sample Count: {samples.Length}");
        ImGui.Text($"Last Sample UTC: {FormatUtc(latest.AtUtc)}");
        ImGui.Text($"CPU (current): {latest.CpuPercent.ToString("0.0", CultureInfo.InvariantCulture)}%");
        ImGui.Text($"CPU (avg/max): {avgCpu.ToString("0.0", CultureInfo.InvariantCulture)}% / {maxCpu.ToString("0.0", CultureInfo.InvariantCulture)}%");
        ImGui.Text($"Working Set (current/peak): {FormatMiB(latest.WorkingSetBytes)} / {FormatMiB(maxWorkingSet)}");
        ImGui.Text($"Private Bytes (current/peak): {FormatMiB(latest.PrivateBytes)} / {FormatMiB(maxPrivate)}");
        ImGui.Text($"GC Heap (current/peak): {FormatMiB(latest.GcHeapBytes)} / {FormatMiB(maxGcHeap)}");
        ImGui.Text($"Threads (current/avg/max): {latest.ThreadCount} / {avgThreads.ToString("0.0", CultureInfo.InvariantCulture)} / {maxThreads}");
        ImGui.Text($"GC Collections since tab init (Gen0/Gen1/Gen2): {GC.CollectionCount(0) - _gcGen0Baseline}/{GC.CollectionCount(1) - _gcGen1Baseline}/{GC.CollectionCount(2) - _gcGen2Baseline}");
        ImGui.TextWrapped("These values are process-level and include game host workload.");
        ImGui.Separator();
        ImGui.TextColored(ShrinkUColors.Accent, "ShrinkU Attributed Workload (PerfTrace, last 60s)");
        ImGui.Text($"Operations in window: {perfSummary.WindowOperationCount}");
        ImGui.Text($"Tracked work time: {perfSummary.WindowTotalMs} ms");
        ImGui.Text($"Attribution utilization: {perfSummary.WindowUtilizationPercent.ToString("0.0", CultureInfo.InvariantCulture)}%");
        ImGui.TextWrapped("Attribution utilization can exceed 100% when operations run in parallel.");
        if (perfSummary.TopOperations.Count > 0)
        {
            ImGui.Separator();
            ImGui.TextColored(ShrinkUColors.Accent, "Top operation cost");
            for (int i = 0; i < perfSummary.TopOperations.Count; i++)
            {
                var op = perfSummary.TopOperations[i];
                ImGui.Text($"{i + 1}. {op.Name} - total {op.WindowTotalMs} ms, count {op.WindowCount}, max {op.MaxMs} ms, last {op.LastMs} ms");
            }
        }
    }

    private void CaptureSystemPerfSample()
    {
        var now = DateTime.UtcNow;
        if (_lastSystemPerfSampleUtc != DateTime.MinValue && (now - _lastSystemPerfSampleUtc).TotalSeconds < 1.0)
            return;

        using var process = Process.GetCurrentProcess();
        process.Refresh();
        var cpuTotal = process.TotalProcessorTime;
        var elapsedMs = _lastSystemPerfSampleUtc == DateTime.MinValue ? 0d : (now - _lastSystemPerfSampleUtc).TotalMilliseconds;
        var cpuPercent = 0d;
        if (_systemPerfInitialized && elapsedMs > 1)
        {
            var cpuDeltaMs = (cpuTotal - _lastProcessCpuTime).TotalMilliseconds;
            cpuPercent = (cpuDeltaMs / (elapsedMs * Math.Max(1, Environment.ProcessorCount))) * 100d;
            if (cpuPercent < 0d)
                cpuPercent = 0d;
        }

        _lastSystemPerfSampleUtc = now;
        _lastProcessCpuTime = cpuTotal;
        _systemPerfInitialized = true;

        _systemPerfSamples.Enqueue(new SystemPerfSample
        {
            AtUtc = now,
            CpuPercent = cpuPercent,
            WorkingSetBytes = process.WorkingSet64,
            PrivateBytes = process.PrivateMemorySize64,
            GcHeapBytes = GC.GetTotalMemory(false),
            ThreadCount = process.Threads.Count,
        });
        while (_systemPerfSamples.Count > 180)
            _systemPerfSamples.Dequeue();
    }

    private void DrawPenumbraModDebugTab()
    {
        if (_penumbraModPaths.Count == 0 && !_penumbraModListLoading)
            RefreshPenumbraModList();

        if (ImGui.Button("Refresh Mod List"))
            RefreshPenumbraModList();
        ImGui.SameLine();
        if (ImGui.Button("Refresh Selected Data"))
            LoadPenumbraModData(forceReload: true);

        var mods = _penumbraModPaths.Keys.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
        if (mods.Count > 0 && string.IsNullOrWhiteSpace(_penumbraModSelected))
            _penumbraModSelected = mods[0];

        var selectedIdx = Math.Max(0, mods.FindIndex(m => string.Equals(m, _penumbraModSelected, StringComparison.OrdinalIgnoreCase)));
        var preview = mods.Count > 0 ? mods[selectedIdx] : "<no mods>";
        ImGui.SetNextItemWidth(420f);
        if (ImGui.BeginCombo("Mod", preview))
        {
            for (int i = 0; i < mods.Count; i++)
            {
                var mod = mods[i];
                var selected = string.Equals(mod, _penumbraModSelected, StringComparison.OrdinalIgnoreCase);
                if (ImGui.Selectable(mod, selected))
                {
                    _penumbraModSelected = mod;
                    LoadPenumbraModData(forceReload: true);
                }
                if (selected)
                    ImGui.SetItemDefaultFocus();
            }
            ImGui.EndCombo();
        }

        ImGui.Separator();
        if (_penumbraModListLoading)
            ImGui.TextUnformatted("Loading mod list...");
        if (_penumbraModDataLoading)
            ImGui.TextUnformatted("Loading selected mod data...");
        if (!string.IsNullOrWhiteSpace(_penumbraModDataError))
            ImGui.TextWrapped($"Error: {_penumbraModDataError}");
        var d = _penumbraModData;
        if (d == null)
            return;

        var metadataTag = string.Equals(d.MetadataSource, "penumbra-ipc-modlist-adapter", StringComparison.OrdinalIgnoreCase)
            ? "[API]"
            : (string.Equals(d.MetadataSource, "meta.json-fallback", StringComparison.OrdinalIgnoreCase) ? "[FALLBACK]" : "[UNKNOWN]");
        var fallbackUsed = string.Equals(d.MetadataSource, "meta.json-fallback", StringComparison.OrdinalIgnoreCase);

        DrawTaggedLine("[API]", $"API Available: {(d.ApiAvailable ? "Yes" : "No")}");
        DrawTaggedLine("[INPUT]", $"ModDir: {d.ModDirectory}");
        DrawTaggedLine("[API]", $"Penumbra Root: {SafeText(d.PenumbraRoot)}");
        DrawTaggedLine("[API]", $"Penumbra Path: {SafeText(d.PenumbraPath)}");
        DrawTaggedLine("[API]", $"Display Name: {SafeText(d.DisplayName)}");
        DrawTaggedLine("[DERIVED]", $"Metadata source: {SafeText(d.MetadataSource)}");
        DrawTaggedLine("[DERIVED]", $"Metadata fallback used: {(fallbackUsed ? "Yes" : "No")}");
        DrawTaggedLine(metadataTag, $"Version: {SafeText(d.Version)}");
        DrawTaggedLine(metadataTag, $"Author: {SafeText(d.Author)}");
        DrawTaggedLine(metadataTag, $"Website: {SafeText(d.Website)}");
        DrawTaggedWrappedLine(metadataTag, $"Description: {SafeText(d.Description)}");
        DrawTaggedWrappedLine("[API]", $"Tags: {(d.Tags.Count > 0 ? string.Join(", ", d.Tags) : "-")}");
        DrawTaggedLine("[API]", $"Collection: {SafeText(d.CollectionName)}");
        DrawTaggedLine("[API]", $"Collection ID: {(d.CollectionId.HasValue ? d.CollectionId.Value.ToString() : "-")}");
        DrawTaggedLine("[API]", $"Enabled: {(d.Enabled.HasValue ? (d.Enabled.Value ? "Yes" : "No") : "-")}");
        DrawTaggedLine("[API]", $"Priority: {(d.Priority.HasValue ? d.Priority.Value.ToString() : "-")}");
        DrawTaggedLine("[API]", $"Inherited: {(d.Inherited.HasValue ? (d.Inherited.Value ? "Yes" : "No") : "-")}");
        DrawTaggedLine("[API]", $"Temporary: {(d.Temporary.HasValue ? (d.Temporary.Value ? "Yes" : "No") : "-")}");
        DrawTaggedLine("[META]", $"Loaded UTC: {d.LoadedAtUtc:O}");
        ImGui.Separator();
        DrawTaggedLine("[LEGEND]", "[API] Penumbra IPC/API");
        DrawTaggedLine("[LEGEND]", "[FALLBACK] meta.json fallback");
        DrawTaggedLine("[LEGEND]", "[INPUT] user selection/mod key");
        DrawTaggedLine("[LEGEND]", "[DERIVED] calculated from other fields");
        DrawTaggedLine("[LEGEND]", "[META] debug load timestamp");
    }

    private void RefreshPenumbraModList()
    {
        if (_penumbraModListLoading)
            return;
        _penumbraModListLoading = true;
        _ = Task.Run(async () =>
        {
            try
            {
                var paths = await _conversionService.GetModPathsAsync().ConfigureAwait(false);
                _penumbraModPaths = new Dictionary<string, string>(paths, StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception ex)
            {
                _penumbraModDataError = ex.Message;
            }
            finally
            {
                _penumbraModListLoading = false;
            }
        });
    }

    private void LoadPenumbraModData(bool forceReload = false)
    {
        var mod = (_penumbraModSelected ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(mod))
            return;
        if (!forceReload && _penumbraModData != null && string.Equals(_penumbraModData.ModDirectory, mod, StringComparison.OrdinalIgnoreCase))
            return;
        if (_penumbraModDataLoading)
            return;

        _penumbraModDataLoading = true;
        _penumbraModDataError = string.Empty;
        _penumbraModLoadVersion++;
        var version = _penumbraModLoadVersion;
        _ = Task.Run(async () =>
        {
            try
            {
                var api = _conversionService.IsPenumbraApiAvailable();
                var root = _conversionService.GetPenumbraModDirectory();
                string path = string.Empty;
                if (_penumbraModPaths.TryGetValue(mod, out var p))
                    path = p ?? string.Empty;
                var name = await _conversionService.GetModDisplayNameAsync(mod).ConfigureAwait(false);
                var tags = await _conversionService.GetModTagsAsync(mod).ConfigureAwait(false);
                var meta = await _conversionService.GetModMetadataAsync(mod).ConfigureAwait(false);
                var coll = await _conversionService.GetCurrentCollectionAsync().ConfigureAwait(false);
                bool? enabled = null;
                int? priority = null;
                bool? inherited = null;
                bool? temporary = null;
                if (coll.HasValue)
                {
                    var states = await _conversionService.GetAllModEnabledStatesAsync(coll.Value.Id).ConfigureAwait(false);
                    if (states.TryGetValue(mod, out var st))
                    {
                        enabled = st.Enabled;
                        priority = st.Priority;
                        inherited = st.Inherited;
                        temporary = st.Temporary;
                    }
                }
                var snapshot = new PenumbraModDebugSnapshot
                {
                    ApiAvailable = api,
                    ModDirectory = mod,
                    PenumbraRoot = root,
                    PenumbraPath = path,
                    DisplayName = name,
                    MetadataSource = meta?.Source ?? string.Empty,
                    Author = meta?.Author,
                    Version = meta?.Version,
                    Description = meta?.Description,
                    Website = meta?.Website,
                    CollectionId = coll?.Id,
                    CollectionName = coll?.Name ?? string.Empty,
                    Enabled = enabled,
                    Priority = priority,
                    Inherited = inherited,
                    Temporary = temporary,
                    Tags = tags ?? new List<string>(),
                    LoadedAtUtc = DateTime.UtcNow
                };
                if (version == _penumbraModLoadVersion)
                    _penumbraModData = snapshot;
            }
            catch (Exception ex)
            {
                if (version == _penumbraModLoadVersion)
                    _penumbraModDataError = ex.Message;
            }
            finally
            {
                if (version == _penumbraModLoadVersion)
                    _penumbraModDataLoading = false;
            }
        });
    }

    private void DrawList(System.Collections.Generic.IReadOnlyList<(DateTime atUtc, string message)> entries)
    {
        var style = ImGui.GetStyle();
        for (int i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            var ts = e.atUtc.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
            ImGui.TextUnformatted(ts);
            ImGui.SameLine();
            ImGui.TextWrapped(e.message);
        }
    }

    private void DrawListFiltered(System.Collections.Generic.IReadOnlyList<(DateTime atUtc, string message)> entries, string filter)
    {
        var style = ImGui.GetStyle();
        var f = filter ?? string.Empty;
        for (int i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            if (f.Length > 0 && e.message.IndexOf(f, StringComparison.OrdinalIgnoreCase) < 0)
                continue;
            var ts = e.atUtc.ToString("HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);
            ImGui.TextUnformatted(ts);
            ImGui.SameLine();
            ImGui.TextWrapped(e.message);
        }
    }

    private static string FormatUtc(DateTime utc)
    {
        if (utc == DateTime.MinValue)
            return "n/a";
        return utc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
    }

    private static string FormatBool(bool value)
    {
        return value ? "Yes" : "No";
    }

    private static string SafeText(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "-" : value;
    }

    private static string FormatMiB(long bytes)
    {
        var mib = bytes / (1024d * 1024d);
        return $"{mib.ToString("0.0", CultureInfo.InvariantCulture)} MiB";
    }

    private static Vector4 GetTagColor(string tag)
    {
        return tag switch
        {
            "[API]" => new Vector4(0.36f, 0.72f, 1.00f, 1f),
            "[FALLBACK]" => new Vector4(1.00f, 0.70f, 0.28f, 1f),
            "[DERIVED]" => new Vector4(0.66f, 0.84f, 0.66f, 1f),
            "[INPUT]" => new Vector4(0.82f, 0.82f, 0.82f, 1f),
            "[META]" => new Vector4(0.72f, 0.62f, 0.96f, 1f),
            "[LEGEND]" => new Vector4(0.78f, 0.78f, 0.78f, 1f),
            _ => new Vector4(0.96f, 0.40f, 0.40f, 1f),
        };
    }

    private static void DrawTaggedLine(string tag, string text)
    {
        ImGui.TextColored(GetTagColor(tag), tag);
        ImGui.SameLine();
        ImGui.TextUnformatted(text);
    }

    private static void DrawTaggedWrappedLine(string tag, string text)
    {
        ImGui.TextColored(GetTagColor(tag), tag);
        ImGui.SameLine();
        ImGui.TextWrapped(text);
    }
}
