using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Services;
using System;
using System.Globalization;
using System.Numerics;

namespace ShrinkU.UI;

public sealed class DebugUI : Window
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly DebugTraceService _debugTrace;
    private readonly PenumbraFolderWatcherService _penumbraFolderWatcher;
    private readonly BackupFolderWatcherService _backupFolderWatcher;
    private string _actionFilter = string.Empty;

    public DebugUI(ILogger logger, ShrinkUConfigService configService, DebugTraceService debugTrace, PenumbraFolderWatcherService penumbraFolderWatcher, BackupFolderWatcherService backupFolderWatcher)
        : base("ShrinkU Debug###ShrinkUDebugUI")
    {
        _logger = logger;
        _configService = configService;
        _debugTrace = debugTrace;
        _penumbraFolderWatcher = penumbraFolderWatcher;
        _backupFolderWatcher = backupFolderWatcher;

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
        ImGui.TextColored(ShrinkUColors.Accent, "Penumbra Folder Watcher");
        ImGui.Separator();

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
        return utc.ToLocalTime().ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
    }

    private static string FormatBool(bool value)
    {
        return value ? "Yes" : "No";
    }

    private static string SafeText(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "-" : value;
    }
}
