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
    private string _actionFilter = string.Empty;

    public DebugUI(ILogger logger, ShrinkUConfigService configService, DebugTraceService debugTrace)
        : base("ShrinkU Debug###ShrinkUDebugUI")
    {
        _logger = logger;
        _configService = configService;
        _debugTrace = debugTrace;

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(720, 360),
            MaximumSize = new Vector2(1920, 1080),
        };
    }

    public override void Draw()
    {
        UiHeader.DrawAccentHeaderBar();

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
}
