using Dalamud.Bindings.ImGui;
using System;
using System.Numerics;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private void DrawSplitter_ViewImpl(float totalWidth, ref float leftWidth)
    {
        var height = ImGui.GetContentRegionAvail().Y;
        ImGui.InvisibleButton("##splitter", new Vector2(4f, height));
        if (ImGui.IsItemActive())
        {
            var delta = ImGui.GetIO().MouseDelta.X;
            leftWidth = Math.Max(360f, Math.Min(totalWidth - 360f, leftWidth + delta));
            _leftPanelWidthPx = leftWidth;
            _leftPanelWidthRatio = Math.Clamp(leftWidth / totalWidth, 0.25f, 0.75f);
            _leftWidthDirty = true;
        }
        else if (_leftWidthDirty)
        {
            _configService.Current.LeftPanelWidthPx = _leftPanelWidthPx;
            _configService.Save();
            _leftWidthDirty = false;
        }
        var drawList = ImGui.GetWindowDrawList();
        var min = ImGui.GetItemRectMin();
        var max = ImGui.GetItemRectMax();
        drawList.AddRectFilled(min, max, ImGui.GetColorU32(new Vector4(0.3f, 0.3f, 0.3f, 0.6f)));
        ImGui.SameLine();
    }
}