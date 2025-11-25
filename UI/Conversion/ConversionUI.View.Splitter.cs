using Dalamud.Bindings.ImGui;
using System;
using System.Numerics;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private void DrawSplitter_ViewImpl(float totalWidth, ref float leftWidth)
    {
        var height = ImGui.GetContentRegionAvail().Y;
        ImGui.SameLine();
    }
}