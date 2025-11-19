using Dalamud.Bindings.ImGui;
using System;
using System.Numerics;

namespace ShrinkU.UI;

public static class UiHeader
{
    public static void DrawAccentHeaderBar(float height = 2f, float spacing = 6f)
    {
        var headerStart = ImGui.GetCursorScreenPos();
        var headerWidth = MathF.Max(1f, ImGui.GetContentRegionAvail().X);
        var headerEnd = new Vector2(headerStart.X + headerWidth, headerStart.Y + height);
        ImGui.GetWindowDrawList().AddRectFilled(headerStart, headerEnd, ShrinkUColors.ToImGuiColor(ShrinkUColors.Accent));
        ImGui.Dummy(new Vector2(0, spacing));
    }
}