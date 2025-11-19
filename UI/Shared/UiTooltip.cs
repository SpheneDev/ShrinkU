using Dalamud.Bindings.ImGui;
using System.Numerics;

namespace ShrinkU.UI;

public static class UiTooltip
{
    public static void Show(string text)
    {
        if (!ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            return;
        ImGui.BeginTooltip();
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.TooltipText);
        ImGui.TextUnformatted(text);
        ImGui.PopStyleColor();
        ImGui.EndTooltip();
    }

    public static void ShowWrapped(string text, float maxWidth)
    {
        if (!ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            return;
        ImGui.BeginTooltip();
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.TooltipText);
        ImGui.PushTextWrapPos(ImGui.GetCursorPosX() + maxWidth);
        ImGui.TextUnformatted(text);
        ImGui.PopTextWrapPos();
        ImGui.PopStyleColor();
        ImGui.EndTooltip();
    }
}