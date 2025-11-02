using Dalamud.Bindings.ImGui;
using System.Numerics;

namespace ShrinkU.UI;

public static class ShrinkUColors
{
    // Brand accent color (#bb9be9) for UI highlights
    public static readonly Vector4 Accent = new(0.733f, 0.608f, 0.914f, 1f);
    public static readonly Vector4 AccentHovered = new(0.78f, 0.65f, 0.95f, 1f);
    public static readonly Vector4 AccentActive = new(0.68f, 0.56f, 0.85f, 1f);
    public static readonly Vector4 WarningLight = new(1.0f, 0.40f, 0.40f, 1f);

    // Utility helpers
    public static uint ToImGuiColor(Vector4 color) => ImGui.ColorConvertFloat4ToU32(color);
    public static Vector4 WithAlpha(Vector4 color, float alpha) => new(color.X, color.Y, color.Z, alpha);
}