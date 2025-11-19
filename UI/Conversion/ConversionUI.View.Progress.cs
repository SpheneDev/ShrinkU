using Dalamud.Bindings.ImGui;
using System.Numerics;
using Microsoft.Extensions.Logging;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawProgress_ViewImpl()
    {
        if (!_running)
            return;

        var displayMod = _currentRestoreMod;
        if (!string.IsNullOrEmpty(displayMod) && _modDisplayNames.TryGetValue(displayMod, out var dn))
            displayMod = dn;

        if (!string.IsNullOrEmpty(displayMod))
        {
            ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Restoring");
            ImGui.Text($"Mod: {displayMod} ({_currentRestoreModIndex}/{_currentRestoreModTotal})");
        }
        else if (!string.IsNullOrEmpty(_currentModName))
        {
            var dn2 = _modDisplayNames.TryGetValue(_currentModName, out var nm) ? nm : _currentModName;
            ImGui.TextColored(new Vector4(0.40f, 0.85f, 0.40f, 1f), "Converting");
            ImGui.Text($"Mod: {dn2} ({_currentModIndex}/{_totalMods})");
            ImGui.Text($"Converted: {_convertedCount}");
        }

        if (!string.IsNullOrEmpty(_currentTexture))
            ImGui.Text($"File: {_currentTexture}");

        ImGui.BeginDisabled(!_running);
        if (ImGui.Button("Cancel"))
        {
            _restoreAfterCancel = true;
            _cancelTargetMod = _currentModName;
            _conversionService.Cancel();
            _restoreCancellationTokenSource?.Cancel();
            _logger.LogDebug("Cancel pressed; will restore current mod {mod} after conversion stops", _cancelTargetMod);
        }
        ImGui.EndDisabled();
        ShowTooltip("Cancel the current conversion or restore operation.");
    }
}