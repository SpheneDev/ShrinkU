using Dalamud.Bindings.ImGui;
using System.Numerics;
using Microsoft.Extensions.Logging;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawProgress_ViewImpl()
    {
        if (!(_running || _conversionService.IsConverting))
            return;

        var displayMod = _currentRestoreMod;
        if (!string.IsNullOrEmpty(displayMod) && _modDisplayNames.TryGetValue(displayMod, out var dn))
            displayMod = dn;

        if (!string.IsNullOrEmpty(displayMod))
        {
            ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Restoring");
            ImGui.Text($"Mod: {displayMod} ({_currentRestoreModIndex}/{_currentRestoreModTotal})");
            var total = _currentRestoreModTotal > 0 ? _currentRestoreModTotal : _backupTotal;
            var current = _currentRestoreModTotal > 0 ? _currentRestoreModIndex : _backupIndex;
            float frac = (total > 0) ? (float)current / total : 0f;
            var width = ImGui.GetContentRegionAvail().X;
            ImGui.ProgressBar(frac, new Vector2(width, 0), "");
        }
        else if (!string.IsNullOrEmpty(_currentModName) || _conversionService.IsConverting)
        {
            var modName = string.IsNullOrEmpty(_currentModName) ? string.Empty : _currentModName;
            var dn2 = !string.IsNullOrEmpty(modName) && _modDisplayNames.TryGetValue(modName, out var nm) ? nm : modName;
            ImGui.TextColored(new Vector4(0.40f, 0.85f, 0.40f, 1f), "Converting");
            if (!string.IsNullOrEmpty(dn2))
                ImGui.Text($"Mod: {dn2} ({_currentModIndex}/{_totalMods})");
            float overall = (_totalMods > 0) ? (float)_currentModIndex / _totalMods : 0f;
            var width = ImGui.GetContentRegionAvail().X;
            ImGui.ProgressBar(overall, new Vector2(width, 0), "");
            if (_currentModTotalFiles > 0)
            {
                float perMod = (float)_convertedCount / _currentModTotalFiles;
                ImGui.ProgressBar(perMod, new Vector2(width, 0), "");
            }
        }

        if (!string.IsNullOrEmpty(_currentTexture))
            ImGui.Text($"File: {_currentTexture}");

        ImGui.BeginDisabled(!(_running || _conversionService.IsConverting));
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