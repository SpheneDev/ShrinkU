using Dalamud.Bindings.ImGui;
using System;
using System.Numerics;
using Microsoft.Extensions.Logging;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private void DrawProgress_ViewImpl()
    {
        if (!(_running || _conversionService.IsConverting))
            return;

        var width = ImGui.GetContentRegionAvail().X;
        
        // --- RESTORE MODE ---
        var displayMod = _currentRestoreMod;
        if (!string.IsNullOrEmpty(displayMod) && _modDisplayNames.TryGetValue(displayMod, out var dn))
            displayMod = dn;

        if (!string.IsNullOrEmpty(displayMod))
        {
            ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Restoring Mods");
            ImGui.Separator();
            
            // 1. Batch Progress
            if (_restoreModsTotal > 0)
            {
                var elapsed = (_bulkStartedAt == DateTime.MinValue) ? TimeSpan.Zero : (DateTime.UtcNow - _bulkStartedAt);
                var doneCount = Math.Max(0, _restoreModsDone - 1);
                var doneDisplay = Math.Max(0, _restoreModsDone);
                var remaining = Math.Max(0, _restoreModsTotal - doneDisplay);
                
                // Calculate ETA based on completed items + current partial
                // To avoid divide by zero or instability at start, we use a simple average if doneCount > 0
                var etaSec = 0;
                if (doneCount > 0)
                {
                     etaSec = (int)Math.Round(elapsed.TotalSeconds / doneCount * (_restoreModsTotal - doneCount));
                }
                
                var m = etaSec / 60;
                var s = etaSec % 60;
                
                float currentModFrac = (_currentRestoreModTotal > 0) ? (float)Math.Min(_currentRestoreModIndex, _currentRestoreModTotal) / _currentRestoreModTotal : 0f;
                float batchFrac = (float)(doneCount + currentModFrac) / _restoreModsTotal;
                
                ImGui.Text($"Overall Progress: {doneDisplay} of {_restoreModsTotal} Mods");
                ImGui.ProgressBar(batchFrac, new Vector2(width, 0), $"{batchFrac:P0}");
                ImGui.TextColored(new Vector4(0.7f, 0.7f, 0.7f, 1f), $"Elapsed: {elapsed.Hours:D2}:{elapsed.Minutes:D2}:{elapsed.Seconds:D2} • ETA: {m}:{s:D2}");
            }
            
            ImGui.Spacing();
            ImGui.Separator();
            ImGui.Spacing();
            
            // 2. Current Mod Progress
            ImGui.Text($"Current Mod: {displayMod}");
            var total = _currentRestoreModTotal > 0 ? _currentRestoreModTotal : _backupTotal;
            var current = _currentRestoreModTotal > 0 ? _currentRestoreModIndex : _backupIndex;
            
            // Clamp current to total for display safety
            current = Math.Min(current, total);
            
            float frac = (total > 0) ? (float)current / total : 0f;
            
            ImGui.ProgressBar(frac, new Vector2(width, 0), $"{current}/{total}");
            
            var modElapsed = (_currentModStartedAt == DateTime.MinValue) ? TimeSpan.Zero : (DateTime.UtcNow - _currentModStartedAt);
            ImGui.TextColored(new Vector4(0.7f, 0.7f, 0.7f, 1f), $"Mod Elapsed: {modElapsed.Hours:D2}:{modElapsed.Minutes:D2}:{modElapsed.Seconds:D2}");
        }
        // --- BACKUP MODE (Standalone) ---
        else if (_backupTotal > 0 && string.IsNullOrEmpty(_currentRestoreMod) && !_conversionService.IsConverting)
        {
            ImGui.TextColored(new Vector4(0.40f, 0.60f, 0.90f, 1f), "Backing up");
            ImGui.Separator();
            float frac = (_backupTotal > 0) ? (float)_backupIndex / _backupTotal : 0f;
            ImGui.Text($"Backing up files...");
            ImGui.ProgressBar(frac, new Vector2(width, 0), $"{_backupIndex}/{_backupTotal}");
        }
        // --- CONVERT MODE ---
        else if (_conversionService.IsConverting || _totalMods > 0 || !string.IsNullOrEmpty(_currentModName))
        {
            var modName = string.IsNullOrEmpty(_currentModName) ? string.Empty : _currentModName;
            var dn2 = !string.IsNullOrEmpty(modName) && _modDisplayNames.TryGetValue(modName, out var nm) ? nm : modName;
            
            ImGui.TextColored(new Vector4(0.40f, 0.85f, 0.40f, 1f), "Converting Mods");
            ImGui.Separator();

            // 1. Batch Progress
            if (_totalMods > 0)
            {
                var elapsed = (_bulkStartedAt == DateTime.MinValue) ? TimeSpan.Zero : (DateTime.UtcNow - _bulkStartedAt);
                var doneCount = Math.Max(0, _currentModIndex - 1);
                var doneDisplay = Math.Max(0, _currentModIndex);
                
                var etaSec = 0;
                if (doneCount > 0)
                {
                     etaSec = (int)Math.Round(elapsed.TotalSeconds / doneCount * (_totalMods - doneCount));
                }

                var m = etaSec / 60;
                var s = etaSec % 60;
                
                float currentModFrac = (_currentModTotalFiles > 0) ? (float)Math.Min(_convertedCount, _currentModTotalFiles) / _currentModTotalFiles : 0f;
                float batchFrac = (float)(doneCount + currentModFrac) / _totalMods;

                ImGui.Text($"Overall Progress: {doneDisplay} of {_totalMods} Mods");
                ImGui.ProgressBar(batchFrac, new Vector2(width, 0), $"{batchFrac:P0}");
                ImGui.TextColored(new Vector4(0.7f, 0.7f, 0.7f, 1f), $"Backups created: {_bulkBackedUpMods.Count} • Elapsed: {elapsed.Hours:D2}:{elapsed.Minutes:D2}:{elapsed.Seconds:D2} • ETA: {m}:{s:D2}");
            }

            ImGui.Spacing();
            ImGui.Separator();
            ImGui.Spacing();

            // 2. Current Mod Progress
            if (!string.IsNullOrEmpty(dn2))
                ImGui.Text($"Current Mod: {dn2}");
            
            if (_currentModTotalFiles > 0)
            {
                 var current = Math.Min(_convertedCount, _currentModTotalFiles);
                 float perMod = (float)current / _currentModTotalFiles;
                 ImGui.ProgressBar(perMod, new Vector2(width, 0), $"{current}/{_currentModTotalFiles}");
                 
                 var modElapsed = (_currentModStartedAt == DateTime.MinValue) ? TimeSpan.Zero : (DateTime.UtcNow - _currentModStartedAt);
                 ImGui.TextColored(new Vector4(0.7f, 0.7f, 0.7f, 1f), $"Mod Elapsed: {modElapsed.Hours:D2}:{modElapsed.Minutes:D2}:{modElapsed.Seconds:D2}");
            }
            else 
            {
                 // Indeterminate or starting
                 ImGui.ProgressBar(0f, new Vector2(width, 0), "Preparing...");
            }
        }

        // --- FOOTER ---
        if (!string.IsNullOrEmpty(_currentTexture))
        {
            ImGui.Spacing();
            ImGui.TextColored(new Vector4(0.6f, 0.6f, 0.6f, 1f), $"Processing: {_currentTexture}");
        }
        
        ImGui.Spacing();
        ImGui.Separator();
        ImGui.Spacing();

        ImGui.BeginDisabled(!(_running || _conversionService.IsConverting));
        if (ImGui.Button("Cancel Operation"))
        {
            _restoreAfterCancel = true;
            _cancelTargetMod = _currentModName;
            _conversionService.Cancel();
            _restoreCancellationTokenSource?.Cancel();
            _logger.LogDebug("Cancel pressed; will restore current mod {mod} after conversion stops", _cancelTargetMod);
        }
        ImGui.EndDisabled();
        ShowTooltip("Cancel the current conversion or restore operation. Note: If a mod is partially converted, it will be restored.");
    }
}
