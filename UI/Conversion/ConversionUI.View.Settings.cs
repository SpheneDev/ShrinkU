using Dalamud.Bindings.ImGui;
using System;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using ShrinkU.Configuration;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private void DrawSettings_ViewImpl()
    {
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Texture Settings");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);
        var mode = _configService.Current.TextureProcessingMode;
        var controller = _configService.Current.AutomaticControllerName ?? string.Empty;
        var spheneIntegrated = !string.IsNullOrWhiteSpace(controller) || _configService.Current.AutomaticHandledBySphene;
        var modeDisplay = mode == TextureProcessingMode.Automatic && spheneIntegrated
            ? "Automatic (handled by Sphene)"
            : mode.ToString();
        if (ImGui.BeginCombo("Mode", modeDisplay))
        {
            if (ImGui.Selectable("Manual", mode == TextureProcessingMode.Manual))
            {
                _configService.Current.TextureProcessingMode = TextureProcessingMode.Manual;
                _configService.Save();
                try { OnModeChanged(); } catch { }
            }

            if (spheneIntegrated)
            {
                var isAuto = mode == TextureProcessingMode.Automatic;
                if (ImGui.Selectable("Automatic (handled by Sphene)", isAuto))
                {
                    _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                    _configService.Current.AutomaticHandledBySphene = true;
                    _configService.Current.AutomaticControllerName = string.IsNullOrWhiteSpace(controller) ? "Sphene" : controller;
                    _configService.Save();
                    try { OnModeChanged(); } catch { }
                }
            }
            else
            {
                if (ImGui.Selectable("Automatic", mode == TextureProcessingMode.Automatic))
                {
                    _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                    _configService.Current.AutomaticHandledBySphene = false;
                    _configService.Current.AutomaticControllerName = string.Empty;
                    _configService.Save();
                    try { OnModeChanged(); } catch { }
                }
            }
            ImGui.EndCombo();
        }

        bool autoRestore = _configService.Current.AutoRestoreInefficientMods;
        if (ImGui.Checkbox("Auto-restore backups for inefficient mods", ref autoRestore))
        {
            _configService.Current.AutoRestoreInefficientMods = autoRestore;
            _configService.Save();
        }

        ImGui.Text("Backup Folder:");
        ImGui.SameLine();
        ImGui.TextWrapped(_configService.Current.BackupFolderPath);
        if (ImGui.Button("Browse..."))
        {
            OpenFolderPicker();
        }
        ImGui.SameLine();
        if (ImGui.Button("Open Folder"))
        {
            try
            {
                var path = _configService.Current.BackupFolderPath;
                if (!string.IsNullOrWhiteSpace(path))
                {
                    try { Directory.CreateDirectory(path); } catch { }
                    try
                    {
                        Process.Start(new ProcessStartInfo("explorer.exe", path) { UseShellExecute = true });
                    }
                    catch { }
                }
            }
            catch { }
        }
        ImGui.SameLine();
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        var openSettingsClicked = ImGui.Button("Settings");
        ImGui.PopStyleColor(4);
        if (openSettingsClicked)
        {
            try { _openSettings?.Invoke(); } catch { }
        }
        try { }
        catch { }
    }
}
