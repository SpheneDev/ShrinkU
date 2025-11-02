using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Services;
using System.Numerics;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Windows.Forms;

namespace ShrinkU.UI;

public sealed class SettingsUI : Window
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly TextureConversionService _conversionService;

    // Tag filtering input state (persisted via config on Apply)
    private string _excludedTagsInput = string.Empty;

    public SettingsUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService)
        : base("ShrinkU Settings###ShrinkUSettingsUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(520, 300),
            MaximumSize = new Vector2(1920, 1080),
        };

        // Initialize excluded tags input from config
        var savedTags = _configService.Current.ExcludedModTags ?? new List<string>();
        _excludedTagsInput = string.Join(", ", savedTags);
    }

    // Helper to show a tooltip when the last item is hovered (also when disabled)
    private static void ShowTooltip(string text)
    {
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip(text);
    }

    public override void Draw()
    {
        if (ImGui.BeginTabBar("SettingsTabs"))
        {
            if (ImGui.BeginTabItem("General"))
            {
                ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Texture Settings");
                var mode = _configService.Current.TextureProcessingMode;
                if (ImGui.BeginCombo("Mode", mode.ToString()))
                {
                    if (ImGui.Selectable("Manual", mode == TextureProcessingMode.Manual))
                    {
                        _configService.Current.TextureProcessingMode = TextureProcessingMode.Manual;
                        _configService.Save();
                    }
                    if (ImGui.Selectable("Automatic", mode == TextureProcessingMode.Automatic))
                    {
                        _configService.Current.TextureProcessingMode = TextureProcessingMode.Automatic;
                        _configService.Save();
                    }
                    ImGui.EndCombo();
                }
                ShowTooltip("Select how ShrinkU processes textures: Manual or Automatic.");

                bool backup = _configService.Current.EnableBackupBeforeConversion;
                if (ImGui.Checkbox("Enable backup before conversion", ref backup))
                {
                    _configService.Current.EnableBackupBeforeConversion = backup;
                    _configService.Save();
                }
                ShowTooltip("Create a backup of original textures before converting.");
                bool zip = _configService.Current.EnableZipCompressionForBackups;
                if (ImGui.Checkbox("ZIP backups by default", ref zip))
                {
                    _configService.Current.EnableZipCompressionForBackups = zip;
                    _configService.Save();
                }
                ShowTooltip("Compress backup sessions into ZIP archives by default.");
                bool deleteOriginals = _configService.Current.DeleteOriginalBackupsAfterCompression;
                if (ImGui.Checkbox("Delete originals after ZIP", ref deleteOriginals))
                {
                    _configService.Current.DeleteOriginalBackupsAfterCompression = deleteOriginals;
                    _configService.Save();
                }
                ShowTooltip("Delete uncompressed backup folders after ZIP is created.");

                // Backup folder selection
                ImGui.Text("Backup Folder:");
                ImGui.SameLine();
                ImGui.TextWrapped(_configService.Current.BackupFolderPath);
                var browseClicked = ImGui.Button("Browse...");
                ShowTooltip("Choose where ShrinkU stores its backup files.");
                if (browseClicked)
                {
                    OpenFolderPicker();
                }
                ImGui.SameLine();
                var openClicked = ImGui.Button("Open Folder");
                ShowTooltip("Open the current backup folder in Explorer.");
                if (openClicked)
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

                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem("Extras"))
            {
                ImGui.TextColored(new Vector4(0.90f, 0.77f, 0.35f, 1f), "Filters");
                ImGui.Text("Excluded tags:");
                ImGui.SameLine();
                ImGui.SetNextItemWidth(220f);
                ImGui.InputTextWithHint("##excludedTagsSettings", "comma-separated", ref _excludedTagsInput, 256);
                ShowTooltip("Enter comma-separated tags to exclude matching mods.");
                ImGui.SameLine();
                var applyClicked = ImGui.Button("Apply");
                ShowTooltip("Save and apply excluded tags to future scans.");
                if (applyClicked)
                {
                    var tags = _excludedTagsInput
                        .Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries)
                        .Select(NormalizeTag)
                        .Where(s => s.Length > 0)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    _configService.Current.ExcludedModTags = tags;
                    _configService.Save();
                }
                ImGui.EndTabItem();
            }

            ImGui.EndTabBar();
        }
    }

    private static string NormalizeTag(string input)
    {
        return (input ?? string.Empty).Trim();
    }

    private void OpenFolderPicker()
    {
        try
        {
            using var dialog = new FolderBrowserDialog();
            dialog.Description = "Select backup folder for ShrinkU";
            dialog.SelectedPath = _configService.Current.BackupFolderPath;
            dialog.ShowNewFolderButton = true;

            if (dialog.ShowDialog() == DialogResult.OK && !string.IsNullOrWhiteSpace(dialog.SelectedPath))
            {
                _configService.Current.BackupFolderPath = dialog.SelectedPath;
                _configService.Save();
                _logger.LogDebug("Backup folder changed to: {path}", dialog.SelectedPath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open folder picker");
        }
    }
}