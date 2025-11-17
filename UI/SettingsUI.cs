using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
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
    private readonly Action? _openReleaseNotes;

    // Tag filtering input state (persisted via config on Apply)
    private string _excludedTagsInput = string.Empty;
    private List<string> _excludedTagsEditable = new();

    public SettingsUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, Action? openReleaseNotes = null)
        : base("ShrinkU Settings###ShrinkUSettingsUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _openReleaseNotes = openReleaseNotes;

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(520, 300),
            MaximumSize = new Vector2(1920, 1080),
        };

        // Initialize excluded tags list from config; leave input empty for new additions only
        var savedTags = _configService.Current.ExcludedModTags ?? new List<string>();
        _excludedTagsInput = string.Empty;
        _excludedTagsEditable = savedTags
            .Select(NormalizeTag)
            .Where(s => s.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    // Helper to show a tooltip when the last item is hovered (also when disabled)
    private static void ShowTooltip(string text)
    {
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip(text);
    }

    // Helper to show a width-constrained tooltip with wrapped text
    private static void ShowTooltipWrapped(string text, float maxWidth)
    {
        if (!ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            return;

        ImGui.BeginTooltip();
        ImGui.PushTextWrapPos(ImGui.GetCursorPosX() + maxWidth);
        ImGui.TextUnformatted(text);
        ImGui.PopTextWrapPos();
        ImGui.EndTooltip();
    }

    public override void Draw()
    {
        // Subtle accent header bar for branding
        var headerStart = ImGui.GetCursorScreenPos();
        var headerWidth = Math.Max(1f, ImGui.GetContentRegionAvail().X);
        var headerEnd = new Vector2(headerStart.X + headerWidth, headerStart.Y + 2f);
        ImGui.GetWindowDrawList().AddRectFilled(headerStart, headerEnd, ShrinkUColors.ToImGuiColor(ShrinkUColors.Accent));
        ImGui.Dummy(new Vector2(0, 6f));

        if (ImGui.BeginTabBar("SettingsTabs"))
        {
            if (ImGui.BeginTabItem("General"))
            {
                ImGui.SetWindowFontScale(1.15f);
                ImGui.TextColored(ShrinkUColors.Accent, "Texture Settings");
                ImGui.Dummy(new Vector2(0, 6f));
                ImGui.SetWindowFontScale(1.0f);

                // Open Release Notes button
                if (ImGui.Button("Open Release Notes"))
                {
                    try { _openReleaseNotes?.Invoke(); } catch { }
                }
                ShowTooltip("Open recent changes and highlights for ShrinkU.");
                ImGui.Spacing();
                ImGui.TextColored(ShrinkUColors.Accent, "Processing");
                ImGui.Spacing();
                var mode = _configService.Current.TextureProcessingMode;
                var controller = _configService.Current.AutomaticControllerName ?? string.Empty;
                var spheneIntegrated = !string.IsNullOrWhiteSpace(controller) || _configService.Current.AutomaticHandledBySphene;
                var modeDisplay = mode == TextureProcessingMode.Automatic && spheneIntegrated ? "Automatic (handled by Sphene)" : mode.ToString();
                if (ImGui.BeginCombo("Mode", modeDisplay))
                {
                    if (ImGui.Selectable("Manual", mode == TextureProcessingMode.Manual))
                    {
                        _configService.Current.TextureProcessingMode = TextureProcessingMode.Manual;
                        _configService.Current.AutomaticHandledBySphene = false;
                        _configService.Current.AutomaticControllerName = string.Empty;
                        _configService.Save();
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
                        }
                    }
                    ImGui.EndCombo();
                }
                ShowTooltip("Select how ShrinkU processes textures. When integrated with Sphene, only the Sphene-handled automatic mode is available.");

                ImGui.Separator();
                ImGui.TextColored(ShrinkUColors.Accent, "Backups");
                ImGui.Spacing();
                var texturesBackup = _configService.Current.EnableBackupBeforeConversion;
                var fullModBackup = _configService.Current.EnableFullModBackupBeforeConversion;
                if (!texturesBackup && !fullModBackup)
                {
                    _configService.Current.EnableBackupBeforeConversion = true;
                    _configService.Current.EnableFullModBackupBeforeConversion = false;
                    _configService.Save();
                    texturesBackup = true;
                    fullModBackup = false;
                }
                string currentBackupMode = (texturesBackup, fullModBackup) switch
                {
                    (true, false) => "Texture",
                    (false, true) => "Full Mod",
                    (true, true) => "Both",
                    _ => "Texture",
                };
                ImGui.TextUnformatted("Backup Mode:");
                ImGui.SameLine();
                if (ImGui.BeginCombo("##ShrinkUBackupMode", currentBackupMode))
                {
                    if (ImGui.Selectable("Texture", currentBackupMode == "Texture"))
                    {
                        _configService.Current.EnableBackupBeforeConversion = true;
                        _configService.Current.EnableFullModBackupBeforeConversion = false;
                        _configService.Save();
                    }
                    if (ImGui.Selectable("Full Mod", currentBackupMode == "Full Mod"))
                    {
                        _configService.Current.EnableBackupBeforeConversion = false;
                        _configService.Current.EnableFullModBackupBeforeConversion = true;
                        _configService.Save();
                    }
                    if (ImGui.Selectable("Both", currentBackupMode == "Both"))
                    {
                        _configService.Current.EnableBackupBeforeConversion = true;
                        _configService.Current.EnableFullModBackupBeforeConversion = true;
                        _configService.Save();
                    }
                    ImGui.EndCombo();
                }
                ShowTooltipWrapped("Backup Modes:\n- Texture: smaller storage; per-file restore\n- Full Mod: larger storage; safer full restore\n- Both: combines advantages; highest storage use", 420f);

                ImGui.Separator();
                ImGui.TextColored(ShrinkUColors.Accent, "Restore");
                ImGui.Spacing();
                var preferPmp = _configService.Current.PreferPmpRestoreWhenAvailable;
                if (ImGui.Checkbox("Prefer PMP restore when available", ref preferPmp))
                {
                    _configService.Current.PreferPmpRestoreWhenAvailable = preferPmp;
                    if (preferPmp)
                    {
                        _configService.Current.EnableBackupBeforeConversion = false;
                    }
                    _configService.Save();
                    _logger.LogDebug("Updated preference for PMP restore: {value}", preferPmp);
                }
                ShowTooltipWrapped("If a full-mod PMP backup exists, ShrinkU will prefer restoring it. When this is enabled, per-texture backups are disabled unless you explicitly enable 'Enable backup before conversion'.", 420f);
                bool autoRestore = _configService.Current.AutoRestoreInefficientMods;
                if (ImGui.Checkbox("Auto-restore backups for inefficient mods", ref autoRestore))
                {
                    _configService.Current.AutoRestoreInefficientMods = autoRestore;
                    _configService.Save();
                }
                ShowTooltip("Automatically restore the latest backup when a mod becomes larger after conversion.");

                ImGui.Separator();
                ImGui.TextColored(ShrinkUColors.Accent, "Overview");
                ImGui.Spacing();
                bool showFiles = _configService.Current.ShowModFilesInOverview;
                if (ImGui.Checkbox("Show file rows in overview", ref showFiles))
                {
                    _configService.Current.ShowModFilesInOverview = showFiles;
                    _configService.Save();
                }
                ShowTooltip("Display individual files under each mod in the overview table.");
                bool includeHiddenOnConvert = _configService.Current.IncludeHiddenModTexturesOnConvert;
                if (ImGui.Checkbox("Include hidden mod textures on Convert (UI)", ref includeHiddenOnConvert))
                {
                    _configService.Current.IncludeHiddenModTexturesOnConvert = includeHiddenOnConvert;
                    _configService.Save();
                }
                ShowTooltip("When converting via ShrinkU UI, include non-visible mod textures even if filters hide them. Sphene automatic behavior remains unchanged.");

                ImGui.Separator();
                ImGui.TextColored(ShrinkUColors.Accent, "Storage");
                ImGui.Spacing();
                ImGui.Text("Backup Folder:");
                ImGui.SameLine();
                ImGui.TextWrapped(_configService.Current.BackupFolderPath);
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
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
                ImGui.PopStyleColor(4);

                try
                {
                    var backupDir = _configService.Current.BackupFolderPath;
                    if (!string.IsNullOrWhiteSpace(backupDir) && Directory.Exists(backupDir))
                    {
                        long zipBytes = 0;
                        int zipCount = 0;
                        long pmpBytes = 0;
                        int pmpCount = 0;
                        foreach (var file in Directory.EnumerateFiles(backupDir, "*", SearchOption.AllDirectories))
                        {
                            try
                            {
                                var name = Path.GetFileName(file) ?? string.Empty;
                                var len = new FileInfo(file).Length;
                                if (name.StartsWith("backup_", StringComparison.OrdinalIgnoreCase) && name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                                {
                                    zipCount++;
                                    zipBytes += len;
                                }
                                else if (name.StartsWith("mod_backup_", StringComparison.OrdinalIgnoreCase) && name.EndsWith(".pmp", StringComparison.OrdinalIgnoreCase))
                                {
                                    pmpCount++;
                                    pmpBytes += len;
                                }
                            }
                            catch { }
                        }
                        ImGui.Spacing();
                        ImGui.TextColored(ShrinkUColors.Accent, "Backup Summary");
                        ImGui.Text($"ZIP Backups: {zipCount} ({FormatSize(zipBytes)})");
                        ImGui.Text($"PMP Backups: {pmpCount} ({FormatSize(pmpBytes)})");
                    }
                }
                catch { }

                ImGui.EndTabItem();
            }

            if (ImGui.BeginTabItem("Filters"))
            {
                ImGui.SetWindowFontScale(1.15f);
                ImGui.TextColored(ShrinkUColors.Accent, "Filters");
                ImGui.Dummy(new Vector2(0, 6f));
                ImGui.SetWindowFontScale(1.0f);
                ImGui.Text("Excluded tags:");
                ImGui.SameLine();
                ImGui.SetNextItemWidth(220f);
                // Add tag on Enter and show it below in the list
                var enterAdd = ImGui.InputTextWithHint("##excludedTagsAdd", "type tag and press Enter", ref _excludedTagsInput, 128, ImGuiInputTextFlags.EnterReturnsTrue);
                ShowTooltip("Type a tag and press Enter to add it.");
                if (enterAdd)
                {
                    var tag = NormalizeTag(_excludedTagsInput);
                    if (tag.Length > 0 && !_excludedTagsEditable.Contains(tag, StringComparer.OrdinalIgnoreCase))
                    {
                        _excludedTagsEditable.Add(tag);
                        // Persist immediately and notify listeners
                        var tags = _excludedTagsEditable
                            .Select(NormalizeTag)
                            .Where(s => s.Length > 0)
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .ToList();
                        _configService.UpdateExcludedTags(tags);
                    }
                    _excludedTagsInput = string.Empty;
                }

                // Show current excluded tags as chips with a trailing X to remove, wrapping inside a framed field
                ImGui.Spacing();
                ImGui.Text("Current excluded:");
                var fieldSize = new Vector2(-1, 100f);
                ImGui.BeginChild("##ExcludedTagsField", fieldSize, true);
                var style = ImGui.GetStyle();
                var itemSpacingX = style.ItemSpacing.X;
                var chipPaddingX = 6f;
                var chipPaddingY = 4f;
                var chipRounding = 4f;
                var chipSpacingX = itemSpacingX;
                for (int i = 0; i < _excludedTagsEditable.Count; i++)
                {
                    var tag = _excludedTagsEditable[i];
                    // Measure chip
                    var textSize = ImGui.CalcTextSize(tag);
                    var iconSize = ImGui.CalcTextSize(FontAwesomeIcon.Times.ToIconString());
                    var xButtonWidth = iconSize.X + 6f;
                    var chipHeight = ImGui.GetTextLineHeight() + (chipPaddingY * 2f);
                    var chipWidth = textSize.X + xButtonWidth + (chipPaddingX * 2f) + 2f;

                    // Wrap to next line if not enough width
                    var availX = ImGui.GetContentRegionAvail().X;
                    if (chipWidth > availX && ImGui.GetCursorPosX() > 0f)
                    {
                        ImGui.NewLine();
                    }

                    var startLocal = ImGui.GetCursorPos();
                    var startScreen = ImGui.GetCursorScreenPos();
                    var endScreen = new Vector2(startScreen.X + chipWidth, startScreen.Y + chipHeight);

                    // Draw chip background
                    var drawList = ImGui.GetWindowDrawList();
                    var bgCol = ImGui.GetColorU32(ImGuiCol.FrameBg);
                    drawList.AddRectFilled(startScreen, endScreen, bgCol, chipRounding);
                    var borderCol = ImGui.GetColorU32(ImGuiCol.Border);
                    drawList.AddRect(startScreen, endScreen, borderCol, chipRounding);

                    // Calculate vertical centering for both text and button
                    var textHeight = ImGui.GetTextLineHeight();
                    var verticalCenter = startLocal.Y + (chipHeight - textHeight) * 0.5f;

                    // Draw tag text
                    ImGui.SetCursorPos(new Vector2(startLocal.X + chipPaddingX, verticalCenter));
                    ImGui.TextUnformatted(tag);

                    // Draw remove X button inside the chip
                    var xPosLocal = new Vector2(startLocal.X + chipWidth - chipPaddingX - (xButtonWidth), verticalCenter);
                    ImGui.SetCursorPos(xPosLocal);
                    
                    ImGui.PushFont(UiBuilder.IconFont);
                    ImGui.PushStyleVar(ImGuiStyleVar.FramePadding, new Vector2(2f, 2f)); // Minimal button padding
                    if (ImGui.SmallButton($"{FontAwesomeIcon.Times.ToIconString()}##remove-chip-{i}"))
                    {
                        ImGui.PopStyleVar(); 
                        ImGui.PopFont();
                        _excludedTagsEditable.RemoveAt(i);
                        i--; // adjust index after removal
                        // Persist immediately and notify listeners
                        var tags = _excludedTagsEditable
                            .Select(NormalizeTag)
                            .Where(s => s.Length > 0)
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .ToList();
                        _configService.UpdateExcludedTags(tags);
                        // After removal, continue to next chip position
                        ImGui.SetCursorPos(new Vector2(startLocal.X + chipWidth + chipSpacingX, startLocal.Y));
                        continue;
                    }
                    ImGui.PopStyleVar();
                    ImGui.PopFont();

                    // Advance cursor to next chip position
                    ImGui.SetCursorPos(new Vector2(startLocal.X + chipWidth + chipSpacingX, startLocal.Y));
                }
                ImGui.EndChild();

                // Apply button removed: changes are saved immediately on add/remove
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

    private string FormatSize(long bytes)
    {
        if (bytes < 0) return "-";
        string[] units = { "B", "KB", "MB", "GB", "TB" };
        double size = bytes;
        int unit = 0;
        while (size >= 1024 && unit < units.Length - 1)
        {
            size /= 1024;
            unit++;
        }
        return unit == 0 ? $"{bytes} {units[unit]}" : $"{size:0.##} {units[unit]}";
    }
}