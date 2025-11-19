using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System.Numerics;
using System;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;

namespace ShrinkU.UI;

public sealed class FirstRunSetupUI : Window
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private string _selectedFolder = string.Empty;

    public Action? OnCompleted;

    public FirstRunSetupUI(ILogger logger, ShrinkUConfigService configService)
        : base("ShrinkU First Start Setup###ShrinkUFirstRunSetupUI")
    {
        _logger = logger;
        _configService = configService;

        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(540, 380),
            MaximumSize = new Vector2(1920, 1080),
        };

        _selectedFolder = _configService.Current.BackupFolderPath ?? string.Empty;
    }

    

    public override void Draw()
    {
        UiHeader.DrawAccentHeaderBar();

        ImGui.TextColored(ShrinkUColors.Accent, "Welcome to ShrinkU");
        ImGui.Separator();
        ImGui.TextWrapped("ShrinkU helps you reduce texture sizes from Penumbra mods to save disk space and improve performance. It can back up originals and restore them later.");

        ImGui.Spacing();
        ImGui.TextColored(ShrinkUColors.Accent, "What ShrinkU does:");
        ImGui.BulletText("Scan Penumbra mod folders for textures");
        ImGui.BulletText("Optionally back up originals before conversion");
        ImGui.BulletText("Convert textures to smaller formats");
        ImGui.BulletText("Restore from backups if needed");

        ImGui.Spacing();
        ImGui.TextColored(ShrinkUColors.Accent, "Before you start:");
        ImGui.TextWrapped("Please choose a folder where ShrinkU will store backups. You can change this later in Settings.");

        ImGui.Spacing();
        ImGui.Text("Backup Folder:");
        ImGui.SameLine();
        ImGui.TextWrapped(string.IsNullOrWhiteSpace(_selectedFolder) ? "(not selected)" : _selectedFolder);

        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        var browseClicked = ImGui.Button("Browse...");
        UiTooltip.Show("Choose the folder where backups will be stored.");
        if (browseClicked)
        {
            OpenFolderPicker();
        }
        ImGui.SameLine();
        bool canOpen = !string.IsNullOrWhiteSpace(_selectedFolder);
        if (!canOpen)
            ImGui.BeginDisabled();
        var openClicked = ImGui.Button("Open Folder");
        UiTooltip.Show("Open the selected backup folder in Explorer.");
        if (openClicked)
        {
            TryOpenFolder(_selectedFolder);
        }
        if (!canOpen)
            ImGui.EndDisabled();
        ImGui.PopStyleColor(4);

        ImGui.Spacing();
        bool canComplete = DirectoryExistsSafe(_selectedFolder);
        if (!canComplete)
            ImGui.BeginDisabled();

        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        var completeClicked = ImGui.Button("Complete Setup");
        UiTooltip.Show("Finish setup and enable the plugin.");
        if (completeClicked)
        {
            try
            {
                _configService.Current.BackupFolderPath = _selectedFolder;
                _configService.Current.FirstRunCompleted = true;
                _configService.Save();
                _logger.LogDebug("First run setup completed. Backup folder: {path}", _selectedFolder);
            }
            catch { }

            try
            {
                OnCompleted?.Invoke();
            }
            catch { }

            IsOpen = false;
        }
        ImGui.PopStyleColor(3);

        if (!canComplete)
            ImGui.EndDisabled();
    }

    private void OpenFolderPicker()
    {
        try
        {
            using var dialog = new FolderBrowserDialog();
            dialog.Description = "Select backup folder for ShrinkU";
            dialog.SelectedPath = string.IsNullOrWhiteSpace(_selectedFolder) ? _configService.Current.BackupFolderPath : _selectedFolder;
            dialog.ShowNewFolderButton = true;

            if (dialog.ShowDialog() == DialogResult.OK && !string.IsNullOrWhiteSpace(dialog.SelectedPath))
            {
                _selectedFolder = dialog.SelectedPath;
                try { Directory.CreateDirectory(_selectedFolder); } catch { }
                _logger.LogDebug("Setup selected backup folder: {path}", _selectedFolder);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open folder picker during setup");
        }
    }

    private static bool DirectoryExistsSafe(string path)
    {
        try { return !string.IsNullOrWhiteSpace(path) && Directory.Exists(path); }
        catch { return false; }
    }

    private static void TryOpenFolder(string path)
    {
        try
        {
            if (!string.IsNullOrWhiteSpace(path))
            {
                try { Directory.CreateDirectory(path); } catch { }
                Process.Start(new ProcessStartInfo("explorer.exe", path) { UseShellExecute = true });
            }
        }
        catch { }
    }
}