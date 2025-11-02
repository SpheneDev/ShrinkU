using Dalamud.Configuration;
using System;
using System.IO;
using System.Collections.Generic;

namespace ShrinkU.Configuration;

public enum TextureProcessingMode
{
    Manual = 0,
    Automatic = 1,
}

public sealed class ShrinkUConfig : IPluginConfiguration
{
    public int Version { get; set; } = 1;
    // Texture processing settings
    public TextureProcessingMode TextureProcessingMode { get; set; } = TextureProcessingMode.Manual;
    public bool EnableBackupBeforeConversion { get; set; } = true;
    public bool EnableZipCompressionForBackups { get; set; } = true;
    public bool DeleteOriginalBackupsAfterCompression { get; set; } = true;
    public bool StrictPerModRestore { get; set; } = true;
    // Conversion filters
    public List<string> ExcludedModTags { get; set; } = new List<string> { "UI" };
    public List<string> KnownModTags { get; set; } = new List<string>();
    
    // Backup folder configuration
    public string BackupFolderPath { get; set; } = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "ShrinkU", "Backups");

    // UI layout persistence
    public float LeftPanelWidthPx { get; set; } = 0f;
    public float ScannedFilesFirstColWidth { get; set; } = 30f;
    public float ScannedFilesSizeColWidth { get; set; } = 85f;
    public float ScannedFilesActionColWidth { get; set; } = 60f;

    // Conversion UI settings (persisted)
    public bool UseFolderStructure { get; set; } = true;
    public bool FilterPenumbraUsedOnly { get; set; } = false;
    public bool FilterNonConvertibleMods { get; set; } = true;
    public string ScanSortKey { get; set; } = "ModName"; // "FileName" or "ModName"
    public bool ScanSortAsc { get; set; } = true;
    // Control visibility of individual file rows in the overview table
    public bool ShowModFilesInOverview { get; set; } = false;

    // First-run setup gating
    public bool FirstRunCompleted { get; set; } = false;
}