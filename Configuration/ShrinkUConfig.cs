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
    public bool AutomaticHandledBySphene { get; set; } = false;
    public string AutomaticControllerName { get; set; } = string.Empty;
    public bool EnableBackupBeforeConversion { get; set; } = false;
    public bool EnableFullModBackupBeforeConversion { get; set; } = true;
    public bool EnableZipCompressionForBackups { get; set; } = true;
    public bool DeleteOriginalBackupsAfterCompression { get; set; } = true;
    public bool StrictPerModRestore { get; set; } = true;
    public bool DeleteOldBackupsOnVersionChange { get; set; } = false;
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
    public bool FilterPenumbraUsedOnly { get; set; } = false;
    public bool FilterNonConvertibleMods { get; set; } = true;
    public string ScanSortKey { get; set; } = "ModName"; // "FileName" or "ModName"
    public bool ScanSortAsc { get; set; } = true;
    // Control visibility of individual file rows in the overview table
    public bool ShowModFilesInOverview { get; set; } = false;
    // When converting via ShrinkU UI, include hidden mod textures even if filtered out
    public bool IncludeHiddenModTexturesOnConvert { get; set; } = true;

    // Inefficient mods handling
    public List<string> InefficientMods { get; set; } = new List<string>();
    public bool HideInefficientMods { get; set; } = false;
    public bool AutoRestoreInefficientMods { get; set; } = true;

    public Dictionary<string, ExternalChangeMarker> ExternalConvertedMods { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    // First-run setup gating
    public bool FirstRunCompleted { get; set; } = false;

    // Release changelog tracking
    public string LastSeenReleaseChangelogVersion { get; set; } = string.Empty;
    public string ReleaseChangelogUrl { get; set; } = "https://sphene.online/shrinku/change_log.json";

    // Debug tracing controls
    public bool DebugTraceModStateChanges { get; set; } = false;
    public bool DebugTraceUiRefresh { get; set; } = false;
    public bool DebugTraceActions { get; set; } = false;

    public int MaxStartupThreads { get; set; } = Math.Max(1, Math.Min(Environment.ProcessorCount, 4));
    public int StartupMaxDurationSeconds { get; set; } = 30;
    public int StartupFolderTimeoutSeconds { get; set; } = 10;
    public int StartupCpuLimitPercent { get; set; } = 80;
    public bool EnableStartupPriorityQueue { get; set; } = true;
}

public sealed class ExternalChangeMarker
{
    public string Reason { get; set; } = string.Empty;
    public DateTime AtUtc { get; set; } = DateTime.MinValue;
}
