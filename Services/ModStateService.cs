using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace ShrinkU.Services;

public sealed class ModStateService
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _config;
    private readonly object _lock = new();
    private Dictionary<string, ModStateEntry> _state = new(StringComparer.OrdinalIgnoreCase);

    public ModStateService(ILogger logger, ShrinkUConfigService config)
    {
        _logger = logger;
        _config = config;
        try { Load(); } catch { }
    }

    private string GetPath()
    {
        var root = _config.Current.BackupFolderPath;
        try { Directory.CreateDirectory(root); } catch { }
        return Path.Combine(root, "mod_state.json");
    }

    public IReadOnlyDictionary<string, ModStateEntry> Snapshot()
    {
        lock (_lock) { return new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase); }
    }

    public ModStateEntry Get(string mod)
    {
        lock (_lock)
        {
            if (!_state.TryGetValue(mod, out var e))
            {
                e = new ModStateEntry { ModFolderName = mod, LastUpdatedUtc = DateTime.UtcNow };
                _state[mod] = e;
            }
            return e;
        }
    }

    public void UpdateBackupFlags(string mod, bool hasTexBackup, bool hasPmpBackup)
    {
        lock (_lock)
        {
            var e = Get(mod);
            e.HasTextureBackup = hasTexBackup;
            e.HasPmpBackup = hasPmpBackup;
            e.LastUpdatedUtc = DateTime.UtcNow;
            Save();
        }
    }

    public void UpdateSavings(string mod, long originalBytes, long currentBytes, int comparedFiles)
    {
        lock (_lock)
        {
            var e = Get(mod);
            e.OriginalBytes = originalBytes;
            e.CurrentBytes = currentBytes;
            e.ComparedFiles = comparedFiles;
            e.LastUpdatedUtc = DateTime.UtcNow;
            Save();
        }
    }

    public void UpdateInstalledButNotConverted(string mod, bool flag)
    {
        lock (_lock)
        {
            var e = Get(mod);
            e.InstalledButNotConverted = flag;
            e.LastUpdatedUtc = DateTime.UtcNow;
            Save();
        }
    }

    public void Load()
    {
        var path = GetPath();
        if (!File.Exists(path)) return;
        try
        {
            var json = File.ReadAllText(path);
            var dict = JsonSerializer.Deserialize<Dictionary<string, ModStateEntry>>(json) ?? new();
            lock (_lock) { _state = new Dictionary<string, ModStateEntry>(dict, StringComparer.OrdinalIgnoreCase); }
        }
        catch { }
    }

    public void Save()
    {
        try
        {
            var path = GetPath();
            var tmp = path + ".tmp";
            var json = JsonSerializer.Serialize(_state);
            File.WriteAllText(tmp, json);
            if (File.Exists(path)) File.Delete(path);
            File.Move(tmp, path);
        }
        catch { }
    }
}

public sealed class ModStateEntry
{
    public string ModFolderName { get; set; } = string.Empty;
    public bool HasTextureBackup { get; set; } = false;
    public bool HasPmpBackup { get; set; } = false;
    public bool InstalledButNotConverted { get; set; } = false;
    public long OriginalBytes { get; set; } = 0L;
    public long CurrentBytes { get; set; } = 0L;
    public int ComparedFiles { get; set; } = 0;
    public DateTime LastUpdatedUtc { get; set; } = DateTime.MinValue;
}