using Dalamud.Plugin;
using Dalamud.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text.Json;

namespace ShrinkU.Configuration;

public sealed class ShrinkUConfigService
{
    private readonly IDalamudPluginInterface _pi;
    private readonly ILogger _logger;
    private ShrinkUConfig _current = new();
    private FileSystemWatcher? _watcher;
    private volatile bool _suppressWatch;
    private DateTime _lastExternalReloadUtc = DateTime.MinValue;
    private DateTime _lastWriteUtc = DateTime.MinValue;

    public event Action? OnExcludedTagsUpdated;
    public event Action? OnExcludedModsUpdated;

    public ShrinkUConfigService(IDalamudPluginInterface pi, ILogger logger)
    {
        _pi = pi;
        _logger = logger;
        Load();
        SetupWatcher();
    }

    public ShrinkUConfig Current => _current;

    public void Save()
    {
        try
        {
            // Ensure ExcludedModTags are normalized and distinct before persisting
            try
            {
                var tags = _current.ExcludedModTags ?? new List<string>();
                _current.ExcludedModTags = tags
                    .Select(NormalizeTag)
                    .Where(s => s.Length > 0)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }
            catch { }

            var cfgRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "XIVLauncher", "pluginConfigs");
            if (!string.IsNullOrWhiteSpace(cfgRoot))
            {
                try { Directory.CreateDirectory(cfgRoot); } catch { }
                var path = Path.Combine(cfgRoot, "ShrinkU.json");
                try
                {
                    _suppressWatch = true;
                    var opts = new JsonSerializerOptions { WriteIndented = true };
                    var json = JsonSerializer.Serialize(_current, opts);
                    File.WriteAllText(path, json);
                    _lastWriteUtc = DateTime.UtcNow;
                    _logger.LogDebug("Saved ShrinkU configuration to file: {path}", path);
                    try { OnExcludedModsUpdated?.Invoke(); } catch { }
                }
                catch { }
                finally { _suppressWatch = false; }
            }
            else
            {
                // No ConfigDirectory available; skip API save to avoid creating Sphene.json
            }
        }
        catch
        {
            // Swallow to avoid noisy logs
        }
    }

    // Update excluded tags, persist immediately, and notify listeners
    public void UpdateExcludedTags(List<string> tags)
    {
        try
        {
            _current.ExcludedModTags = tags ?? new List<string>();
            Save();
            try { OnExcludedTagsUpdated?.Invoke(); } catch { }
        }
        catch
        {
            // Swallow to avoid noisy logs
        }
    }

    private void Load()
    {
        try
        {
            var cfgRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "XIVLauncher", "pluginConfigs");
            var path = string.IsNullOrWhiteSpace(cfgRoot) ? string.Empty : Path.Combine(cfgRoot, "ShrinkU.json");
            bool loadedFromFile = false;
            if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
            {
                try
                {
                    var json = File.ReadAllText(path);
                    var cfgFile = JsonSerializer.Deserialize<ShrinkUConfig>(json);
                    if (cfgFile != null)
                    {
                        _current = cfgFile;
                        loadedFromFile = true;
                        _logger.LogDebug("Loaded ShrinkU configuration from file: {path}", path);
                    }
                }
                catch { }
            }

            if (!loadedFromFile)
            {
                try
                {
                    if (!string.IsNullOrWhiteSpace(path))
                    {
                        var opts = new JsonSerializerOptions { WriteIndented = true };
                        var json = JsonSerializer.Serialize(_current, opts);
                        File.WriteAllText(path, json);
                        _logger.LogDebug("Initialized ShrinkU configuration file: {path}", path);
                    }
                }
                catch { }
            }
            // Normalize and deduplicate any existing tags to avoid repeated entries
            try
            {
                var tags = _current.ExcludedModTags ?? new List<string>();
                _current.ExcludedModTags = tags
                    .Select(NormalizeTag)
                    .Where(s => s.Length > 0)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }
            catch { }
            try
            {
                var mods = _current.ExcludedMods ?? new HashSet<string>();
                var norm = mods
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .Select(s => s.Trim())
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
                _current.ExcludedMods = new HashSet<string>(norm, StringComparer.OrdinalIgnoreCase);
                _logger.LogDebug("[TRACE-EXCLUDE-SPHENE] Normalized excluded mods: count={count}", _current.ExcludedMods.Count);
            }
            catch { }
            _logger.LogDebug("Loaded ShrinkU configuration");
        }
        catch
        {
            // Swallow to avoid noisy logs
        }
    }

    private void SetupWatcher()
    {
        try
        {
            var cfgRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "XIVLauncher", "pluginConfigs");
            var path = string.IsNullOrWhiteSpace(cfgRoot) ? string.Empty : Path.Combine(cfgRoot, "ShrinkU.json");
            if (string.IsNullOrWhiteSpace(path)) return;
            var dir = Path.GetDirectoryName(path);
            var file = Path.GetFileName(path);
            if (string.IsNullOrWhiteSpace(dir) || string.IsNullOrWhiteSpace(file)) return;
            _watcher = new FileSystemWatcher(dir, file)
            {
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.Size,
                EnableRaisingEvents = true,
            };
            FileSystemEventHandler handler = (s, e) => { HandleExternalConfigChange(path); };
            RenamedEventHandler renamed = (s, e) => { HandleExternalConfigChange(path); };
            _watcher.Changed += handler;
            _watcher.Created += handler;
            _watcher.Renamed += renamed;
        }
        catch { }
    }

    private void HandleExternalConfigChange(string path)
    {
        if (_suppressWatch) return;
        var now = DateTime.UtcNow;
        if ((now - _lastWriteUtc) < TimeSpan.FromMilliseconds(800)) return;
        if ((now - _lastExternalReloadUtc) < TimeSpan.FromMilliseconds(200)) return;
        _lastExternalReloadUtc = now;
        try
        {
            var json = File.ReadAllText(path);
            var cfgFile = JsonSerializer.Deserialize<ShrinkUConfig>(json);
            if (cfgFile == null) return;
            var prevTags = _current.ExcludedModTags ?? new List<string>();
            var prevMods = _current.ExcludedMods ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _current = cfgFile;
            var newTags = _current.ExcludedModTags ?? new List<string>();
            var newMods = _current.ExcludedMods ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            bool tagsChanged = !prevTags.SequenceEqual(newTags, StringComparer.OrdinalIgnoreCase);
            bool modsChanged = !prevMods.SetEquals(newMods);
            if (modsChanged) { try { OnExcludedModsUpdated?.Invoke(); } catch { } }
            if (tagsChanged) { try { OnExcludedTagsUpdated?.Invoke(); } catch { } }
            _logger.LogDebug("ShrinkU configuration reloaded due to external change");
            try
            {
                var sample = string.Join("|", newMods.Take(3));
                _logger.LogDebug("[TRACE-EXCLUDE-SPHENE] External change: excludedMods={count} sample={sample}", newMods.Count, sample);
            }
            catch { }
        }
        catch { }
    }

    private static string NormalizeTag(string tag)
    {
        return (tag ?? string.Empty).Trim();
    }
}
