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

    public event Action? OnExcludedTagsUpdated;

    public ShrinkUConfigService(IDalamudPluginInterface pi, ILogger logger)
    {
        _pi = pi;
        _logger = logger;
        Load();
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
                    var opts = new JsonSerializerOptions { WriteIndented = true };
                    var json = JsonSerializer.Serialize(_current, opts);
                    File.WriteAllText(path, json);
                    _logger.LogDebug("Saved ShrinkU configuration to file: {path}", path);
                }
                catch { }
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
            _logger.LogDebug("Loaded ShrinkU configuration");
        }
        catch
        {
            // Swallow to avoid noisy logs
        }
    }

    private static string NormalizeTag(string tag)
    {
        return (tag ?? string.Empty).Trim();
    }
}