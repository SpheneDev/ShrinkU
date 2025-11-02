using Dalamud.Plugin;
using Dalamud.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

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
            _pi.SavePluginConfig(_current);
            _logger.LogDebug("Saved ShrinkU configuration");
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
            var cfg = _pi.GetPluginConfig() as ShrinkUConfig;
            if (cfg != null)
                _current = cfg;
            _logger.LogDebug("Loaded ShrinkU configuration");
        }
        catch
        {
            // Swallow to avoid noisy logs
        }
    }
}