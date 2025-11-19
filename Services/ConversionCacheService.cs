using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace ShrinkU.Services;

public sealed class ConversionCacheService
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _config;
    private readonly TextureBackupService _backup;
    private readonly ModStateService _modState;

    private readonly ConcurrentDictionary<string, bool> _modsWithBackupCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _modsWithTexBackupCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _modsWithPmpCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, (string version, string author, DateTime createdUtc, string pmpFileName)> _modsPmpMetaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, (string version, string author, DateTime createdUtc, string zipFileName)> _modsZipMetaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _fileSizeCache = new(StringComparer.OrdinalIgnoreCase);

    public ConversionCacheService(ILogger logger, ShrinkUConfigService config, TextureBackupService backup, ModStateService modState)
    {
        _logger = logger;
        _config = config;
        _backup = backup;
        _modState = modState;
    }

    public void SetModHasBackup(string mod, bool value) { _modsWithBackupCache[mod] = value; }
    public void SetModHasPmp(string mod, bool value) { _modsWithPmpCache[mod] = value; }
    public void ClearModCaches(string mod)
    {
        try { _modsWithBackupCache.TryRemove(mod, out _); } catch { }
        try { _modsWithTexBackupCache.TryRemove(mod, out _); } catch { }
        try { _modsWithPmpCache.TryRemove(mod, out _); } catch { }
        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
        try { (string version, string author, DateTime createdUtc, string zipFileName) _rz; _modsZipMetaCache.TryRemove(mod, out _rz); } catch { }
    }
    public void ClearAll()
    {
        try { _modsWithBackupCache.Clear(); } catch { }
        try { _modsWithTexBackupCache.Clear(); } catch { }
        try { _modsWithPmpCache.Clear(); } catch { }
        try { _modsPmpMetaCache.Clear(); } catch { }
        try { _modsZipMetaCache.Clear(); } catch { }
        try { _fileSizeCache.Clear(); } catch { }
    }

    public bool GetOrQueryModBackup(string mod)
    {
        try
        {
            var snap = _modState.Snapshot();
            if (snap.TryGetValue(mod, out var e))
                return e.HasTextureBackup || e.HasPmpBackup;
        }
        catch { }
        return false;
    }

    public bool GetOrQueryModTextureBackup(string mod)
    {
        try
        {
            var snap = _modState.Snapshot();
            if (snap.TryGetValue(mod, out var e))
                return e.HasTextureBackup;
        }
        catch { }
        return false;
    }

    public bool GetOrQueryModPmp(string mod)
    {
        try
        {
            var snap = _modState.Snapshot();
            if (snap.TryGetValue(mod, out var e))
                return e.HasPmpBackup;
        }
        catch { }
        return false;
    }

    public (string version, string author, DateTime createdUtc, string pmpFileName)? GetOrQueryModPmpMeta(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod)) return null;
            if (_modsPmpMetaCache.TryGetValue(mod, out var v)) return v;
            var meta = _backup.GetLatestPmpManifestForModAsync(mod).GetAwaiter().GetResult();
            if (meta.HasValue)
            {
                _modsPmpMetaCache[mod] = meta.Value;
                return meta.Value;
            }
            return null;
        }
        catch { return null; }
    }

    public (string version, string author, DateTime createdUtc, string zipFileName)? GetOrQueryModZipMeta(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod)) return null;
            if (_modsZipMetaCache.TryGetValue(mod, out var v)) return v;
            var meta = _backup.GetLatestZipMetaForModAsync(mod).GetAwaiter().GetResult();
            if (meta.HasValue)
            {
                _modsZipMetaCache[mod] = meta.Value;
                return meta.Value;
            }
            return null;
        }
        catch { return null; }
    }

    public long GetCachedOrComputeSize(string path)
    {
        if (_fileSizeCache.TryGetValue(path, out var size))
            return size;
        size = GetFileSizeSafe(path);
        _fileSizeCache[path] = size;
        return size;
    }

    public void WarmupFileSizeCache(IEnumerable<string> files)
    {
        try
        {
            foreach (var f in files)
            {
                var size = GetFileSizeSafe(f);
                _fileSizeCache[f] = size;
            }
        }
        catch { }
    }

    private static long GetFileSizeSafe(string path)
    {
        try
        {
            var fi = new FileInfo(path);
            return fi.Exists ? fi.Length : -1;
        }
        catch { return -1; }
    }
}