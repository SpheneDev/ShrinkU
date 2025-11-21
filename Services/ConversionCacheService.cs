using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

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
    private readonly ConcurrentDictionary<string, byte> _sizeComputeInFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly System.Threading.SemaphoreSlim _sizeComputeSemaphore = new System.Threading.SemaphoreSlim(4);
    private readonly ConcurrentQueue<string> _sizeQueue = new();
    private readonly System.Threading.SemaphoreSlim _queueSignal = new System.Threading.SemaphoreSlim(0);
    private System.Threading.Tasks.Task? _sizeWorkerTask = null;
    private CancellationTokenSource? _sizeWorkerCts = null;
    private readonly ConcurrentDictionary<string, (string version, string author)> _modsLiveMetaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Task<(string version, string author)?>> _modsLiveMetaTasks = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, System.Threading.Tasks.Task<(string version, string author, DateTime createdUtc, string pmpFileName)?>> _modsPmpMetaTasks = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, System.Threading.Tasks.Task<(string version, string author, DateTime createdUtc, string zipFileName)?>> _modsZipMetaTasks = new(StringComparer.OrdinalIgnoreCase);

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
            var snap = _modState.Snapshot();
            if (snap.TryGetValue(mod, out var e) && e != null)
            {
                var name = e.LatestPmpBackupFileName ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(name))
                {
                    var ver = e.LatestPmpBackupVersion ?? string.Empty;
                    var auth = e.CurrentAuthor ?? string.Empty;
                    var created = e.LatestPmpBackupCreatedUtc;
                    var meta = (ver, auth, created, name);
                    _modsPmpMetaCache[mod] = meta;
                    return meta;
                }
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
            var snap = _modState.Snapshot();
            if (snap.TryGetValue(mod, out var e) && e != null)
            {
                var name = e.LatestZipBackupFileName ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(name))
                {
                    var ver = e.LatestZipBackupVersion ?? string.Empty;
                    var auth = e.CurrentAuthor ?? string.Empty;
                    var created = e.LatestZipBackupCreatedUtc;
                    var meta = (ver, auth, created, name);
                    _modsZipMetaCache[mod] = meta;
                    return meta;
                }
            }
            return null;
        }
        catch { return null; }
    }

    public (string version, string author)? GetOrQueryModLiveMeta(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod)) return null;
            if (_modsLiveMetaCache.TryGetValue(mod, out var v)) return v;
            if (_modsLiveMetaTasks.TryGetValue(mod, out var task))
            {
                if (task.Status == System.Threading.Tasks.TaskStatus.RanToCompletion && task.Result.HasValue)
                {
                    _modsLiveMetaCache[mod] = task.Result.Value;
                    return task.Result.Value;
                }
                return null;
            }
            _modsLiveMetaTasks[mod] = System.Threading.Tasks.Task.Run<(string version, string author)?>(() =>
            {
                try
                {
                    var live = _backup.GetLiveModMeta(mod);
                    return (live.version, live.author);
                }
                catch { return null; }
            });
            _modsLiveMetaTasks[mod].ContinueWith(t =>
            {
                if (t.Status == System.Threading.Tasks.TaskStatus.RanToCompletion && t.Result.HasValue)
                {
                    _modsLiveMetaCache[mod] = t.Result.Value;
                }
            }, System.Threading.Tasks.TaskScheduler.Default);
            return null;
        }
        catch { return null; }
    }

    public long GetCachedOrComputeSize(string path)
    {
        if (_fileSizeCache.TryGetValue(path, out var size))
            return size;
        if (_sizeComputeInFlight.TryAdd(path, 1))
        {
            EnsureSizeWorkerStarted();
            _sizeQueue.Enqueue(path);
            _queueSignal.Release();
        }
        return -1;
    }

    public void WarmupFileSizeCache(IEnumerable<string> files)
    {
        try
        {
            foreach (var f in files)
            {
                if (string.IsNullOrWhiteSpace(f))
                    continue;
                if (_fileSizeCache.ContainsKey(f))
                    continue;
                if (_sizeComputeInFlight.TryAdd(f, 1))
                {
                    EnsureSizeWorkerStarted();
                    _sizeQueue.Enqueue(f);
                    _queueSignal.Release();
                }
            }
        }
        catch { }
    }

    private void EnsureSizeWorkerStarted()
    {
        if (_sizeWorkerTask != null && !_sizeWorkerTask.IsCompleted)
            return;
        _sizeWorkerCts?.Cancel();
        _sizeWorkerCts?.Dispose();
        _sizeWorkerCts = new CancellationTokenSource();
        var token = _sizeWorkerCts.Token;
        _sizeWorkerTask = System.Threading.Tasks.Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await _queueSignal.WaitAsync(token).ConfigureAwait(false);
                    while (_sizeQueue.TryDequeue(out var path))
                    {
                        try
                        {
                            if (string.IsNullOrWhiteSpace(path))
                                continue;
                            if (_fileSizeCache.TryGetValue(path, out _))
                            {
                                _sizeComputeInFlight.TryRemove(path, out _);
                                continue;
                            }
                            await _sizeComputeSemaphore.WaitAsync(token).ConfigureAwait(false);
                            try
                            {
                                var trace = ShrinkU.Helpers.PerfTrace.Step(_logger, $"FileSize {Path.GetFileName(path)}");
                                var s = GetFileSizeSafe(path);
                                _fileSizeCache[path] = s;
                                trace.Dispose();
                            }
                            finally
                            {
                                _sizeComputeSemaphore.Release();
                                _sizeComputeInFlight.TryRemove(path, out _);
                            }
                        }
                        catch
                        {
                            _sizeComputeInFlight.TryRemove(path, out _);
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch { }
            }
        });
    }

    public void Dispose()
    {
        try
        {
            _sizeWorkerCts?.Cancel();
            _queueSignal.Release();
        }
        catch { }
        try { _sizeWorkerTask = null; } catch { }
        try { _sizeWorkerCts?.Dispose(); } catch { }
        _sizeWorkerCts = null;
        try { _sizeComputeSemaphore?.Dispose(); } catch { }
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
