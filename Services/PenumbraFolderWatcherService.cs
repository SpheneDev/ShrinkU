using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ShrinkU.Services;

public sealed class PenumbraFolderWatcherService : IDisposable
{
    private readonly ILogger _logger;
    private readonly PenumbraIpc _penumbraIpc;
    private readonly ModStateService _modStateService;
    private readonly object _lock = new();
    private FileSystemWatcher? _watcher;
    private CancellationTokenSource? _debounceCts;
    private CancellationTokenSource? _rescanCts;
    private readonly List<(string Kind, string Path)> _pendingChanges = new();
    private Dictionary<string, long> _dirIndex = new(StringComparer.Ordinal);
    private bool _dirIndexReady;
    private volatile bool _stoppingWatcher;
    private volatile bool _disposing;
    private int _disposeStarted;
    private string _rootPath = string.Empty;
    private bool _watcherActive;
    private DateTime _lastEventUtc = DateTime.MinValue;
    private string _lastEventKind = string.Empty;
    private string _lastEventPath = string.Empty;
    private int _eventBurstCount;
    private DateTime _lastScanUtc = DateTime.MinValue;
    private int _lastScanDurationMs;
    private int _lastScanDirectoryCount;
    private DateTime _lastScanMaxWriteUtc = DateTime.MinValue;
    private string _lastFingerprint = string.Empty;
    private string _storedFingerprint = string.Empty;
    private DateTime _storedFingerprintUtc = DateTime.MinValue;
    private string _startupStoredFingerprint = string.Empty;
    private DateTime _startupStoredFingerprintUtc = DateTime.MinValue;
    private string _startupStoredRootPath = string.Empty;
    private string _lastError = string.Empty;
    private string _lastChangeReason = string.Empty;
    private bool _startupDiffDetected;

    public event Action<string>? OnFolderChanged;

    public PenumbraFolderWatcherService(ILogger logger, PenumbraIpc penumbraIpc, ModStateService modStateService)
    {
        _logger = logger;
        _penumbraIpc = penumbraIpc;
        _modStateService = modStateService;
        try
        {
            _storedFingerprint = _modStateService.GetPenumbraFolderFingerprint();
            _storedFingerprintUtc = _modStateService.GetPenumbraFolderFingerprintUtc();
            _rootPath = _modStateService.GetPenumbraRootPath();
            _startupStoredFingerprint = _storedFingerprint;
            _startupStoredFingerprintUtc = _storedFingerprintUtc;
            _startupStoredRootPath = _rootPath;
        }
        catch { }
        _penumbraIpc.PenumbraEnabledChanged += OnPenumbraEnabledChanged;
        _penumbraIpc.ModsChanged += OnPenumbraModsChanged;
        _penumbraIpc.ModPathChanged += OnPenumbraModPathChanged;
        TryStartOrUpdate("init");
    }

    public PenumbraFolderWatcherStatus GetStatus()
    {
        lock (_lock)
        {
            return new PenumbraFolderWatcherStatus
            {
                WatcherActive = _watcherActive,
                RootPath = _rootPath,
                LastEventUtc = _lastEventUtc,
                LastEventKind = _lastEventKind,
                LastEventPath = _lastEventPath,
                EventBurstCount = _eventBurstCount,
                LastScanUtc = _lastScanUtc,
                LastScanDurationMs = _lastScanDurationMs,
                LastScanDirectoryCount = _lastScanDirectoryCount,
                LastScanMaxWriteUtc = _lastScanMaxWriteUtc,
                LastFingerprint = _lastFingerprint,
                StoredFingerprint = _storedFingerprint,
                StoredFingerprintUtc = _storedFingerprintUtc,
                StartupStoredFingerprint = _startupStoredFingerprint,
                StartupStoredFingerprintUtc = _startupStoredFingerprintUtc,
                StartupStoredRootPath = _startupStoredRootPath,
                StartupStoredFingerprintMatchesCurrent = string.Equals(_startupStoredFingerprint, _lastFingerprint, StringComparison.Ordinal),
                StartupStoredRootMatchesCurrent = string.Equals(_startupStoredRootPath, _rootPath, StringComparison.OrdinalIgnoreCase),
                LastError = _lastError,
                LastChangeReason = _lastChangeReason,
                StartupDiffDetected = _startupDiffDetected,
            };
        }
    }

    public bool IsStartupSnapshotUnchanged()
    {
        lock (_lock)
        {
            if (string.IsNullOrWhiteSpace(_startupStoredFingerprint))
                return false;
            if (string.IsNullOrWhiteSpace(_lastFingerprint))
                return false;
            if (!string.Equals(_startupStoredRootPath, _rootPath, StringComparison.OrdinalIgnoreCase))
                return false;
            return string.Equals(_startupStoredFingerprint, _lastFingerprint, StringComparison.Ordinal);
        }
    }

    public async Task<bool> WaitForInitialScanAsync(TimeSpan timeout, CancellationToken token)
    {
        var start = DateTime.UtcNow;
        while (!token.IsCancellationRequested && (DateTime.UtcNow - start) < timeout)
        {
            bool ready;
            lock (_lock)
            {
                ready = _lastScanUtc != DateTime.MinValue && !string.IsNullOrWhiteSpace(_lastFingerprint);
            }
            if (ready)
                return true;
            try { await Task.Delay(50, token).ConfigureAwait(false); }
            catch (TaskCanceledException) { return false; }
        }
        return false;
    }

    private void OnPenumbraEnabledChanged(bool enabled)
    {
        if (enabled) TryStartOrUpdate("penumbra-enabled");
        else TryStartOrUpdate("penumbra-disabled");
    }

    private void OnPenumbraModsChanged()
    {
        TryStartOrUpdate("mods-changed");
    }

    private void OnPenumbraModPathChanged(string modDir, string fullPath)
    {
        TryStartOrUpdate("mod-path-changed");
    }

    private void TryStartOrUpdate(string reason, bool forceResync = true)
    {
        var root = ResolvePenumbraRootPath();
        if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
        {
            StopWatcher("root-missing");
            return;
        }

        if (!string.Equals(_rootPath, root, StringComparison.OrdinalIgnoreCase) || _watcher == null || !_watcherActive)
        {
            StartWatcher(root);
        }

        ScheduleResync(reason, force: forceResync);
    }

    private string ResolvePenumbraRootPath()
    {
        try
        {
            var fromIpc = _penumbraIpc.ModDirectory ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(fromIpc) && Directory.Exists(fromIpc))
                return fromIpc;
        }
        catch { }
        try
        {
            var fromState = _modStateService.GetPenumbraRootPath();
            if (!string.IsNullOrWhiteSpace(fromState) && Directory.Exists(fromState))
                return fromState;
        }
        catch { }
        return _rootPath ?? string.Empty;
    }

    private void StartWatcher(string root)
    {
        StopWatcher("restart");
        _rootPath = root;
        try
        {
            _watcher = new FileSystemWatcher(root)
            {
                IncludeSubdirectories = true,
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true,
                InternalBufferSize = 64 * 1024,
            };
            _watcher.Changed += OnFsEvent;
            _watcher.Created += OnFsEvent;
            _watcher.Deleted += OnFsEvent;
            _watcher.Renamed += OnFsRenamed;
            _watcher.Error += OnFsError;
            _watcher.Disposed += OnWatcherDisposed;
            lock (_lock)
            {
                _watcherActive = true;
                _lastError = string.Empty;
            }
            _logger.LogDebug("Penumbra folder watcher started: {root}", root);
        }
        catch (Exception ex)
        {
            lock (_lock) { _lastError = ex.Message; _watcherActive = false; }
            _logger.LogDebug(ex, "Failed to start Penumbra folder watcher");
        }
    }

    private void StopWatcher(string reason)
    {
        var shouldLog = false;
        _stoppingWatcher = true;
        try
        {
            if (_watcher != null)
            {
                shouldLog = true;
                _watcher.EnableRaisingEvents = false;
                _watcher.Changed -= OnFsEvent;
                _watcher.Created -= OnFsEvent;
                _watcher.Deleted -= OnFsEvent;
                _watcher.Renamed -= OnFsRenamed;
                _watcher.Error -= OnFsError;
                _watcher.Disposed -= OnWatcherDisposed;
                _watcher.Dispose();
            }
        }
        catch { }
        _watcher = null;
        lock (_lock)
        {
            if (_watcherActive)
                shouldLog = true;
            _watcherActive = false;
            _lastChangeReason = reason;
        }
        _stoppingWatcher = false;
        if (shouldLog)
            _logger.LogDebug("Penumbra folder watcher stopped: {reason}", reason);
    }

    private void OnFsEvent(object sender, FileSystemEventArgs e)
    {
        RecordEvent(e.ChangeType.ToString(), e.FullPath);
        ScheduleDebouncedResync("fs-change");
    }

    private void OnFsRenamed(object sender, RenamedEventArgs e)
    {
        RecordEvent("Renamed", e.FullPath);
        ScheduleDebouncedResync("fs-rename");
    }

    private void OnFsError(object sender, ErrorEventArgs e)
    {
        var msg = e.GetException()?.Message ?? "unknown error";
        lock (_lock) { _lastError = msg; _lastChangeReason = "watcher-error"; }
        TryStartOrUpdate("watcher-error", forceResync: true);
    }

    private void OnWatcherDisposed(object? sender, EventArgs e)
    {
        if (_disposing || _stoppingWatcher)
            return;
        Task.Run(async () =>
        {
            try
            {
                await Task.Delay(200).ConfigureAwait(false);
                if (_disposing || _stoppingWatcher)
                    return;
                TryStartOrUpdate("watcher-disposed", forceResync: true);
            }
            catch { }
        });
    }

    private void RecordEvent(string kind, string path)
    {
        lock (_lock)
        {
            _lastEventUtc = DateTime.UtcNow;
            _lastEventKind = kind ?? string.Empty;
            _lastEventPath = path ?? string.Empty;
            _eventBurstCount++;
            _pendingChanges.Add((kind ?? string.Empty, path ?? string.Empty));
            if (_pendingChanges.Count > 256)
                _pendingChanges.RemoveRange(0, _pendingChanges.Count - 256);
        }
    }

    private void ScheduleDebouncedResync(string reason)
    {
        try
        {
            _debounceCts?.Cancel();
            _debounceCts?.Dispose();
            _debounceCts = new CancellationTokenSource();
            var token = _debounceCts.Token;
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(700, token).ConfigureAwait(false);
                    if (token.IsCancellationRequested)
                        return;
                    ScheduleResync(reason, force: false);
                }
                catch (TaskCanceledException) { }
                catch { }
            }, token);
        }
        catch { }
    }

    private void ScheduleResync(string reason, bool force)
    {
        try
        {
            _rescanCts?.Cancel();
            _rescanCts?.Dispose();
            _rescanCts = new CancellationTokenSource();
            var token = _rescanCts.Token;
            Task.Run(async () =>
            {
                try
                {
                    if (!force)
                        await Task.Delay(250, token).ConfigureAwait(false);
                    if (token.IsCancellationRequested)
                        return;
                    await RescanAsync(reason, force, token).ConfigureAwait(false);
                }
                catch (TaskCanceledException) { }
                catch (Exception ex)
                {
                    lock (_lock) { _lastError = ex.Message; }
                    _logger.LogDebug(ex, "Penumbra folder rescan failed");
                }
            }, token);
        }
        catch { }
    }

    private async Task RescanAsync(string reason, bool force, CancellationToken token)
    {
        var root = _rootPath;
        if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
            return;

        var now = DateTime.UtcNow;
        if (!force)
        {
            var last = _lastScanUtc;
            if ((now - last) < TimeSpan.FromSeconds(4))
                return;
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        int dirCount = 0;
        DateTime maxWriteUtc = DateTime.MinValue;
        string err = string.Empty;
        string fingerprint;
        bool usedIncremental = false;
        List<(string Kind, string Path)> changes = new();
        lock (_lock)
        {
            if (_pendingChanges.Count > 0)
            {
                changes = new List<(string Kind, string Path)>(_pendingChanges);
                _pendingChanges.Clear();
            }
        }
        if (!force && _dirIndexReady && changes.Count > 0)
        {
            fingerprint = await Task.Run(() =>
            {
                return TryIncrementalFingerprint(root, changes, token, out dirCount, out maxWriteUtc, out err, out usedIncremental);
            }, token).ConfigureAwait(false);
        }
        else
        {
            Dictionary<string, long> fullIndex = new(StringComparer.Ordinal);
            fingerprint = await Task.Run(() =>
            {
                return ComputeFullFingerprint(root, token, out dirCount, out maxWriteUtc, out err, out fullIndex);
            }, token).ConfigureAwait(false);
            lock (_lock)
            {
                _dirIndex = new Dictionary<string, long>(fullIndex, StringComparer.Ordinal);
                _dirIndexReady = _dirIndex.Count > 0;
            }
        }
        if (string.IsNullOrWhiteSpace(fingerprint))
        {
            Dictionary<string, long> fullIndex = new(StringComparer.Ordinal);
            fingerprint = await Task.Run(() =>
            {
                return ComputeFullFingerprint(root, token, out dirCount, out maxWriteUtc, out err, out fullIndex);
            }, token).ConfigureAwait(false);
            lock (_lock)
            {
                _dirIndex = new Dictionary<string, long>(fullIndex, StringComparer.Ordinal);
                _dirIndexReady = _dirIndex.Count > 0;
            }
        }
        sw.Stop();

        bool changed;
        bool startupDiff = false;
        lock (_lock)
        {
            _lastScanUtc = now;
            _lastScanDurationMs = (int)sw.ElapsedMilliseconds;
            _lastScanDirectoryCount = dirCount;
            _lastScanMaxWriteUtc = maxWriteUtc;
            _lastFingerprint = fingerprint;
            _eventBurstCount = 0;
            if (usedIncremental)
                _lastChangeReason = reason + "-incremental";
            if (!string.IsNullOrWhiteSpace(err))
                _lastError = err;
        }

        lock (_lock)
        {
            changed = !string.Equals(_storedFingerprint, fingerprint, StringComparison.Ordinal);
            _lastChangeReason = reason;
            if (changed && (reason == "init" || reason == "penumbra-enabled"))
            {
                _startupDiffDetected = true;
                startupDiff = true;
            }
        }

        if (changed || force)
        {
            try { OnFolderChanged?.Invoke(startupDiff ? "startup-diff" : reason); } catch { }
        }

        if (changed || force || string.IsNullOrWhiteSpace(_storedFingerprint))
        {
            try
            {
                _modStateService.SetPenumbraFolderFingerprint(fingerprint, root);
                lock (_lock)
                {
                    _storedFingerprint = fingerprint;
                    _storedFingerprintUtc = DateTime.UtcNow;
                }
            }
            catch { }
        }
    }

    private string TryIncrementalFingerprint(string root, List<(string Kind, string Path)> changes, CancellationToken token, out int dirCount, out DateTime maxWriteUtc, out string error, out bool usedIncremental)
    {
        dirCount = 0;
        maxWriteUtc = DateTime.MinValue;
        error = string.Empty;
        usedIncremental = false;
        try
        {
            var index = new Dictionary<string, long>(_dirIndex, StringComparer.Ordinal);
            var rootFull = Path.GetFullPath(root).TrimEnd('\\', '/');
            foreach (var change in changes)
            {
                token.ThrowIfCancellationRequested();
                var full = change.Path ?? string.Empty;
                if (string.IsNullOrWhiteSpace(full))
                    continue;
                string candidate = full;
                try
                {
                    if (!Directory.Exists(candidate))
                        candidate = Path.GetDirectoryName(candidate) ?? candidate;
                }
                catch { }
                if (string.IsNullOrWhiteSpace(candidate))
                    continue;
                string candFull;
                try { candFull = Path.GetFullPath(candidate).TrimEnd('\\', '/'); } catch { continue; }
                if (!candFull.StartsWith(rootFull, StringComparison.OrdinalIgnoreCase))
                    continue;
                var relPrefix = NormalizeRelative(root, candFull);
                RemovePrefix(index, relPrefix);
                if (Directory.Exists(candFull))
                {
                    var deep = string.Equals(change.Kind, "Created", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(change.Kind, "Renamed", StringComparison.OrdinalIgnoreCase);
                    UpsertDirectory(index, root, candFull, deep, token, ref maxWriteUtc);
                }
                var parent = candFull;
                while (!string.IsNullOrWhiteSpace(parent) && parent.StartsWith(rootFull, StringComparison.OrdinalIgnoreCase))
                {
                    if (!Directory.Exists(parent))
                        break;
                    var rel = NormalizeRelative(root, parent);
                    DateTime writeUtc;
                    try { writeUtc = Directory.GetLastWriteTimeUtc(parent); } catch { writeUtc = DateTime.MinValue; }
                    index[rel] = writeUtc.Ticks;
                    if (writeUtc > maxWriteUtc)
                        maxWriteUtc = writeUtc;
                    if (string.Equals(parent, rootFull, StringComparison.OrdinalIgnoreCase))
                        break;
                    try { parent = Path.GetDirectoryName(parent) ?? string.Empty; } catch { break; }
                }
            }
            if (maxWriteUtc == DateTime.MinValue)
            {
                foreach (var t in index.Values)
                {
                    if (t <= 0) continue;
                    var dt = new DateTime(t, DateTimeKind.Utc);
                    if (dt > maxWriteUtc) maxWriteUtc = dt;
                }
            }
            dirCount = index.Count;
            var fingerprint = ComputeFingerprintFromIndex(index);
            lock (_lock)
            {
                _dirIndex = index;
                _dirIndexReady = true;
            }
            usedIncremental = true;
            return fingerprint;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return string.Empty;
        }
    }

    private static string ComputeFullFingerprint(string root, CancellationToken token, out int dirCount, out DateTime maxWriteUtc, out string error, out Dictionary<string, long> fullIndex)
    {
        dirCount = 0;
        maxWriteUtc = DateTime.MinValue;
        error = string.Empty;
        fullIndex = new Dictionary<string, long>(StringComparer.Ordinal);
        try
        {
            var index = new Dictionary<string, long>(StringComparer.Ordinal);
            var stack = new Stack<string>();
            stack.Push(root);
            while (stack.Count > 0)
            {
                token.ThrowIfCancellationRequested();
                var dir = stack.Pop();
                dirCount++;
                DateTime writeUtc;
                try { writeUtc = Directory.GetLastWriteTimeUtc(dir); }
                catch { writeUtc = DateTime.MinValue; }
                if (writeUtc > maxWriteUtc)
                    maxWriteUtc = writeUtc;

                var rel = Path.GetRelativePath(root, dir).Replace('\\', '/');
                if (string.IsNullOrWhiteSpace(rel))
                    rel = ".";
                index[rel] = writeUtc.Ticks;

                try
                {
                    foreach (var sub in Directory.EnumerateDirectories(dir))
                    {
                        if (!string.IsNullOrWhiteSpace(sub))
                            stack.Push(sub);
                    }
                }
                catch { }
            }
            fullIndex = index;
            return ComputeFingerprintFromIndex(index);
        }
        catch (Exception ex)
        {
            error = ex.Message;
            fullIndex = new Dictionary<string, long>(StringComparer.Ordinal);
            return string.Empty;
        }
    }

    private static void RemovePrefix(Dictionary<string, long> index, string relPrefix)
    {
        if (string.IsNullOrWhiteSpace(relPrefix))
            return;
        var keys = new List<string>();
        foreach (var k in index.Keys)
        {
            if (string.Equals(k, relPrefix, StringComparison.Ordinal) || k.StartsWith(relPrefix + "/", StringComparison.Ordinal))
                keys.Add(k);
        }
        for (int i = 0; i < keys.Count; i++)
            index.Remove(keys[i]);
    }

    private static void UpsertDirectory(Dictionary<string, long> index, string root, string startDir, bool deep, CancellationToken token, ref DateTime maxWriteUtc)
    {
        var stack = new Stack<string>();
        stack.Push(startDir);
        while (stack.Count > 0)
        {
            token.ThrowIfCancellationRequested();
            var dir = stack.Pop();
            if (!Directory.Exists(dir))
                continue;
            DateTime writeUtc;
            try { writeUtc = Directory.GetLastWriteTimeUtc(dir); } catch { writeUtc = DateTime.MinValue; }
            if (writeUtc > maxWriteUtc) maxWriteUtc = writeUtc;
            var rel = NormalizeRelative(root, dir);
            index[rel] = writeUtc.Ticks;
            if (!deep)
                continue;
            try
            {
                foreach (var sub in Directory.EnumerateDirectories(dir))
                    stack.Push(sub);
            }
            catch { }
        }
    }

    private static string NormalizeRelative(string root, string dir)
    {
        var rel = Path.GetRelativePath(root, dir).Replace('\\', '/');
        if (string.IsNullOrWhiteSpace(rel))
            rel = ".";
        return rel;
    }

    private static string ComputeFingerprintFromIndex(Dictionary<string, long> index)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var keys = new List<string>(index.Keys);
        keys.Sort(StringComparer.Ordinal);
        for (int i = 0; i < keys.Count; i++)
        {
            var k = keys[i];
            var payload = k + "|" + index[k];
            var bytes = Encoding.UTF8.GetBytes(payload);
            hasher.AppendData(bytes);
        }
        return Convert.ToHexString(hasher.GetHashAndReset());
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
            return;
        _disposing = true;
        try { _penumbraIpc.PenumbraEnabledChanged -= OnPenumbraEnabledChanged; } catch { }
        try { _penumbraIpc.ModsChanged -= OnPenumbraModsChanged; } catch { }
        try { _penumbraIpc.ModPathChanged -= OnPenumbraModPathChanged; } catch { }
        try { _debounceCts?.Cancel(); } catch { }
        try { _debounceCts?.Dispose(); } catch { }
        _debounceCts = null;
        try { _rescanCts?.Cancel(); } catch { }
        try { _rescanCts?.Dispose(); } catch { }
        _rescanCts = null;
        StopWatcher("disposed");
    }
}

public sealed class PenumbraFolderWatcherStatus
{
    public bool WatcherActive { get; set; }
    public string RootPath { get; set; } = string.Empty;
    public DateTime LastEventUtc { get; set; }
    public string LastEventKind { get; set; } = string.Empty;
    public string LastEventPath { get; set; } = string.Empty;
    public int EventBurstCount { get; set; }
    public DateTime LastScanUtc { get; set; }
    public int LastScanDurationMs { get; set; }
    public int LastScanDirectoryCount { get; set; }
    public DateTime LastScanMaxWriteUtc { get; set; }
    public string LastFingerprint { get; set; } = string.Empty;
    public string StoredFingerprint { get; set; } = string.Empty;
    public DateTime StoredFingerprintUtc { get; set; }
    public string StartupStoredFingerprint { get; set; } = string.Empty;
    public DateTime StartupStoredFingerprintUtc { get; set; }
    public string StartupStoredRootPath { get; set; } = string.Empty;
    public bool StartupStoredFingerprintMatchesCurrent { get; set; }
    public bool StartupStoredRootMatchesCurrent { get; set; }
    public string LastError { get; set; } = string.Empty;
    public string LastChangeReason { get; set; } = string.Empty;
    public bool StartupDiffDetected { get; set; }
}
