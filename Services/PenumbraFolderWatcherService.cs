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

    private void OnPenumbraEnabledChanged(bool enabled)
    {
        if (enabled)
        {
            TryStartOrUpdate("penumbra-enabled");
        }
        else
        {
            StopWatcher("penumbra-disabled");
        }
    }

    private void TryStartOrUpdate(string reason)
    {
        if (!_penumbraIpc.APIAvailable)
        {
            StopWatcher("api-unavailable");
            return;
        }

        var root = _penumbraIpc.ModDirectory ?? string.Empty;
        if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
        {
            StopWatcher("root-missing");
            return;
        }

        if (!string.Equals(_rootPath, root, StringComparison.OrdinalIgnoreCase))
        {
            StartWatcher(root);
        }

        ScheduleResync(reason, force: true);
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
        try
        {
            if (_watcher != null)
            {
                _watcher.EnableRaisingEvents = false;
                _watcher.Changed -= OnFsEvent;
                _watcher.Created -= OnFsEvent;
                _watcher.Deleted -= OnFsEvent;
                _watcher.Renamed -= OnFsRenamed;
                _watcher.Error -= OnFsError;
                _watcher.Dispose();
            }
        }
        catch { }
        _watcher = null;
        lock (_lock)
        {
            _watcherActive = false;
            _lastChangeReason = reason;
        }
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
        ScheduleResync("watcher-error", force: true);
    }

    private void RecordEvent(string kind, string path)
    {
        lock (_lock)
        {
            _lastEventUtc = DateTime.UtcNow;
            _lastEventKind = kind ?? string.Empty;
            _lastEventPath = path ?? string.Empty;
            _eventBurstCount++;
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
        var fingerprint = await Task.Run(() =>
        {
            return ComputeFolderFingerprint(root, token, out dirCount, out maxWriteUtc, out err);
        }, token).ConfigureAwait(false);
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

    private static string ComputeFolderFingerprint(string root, CancellationToken token, out int dirCount, out DateTime maxWriteUtc, out string error)
    {
        dirCount = 0;
        maxWriteUtc = DateTime.MinValue;
        error = string.Empty;
        try
        {
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
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
                var payload = rel + "|" + writeUtc.Ticks;
                var bytes = Encoding.UTF8.GetBytes(payload);
                hasher.AppendData(bytes);

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
            var hash = hasher.GetHashAndReset();
            return Convert.ToHexString(hash);
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return string.Empty;
        }
    }

    public void Dispose()
    {
        try { _penumbraIpc.PenumbraEnabledChanged -= OnPenumbraEnabledChanged; } catch { }
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
