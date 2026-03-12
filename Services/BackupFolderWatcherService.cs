using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ShrinkU.Services;

public sealed class BackupFolderWatcherService : IDisposable
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly TextureBackupService _backupService;
    private readonly object _lock = new();
    private FileSystemWatcher? _watcher;
    private CancellationTokenSource? _debounceCts;
    private string _rootPath = string.Empty;
    private readonly HashSet<string> _pendingMods = new(StringComparer.OrdinalIgnoreCase);
    private bool _watcherActive;
    private DateTime _lastEventUtc = DateTime.MinValue;
    private string _lastEventKind = string.Empty;
    private string _lastEventPath = string.Empty;
    private int _eventBurstCount;
    private DateTime _lastRefreshUtc = DateTime.MinValue;
    private int _lastRefreshDurationMs;
    private int _lastScanDirectoryCount;
    private DateTime _lastScanMaxWriteUtc = DateTime.MinValue;
    private string _storedFingerprint = string.Empty;
    private DateTime _storedFingerprintUtc = DateTime.MinValue;
    private bool _storedFingerprintMatchesCurrent;
    private string _lastError = string.Empty;

    public BackupFolderWatcherService(ILogger logger, ShrinkUConfigService configService, TextureBackupService backupService)
    {
        _logger = logger;
        _configService = configService;
        _backupService = backupService;
        try { _backupService.BackupFolderPathChanged += OnBackupFolderPathChanged; } catch { }
        TryStartOrUpdate();
    }

    public BackupFolderWatcherStatus GetStatus()
    {
        lock (_lock)
        {
            return new BackupFolderWatcherStatus
            {
                WatcherActive = _watcherActive,
                RootPath = _rootPath,
                LastEventUtc = _lastEventUtc,
                LastEventKind = _lastEventKind,
                LastEventPath = _lastEventPath,
                EventBurstCount = _eventBurstCount,
                LastRefreshUtc = _lastRefreshUtc,
                LastRefreshDurationMs = _lastRefreshDurationMs,
                LastScanDirectoryCount = _lastScanDirectoryCount,
                LastScanMaxWriteUtc = _lastScanMaxWriteUtc,
                StoredFingerprintUtc = _storedFingerprintUtc,
                StoredFingerprintMatchesCurrent = _storedFingerprintMatchesCurrent,
                LastError = _lastError,
            };
        }
    }

    private void OnBackupFolderPathChanged(string path)
    {
        TryStartOrUpdate();
    }

    private void TryStartOrUpdate()
    {
        var root = _configService.Current.BackupFolderPath ?? string.Empty;
        if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
        {
            StopWatcher();
            return;
        }
        if (string.Equals(_rootPath, root, StringComparison.OrdinalIgnoreCase) && _watcher != null)
            return;
        StartWatcher(root);
    }

    private void StartWatcher(string root)
    {
        StopWatcher();
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
            lock (_lock) { _watcherActive = true; _lastError = string.Empty; }
            _logger.LogDebug("Backup folder watcher started: {root}", root);
        }
        catch (Exception ex)
        {
            lock (_lock) { _watcherActive = false; _lastError = ex.Message; }
            _logger.LogDebug(ex, "Failed to start backup folder watcher");
        }
    }

    private void StopWatcher()
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
        lock (_lock) { _watcherActive = false; }
    }

    private void OnFsEvent(object sender, FileSystemEventArgs e)
    {
        if (IsInternalStatePath(e.FullPath))
            return;
        RecordEvent(e.ChangeType.ToString(), e.FullPath);
        ScheduleDebouncedRefresh();
    }

    private void OnFsRenamed(object sender, RenamedEventArgs e)
    {
        if (IsInternalStatePath(e.FullPath))
            return;
        RecordEvent("Renamed", e.FullPath);
        ScheduleDebouncedRefresh();
    }

    private void OnFsError(object sender, ErrorEventArgs e)
    {
        var msg = e.GetException()?.Message ?? "unknown error";
        lock (_lock) { _lastError = msg; }
        ScheduleDebouncedRefresh();
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
        TryTrackModFromPath(path ?? string.Empty);
    }

    private void TryTrackModFromPath(string path)
    {
        try
        {
            var root = _rootPath;
            if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(path))
                return;
            var rel = Path.GetRelativePath(root, path);
            if (string.IsNullOrWhiteSpace(rel) || rel.StartsWith("..", StringComparison.Ordinal))
                return;
            var parts = rel.Split(new[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0)
                return;
            var mod = parts[0];
            if (string.IsNullOrWhiteSpace(mod))
                return;
            if (mod.Equals("mod_state", StringComparison.OrdinalIgnoreCase))
                return;
            if (mod.Equals("mod_state.json", StringComparison.OrdinalIgnoreCase))
                return;
            if (mod.StartsWith("mod_state.json.tmp.", StringComparison.OrdinalIgnoreCase))
                return;
            if (mod.StartsWith("mod_state.json`", StringComparison.OrdinalIgnoreCase) && mod.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
                return;
            if (mod.StartsWith("session_", StringComparison.OrdinalIgnoreCase))
            {
                if (parts.Length < 2)
                    return;
                mod = parts[1];
            }
            if (PenumbraIpc.ShouldSkipModRootFolder(mod))
                return;
            lock (_lock)
            {
                _pendingMods.Add(mod);
            }
        }
        catch { }
    }

    private void ScheduleDebouncedRefresh()
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
                    await Task.Delay(800, token).ConfigureAwait(false);
                    if (token.IsCancellationRequested)
                        return;
                    bool unchanged = _backupService.IsBackupFolderFingerprintUnchanged();
                    int dirCount = 0;
                    DateTime maxWriteUtc = DateTime.MinValue;
                    string fingerprint = string.Empty;
                    try
                    {
                        var root = _configService.Current.BackupFolderPath ?? string.Empty;
                        if (!string.IsNullOrWhiteSpace(root) && Directory.Exists(root))
                            fingerprint = ComputeBackupFolderFingerprint(root, out dirCount, out maxWriteUtc);
                    }
                    catch { }
                    lock (_lock)
                    {
                        _lastScanDirectoryCount = dirCount;
                        _lastScanMaxWriteUtc = maxWriteUtc;
                        _storedFingerprint = fingerprint;
                        _storedFingerprintUtc = DateTime.UtcNow;
                        _storedFingerprintMatchesCurrent = unchanged;
                    }
                    if (unchanged)
                        return;
                    HashSet<string> pending;
                    lock (_lock)
                    {
                        pending = new HashSet<string>(_pendingMods, StringComparer.OrdinalIgnoreCase);
                        _pendingMods.Clear();
                    }
                    if (pending.Count == 0 && IsModStatePath(_lastEventPath))
                        return;
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    if (pending.Count == 1)
                    {
                        var mod = pending.First();
                        await _backupService.RefreshBackupStateForModAsync(mod).ConfigureAwait(false);
                    }
                    else
                    {
                        await _backupService.RefreshAllBackupStateAsync().ConfigureAwait(false);
                    }
                    sw.Stop();
                    lock (_lock)
                    {
                        _lastRefreshUtc = DateTime.UtcNow;
                        _lastRefreshDurationMs = (int)sw.ElapsedMilliseconds;
                        _eventBurstCount = 0;
                    }
                }
                catch (TaskCanceledException) { }
                catch (Exception ex)
                {
                    lock (_lock) { _lastError = ex.Message; }
                    _logger.LogDebug(ex, "Backup folder watcher refresh failed");
                }
            }, token);
        }
        catch { }
    }

    private static bool IsInternalStatePath(string path)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(path))
                return false;
            var name = Path.GetFileName(path) ?? string.Empty;
            if (string.Equals(name, "mod_state.json", StringComparison.OrdinalIgnoreCase))
                return true;
            if (name.StartsWith("mod_state.json.tmp.", StringComparison.OrdinalIgnoreCase))
                return true;
            if (name.StartsWith("mod_state.json`", StringComparison.OrdinalIgnoreCase) && name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
                return true;
            if (name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
            {
                if (path.IndexOf($"{Path.DirectorySeparatorChar}mod_state{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase) >= 0
                    || path.EndsWith($"{Path.DirectorySeparatorChar}mod_state", StringComparison.OrdinalIgnoreCase)
                    || path.IndexOf("/mod_state/", StringComparison.OrdinalIgnoreCase) >= 0
                    || path.EndsWith("/mod_state", StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return path.IndexOf($"{Path.DirectorySeparatorChar}mod_state{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase) >= 0
                || path.EndsWith($"{Path.DirectorySeparatorChar}mod_state", StringComparison.OrdinalIgnoreCase)
                || path.IndexOf("/mod_state/", StringComparison.OrdinalIgnoreCase) >= 0;
        }
        catch { return false; }
    }

    private static bool IsModStatePath(string path)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(path))
                return false;
            var name = Path.GetFileName(path);
            if (string.Equals(name, "mod_state.json", StringComparison.OrdinalIgnoreCase))
                return true;
            if (!string.IsNullOrWhiteSpace(name) && name.StartsWith("mod_state.json.tmp.", StringComparison.OrdinalIgnoreCase))
                return true;
            if (!string.IsNullOrWhiteSpace(name) && name.StartsWith("mod_state.json`", StringComparison.OrdinalIgnoreCase) && name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
                return true;
            if (!string.IsNullOrWhiteSpace(name) && name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
            {
                if (path.IndexOf($"{Path.DirectorySeparatorChar}mod_state{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase) >= 0
                    || path.EndsWith($"{Path.DirectorySeparatorChar}mod_state", StringComparison.OrdinalIgnoreCase)
                    || path.IndexOf("/mod_state/", StringComparison.OrdinalIgnoreCase) >= 0
                    || path.EndsWith("/mod_state", StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return path.IndexOf($"{Path.DirectorySeparatorChar}mod_state{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase) >= 0
                || path.EndsWith($"{Path.DirectorySeparatorChar}mod_state", StringComparison.OrdinalIgnoreCase)
                || path.IndexOf("/mod_state/", StringComparison.OrdinalIgnoreCase) >= 0;
        }
        catch { return false; }
    }

    private static string ComputeBackupFolderFingerprint(string backupDirectory, out int dirCount, out DateTime maxWriteUtc)
    {
        dirCount = 0;
        maxWriteUtc = DateTime.MinValue;
        try
        {
            var entries = new System.Collections.Generic.List<string>(4096);

            foreach (var dir in Directory.EnumerateDirectories(backupDirectory, "*", SearchOption.TopDirectoryOnly))
            {
                dirCount++;
                DateTime writeUtc;
                try { writeUtc = Directory.GetLastWriteTimeUtc(dir); } catch { writeUtc = DateTime.MinValue; }
                if (writeUtc > maxWriteUtc)
                    maxWriteUtc = writeUtc;

                var name = Path.GetFileName(dir) ?? string.Empty;
                if (string.IsNullOrWhiteSpace(name))
                    continue;
                if (name.Equals("mod_state", StringComparison.OrdinalIgnoreCase))
                    continue;
                if (name.StartsWith("session_", StringComparison.OrdinalIgnoreCase))
                    continue;

                try
                {
                    var latestZip = Directory.EnumerateFiles(dir, "backup_*.zip").OrderByDescending(f => f, StringComparer.Ordinal).FirstOrDefault();
                    if (!string.IsNullOrEmpty(latestZip) && File.Exists(latestZip))
                    {
                        var fi = new FileInfo(latestZip);
                        entries.Add($"zip|{name}|{fi.Name}|{fi.CreationTimeUtc.Ticks}|{fi.Length}");
                    }
                }
                catch { }

                try
                {
                    var latestPmp = Directory.EnumerateFiles(dir, "*.pmp").OrderByDescending(f => f, StringComparer.Ordinal).FirstOrDefault();
                    if (!string.IsNullOrEmpty(latestPmp) && File.Exists(latestPmp))
                    {
                        var fi = new FileInfo(latestPmp);
                        entries.Add($"pmp|{name}|{fi.Name}|{fi.CreationTimeUtc.Ticks}|{fi.Length}");
                    }
                }
                catch { }
            }

            foreach (var session in Directory.EnumerateDirectories(backupDirectory, "session_*", SearchOption.TopDirectoryOnly))
            {
                dirCount++;
                DateTime writeUtc;
                try { writeUtc = Directory.GetLastWriteTimeUtc(session); } catch { writeUtc = DateTime.MinValue; }
                if (writeUtc > maxWriteUtc)
                    maxWriteUtc = writeUtc;

                var sessionName = Path.GetFileName(session) ?? string.Empty;
                if (string.IsNullOrWhiteSpace(sessionName))
                    continue;
                try
                {
                    var count = Directory.EnumerateFiles(session, "*", SearchOption.AllDirectories).Count();
                    entries.Add($"session|{sessionName}|{count}");
                }
                catch { }
            }

            entries.Sort(StringComparer.Ordinal);
            var joined = string.Join('\n', entries);
            var bytes = System.Text.Encoding.UTF8.GetBytes(joined);
            using var sha1 = System.Security.Cryptography.SHA1.Create();
            var hash = sha1.ComputeHash(bytes);
            return Convert.ToHexString(hash);
        }
        catch
        {
            return string.Empty;
        }
    }

    public void Dispose()
    {
        try { _backupService.BackupFolderPathChanged -= OnBackupFolderPathChanged; } catch { }
        try { _debounceCts?.Cancel(); } catch { }
        try { _debounceCts?.Dispose(); } catch { }
        _debounceCts = null;
        StopWatcher();
    }
}

public sealed class BackupFolderWatcherStatus
{
    public bool WatcherActive { get; set; }
    public string RootPath { get; set; } = string.Empty;
    public DateTime LastEventUtc { get; set; }
    public string LastEventKind { get; set; } = string.Empty;
    public string LastEventPath { get; set; } = string.Empty;
    public int EventBurstCount { get; set; }
    public DateTime LastRefreshUtc { get; set; }
    public int LastRefreshDurationMs { get; set; }
    public int LastScanDirectoryCount { get; set; }
    public DateTime LastScanMaxWriteUtc { get; set; }
    public DateTime StoredFingerprintUtc { get; set; }
    public bool StoredFingerprintMatchesCurrent { get; set; }
    public string LastError { get; set; } = string.Empty;
}
