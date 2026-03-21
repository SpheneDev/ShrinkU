using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Helpers;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace ShrinkU.Services;

public sealed class ModStateService
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _config;
    private readonly object _lock = new();
    private Dictionary<string, ModStateEntry> _state = new(StringComparer.OrdinalIgnoreCase);
    private ModStateMeta _meta = new();
    public event Action? OnStateSaved;
    public event Action<string>? OnEntryChanged;
    private volatile bool _savingEnabled = true;
    private readonly DebugTraceService? _debug;
    private volatile string _lastSaveReason = string.Empty;
    private volatile string _lastChangedMod = string.Empty;
    private volatile bool _batching = false;
    private volatile bool _batchDirty = false;
    private DateTime _lastLoadedWriteUtc = DateTime.MinValue;
    private long _lastLoadedLength = 0;
    private static readonly byte[] s_newLineUtf8 = new[] { (byte)'\n' };
    private static readonly JsonSerializerOptions s_stateFingerprintJsonOptions = new JsonSerializerOptions { WriteIndented = false };
    private static readonly PropertyInfo[] s_stateFingerprintProperties = CreateStateFingerprintProperties();
    private readonly bool _useSqliteStorage;
    private readonly string _sqlitePath;
    private readonly object _sqliteLock = new();

    public ModStateService(ILogger logger, ShrinkUConfigService config, DebugTraceService? debugTrace = null)
    {
        _logger = logger;
        _config = config;
        _debug = debugTrace;
        _useSqliteStorage = _config.Current.ModStateStorageMode == ModStateStorageMode.SqliteDatabase;
        _sqlitePath = Path.Combine(_config.GetPluginDataDirectory(), "ShrinkU.mod_state.db");
        if (_useSqliteStorage)
        {
            try { EnsureSqliteSchema(); } catch { }
        }
        try { Load(); } catch (Exception ex) { try { _logger.LogError(ex, "Failed to initialize mod state"); } catch { } }
    }

    private string GetPath()
    {
        var root = _config.Current.BackupFolderPath;
        try { Directory.CreateDirectory(root); } catch { }
        return Path.Combine(root, "mod_state.json");
    }

    public string GetStateFilePath()
    {
        return _useSqliteStorage ? _sqlitePath : GetPath();
    }

    public string GetSqliteFilePath()
    {
        return _sqlitePath;
    }

    public string GetPathForLegacyJsonState()
    {
        return GetPath();
    }

    public Task<ModStateMigrationResult> MigrateJsonStateToSqliteAsync(IProgress<ModStateMigrationProgress>? progress = null, CancellationToken cancellationToken = default)
    {
        return Task.Run(() =>
        {
            var startedUtc = DateTime.UtcNow;
            var sw = Stopwatch.StartNew();
            var sourcePath = GetPath();
            var detailDir = GetDetailDir();
            var result = new ModStateMigrationResult
            {
                SourceJsonPath = sourcePath,
                TargetSqlitePath = _sqlitePath,
                StartedUtc = startedUtc,
            };

            if (!File.Exists(sourcePath))
            {
                result.Success = false;
                result.Message = "Source JSON state file not found.";
                result.FinishedUtc = DateTime.UtcNow;
                result.Duration = sw.Elapsed;
                return result;
            }

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                progress?.Report(new ModStateMigrationProgress
                {
                    Stage = "Loading JSON state",
                    TotalSteps = 1,
                    CompletedSteps = 0,
                });

                var (entries, meta) = LoadJsonStateForMigration(sourcePath);
                result.MigratedEntries = entries.Count;

                progress?.Report(new ModStateMigrationProgress
                {
                    Stage = "Writing state snapshot to SQLite",
                    TotalSteps = 1 + entries.Count,
                    CompletedSteps = 1,
                    MigratedEntries = entries.Count,
                });

                SaveSnapshotToSqlite(entries, meta);

                var done = 1;
                var total = 1 + entries.Count;
                foreach (var mod in entries.Keys.OrderBy(static k => k, StringComparer.OrdinalIgnoreCase))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    done++;

                    var path = Path.Combine(detailDir, Sanitize(mod) + ".json");
                    if (!File.Exists(path))
                    {
                        result.MissingDetailFiles++;
                        progress?.Report(new ModStateMigrationProgress
                        {
                            Stage = $"Scanning details ({mod})",
                            TotalSteps = total,
                            CompletedSteps = done,
                            MigratedEntries = result.MigratedEntries,
                            MigratedDetailFiles = result.MigratedDetailFiles,
                            MissingDetailFiles = result.MissingDetailFiles,
                        });
                        continue;
                    }

                    try
                    {
                        var json = File.ReadAllText(path);
                        if (!string.IsNullOrWhiteSpace(json))
                        {
                            WriteDetailToSqlite(mod, json);
                            result.MigratedDetailFiles++;
                        }
                    }
                    catch
                    {
                        result.DetailReadErrors++;
                    }

                    progress?.Report(new ModStateMigrationProgress
                    {
                        Stage = $"Migrating details ({mod})",
                        TotalSteps = total,
                        CompletedSteps = done,
                        MigratedEntries = result.MigratedEntries,
                        MigratedDetailFiles = result.MigratedDetailFiles,
                        MissingDetailFiles = result.MissingDetailFiles,
                        DetailReadErrors = result.DetailReadErrors,
                    });
                }

                result.Success = true;
                result.Message = "Migration completed.";
            }
            catch (OperationCanceledException)
            {
                result.Success = false;
                result.Message = "Migration canceled.";
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Message = $"Migration failed: {ex.Message}";
                try { _logger.LogError(ex, "JSON to SQLite migration failed"); } catch { }
            }
            finally
            {
                sw.Stop();
                result.FinishedUtc = DateTime.UtcNow;
                result.Duration = sw.Elapsed;
                progress?.Report(new ModStateMigrationProgress
                {
                    Stage = result.Success ? "Completed" : "Failed",
                    TotalSteps = 1,
                    CompletedSteps = 1,
                    MigratedEntries = result.MigratedEntries,
                    MigratedDetailFiles = result.MigratedDetailFiles,
                    MissingDetailFiles = result.MissingDetailFiles,
                    DetailReadErrors = result.DetailReadErrors,
                    IsCompleted = true,
                    IsSuccess = result.Success,
                });
            }

            return result;
        }, cancellationToken);
    }

    public bool ReloadIfChanged()
    {
        if (_useSqliteStorage)
        {
            return false;
        }

        try
        {
            var path = GetPath();
            if (!File.Exists(path)) return false;
            var fi = new FileInfo(path);
            if (fi.LastWriteTimeUtc == _lastLoadedWriteUtc && fi.Length == _lastLoadedLength)
                return false;
            Load();
            try { OnStateSaved?.Invoke(); } catch { }
            return true;
        }
        catch { return false; }
    }

    public IReadOnlyDictionary<string, ModStateEntry> Snapshot()
    {
        lock (_lock) { return new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase); }
    }

    public ModStateEntry Get(string mod)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return new ModStateEntry { ModFolderName = string.Empty, LastUpdatedUtc = DateTime.UtcNow };
        if (IsTempModStateArtifactName(mod))
            return new ModStateEntry { ModFolderName = mod, LastUpdatedUtc = DateTime.UtcNow };
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

    public string GetBackupFolderFingerprint()
    {
        lock (_lock)
        {
            return _meta.BackupFolderFingerprint ?? string.Empty;
        }
    }

    public DateTime GetBackupFolderFingerprintUtc()
    {
        lock (_lock)
        {
            return _meta.BackupFolderFingerprintUtc;
        }
    }

    public string GetPenumbraFolderFingerprint()
    {
        lock (_lock)
        {
            return _meta.PenumbraFolderFingerprint ?? string.Empty;
        }
    }

    public DateTime GetPenumbraFolderFingerprintUtc()
    {
        lock (_lock)
        {
            return _meta.PenumbraFolderFingerprintUtc;
        }
    }

    public string GetPenumbraRootPath()
    {
        lock (_lock)
        {
            return _meta.PenumbraRootPath ?? string.Empty;
        }
    }

    public void SetBackupFolderFingerprint(string fingerprint)
    {
        if (fingerprint == null) fingerprint = string.Empty;
        lock (_lock)
        {
            if (string.Equals(_meta.BackupFolderFingerprint, fingerprint, StringComparison.OrdinalIgnoreCase))
                return;

            _meta.BackupFolderFingerprint = fingerprint;
            _meta.BackupFolderFingerprintUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(SetBackupFolderFingerprint);
            ScheduleSave();
        }
    }

    public void SetPenumbraFolderFingerprint(string fingerprint, string rootPath)
    {
        if (fingerprint == null) fingerprint = string.Empty;
        if (rootPath == null) rootPath = string.Empty;
        lock (_lock)
        {
            bool sameFingerprint = string.Equals(_meta.PenumbraFolderFingerprint, fingerprint, StringComparison.OrdinalIgnoreCase);
            bool sameRoot = string.Equals(_meta.PenumbraRootPath, rootPath, StringComparison.OrdinalIgnoreCase);
            if (sameFingerprint && sameRoot)
                return;

            _meta.PenumbraFolderFingerprint = fingerprint;
            _meta.PenumbraRootPath = rootPath;
            _meta.PenumbraFolderFingerprintUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(SetPenumbraFolderFingerprint);
            ScheduleSave();
        }
    }

    public string GetStateFingerprint()
    {
        lock (_lock)
        {
            return _meta.StateFingerprint ?? string.Empty;
        }
    }

    public DateTime GetStateFingerprintUtc()
    {
        lock (_lock)
        {
            return _meta.StateFingerprintUtc;
        }
    }

    public void SetStateFingerprint(string fingerprint)
    {
        if (fingerprint == null) fingerprint = string.Empty;
        lock (_lock)
        {
            if (string.Equals(_meta.StateFingerprint, fingerprint, StringComparison.OrdinalIgnoreCase))
                return;

            _meta.StateFingerprint = fingerprint;
            _meta.StateFingerprintUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(SetStateFingerprint);
            ScheduleSave();
        }
    }

    public string ComputeStateFingerprintSnapshot()
    {
        try
        {
            Dictionary<string, ModStateEntry> entriesSnapshot;
            lock (_lock)
            {
                entriesSnapshot = new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase);
            }
            return ComputeStateFingerprint(entriesSnapshot);
        }
        catch { return string.Empty; }
    }

    public void UpdateBackupFlags(string mod, bool hasTexBackup, bool hasPmpBackup)
    {
        lock (_lock)
        {
            var e = Get(mod);
            bool changed = e.HasTextureBackup != hasTexBackup || e.HasPmpBackup != hasPmpBackup;
            if (!changed) return;
            e.HasTextureBackup = hasTexBackup;
            e.HasPmpBackup = hasPmpBackup;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateBackupFlags);
            _lastChangedMod = mod;
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateSavings(string mod, long originalBytes, long currentBytes, int comparedFiles)
    {
        lock (_lock)
        {
            var e = Get(mod);
            if (e.OriginalBytes <= 0)
            {
                e.OriginalBytes = originalBytes;
            }
            else if (originalBytes > 0 && Math.Abs(e.OriginalBytes - originalBytes) > 1024 * 1024) // >1MB diff
            {
                // Trace if we are ignoring a significantly different original size
                try { _logger.LogDebug("[TRACE-ORIGINAL] UpdateSavings ignored new original size {new} vs existing {old} for {mod}", originalBytes, e.OriginalBytes, mod); } catch { }
            }

            if (currentBytes == 0 && e.CurrentBytes > 0)
            {
                try { _logger.LogDebug("[TRACE-ZERO-BUG] UpdateSavings reset CurrentBytes to 0 for {mod}. Prev={prev}. Trace: {trace}", mod, e.CurrentBytes, Environment.StackTrace); } catch { }
            }

            if (!e.InstalledButNotConverted)
            {
                e.CurrentBytes = currentBytes;
                e.ComparedFiles = comparedFiles;
            }
            else
            {
                e.CurrentBytes = 0;
                e.ComparedFiles = 0;
            }
            e.BytesSaved = Math.Max(0, e.OriginalBytes - e.CurrentBytes);
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateSavings);
            _lastChangedMod = mod;
            try { _logger.LogDebug("[ShrinkU] ModState UpdateSavings: mod={mod} original={orig} current={cur} comparedFiles={cmp}", mod, originalBytes, currentBytes, comparedFiles); } catch { }
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateOriginalBytesFromRestore(string mod, long originalBytes, int comparedFiles)
    {
        lock (_lock)
        {
            var e = Get(mod);
            var o = Math.Max(0, originalBytes);
            bool changed = e.OriginalBytes != o || e.CurrentBytes != 0 || e.ComparedFiles != 0;
            if (!changed) return;
            e.OriginalBytes = o;
            e.CurrentBytes = 0;
            e.ComparedFiles = 0;
            e.BytesSaved = Math.Max(0, e.OriginalBytes - e.CurrentBytes);
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateOriginalBytesFromRestore);
            _lastChangedMod = mod;
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateCurrentModInfo(string mod, string? absolutePath, string? relativePath, string? version, string? author, string? relativeModName)
    {
        lock (_lock)
        {
            var e = Get(mod);
            string abs = absolutePath ?? string.Empty;
            string rel = string.IsNullOrWhiteSpace(relativePath) ? (e.PenumbraRelativePath ?? string.Empty) : relativePath ?? string.Empty;
            string ver = version ?? string.Empty;
            string auth = author ?? string.Empty;
            string rname = string.IsNullOrWhiteSpace(relativeModName) ? (e.RelativeModName ?? string.Empty) : relativeModName ?? string.Empty;
            bool versionChanged = !string.Equals(e.CurrentVersion ?? string.Empty, ver ?? string.Empty, StringComparison.Ordinal);
            bool changed = !string.Equals(e.ModAbsolutePath ?? string.Empty, abs ?? string.Empty, StringComparison.Ordinal)
                || !string.Equals(e.PenumbraRelativePath ?? string.Empty, rel ?? string.Empty, StringComparison.Ordinal)
                || versionChanged
                || !string.Equals(e.CurrentAuthor ?? string.Empty, auth ?? string.Empty, StringComparison.Ordinal)
                || !string.Equals(e.RelativeModName ?? string.Empty, rname ?? string.Empty, StringComparison.Ordinal);

            // LOGIC-FIX: If version changes and we have no backup, reset OriginalBytes to allow re-scan.
            // This handles mod updates (v1 -> v2) where size changes, while protecting converted mods (which have backup).
            if (versionChanged && !e.HasTextureBackup && !e.HasPmpBackup && e.OriginalBytes > 0)
            {
                try { _logger.LogDebug($"[ModState] Version change for '{mod}' ({e.CurrentVersion} -> {ver}) without backup. Resetting OriginalBytes for re-scan."); } catch { }
                e.OriginalBytes = 0;
                e.CurrentBytes = 0;
                e.ComparedFiles = 0;
                e.BytesSaved = 0;
                changed = true;
            }

            // Ensure ModUid and path segments are populated even if no other fields change
            if (!changed)
            {
                bool needsUid = string.IsNullOrWhiteSpace(e.ModUid);
                bool needsSegs = e.PenumbraPathSegments == null || e.PenumbraPathSegments.Count == 0;
                if (!needsUid && !needsSegs) return;
                try { if (needsSegs) e.PenumbraPathSegments = (rel ?? string.Empty).Replace('\\', '/').Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries).ToList(); } catch { e.PenumbraPathSegments = new List<string>(); }
                try { if (needsUid) e.ModUid = ComputeModUid(mod, e.CurrentAuthor); } catch { }
                e.LastUpdatedUtc = DateTime.UtcNow;
                _lastSaveReason = nameof(UpdateCurrentModInfo);
                ScheduleSave();
                if (!_batching)
                    try { OnEntryChanged?.Invoke(mod); } catch { }
                return;
            }
            e.ModAbsolutePath = abs ?? string.Empty;
            e.PenumbraRelativePath = rel ?? string.Empty;
            try { e.PenumbraPathSegments = (rel ?? string.Empty).Replace('\\', '/').Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries).ToList(); } catch { e.PenumbraPathSegments = new List<string>(); }
            e.CurrentVersion = ver ?? string.Empty;
            e.CurrentAuthor = auth ?? string.Empty;
            e.RelativeModName = rname ?? string.Empty;
            try { if (string.IsNullOrWhiteSpace(e.ModUid)) e.ModUid = ComputeModUid(mod, e.CurrentAuthor); } catch { }
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateCurrentModInfo);
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateLatestBackupsInfo(string mod, string? zipFileName, string? zipVersion, DateTime zipCreatedUtc, string? pmpFileName, string? pmpVersion, DateTime pmpCreatedUtc)
    {
        lock (_lock)
        {
            var e = Get(mod);
            var zf = zipFileName ?? string.Empty;
            var zv = zipVersion ?? string.Empty;
            var pf = pmpFileName ?? string.Empty;
            var pv = pmpVersion ?? string.Empty;
            bool changed = !string.Equals(e.LatestZipBackupFileName, zf, StringComparison.Ordinal)
                || !string.Equals(e.LatestZipBackupVersion, zv, StringComparison.Ordinal)
                || e.LatestZipBackupCreatedUtc != zipCreatedUtc
                || !string.Equals(e.LatestPmpBackupFileName, pf, StringComparison.Ordinal)
                || !string.Equals(e.LatestPmpBackupVersion, pv, StringComparison.Ordinal)
                || e.LatestPmpBackupCreatedUtc != pmpCreatedUtc;
            if (!changed) return;
            e.LatestZipBackupFileName = zf;
            e.LatestZipBackupVersion = zv;
            e.LatestZipBackupCreatedUtc = zipCreatedUtc;
            e.LatestPmpBackupFileName = pf;
            e.LatestPmpBackupVersion = pv;
            e.LatestPmpBackupCreatedUtc = pmpCreatedUtc;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateLatestBackupsInfo);
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateInstalledButNotConverted(string mod, bool flag)
    {
        lock (_lock)
        {
            var e = Get(mod);
            if (e.InstalledButNotConverted == flag) return;
            e.InstalledButNotConverted = flag;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateInstalledButNotConverted);
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateEnabledState(string mod, bool enabled, int priority)
    {
        lock (_lock)
        {
            var e = Get(mod);
            bool changed = e.Enabled != enabled || e.Priority != priority;
            if (!changed) return;
            e.Enabled = enabled;
            e.Priority = priority;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateEnabledState);
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateDisplayAndTags(string mod, string? displayName, IReadOnlyList<string>? tags)
    {
        lock (_lock)
        {
            var e = Get(mod);
            var dn = displayName ?? string.Empty;
            var tg = (tags ?? Array.Empty<string>()).Select(t => (t ?? string.Empty).Trim()).Where(t => t.Length > 0).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var tgSorted = tg.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
            var eSorted = (e.Tags ?? new List<string>()).OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList();
            bool tagsEqual = tgSorted.Count == eSorted.Count && tgSorted.Zip(eSorted, (a, b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase)).All(eq => eq);
            bool changed = !string.Equals(e.DisplayName, dn, StringComparison.Ordinal) || !tagsEqual;
            if (!changed) return;
            e.DisplayName = dn;
            e.Tags = tg;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateDisplayAndTags);
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateTextureFiles(string mod, IReadOnlyList<string>? files, DateTime? lastWriteUtc = null)
    {
        var list = (files ?? Array.Empty<string>()).Where(f => !string.IsNullOrWhiteSpace(f)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        Dictionary<string, long> sizes = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < list.Count; i++)
        {
            var path = list[i];
            if (string.IsNullOrWhiteSpace(path))
                continue;
            try
            {
                var fi = new FileInfo(path);
                if (fi.Exists)
                    sizes[path] = fi.Length;
            }
            catch { }
        }

        var shouldNotify = false;
        lock (_lock)
        {
            var e = Get(mod);
            bool sameList = e.TextureFiles != null && e.TextureFiles.Count == list.Count;
            if (sameList)
            {
                var prevSet = new HashSet<string>(e.TextureFiles ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < list.Count; i++)
                {
                    if (!prevSet.Contains(list[i]))
                    {
                        sameList = false;
                        break;
                    }
                }
            }
            var sameWrite = !lastWriteUtc.HasValue || e.LastKnownWriteUtc == lastWriteUtc.Value;
            if (sameList && e.TotalTextures == list.Count && sameWrite && !e.NeedsRescan)
                return;
            if (list.Count == 0 && e.TotalTextures > 0)
            {
                try { _logger.LogDebug("[TRACE-ZERO-BUG] UpdateTextureFiles reset TotalTextures to 0 for {mod}. Prev={prev}. Trace: {trace}", mod, e.TotalTextures, Environment.StackTrace); } catch { }
            }
            e.TotalTextures = list.Count;
            e.TextureFiles = list;
            e.LastScanUtc = DateTime.UtcNow;
            if (lastWriteUtc.HasValue)
            {
                e.LastKnownWriteUtc = lastWriteUtc.Value;
            }
            e.NeedsRescan = false;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateTextureFiles);
            _lastChangedMod = mod;
            ScheduleSave();
            shouldNotify = !_batching;
        }
        WriteDetail(mod, list, null, sizes);
        if (shouldNotify)
            try { OnEntryChanged?.Invoke(mod); } catch { }
    }

    public void UpdateUsedTextureFiles(string mod, IReadOnlyList<string>? files)
    {
        lock (_lock)
        {
            var e = Get(mod);
            var list = (files ?? Array.Empty<string>()).Where(f => !string.IsNullOrWhiteSpace(f)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            var prev = ReadDetailUsed(mod);
            bool sameCount = list.Count == prev.Count;
            var prevSet = new HashSet<string>(prev, StringComparer.OrdinalIgnoreCase);
            bool sameItems = sameCount && list.All(x => prevSet.Contains(x));
            var flag = list.Count > 0;
            if (sameItems && e.UsedInCollection == flag && e.UsedTextureCount == list.Count) return;
            e.UsedInCollection = flag;
            e.UsedTextureCount = list.Count;
            e.UsedTextureFiles = list;
            e.LastUpdatedUtc = DateTime.UtcNow;
            WriteDetail(mod, null, list, null);
            _lastSaveReason = nameof(UpdateUsedTextureFiles);
            _lastChangedMod = mod;
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void UpdateTextureCount(string mod, int total)
    {
        lock (_lock)
        {
            var e = Get(mod);
            if (e.TotalTextures == total) return;
            if (total == 0 && e.TotalTextures > 0)
            {
                try { _logger.LogDebug("[TRACE-ZERO-BUG] UpdateTextureCount reset TotalTextures to 0 for {mod}. Prev={prev}. Trace: {trace}", mod, e.TotalTextures, Environment.StackTrace); } catch { }
            }
            e.TotalTextures = Math.Max(0, total);
            e.LastScanUtc = DateTime.UtcNow;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateTextureCount);
            _lastChangedMod = mod;
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    private string GetDetailDir()
    {
        var root = _config.Current.BackupFolderPath;
        var dir = Path.Combine(root, "mod_state", "mods");
        try { Directory.CreateDirectory(dir); } catch { }
        return dir;
    }

    private string Sanitize(string mod)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var s = new string(mod.Select(c => invalid.Contains(c) ? '_' : c).ToArray());
        return s;
    }

    private void WriteDetail(string mod, List<string>? textures, List<string>? used, Dictionary<string, long>? sizes)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod))
                return;
            var trace = PerfTrace.Step(_logger, $"ModState Detail {mod}");
            List<string> t = textures ?? ReadDetailTextures(mod);
            List<string> u = used ?? ReadDetailUsed(mod);
            Dictionary<string, long> s = sizes ?? ReadDetailTextureSizes(mod);
            var obj = new ModDetail { TextureFiles = t, UsedTextureFiles = u, TextureFileSizes = s };
            var json = JsonSerializer.Serialize(obj, new JsonSerializerOptions { WriteIndented = true });
            if (_useSqliteStorage)
            {
                WriteDetailToSqlite(mod, json);
            }
            else
            {
                var dir = GetDetailDir();
                var path = Path.Combine(dir, Sanitize(mod) + ".json");
                File.WriteAllText(path, json);
            }
            trace.Dispose();
        }
        catch { }
    }

    public List<string> ReadDetailTextures(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod))
                return new List<string>();
            var json = ReadDetailJson(mod);
            if (string.IsNullOrWhiteSpace(json)) return new List<string>();
            var obj = JsonSerializer.Deserialize<ModDetail>(json) ?? new ModDetail();
            return obj.TextureFiles ?? new List<string>();
        }
        catch { return new List<string>(); }
    }

    public List<string> ReadDetailUsed(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod))
                return new List<string>();
            var json = ReadDetailJson(mod);
            if (string.IsNullOrWhiteSpace(json)) return new List<string>();
            var obj = JsonSerializer.Deserialize<ModDetail>(json) ?? new ModDetail();
            return obj.UsedTextureFiles ?? new List<string>();
        }
        catch { return new List<string>(); }
    }

    public Dictionary<string, long> ReadDetailTextureSizes(string mod)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(mod))
                return new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            var json = ReadDetailJson(mod);
            if (string.IsNullOrWhiteSpace(json)) return new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            var obj = JsonSerializer.Deserialize<ModDetail>(json) ?? new ModDetail();
            return obj.TextureFileSizes ?? new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        }
        catch { return new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase); }
    }

    private string ReadDetailJson(string mod)
    {
        if (_useSqliteStorage)
        {
            return ReadDetailFromSqlite(mod);
        }

        var dir = GetDetailDir();
        var path = Path.Combine(dir, Sanitize(mod) + ".json");
        if (!File.Exists(path))
        {
            return string.Empty;
        }

        return File.ReadAllText(path);
    }

    private sealed class ModDetail
    {
        public List<string>? TextureFiles { get; set; }
        public List<string>? UsedTextureFiles { get; set; }
        public Dictionary<string, long>? TextureFileSizes { get; set; }
    }

    public void Load()
    {
        if (_useSqliteStorage)
        {
            LoadFromSqlite();
            return;
        }

        var path = GetPath();
        if (!File.Exists(path)) return;
        try
        {
            string json = string.Empty;
            for (int attempt = 0; attempt < 3; attempt++)
            {
                try
                {
                    using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 65536, FileOptions.SequentialScan);
                    using var sr = new StreamReader(fs, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
                    json = sr.ReadToEnd();
                    if (!string.IsNullOrWhiteSpace(json)) break;
                }
                catch
                {
                    try { System.Threading.Thread.Sleep(30); } catch { }
                }
            }
            Dictionary<string, ModStateEntry> dict = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind == JsonValueKind.Object
                    && (doc.RootElement.TryGetProperty("entries", out _)
                        || doc.RootElement.TryGetProperty("Entries", out _)))
                {
                    var file = JsonSerializer.Deserialize<ModStateFile>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                        ?? new ModStateFile { Meta = new ModStateMeta(), Entries = new Dictionary<string, ModStateEntry>() };
                    dict = new Dictionary<string, ModStateEntry>(file.Entries ?? new Dictionary<string, ModStateEntry>(), StringComparer.OrdinalIgnoreCase);
                    try
                    {
                        _meta = file.Meta ?? new ModStateMeta();
                    }
                    catch
                    {
                        _meta = new ModStateMeta();
                    }
                }
                else
                {
                    var d0 = JsonSerializer.Deserialize<Dictionary<string, ModStateEntry>>(json) ?? new();
                    dict = new Dictionary<string, ModStateEntry>(d0, StringComparer.OrdinalIgnoreCase);
                }
            }
            catch
            {
                var d0 = JsonSerializer.Deserialize<Dictionary<string, ModStateEntry>>(json) ?? new();
                dict = new Dictionary<string, ModStateEntry>(d0, StringComparer.OrdinalIgnoreCase);
            }
            lock (_lock)
            {
                if (_state.Count > 0 && (dict == null || dict.Count == 0))
                {
                    try { _logger.LogDebug("Load() ignored empty state from disk due to non-empty in-memory state"); } catch { }
                }
                else
                {
                    if (_state.Count > 0 && dict != null)
                    {
                        try
                        {
                            var sampleOld = _state.Values.FirstOrDefault(x => x.TotalTextures > 0 || !string.IsNullOrEmpty(x.CurrentVersion));
                            if (sampleOld != null && dict.TryGetValue(sampleOld.ModFolderName, out var sampleNew))
                            {
                                if (sampleNew.TotalTextures == 0 && string.IsNullOrEmpty(sampleNew.CurrentVersion) && string.IsNullOrEmpty(sampleNew.CurrentAuthor))
                                {
                                    _logger.LogDebug($"[TRACE-WIPE] Load() replacing populated mod '{sampleOld.ModFolderName}' (tex={sampleOld.TotalTextures}, ver={sampleOld.CurrentVersion}) with empty/default entry! Source file size: {_lastLoadedLength}");
                                }
                            }
                        }
                        catch { }
                    }
                    _state = dict ?? new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
                }
                if (_meta == null)
                    _meta = new ModStateMeta();
            }
            List<string>? removedKeys = null;
            lock (_lock)
            {
                foreach (var kv in _state)
                {
                    var key = kv.Key;
                    var entry = kv.Value;
                    if (string.IsNullOrWhiteSpace(key)
                        || string.Equals(key, "Entries", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(key, "Meta", StringComparison.OrdinalIgnoreCase)
                        || IsTempModStateArtifact(key, entry))
                    {
                        removedKeys ??= new List<string>();
                        removedKeys.Add(key);
                    }
                }

                if (removedKeys != null)
                {
                    for (var i = 0; i < removedKeys.Count; i++)
                        _state.Remove(removedKeys[i]);
                }
            }
            if (removedKeys != null && removedKeys.Count > 0)
            {
                try { ScheduleSave(); } catch { }
                try
                {
                    if (_useSqliteStorage)
                    {
                        for (var i = 0; i < removedKeys.Count; i++)
                        {
                            try { DeleteDetailFromSqlite(removedKeys[i]); } catch { }
                        }
                    }
                    else
                    {
                        var dir = GetDetailDir();
                        for (var i = 0; i < removedKeys.Count; i++)
                        {
                            var p = Path.Combine(dir, Sanitize(removedKeys[i]) + ".json");
                            try { if (File.Exists(p)) File.Delete(p); } catch { }
                        }
                    }
                }
                catch { }
            }
            try
            {
                foreach (var kv in _state)
                {
                    var e = kv.Value;
                    if (e == null) continue;
                    if (e.UsedTextureCount <= 0 && e.UsedTextureFiles != null)
                        e.UsedTextureCount = e.UsedTextureFiles.Count;
                    if (e.BytesSaved <= 0 && e.OriginalBytes > 0 && e.CurrentBytes >= 0)
                        e.BytesSaved = Math.Max(0, e.OriginalBytes - e.CurrentBytes);
                }
            }
            catch { }
            try
            {
                string currentStateFingerprint = string.Empty;
                string existingStateFingerprint = string.Empty;
                lock (_lock)
                {
                    existingStateFingerprint = _meta?.StateFingerprint ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(existingStateFingerprint))
                {
                    Dictionary<string, ModStateEntry> snapshot;
                    lock (_lock)
                    {
                        snapshot = new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase);
                    }
                    currentStateFingerprint = ComputeStateFingerprint(snapshot);
                    if (!string.IsNullOrWhiteSpace(currentStateFingerprint))
                    {
                        lock (_lock)
                        {
                            _meta ??= new ModStateMeta();
                            if (string.IsNullOrWhiteSpace(_meta.StateFingerprint))
                            {
                                _meta.StateFingerprint = currentStateFingerprint;
                                _meta.StateFingerprintUtc = DateTime.UtcNow;
                                _lastSaveReason = "InitializeStateFingerprint";
                            }
                        }
                        try { ScheduleSave(); } catch { }
                    }
                }
            }
            catch { }
            try { var fi = new FileInfo(path); _lastLoadedWriteUtc = fi.LastWriteTimeUtc; _lastLoadedLength = fi.Length; } catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogError(ex, "Failed to load mod_state.json from {path}", path); } catch { }
            try
            {
                var dir = Path.GetDirectoryName(path) ?? string.Empty;
                var name = Path.GetFileName(path);
                var stamp = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ");
                var corrupt = Path.Combine(dir, name + $".corrupt_{stamp}");
                try { if (File.Exists(corrupt)) File.Delete(corrupt); } catch { }
                File.Move(path, corrupt);
            }
            catch { }
            try { ScheduleSave(); } catch { }
        }
    }

    private static bool IsTempModStateArtifact(string key, ModStateEntry? entry)
    {
        if (IsTempModStateArtifactName(key)) return true;
        if (entry == null) return false;
        if (IsTempModStateArtifactName(entry.ModFolderName)) return true;
        if (IsTempModStateArtifactName(entry.RelativeModName)) return true;
        if (IsTempModStateArtifactName(entry.DisplayName)) return true;
        if (IsTempModStateArtifactName(Path.GetFileName(entry.ModAbsolutePath ?? string.Empty))) return true;
        return false;
    }

    private static bool IsTempModStateArtifactName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return false;
        var n = name.Trim();
        if (n.StartsWith("mod_state.json~", StringComparison.OrdinalIgnoreCase)) return true;
        if (n.Contains("mod_state.json~", StringComparison.OrdinalIgnoreCase)) return true;
        if (n.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase) && n.Contains("~rf", StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    private DateTime _lastSaveScheduledUtc = DateTime.MinValue;
    private CancellationTokenSource? _saveDebounceCts;
    private readonly object _saveDebounceLock = new();

    public void Save()
    {
        ScheduleSave();
    }

    private void ScheduleSave()
    {
        if (!_savingEnabled) return;
        if (_batching) { _batchDirty = true; return; }
        lock (_saveDebounceLock)
        {
            try { _saveDebounceCts?.Cancel(); } catch { }
            try { _saveDebounceCts?.Dispose(); } catch { }
            _saveDebounceCts = new CancellationTokenSource();
            var token = _saveDebounceCts.Token;
            _lastSaveScheduledUtc = DateTime.UtcNow;
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(300, token).ConfigureAwait(false);
                    if (token.IsCancellationRequested) return;
                    SaveInternal();
                }
                catch (TaskCanceledException) { }
                catch { }
            });
        }
    }

    private readonly object _fileSaveLock = new();

    private void SaveInternal()
    {
        if (!_savingEnabled) return;
        lock (_fileSaveLock)
        {
            try
            {
                var trace = PerfTrace.Step(_logger, "ModState Save");
                Dictionary<string, ModStateEntry> entriesSnapshot;
                ModStateMeta metaSnapshot;
                lock (_lock)
                {
                    entriesSnapshot = new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase);
                    var m = _meta ?? new ModStateMeta();
                    metaSnapshot = new ModStateMeta
                    {
                        SchemaVersion = m.SchemaVersion,
                        GeneratedBy = m.GeneratedBy,
                        CreatedUtc = m.CreatedUtc,
                        LastSavedUtc = m.LastSavedUtc,
                        BackupFolderFingerprint = m.BackupFolderFingerprint,
                        BackupFolderFingerprintUtc = m.BackupFolderFingerprintUtc,
                        StateFingerprint = m.StateFingerprint,
                        StateFingerprintUtc = m.StateFingerprintUtc,
                        PenumbraFolderFingerprint = m.PenumbraFolderFingerprint,
                        PenumbraFolderFingerprintUtc = m.PenumbraFolderFingerprintUtc,
                        PenumbraRootPath = m.PenumbraRootPath,
                    };
                }

                if (_useSqliteStorage)
                {
                    var sqliteElapsed = Stopwatch.StartNew();
                    SaveSnapshotToSqlite(entriesSnapshot, metaSnapshot);
                    sqliteElapsed.Stop();
                    var sqliteElapsedMs = (int)Math.Round(sqliteElapsed.Elapsed.TotalMilliseconds);
                    try
                    {
                        if (_config.Current.DebugTraceModStateChanges)
                        {
                            string caller = _lastSaveReason ?? string.Empty;
                            _logger.LogDebug("mod_state.sqlite saved: caller={caller}, entries={count}, thread={thread}, elapsedMs={elapsed}", caller, _state.Count, System.Environment.CurrentManagedThreadId, sqliteElapsedMs);
                        }
                    }
                    catch { }
                    try { _logger.LogDebug("ModState save: reason={reason} mod={mod} entries={count}", _lastSaveReason, _lastChangedMod, _state.Count); } catch { }
                    try { OnStateSaved?.Invoke(); } catch { }
                    trace.Dispose();
                    return;
                }

                var path = GetPath();
                var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
                var tmp = path + $".tmp.{pid}.{System.Environment.CurrentManagedThreadId}.{DateTime.UtcNow.Ticks}";
                try { var dir = Path.GetDirectoryName(path); if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir); } catch { }

                var sw = Stopwatch.StartNew();
                using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan))
                {
                    using var writer = new Utf8JsonWriter(fs, new JsonWriterOptions { Indented = true });
                    metaSnapshot.SchemaVersion = 4;
                    metaSnapshot.GeneratedBy = GetGeneratedBy();
                    if (metaSnapshot.CreatedUtc == DateTime.MinValue)
                        metaSnapshot.CreatedUtc = _lastLoadedWriteUtc == DateTime.MinValue ? DateTime.UtcNow : _lastLoadedWriteUtc;
                    metaSnapshot.LastSavedUtc = DateTime.UtcNow;
                    metaSnapshot.StateFingerprint = ComputeStateFingerprint(entriesSnapshot);
                    metaSnapshot.StateFingerprintUtc = DateTime.UtcNow;

                    var file = new ModStateFile { Meta = metaSnapshot, Entries = entriesSnapshot };
                    JsonSerializer.Serialize(writer, file, new JsonSerializerOptions { WriteIndented = true });
                    writer.Flush();
                }
                
                var fiTmp = new FileInfo(tmp);
                if (fiTmp.Length == 0)
                {
                     try { _logger.LogError("SaveInternal generated empty file, aborting move"); } catch { }
                     try { File.Delete(tmp); } catch { }
                     return;
                }

                var moved = false;
                int tries = 0;
                while (!moved && tries < 20)
                {
                    tries++;
                    try
                    {
                        if (File.Exists(path))
                            File.Replace(tmp, path, null);
                        else
                            File.Move(tmp, path);
                        moved = true;
                    }
                    catch
                    {
                        try { if (File.Exists(path)) File.Delete(path); } catch { }
                        try
                        {
                            File.Move(tmp, path);
                            moved = true;
                        }
                        catch
                        {
                            try { System.Threading.Thread.Sleep(Math.Min(500, 50 * tries)); } catch { }
                        }
                    }
                }
                
                if (moved)
                {
                    try 
                    { 
                        var fi = new FileInfo(path);
                        _lastLoadedWriteUtc = fi.LastWriteTimeUtc;
                        _lastLoadedLength = fi.Length;
                    } 
                    catch { }
                }

                if (!moved)
                {
                    try
                    {
                        using (var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, 65536, FileOptions.SequentialScan))
                        {
                            using var writer = new Utf8JsonWriter(fs, new JsonWriterOptions { Indented = true });
                            metaSnapshot.SchemaVersion = 3;
                            metaSnapshot.GeneratedBy = GetGeneratedBy();
                            if (metaSnapshot.CreatedUtc == DateTime.MinValue)
                                metaSnapshot.CreatedUtc = _lastLoadedWriteUtc == DateTime.MinValue ? DateTime.UtcNow : _lastLoadedWriteUtc;
                            metaSnapshot.LastSavedUtc = DateTime.UtcNow;
                            metaSnapshot.StateFingerprint = ComputeStateFingerprint(_state);
                            metaSnapshot.StateFingerprintUtc = DateTime.UtcNow;
                            var file2 = new ModStateFile { Meta = metaSnapshot, Entries = _state };
                        JsonSerializer.Serialize(writer, file2, new JsonSerializerOptions { WriteIndented = true });
                        writer.Flush();
                    }
                    moved = true;
                }
                catch
                {
                    try { if (File.Exists(path)) File.Delete(path); } catch { }
                    File.Move(tmp, path);
                }
            }
            var elapsedMs = (int)Math.Round(sw.Elapsed.TotalMilliseconds);
            try
            {
                if (_config.Current.DebugTraceModStateChanges)
                {
                    string caller = _lastSaveReason ?? string.Empty;
                    try
                    {
                        var st = new StackTrace(2, false);
                        var sf = st.GetFrame(0);
                        var m = sf?.GetMethod();
                        if (string.IsNullOrWhiteSpace(caller))
                            caller = m != null ? (m.DeclaringType?.Name + "." + m.Name) : string.Empty;
                    }
                    catch { }
                    _logger.LogDebug("mod_state.json saved: caller={caller}, entries={count}, thread={thread}, elapsedMs={elapsed}", caller, _state.Count, System.Environment.CurrentManagedThreadId, elapsedMs);
                }
            }
            catch { }
            try { _logger.LogDebug("ModState save: reason={reason} mod={mod} entries={count}", _lastSaveReason, _lastChangedMod, _state.Count); } catch { }
            try
            {
                if (_config.Current.DebugTraceModStateChanges)
                {
                    try
                    {
                        var caller = string.IsNullOrWhiteSpace(_lastSaveReason) ? string.Empty : _lastSaveReason;
                        _debug?.AddModState($"save caller={caller} entries={_state.Count} thread={System.Environment.CurrentManagedThreadId} elapsedMs={elapsedMs}");
                    }
                    catch { }
                }
            }
            catch { }
            try { OnStateSaved?.Invoke(); } catch { }
            trace.Dispose();
            }
            catch (Exception ex)
            {
                try { _logger.LogError(ex, "Failed to save mod state to {path}", GetStateFilePath()); } catch { }
                try
                {
                    if (_useSqliteStorage)
                    {
                        return;
                    }
                    var dir = Path.GetDirectoryName(GetPath()) ?? string.Empty;
                    foreach (var f in Directory.EnumerateFiles(dir, "mod_state.json.tmp.*"))
                    {
                        try { File.Delete(f); } catch { }
                    }
                }
                catch { }
            }
        }
    }

    public void SetSavingEnabled(bool enabled)
    {
        _savingEnabled = enabled;
    }

    public void RemoveEntry(string mod)
    {
        lock (_lock)
        {
            if (string.IsNullOrWhiteSpace(mod)) return;
            if (!_state.Remove(mod)) return;
            try
            {
                if (_useSqliteStorage)
                {
                    DeleteDetailFromSqlite(mod);
                }
                else
                {
                    var dir = GetDetailDir();
                    var p = Path.Combine(dir, Sanitize(mod) + ".json");
                    if (File.Exists(p))
                        File.Delete(p);
                }
            }
            catch { }
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void MoveEntry(string fromMod, string toMod)
    {
        lock (_lock)
        {
            if (string.IsNullOrWhiteSpace(fromMod) || string.IsNullOrWhiteSpace(toMod))
                return;
            if (string.Equals(fromMod, toMod, StringComparison.OrdinalIgnoreCase))
                return;
            if (!_state.TryGetValue(fromMod, out var fromEntry) || fromEntry == null)
                return;

            if (_state.TryGetValue(toMod, out var toEntry) && toEntry != null)
            {
                if ((toEntry.TotalTextures <= 0 && fromEntry.TotalTextures > 0) || (toEntry.ComparedFiles <= 0 && fromEntry.ComparedFiles > 0))
                {
                    toEntry.TotalTextures = Math.Max(toEntry.TotalTextures, fromEntry.TotalTextures);
                    toEntry.ComparedFiles = Math.Max(toEntry.ComparedFiles, fromEntry.ComparedFiles);
                    toEntry.OriginalBytes = Math.Max(toEntry.OriginalBytes, fromEntry.OriginalBytes);
                    toEntry.CurrentBytes = Math.Max(toEntry.CurrentBytes, fromEntry.CurrentBytes);
                    toEntry.BytesSaved = Math.Max(toEntry.BytesSaved, fromEntry.BytesSaved);
                }
                if (string.IsNullOrWhiteSpace(toEntry.ModAbsolutePath) && !string.IsNullOrWhiteSpace(fromEntry.ModAbsolutePath))
                    toEntry.ModAbsolutePath = fromEntry.ModAbsolutePath;
                if (string.IsNullOrWhiteSpace(toEntry.PenumbraRelativePath) && !string.IsNullOrWhiteSpace(fromEntry.PenumbraRelativePath))
                    toEntry.PenumbraRelativePath = fromEntry.PenumbraRelativePath;
                if (string.IsNullOrWhiteSpace(toEntry.RelativeModName) && !string.IsNullOrWhiteSpace(fromEntry.RelativeModName))
                    toEntry.RelativeModName = fromEntry.RelativeModName;
                if (string.IsNullOrWhiteSpace(toEntry.CurrentVersion) && !string.IsNullOrWhiteSpace(fromEntry.CurrentVersion))
                    toEntry.CurrentVersion = fromEntry.CurrentVersion;
                if (string.IsNullOrWhiteSpace(toEntry.CurrentAuthor) && !string.IsNullOrWhiteSpace(fromEntry.CurrentAuthor))
                    toEntry.CurrentAuthor = fromEntry.CurrentAuthor;
                if ((toEntry.UsedTextureFiles == null || toEntry.UsedTextureFiles.Count == 0) && fromEntry.UsedTextureFiles != null && fromEntry.UsedTextureFiles.Count > 0)
                    toEntry.UsedTextureFiles = fromEntry.UsedTextureFiles;
                toEntry.UsedTextureCount = Math.Max(toEntry.UsedTextureCount, fromEntry.UsedTextureCount);
                toEntry.LastUpdatedUtc = DateTime.UtcNow;
            }
            else
            {
                _state.Remove(fromMod);
                fromEntry.ModFolderName = toMod;
                fromEntry.LastUpdatedUtc = DateTime.UtcNow;
                _state[toMod] = fromEntry;
            }

            _state.Remove(fromMod);
            try
            {
                if (_useSqliteStorage)
                {
                    MoveDetailInSqlite(fromMod, toMod);
                }
                else
                {
                    var dir = GetDetailDir();
                    var oldPath = Path.Combine(dir, Sanitize(fromMod) + ".json");
                    var newPath = Path.Combine(dir, Sanitize(toMod) + ".json");
                    if (File.Exists(oldPath))
                    {
                        if (!File.Exists(newPath))
                            File.Move(oldPath, newPath);
                        else
                            File.Delete(oldPath);
                    }
                }
            }
            catch { }

            _lastSaveReason = nameof(MoveEntry);
            _lastChangedMod = toMod;
            ScheduleSave();
            if (!_batching)
            {
                try { OnEntryChanged?.Invoke(fromMod); } catch { }
                try { OnEntryChanged?.Invoke(toMod); } catch { }
            }
        }
    }

    public void RecomputeInstalledButNotConverted()
    {
        List<string> changed = new List<string>();
        lock (_lock)
        {
            foreach (var kv in _state)
            {
                var e = kv.Value;
                bool flag = (e.TotalTextures > 0) && !e.HasTextureBackup && !e.HasPmpBackup;
                if (e.InstalledButNotConverted != flag)
                {
                    e.InstalledButNotConverted = flag;
                    e.LastUpdatedUtc = DateTime.UtcNow;
                    changed.Add(kv.Key);
                }
            }
        }
        foreach (var m in changed)
        {
            if (!_batching)
                try { OnEntryChanged?.Invoke(m); } catch { }
        }
        if (!_batching) ScheduleSave();
        else _batchDirty = true;
    }

    public void BeginBatch()
    {
        _batching = true;
        _batchDirty = false;
    }

    public void EndBatch()
    {
        _batching = false;
        if (_batchDirty)
        {
            try { SaveInternal(); } catch { }
            try { OnStateSaved?.Invoke(); } catch { }
            _batchDirty = false;
        }
    }
    public void UpdateExternalChange(string mod, ShrinkU.Configuration.ExternalChangeMarker? marker)
    {
        lock (_lock)
        {
            var e = Get(mod);
            e.ExternalChange = marker;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateExternalChange);
            _lastChangedMod = mod;
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
    }

    public void SetLastConvertUtc(string mod, DateTime at)
    {
        lock (_lock)
        {
            var e = Get(mod);
            e.LastConvertUtc = at;
            e.LastChangeReason = nameof(SetLastConvertUtc);
            e.LastChangeUtc = DateTime.UtcNow;
            ScheduleSave();
        }
    }

    public void SetLastRestoreUtc(string mod, DateTime at)
    {
        lock (_lock)
        {
            var e = Get(mod);
            e.LastRestoreUtc = at;
            e.LastChangeReason = nameof(SetLastRestoreUtc);
            e.LastChangeUtc = DateTime.UtcNow;
            ScheduleSave();
        }
    }

    public void UpdateLatestBackupSummary(string mod, int entriesCount, long totalBytes)
    {
        lock (_lock)
        {
            var e = Get(mod);
            if (e.LatestBackup == null)
                e.LatestBackup = new LatestBackupInfo();
            e.LatestBackup.EntriesCount = Math.Max(0, entriesCount);
            e.LatestBackup.TotalBytes = Math.Max(0, totalBytes);
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateLatestBackupSummary);
            _lastChangedMod = mod;
            ScheduleSave();
        }
    }

    private void EnsureSqliteSchema()
    {
        lock (_sqliteLock)
        {
            using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                              PRAGMA journal_mode=WAL;
                              CREATE TABLE IF NOT EXISTS kv_state(
                                  key TEXT PRIMARY KEY,
                                  value TEXT NOT NULL
                              );
                              CREATE TABLE IF NOT EXISTS mod_state_entry(
                                  mod TEXT PRIMARY KEY,
                                  value TEXT NOT NULL
                              );
                              CREATE TABLE IF NOT EXISTS mod_detail(
                                  mod TEXT PRIMARY KEY,
                                  value TEXT NOT NULL
                              );
                              """;
            cmd.ExecuteNonQuery();
        }
    }

    private static (Dictionary<string, ModStateEntry> Entries, ModStateMeta Meta) LoadJsonStateForMigration(string sourcePath)
    {
        var json = File.ReadAllText(sourcePath);
        var dict = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
        var meta = new ModStateMeta();

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind == JsonValueKind.Object
                && (doc.RootElement.TryGetProperty("entries", out _)
                    || doc.RootElement.TryGetProperty("Entries", out _)))
            {
                var file = JsonSerializer.Deserialize<ModStateFile>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                    ?? new ModStateFile { Meta = new ModStateMeta(), Entries = new Dictionary<string, ModStateEntry>() };
                dict = new Dictionary<string, ModStateEntry>(file.Entries ?? new Dictionary<string, ModStateEntry>(), StringComparer.OrdinalIgnoreCase);
                meta = file.Meta ?? new ModStateMeta();
            }
            else
            {
                var d0 = JsonSerializer.Deserialize<Dictionary<string, ModStateEntry>>(json) ?? new Dictionary<string, ModStateEntry>();
                dict = new Dictionary<string, ModStateEntry>(d0, StringComparer.OrdinalIgnoreCase);
            }
        }
        catch
        {
            var d0 = JsonSerializer.Deserialize<Dictionary<string, ModStateEntry>>(json) ?? new Dictionary<string, ModStateEntry>();
            dict = new Dictionary<string, ModStateEntry>(d0, StringComparer.OrdinalIgnoreCase);
        }

        var removed = new List<string>();
        foreach (var kv in dict)
        {
            var key = kv.Key;
            var entry = kv.Value;
            if (string.IsNullOrWhiteSpace(key)
                || string.Equals(key, "Entries", StringComparison.OrdinalIgnoreCase)
                || string.Equals(key, "Meta", StringComparison.OrdinalIgnoreCase)
                || IsTempModStateArtifact(key, entry))
            {
                removed.Add(key);
            }
        }

        for (var i = 0; i < removed.Count; i++)
        {
            dict.Remove(removed[i]);
        }

        return (dict, meta);
    }

    private void SaveSnapshotToSqlite(Dictionary<string, ModStateEntry> entriesSnapshot, ModStateMeta metaSnapshot)
    {
        metaSnapshot.SchemaVersion = 4;
        metaSnapshot.GeneratedBy = GetGeneratedBy();
        if (metaSnapshot.CreatedUtc == DateTime.MinValue)
            metaSnapshot.CreatedUtc = _lastLoadedWriteUtc == DateTime.MinValue ? DateTime.UtcNow : _lastLoadedWriteUtc;
        metaSnapshot.LastSavedUtc = DateTime.UtcNow;
        metaSnapshot.StateFingerprint = ComputeStateFingerprint(entriesSnapshot);
        metaSnapshot.StateFingerprintUtc = DateTime.UtcNow;
        var metaJson = JsonSerializer.Serialize(metaSnapshot, new JsonSerializerOptions { WriteIndented = true });
        var serializedEntries = new Dictionary<string, string>(entriesSnapshot.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var kv in entriesSnapshot)
        {
            if (string.IsNullOrWhiteSpace(kv.Key))
                continue;
            serializedEntries[kv.Key] = JsonSerializer.Serialize(kv.Value ?? new ModStateEntry { ModFolderName = kv.Key }, new JsonSerializerOptions { WriteIndented = true });
        }

        lock (_sqliteLock)
        {
            EnsureSqliteSchema();
            using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
            conn.Open();
            using var tx = conn.BeginTransaction();
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO kv_state(key,value) VALUES('meta',$value) ON CONFLICT(key) DO UPDATE SET value=excluded.value;";
                cmd.Parameters.AddWithValue("$value", metaJson);
                cmd.ExecuteNonQuery();
            }

            var existingEntries = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using (var read = conn.CreateCommand())
            {
                read.Transaction = tx;
                read.CommandText = "SELECT mod, value FROM mod_state_entry;";
                using var reader = read.ExecuteReader();
                while (reader.Read())
                {
                    var mod = reader.GetString(0);
                    var value = reader.GetString(1);
                    if (!string.IsNullOrWhiteSpace(mod))
                        existingEntries[mod] = value;
                }
            }

            foreach (var kv in serializedEntries)
            {
                if (existingEntries.TryGetValue(kv.Key, out var existing) && string.Equals(existing, kv.Value, StringComparison.Ordinal))
                {
                    existingEntries.Remove(kv.Key);
                    continue;
                }

                using var upsert = conn.CreateCommand();
                upsert.Transaction = tx;
                upsert.CommandText = "INSERT INTO mod_state_entry(mod,value) VALUES($mod,$value) ON CONFLICT(mod) DO UPDATE SET value=excluded.value;";
                upsert.Parameters.AddWithValue("$mod", kv.Key);
                upsert.Parameters.AddWithValue("$value", kv.Value);
                upsert.ExecuteNonQuery();
                existingEntries.Remove(kv.Key);
            }

            foreach (var staleMod in existingEntries.Keys)
            {
                using var del = conn.CreateCommand();
                del.Transaction = tx;
                del.CommandText = "DELETE FROM mod_state_entry WHERE mod=$mod;";
                del.Parameters.AddWithValue("$mod", staleMod);
                del.ExecuteNonQuery();
            }

            tx.Commit();
        }

        _lastLoadedWriteUtc = DateTime.UtcNow;
        _lastLoadedLength = metaJson.Length + serializedEntries.Sum(static kv => kv.Value.Length);
    }

    private void LoadFromSqlite()
    {
        try
        {
            EnsureSqliteSchema();
            string metaJson = string.Empty;
            string legacySnapshotJson = string.Empty;
            var entries = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
            lock (_sqliteLock)
            {
                using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT value FROM kv_state WHERE key='meta' LIMIT 1;";
                    var obj = cmd.ExecuteScalar();
                    metaJson = obj as string ?? string.Empty;
                }

                using (var readEntries = conn.CreateCommand())
                {
                    readEntries.CommandText = "SELECT mod, value FROM mod_state_entry;";
                    using var reader = readEntries.ExecuteReader();
                    while (reader.Read())
                    {
                        var mod = reader.GetString(0);
                        if (string.IsNullOrWhiteSpace(mod))
                            continue;
                        var value = reader.GetString(1);
                        try
                        {
                            var entry = JsonSerializer.Deserialize<ModStateEntry>(value, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                                ?? new ModStateEntry { ModFolderName = mod };
                            if (string.IsNullOrWhiteSpace(entry.ModFolderName))
                                entry.ModFolderName = mod;
                            entries[mod] = entry;
                        }
                        catch
                        {
                            entries[mod] = new ModStateEntry { ModFolderName = mod };
                        }
                    }
                }

                if (entries.Count == 0)
                {
                    using var legacy = conn.CreateCommand();
                    legacy.CommandText = "SELECT value FROM kv_state WHERE key='main' LIMIT 1;";
                    var legacyObj = legacy.ExecuteScalar();
                    legacySnapshotJson = legacyObj as string ?? string.Empty;
                }
            }

            if (entries.Count > 0)
            {
                lock (_lock)
                {
                    _state = new Dictionary<string, ModStateEntry>(entries, StringComparer.OrdinalIgnoreCase);
                    _meta = new ModStateMeta();
                    if (!string.IsNullOrWhiteSpace(metaJson))
                    {
                        try
                        {
                            _meta = JsonSerializer.Deserialize<ModStateMeta>(metaJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new ModStateMeta();
                        }
                        catch
                        {
                            _meta = new ModStateMeta();
                        }
                    }
                }

                _lastLoadedWriteUtc = DateTime.UtcNow;
                _lastLoadedLength = metaJson.Length;
                return;
            }

            if (string.IsNullOrWhiteSpace(legacySnapshotJson))
            {
                lock (_lock)
                {
                    _state = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
                    _meta = new ModStateMeta();
                }

                return;
            }

            var file = JsonSerializer.Deserialize<ModStateFile>(legacySnapshotJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? new ModStateFile { Meta = new ModStateMeta(), Entries = new Dictionary<string, ModStateEntry>() };
            lock (_lock)
            {
                _state = new Dictionary<string, ModStateEntry>(file.Entries ?? new Dictionary<string, ModStateEntry>(), StringComparer.OrdinalIgnoreCase);
                _meta = file.Meta ?? new ModStateMeta();
            }

            _lastLoadedWriteUtc = DateTime.UtcNow;
            _lastLoadedLength = legacySnapshotJson.Length;
            try
            {
                var snapshot = new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase);
                var metaSnapshot = _meta ?? new ModStateMeta();
                SaveSnapshotToSqlite(snapshot, metaSnapshot);
            }
            catch { }
        }
        catch (Exception ex)
        {
            try { _logger.LogError(ex, "Failed to load mod state from sqlite {path}", _sqlitePath); } catch { }
        }
    }

    private string ReadDetailFromSqlite(string mod)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return string.Empty;

        lock (_sqliteLock)
        {
            EnsureSqliteSchema();
            using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT value FROM mod_detail WHERE mod=$mod LIMIT 1;";
            cmd.Parameters.AddWithValue("$mod", mod);
            var obj = cmd.ExecuteScalar();
            return obj as string ?? string.Empty;
        }
    }

    private void WriteDetailToSqlite(string mod, string json)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return;

        lock (_sqliteLock)
        {
            EnsureSqliteSchema();
            using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO mod_detail(mod,value) VALUES($mod,$value) ON CONFLICT(mod) DO UPDATE SET value=excluded.value;";
            cmd.Parameters.AddWithValue("$mod", mod);
            cmd.Parameters.AddWithValue("$value", json);
            cmd.ExecuteNonQuery();
        }
    }

    private void DeleteDetailFromSqlite(string mod)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return;

        lock (_sqliteLock)
        {
            EnsureSqliteSchema();
            using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM mod_detail WHERE mod=$mod;";
            cmd.Parameters.AddWithValue("$mod", mod);
            cmd.ExecuteNonQuery();
        }
    }

    private void MoveDetailInSqlite(string fromMod, string toMod)
    {
        if (string.IsNullOrWhiteSpace(fromMod) || string.IsNullOrWhiteSpace(toMod))
            return;

        if (string.Equals(fromMod, toMod, StringComparison.OrdinalIgnoreCase))
            return;

        lock (_sqliteLock)
        {
            EnsureSqliteSchema();
            using var conn = new SqliteConnection($"Data Source={_sqlitePath};Pooling=True");
            conn.Open();
            using var tx = conn.BeginTransaction();
            string json = string.Empty;
            using (var read = conn.CreateCommand())
            {
                read.Transaction = tx;
                read.CommandText = "SELECT value FROM mod_detail WHERE mod=$mod LIMIT 1;";
                read.Parameters.AddWithValue("$mod", fromMod);
                json = read.ExecuteScalar() as string ?? string.Empty;
            }

            if (!string.IsNullOrWhiteSpace(json))
            {
                using var upsert = conn.CreateCommand();
                upsert.Transaction = tx;
                upsert.CommandText = "INSERT INTO mod_detail(mod,value) VALUES($mod,$value) ON CONFLICT(mod) DO UPDATE SET value=excluded.value;";
                upsert.Parameters.AddWithValue("$mod", toMod);
                upsert.Parameters.AddWithValue("$value", json);
                upsert.ExecuteNonQuery();
            }

            using var del = conn.CreateCommand();
            del.Transaction = tx;
            del.CommandText = "DELETE FROM mod_detail WHERE mod=$mod;";
            del.Parameters.AddWithValue("$mod", fromMod);
            del.ExecuteNonQuery();
            tx.Commit();
        }
    }

    private static string ComputeModUid(string mod, string? author)
    {
        try
        {
            var s = (mod ?? string.Empty) + "|" + (author ?? string.Empty);
            using var sha = System.Security.Cryptography.SHA256.Create();
            var bytes = System.Text.Encoding.UTF8.GetBytes(s);
            var hash = sha.ComputeHash(bytes);
            return Convert.ToHexString(hash);
        }
        catch { return string.Empty; }
    }

    private static PropertyInfo[] CreateStateFingerprintProperties()
    {
        var props = typeof(ModStateEntry).GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var list = new List<PropertyInfo>(props.Length);
        for (int i = 0; i < props.Length; i++)
        {
            var p = props[i];
            if (p.GetIndexParameters().Length != 0)
                continue;
            if (p.IsDefined(typeof(JsonIgnoreAttribute), inherit: true))
                continue;
            list.Add(p);
        }
        list.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
        return list.ToArray();
    }

    private static string ComputeStateFingerprint(IReadOnlyDictionary<string, ModStateEntry> entries)
    {
        try
        {
            var list = new List<KeyValuePair<string, ModStateEntry>>(entries.Count);
            foreach (var kv in entries)
            {
                if (string.IsNullOrWhiteSpace(kv.Key))
                    continue;
                list.Add(kv);
            }
            list.Sort(static (a, b) => StringComparer.OrdinalIgnoreCase.Compare(a.Key, b.Key));

            using var sha = SHA256.Create();
            for (int i = 0; i < list.Count; i++)
            {
                var keyBytes = Encoding.UTF8.GetBytes(list[i].Key ?? string.Empty);
                sha.TransformBlock(keyBytes, 0, keyBytes.Length, null, 0);
                sha.TransformBlock(s_newLineUtf8, 0, s_newLineUtf8.Length, null, 0);

                var entry = list[i].Value ?? new ModStateEntry { ModFolderName = list[i].Key ?? string.Empty };
                for (int p = 0; p < s_stateFingerprintProperties.Length; p++)
                {
                    var prop = s_stateFingerprintProperties[p];
                    var nameBytes = Encoding.UTF8.GetBytes(prop.Name);
                    sha.TransformBlock(nameBytes, 0, nameBytes.Length, null, 0);
                    sha.TransformBlock(s_newLineUtf8, 0, s_newLineUtf8.Length, null, 0);

                    object? value = null;
                    try { value = prop.GetValue(entry); } catch { value = null; }
                    byte[] valueBytes;
                    try
                    {
                        valueBytes = JsonSerializer.SerializeToUtf8Bytes(value, prop.PropertyType, s_stateFingerprintJsonOptions);
                    }
                    catch
                    {
                        valueBytes = Array.Empty<byte>();
                    }
                    sha.TransformBlock(valueBytes, 0, valueBytes.Length, null, 0);
                    sha.TransformBlock(s_newLineUtf8, 0, s_newLineUtf8.Length, null, 0);
                }
            }
            sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            return Convert.ToHexString(sha.Hash ?? Array.Empty<byte>())[..12].ToLowerInvariant();
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string GetGeneratedBy()
    {
        try
        {
            var asm = typeof(ModStateService).Assembly;
            var ver = asm?.GetName()?.Version?.ToString() ?? string.Empty;
            return string.IsNullOrWhiteSpace(ver) ? "ShrinkU" : ("ShrinkU " + ver);
        }
        catch { return "ShrinkU"; }
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
    public int TotalTextures { get; set; } = 0;
    [JsonIgnore]
    public List<string> TextureFiles { get; set; } = new List<string>();
    public DateTime LastUpdatedUtc { get; set; } = DateTime.MinValue;
    public string ModAbsolutePath { get; set; } = string.Empty;
    public string PenumbraRelativePath { get; set; } = string.Empty;
    public string RelativeModName { get; set; } = string.Empty;
    public string CurrentVersion { get; set; } = string.Empty;
    public string CurrentAuthor { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public List<string> Tags { get; set; } = new List<string>();
    public bool Enabled { get; set; } = false;
    public int Priority { get; set; } = 0;
    public bool UsedInCollection { get; set; } = false;
    [JsonIgnore]
    public List<string> UsedTextureFiles { get; set; } = new List<string>();
    public int UsedTextureCount { get; set; } = 0;
    public long BytesSaved { get; set; } = 0L;
    public List<string> PenumbraPathSegments { get; set; } = new List<string>();
    public string ModUid { get; set; } = string.Empty;
    public ShrinkU.Configuration.ExternalChangeMarker? ExternalChange { get; set; } = null;
    public string LatestZipBackupFileName { get; set; } = string.Empty;
    public string LatestZipBackupVersion { get; set; } = string.Empty;
    public DateTime LatestZipBackupCreatedUtc { get; set; } = DateTime.MinValue;
    public string LatestPmpBackupFileName { get; set; } = string.Empty;
    public string LatestPmpBackupVersion { get; set; } = string.Empty;
    public DateTime LatestPmpBackupCreatedUtc { get; set; } = DateTime.MinValue;
    public LatestBackupInfo? LatestBackup { get; set; } = null;
    public DateTime LastScanUtc { get; set; } = DateTime.MinValue;
    public DateTime LastKnownWriteUtc { get; set; } = DateTime.MinValue;
    public DateTime LastConvertUtc { get; set; } = DateTime.MinValue;
    public DateTime LastRestoreUtc { get; set; } = DateTime.MinValue;
    public string LastChangeReason { get; set; } = string.Empty;
    public DateTime LastChangeUtc { get; set; } = DateTime.MinValue;
    public bool AutoConvertEligible { get; set; } = false;
    public bool AutoRestoreEligible { get; set; } = false;
    public bool Pinned { get; set; } = false;
    public string Notes { get; set; } = string.Empty;
    public bool NeedsRescan { get; set; } = false;
    public bool NeedsRebuild { get; set; } = false;
}

public sealed class ModStateMeta
{
    public int SchemaVersion { get; set; } = 4;
    public string GeneratedBy { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; } = DateTime.MinValue;
    public DateTime LastSavedUtc { get; set; } = DateTime.MinValue;
    public string BackupFolderFingerprint { get; set; } = string.Empty;
    public DateTime BackupFolderFingerprintUtc { get; set; } = DateTime.MinValue;
    public string StateFingerprint { get; set; } = string.Empty;
    public DateTime StateFingerprintUtc { get; set; } = DateTime.MinValue;
    public string PenumbraFolderFingerprint { get; set; } = string.Empty;
    public DateTime PenumbraFolderFingerprintUtc { get; set; } = DateTime.MinValue;
    public string PenumbraRootPath { get; set; } = string.Empty;
}

public sealed class ModStateFile
{
    public ModStateMeta Meta { get; set; } = new ModStateMeta();
    public Dictionary<string, ModStateEntry> Entries { get; set; } = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
}

public sealed class ModStateMigrationProgress
{
    public string Stage { get; set; } = string.Empty;
    public int TotalSteps { get; set; } = 0;
    public int CompletedSteps { get; set; } = 0;
    public int MigratedEntries { get; set; } = 0;
    public int MigratedDetailFiles { get; set; } = 0;
    public int MissingDetailFiles { get; set; } = 0;
    public int DetailReadErrors { get; set; } = 0;
    public bool IsCompleted { get; set; } = false;
    public bool IsSuccess { get; set; } = false;
}

public sealed class ModStateMigrationResult
{
    public bool Success { get; set; } = false;
    public string Message { get; set; } = string.Empty;
    public DateTime StartedUtc { get; set; } = DateTime.MinValue;
    public DateTime FinishedUtc { get; set; } = DateTime.MinValue;
    public TimeSpan Duration { get; set; } = TimeSpan.Zero;
    public int MigratedEntries { get; set; } = 0;
    public int MigratedDetailFiles { get; set; } = 0;
    public int MissingDetailFiles { get; set; } = 0;
    public int DetailReadErrors { get; set; } = 0;
    public string SourceJsonPath { get; set; } = string.Empty;
    public string TargetSqlitePath { get; set; } = string.Empty;
}

public sealed class LatestBackupInfo
{
    public string Type { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; } = DateTime.MinValue;
    public int EntriesCount { get; set; } = 0;
    public long TotalBytes { get; set; } = 0L;
    public DateTime VerifiedUtc { get; set; } = DateTime.MinValue;
    public string VerifiedHash { get; set; } = string.Empty;
}
