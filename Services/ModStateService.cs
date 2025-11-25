using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Helpers;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Diagnostics;

namespace ShrinkU.Services;

public sealed class ModStateService
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _config;
    private readonly object _lock = new();
    private Dictionary<string, ModStateEntry> _state = new(StringComparer.OrdinalIgnoreCase);
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

    public ModStateService(ILogger logger, ShrinkUConfigService config, DebugTraceService? debugTrace = null)
    {
        _logger = logger;
        _config = config;
        _debug = debugTrace;
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
        return GetPath();
    }

    public bool ReloadIfChanged()
    {
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

    public void UpdateTextureFiles(string mod, IReadOnlyList<string>? files)
    {
        lock (_lock)
        {
            var e = Get(mod);
            var list = (files ?? Array.Empty<string>()).Where(f => !string.IsNullOrWhiteSpace(f)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            e.TotalTextures = list.Count;
            e.TextureFiles = list;
            e.LastUpdatedUtc = DateTime.UtcNow;
            WriteDetail(mod, list, null);
            _lastSaveReason = nameof(UpdateTextureFiles);
            _lastChangedMod = mod;
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
        }
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
            WriteDetail(mod, null, list);
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

    private void WriteDetail(string mod, List<string>? textures, List<string>? used)
    {
        try
        {
            var trace = PerfTrace.Step(_logger, $"ModState Detail {mod}");
            var dir = GetDetailDir();
            var path = Path.Combine(dir, Sanitize(mod) + ".json");
            List<string> t = textures ?? ReadDetailTextures(mod);
            List<string> u = used ?? ReadDetailUsed(mod);
            var obj = new ModDetail { TextureFiles = t, UsedTextureFiles = u };
            var json = JsonSerializer.Serialize(obj, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(path, json);
            trace.Dispose();
        }
        catch { }
    }

    public List<string> ReadDetailTextures(string mod)
    {
        try
        {
            var dir = GetDetailDir();
            var path = Path.Combine(dir, Sanitize(mod) + ".json");
            if (!File.Exists(path)) return new List<string>();
            var json = File.ReadAllText(path);
            var obj = JsonSerializer.Deserialize<ModDetail>(json) ?? new ModDetail();
            return obj.TextureFiles ?? new List<string>();
        }
        catch { return new List<string>(); }
    }

    public List<string> ReadDetailUsed(string mod)
    {
        try
        {
            var dir = GetDetailDir();
            var path = Path.Combine(dir, Sanitize(mod) + ".json");
            if (!File.Exists(path)) return new List<string>();
            var json = File.ReadAllText(path);
            var obj = JsonSerializer.Deserialize<ModDetail>(json) ?? new ModDetail();
            return obj.UsedTextureFiles ?? new List<string>();
        }
        catch { return new List<string>(); }
    }

    private sealed class ModDetail
    {
        public List<string>? TextureFiles { get; set; }
        public List<string>? UsedTextureFiles { get; set; }
    }

    public void Load()
    {
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
                if (doc.RootElement.ValueKind == JsonValueKind.Object && doc.RootElement.TryGetProperty("entries", out var entriesEl))
                {
                    var file = JsonSerializer.Deserialize<ModStateFile>(json) ?? new ModStateFile { Meta = new ModStateMeta(), Entries = new Dictionary<string, ModStateEntry>() };
                    dict = new Dictionary<string, ModStateEntry>(file.Entries ?? new Dictionary<string, ModStateEntry>(), StringComparer.OrdinalIgnoreCase);
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
                var path = GetPath();
                var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
                var tmp = path + $".tmp.{pid}.{System.Environment.CurrentManagedThreadId}.{DateTime.UtcNow.Ticks}";
                try { var dir = Path.GetDirectoryName(path); if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir); } catch { }
                
                Dictionary<string, ModStateEntry> entriesSnapshot;
                lock (_lock)
                {
                    entriesSnapshot = new Dictionary<string, ModStateEntry>(_state, StringComparer.OrdinalIgnoreCase);
                }

                var sw = Stopwatch.StartNew();
                using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan))
                {
                    using var writer = new Utf8JsonWriter(fs, new JsonWriterOptions { Indented = true });
                    var meta = new ModStateMeta
                    {
                        SchemaVersion = 2,
                        GeneratedBy = GetGeneratedBy(),
                        CreatedUtc = _lastLoadedWriteUtc == DateTime.MinValue ? DateTime.UtcNow : _lastLoadedWriteUtc,
                        LastSavedUtc = DateTime.UtcNow,
                    };
                    var file = new ModStateFile { Meta = meta, Entries = entriesSnapshot };
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
                            var meta2 = new ModStateMeta
                            {
                                SchemaVersion = 2,
                            GeneratedBy = GetGeneratedBy(),
                            CreatedUtc = _lastLoadedWriteUtc == DateTime.MinValue ? DateTime.UtcNow : _lastLoadedWriteUtc,
                            LastSavedUtc = DateTime.UtcNow,
                        };
                        var file2 = new ModStateFile { Meta = meta2, Entries = _state };
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
                try { _logger.LogError(ex, "Failed to save mod_state.json to {path}", GetPath()); } catch { }
                try
                {
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
            ScheduleSave();
            if (!_batching)
                try { OnEntryChanged?.Invoke(mod); } catch { }
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
    public int SchemaVersion { get; set; } = 2;
    public string GeneratedBy { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; } = DateTime.MinValue;
    public DateTime LastSavedUtc { get; set; } = DateTime.MinValue;
}

public sealed class ModStateFile
{
    public ModStateMeta Meta { get; set; } = new ModStateMeta();
    public Dictionary<string, ModStateEntry> Entries { get; set; } = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase);
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
