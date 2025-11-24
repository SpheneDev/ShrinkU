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
                e.OriginalBytes = originalBytes;
            e.CurrentBytes = currentBytes;
            e.ComparedFiles = comparedFiles;
            e.LastUpdatedUtc = DateTime.UtcNow;
            _lastSaveReason = nameof(UpdateSavings);
            _lastChangedMod = mod;
            try { _logger.LogDebug("[ShrinkU] ModState UpdateSavings: mod={mod} original={orig} current={cur} comparedFiles={cmp}", mod, originalBytes, currentBytes, comparedFiles); } catch { }
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
            bool changed = !string.Equals(e.ModAbsolutePath ?? string.Empty, abs ?? string.Empty, StringComparison.Ordinal)
                || !string.Equals(e.PenumbraRelativePath ?? string.Empty, rel ?? string.Empty, StringComparison.Ordinal)
                || !string.Equals(e.CurrentVersion ?? string.Empty, ver ?? string.Empty, StringComparison.Ordinal)
                || !string.Equals(e.CurrentAuthor ?? string.Empty, auth ?? string.Empty, StringComparison.Ordinal)
                || !string.Equals(e.RelativeModName ?? string.Empty, rname ?? string.Empty, StringComparison.Ordinal);
            if (!changed) return;
            e.ModAbsolutePath = abs ?? string.Empty;
            e.PenumbraRelativePath = rel ?? string.Empty;
            e.CurrentVersion = ver ?? string.Empty;
            e.CurrentAuthor = auth ?? string.Empty;
            e.RelativeModName = rname ?? string.Empty;
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
            if (sameItems && e.UsedInCollection == flag) return;
            e.UsedInCollection = flag;
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
            var json = File.ReadAllText(path);
            var dict = JsonSerializer.Deserialize<Dictionary<string, ModStateEntry>>(json) ?? new();
            lock (_lock) { _state = new Dictionary<string, ModStateEntry>(dict, StringComparer.OrdinalIgnoreCase); }
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
            try { lock (_lock) { _state = new Dictionary<string, ModStateEntry>(StringComparer.OrdinalIgnoreCase); } } catch { }
        }
    }

    private DateTime _lastSaveScheduledUtc = DateTime.MinValue;
    private CancellationTokenSource? _saveDebounceCts;
    private readonly object _saveDebounceLock = new();

    public void Save()
    {
        SaveInternal();
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
                    await Task.Delay(600, token).ConfigureAwait(false);
                    if (token.IsCancellationRequested) return;
                    SaveInternal();
                }
                catch (TaskCanceledException) { }
                catch { }
            });
        }
    }

    private void SaveInternal()
    {
        if (!_savingEnabled) return;
        try
        {
            var trace = PerfTrace.Step(_logger, "ModState Save");
            var path = GetPath();
            var tmp = path + ".tmp";
            try { var dir = Path.GetDirectoryName(path); if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir); } catch { }
            var sw = Stopwatch.StartNew();
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan))
            {
                using var writer = new Utf8JsonWriter(fs, new JsonWriterOptions { Indented = true });
                JsonSerializer.Serialize(writer, _state, new JsonSerializerOptions { WriteIndented = true });
                writer.Flush();
            }
            var moved = false;
            for (int i = 0; i < 8 && !moved; i++)
            {
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
                        try { System.Threading.Thread.Sleep(50); } catch { }
                    }
                }
            }
            if (!moved)
            {
                try
                {
                    using (var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.ReadWrite, 65536, FileOptions.SequentialScan))
                    {
                        using var writer = new Utf8JsonWriter(fs, new JsonWriterOptions { Indented = true });
                        JsonSerializer.Serialize(writer, _state, new JsonSerializerOptions { WriteIndented = true });
                        writer.Flush();
                    }
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
            try { var t = GetPath() + ".tmp"; if (File.Exists(t)) File.Delete(t); } catch { }
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
    public List<string> UsedTextureFiles { get; set; } = new List<string>();
    public string LatestZipBackupFileName { get; set; } = string.Empty;
    public string LatestZipBackupVersion { get; set; } = string.Empty;
    public DateTime LatestZipBackupCreatedUtc { get; set; } = DateTime.MinValue;
    public string LatestPmpBackupFileName { get; set; } = string.Empty;
    public string LatestPmpBackupVersion { get; set; } = string.Empty;
    public DateTime LatestPmpBackupCreatedUtc { get; set; } = DateTime.MinValue;
}
