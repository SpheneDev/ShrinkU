using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace ShrinkU.Services;

public sealed class TextureBackupService
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly PenumbraIpc _penumbraIpc;
    private readonly ModStateService _modStateService;
    private static readonly SemaphoreSlim s_backupLock = new(1, 1);

    public TextureBackupService(ILogger logger, ShrinkUConfigService configService, PenumbraIpc penumbraIpc, ModStateService modStateService)
    {
        _logger = logger;
        _configService = configService;
        _penumbraIpc = penumbraIpc;
        _modStateService = modStateService;
    }

    // Verification result for restore operations
    public sealed class RestoreVerificationResult
    {
        public bool Success { get; set; }
        public string FilePath { get; set; } = string.Empty;
        public string ErrorMessage { get; set; } = string.Empty;
        public long ExpectedSize { get; set; }
        public long ActualSize { get; set; }
        public bool FileExists { get; set; }
        public bool SizeMatches { get; set; }
    }

    // Overview DTOs for UI
    public sealed class BackupEntryInfo
    {
        public string BackupFileName { get; set; } = string.Empty;
        public string OriginalFileName { get; set; } = string.Empty;
        public string OriginalPath { get; set; } = string.Empty;
        public string PrefixedOriginalPath { get; set; } = string.Empty;
        public string ModRelativePath { get; set; } = string.Empty;
        public string? ModFolderName { get; set; }
        public string? ModVersion { get; set; }
        public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
    }

    public sealed class BackupSessionInfo
    {
        public string SourcePath { get; set; } = string.Empty; // zip or session dir
        public bool IsZip { get; set; }
        public string DisplayName { get; set; } = string.Empty;
        public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
        public int EntryCount => Entries.Count;
        public List<BackupEntryInfo> Entries { get; set; } = new();
    }

    private sealed class BackupManifestEntry
    {
        public string OriginalPath { get; set; } = string.Empty;
        public string PrefixedOriginalPath { get; set; } = string.Empty;
        public string BackupFileName { get; set; } = string.Empty;
        public string OriginalFileName { get; set; } = string.Empty;
        public string ModRelativePath { get; set; } = string.Empty;
        public string? ModFolderName { get; set; }
        public string? ModVersion { get; set; }
        public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
    }

    private sealed class BackupManifest
    {
        public List<BackupManifestEntry> Entries { get; set; } = new();
    }

    // Aggregated savings statistics DTO
    public sealed class BackupSavingsStats
    {
        public long OriginalTotalBytes { get; set; }
        public long CurrentTotalBytes { get; set; }
        public int ComparedFiles { get; set; }
        public int MissingCurrentFiles { get; set; }
    }

    // Per-mod savings statistics DTO
    public sealed class ModSavingsStats
    {
        public long OriginalBytes { get; set; }
        public long CurrentBytes { get; set; }
        public int ComparedFiles { get; set; }
    }

    public sealed class OrphanBackupInfo
    {
        public string ModFolderName { get; set; } = string.Empty;
        public int ZipCount { get; set; }
        public int PmpCount { get; set; }
        public long TotalBytes { get; set; }
        public string? LatestPmpPath { get; set; }
    }

    // Try to find the real mod directory path (including nested categories) by matching leaf folder name
    private string? TryFindModDirectory(string modFolder)
    {
        try
        {
            var root = _penumbraIpc.ModDirectory;
            if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(modFolder))
                return null;
            try
            {
                var candidates = new List<string>();
                foreach (var dir in Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories))
                {
                    var name = Path.GetFileName(dir);
                    if (!string.IsNullOrWhiteSpace(name) && string.Equals(name, modFolder, StringComparison.OrdinalIgnoreCase))
                        candidates.Add(dir);
                }
                if (candidates.Count > 0)
                {
                    string best = candidates
                        .OrderByDescending(p => p.Count(c => c == Path.DirectorySeparatorChar || c == Path.AltDirectorySeparatorChar))
                        .ThenByDescending(p => p.Length)
                        .First();
                    return best;
                }
            }
            catch { }
            return null;
        }
        catch
        {
            return null;
        }
    }

    // Resolve absolute path to a mod directory from its folder name
    public string? GetModAbsolutePath(string modFolder)
    {
        try
        {
            var root = _penumbraIpc.ModDirectory;
            if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(modFolder))
                return null;
            var found = TryFindModDirectory(modFolder);
            if (!string.IsNullOrWhiteSpace(found))
                return found;
            return Path.Combine(root, modFolder);
        }
        catch
        {
            return null;
        }
    }

    private string GetModPenumbraRelativePath(string modFolder)
    {
        try
        {
            // Prefer direct IPC call: GetModPath returns the full relative path including categories
            try
            {
                var (ec, fullPath, _, _) = _penumbraIpc.GetModPath(modFolder);
                var p = (fullPath ?? string.Empty).Replace('\\', '/');
                try { _logger.LogDebug("PenumbraRelativePath IPC: mod={mod} ec={ec} path={path}", modFolder, ec, p); } catch { }
                if (ec == Penumbra.Api.Enums.PenumbraApiEc.Success && !string.IsNullOrWhiteSpace(p))
                    return p;
            }
            catch { }
            return string.Empty;
        }
        catch { return string.Empty; }
    }

    // Read mod version from meta.json; returns empty string if unavailable
    private string? GetModVersion(string modFolder)
    {
        try
        {
            var modAbs = GetModAbsolutePath(modFolder);
            if (string.IsNullOrWhiteSpace(modAbs))
                return string.Empty;
            var metaPath = Path.Combine(modAbs!, "meta.json");
            if (!File.Exists(metaPath))
                return string.Empty;
            using var s = File.OpenRead(metaPath);
            using var doc = JsonDocument.Parse(s);
            if (doc.RootElement.TryGetProperty("Version", out var vers) && vers.ValueKind == JsonValueKind.String)
            {
                var v = vers.GetString();
                return string.IsNullOrWhiteSpace(v) ? string.Empty : v;
            }
            if (doc.RootElement.TryGetProperty("FileVersion", out var fvers) && fvers.ValueKind == JsonValueKind.String)
            {
                var v = fvers.GetString();
                return string.IsNullOrWhiteSpace(v) ? string.Empty : v;
            }
            if (doc.RootElement.TryGetProperty("VersionString", out var vstr) && vstr.ValueKind == JsonValueKind.String)
            {
                var v = vstr.GetString();
                return string.IsNullOrWhiteSpace(v) ? string.Empty : v;
            }
            return string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    // Read mod author from meta.json; returns empty string if unavailable
    private string? GetModAuthor(string modFolder)
    {
        try
        {
            var modAbs = GetModAbsolutePath(modFolder);
            if (string.IsNullOrWhiteSpace(modAbs))
                return string.Empty;
            var metaPath = Path.Combine(modAbs!, "meta.json");
            if (!File.Exists(metaPath))
                return string.Empty;
            using var s = File.OpenRead(metaPath);
            using var doc = JsonDocument.Parse(s);
            if (doc.RootElement.TryGetProperty("Author", out var author) && author.ValueKind == JsonValueKind.String)
            {
                var a = author.GetString();
                return string.IsNullOrWhiteSpace(a) ? string.Empty : a;
            }
            return string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    private string BuildPrefixedPath(string absolutePath)
    {
        try
        {
            var result = absolutePath;
            var penDir = _penumbraIpc.ModDirectory ?? string.Empty;
            if (!string.IsNullOrEmpty(penDir))
            {
                result = result.Replace(penDir, penDir.EndsWith('\\') ? "{penumbra}\\" : "{penumbra}", StringComparison.OrdinalIgnoreCase);
            }
            // ShrinkU does not use a cache folder prefix; focus on Penumbra prefix
            while (result.Contains("\\\\", StringComparison.Ordinal))
                result = result.Replace("\\\\", "\\", StringComparison.Ordinal);
            return result;
        }
        catch
        {
            return absolutePath;
        }
    }

    private string? ExtractModFolderNameFromPrefixed(string prefixedPath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(prefixedPath) || !prefixedPath.StartsWith("{penumbra}", StringComparison.OrdinalIgnoreCase))
                return null;
            var root = _penumbraIpc.ModDirectory ?? string.Empty;
            if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
                return null;
            var p = prefixedPath.Substring("{penumbra}".Length).TrimStart('\\', '/');
            var segs = p.Split(new[] { '\\', '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (segs.Length == 0)
                return null;
            for (int i = 0; i < segs.Length; i++)
            {
                var candidate = Path.Combine(root, string.Join(Path.DirectorySeparatorChar, segs.Take(i + 1)));
                var metaPath = Path.Combine(candidate, "meta.json");
                if (File.Exists(metaPath))
                    return segs[i];
            }
            return segs.LastOrDefault();
        }
        catch { return null; }
    }

    private string ResolvePrefixedPath(string prefixedOrAbsolutePath)
    {
        try
        {
            var result = prefixedOrAbsolutePath;
            if (result.StartsWith("{penumbra}", StringComparison.OrdinalIgnoreCase))
            {
                var penDir = _penumbraIpc.ModDirectory ?? string.Empty;
                if (!string.IsNullOrEmpty(penDir))
                    result = result.Replace("{penumbra}", penDir, StringComparison.Ordinal);
            }
            // No cache prefix resolution required for ShrinkU
            return result.Replace('/', '\\');
        }
        catch
        {
            return prefixedOrAbsolutePath;
        }
    }

    public Task<List<string>> GetPmpBackupsForModAsync(string modFolderName)
    {
        var result = new List<string>();
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || !Directory.Exists(backupDirectory))
                return Task.FromResult(result);

            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir))
                return Task.FromResult(result);

            result = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp")
                               .OrderByDescending(f => f)
                               .ToList();
        }
        catch { }
        return Task.FromResult(result);
    }

    public Task<(string version, string author, DateTime createdUtc, string pmpFileName)?> GetLatestPmpManifestForModAsync(string modFolderName)
    {
        try
        {
            var e = _modStateService.Get(modFolderName);
            if (string.IsNullOrWhiteSpace(e.LatestPmpBackupFileName))
                return Task.FromResult<(string, string, DateTime, string)?>(null);
            var version = e.LatestPmpBackupVersion ?? string.Empty;
            var author = e.CurrentAuthor ?? string.Empty;
            var created = e.LatestPmpBackupCreatedUtc;
            var name = e.LatestPmpBackupFileName ?? string.Empty;
            return Task.FromResult<(string, string, DateTime, string)?>((version, author, created, name));
        }
        catch { return Task.FromResult<(string, string, DateTime, string)?>(null); }
    }

    // Check whether a mod has any full-mod PMP backup
    public Task<bool> HasPmpBackupForModAsync(string modFolderName)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || !Directory.Exists(backupDirectory))
                return Task.FromResult(false);
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir))
                return Task.FromResult(false);
            var any = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").Any();
            return Task.FromResult(any);
        }
        catch { return Task.FromResult(false); }
    }

    public async Task RefreshAllBackupStateAsync()
    {
        try
        {
            var trace = PerfTrace.Step(_logger, "RefreshAllBackupState total");
            _modStateService.BeginBatch();
            Dictionary<string, string> modPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                modPaths = await _penumbraIpc.GetModPathsAsync().ConfigureAwait(false);
                if ((modPaths == null || modPaths.Count == 0) && _penumbraIpc.APIAvailable)
                {
                    for (var i = 0; i < 5 && (modPaths == null || modPaths.Count == 0); i++)
                    {
                        await Task.Delay(200).ConfigureAwait(false);
                        try { modPaths = await _penumbraIpc.GetModPathsAsync().ConfigureAwait(false); } catch { }
                    }
                }
                try
                {
                    foreach (var kv in modPaths)
                    {
                        var rp = (kv.Value ?? string.Empty).Replace('\\', '/').TrimEnd('/');
                        _logger.LogDebug("PenumbraModPath mapping: {dir} -> {path}", kv.Key, rp);
                    }
                }
                catch { }
            }
            catch { }
            Dictionary<string, string> displayByMod = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var list = _penumbraIpc.GetModList();
                foreach (var kv in list)
                {
                    var dir = kv.Key ?? string.Empty;
                    var name = kv.Value ?? string.Empty;
                    if (!string.IsNullOrWhiteSpace(dir))
                        displayByMod[dir] = name;
                }
            }
            catch { }
            var mods = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                foreach (var kv in _modStateService.Snapshot())
                    mods.Add(kv.Key);
            }
            catch { }

            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (!string.IsNullOrWhiteSpace(backupDirectory) && Directory.Exists(backupDirectory))
                {
                    foreach (var dir in Directory.EnumerateDirectories(backupDirectory, "*", SearchOption.TopDirectoryOnly))
                    {
                        var name = Path.GetFileName(dir);
                        if (string.IsNullOrWhiteSpace(name)) continue;
                        if (name.Equals("mod_state", StringComparison.OrdinalIgnoreCase)) continue;
                        if (name.StartsWith("session_", StringComparison.OrdinalIgnoreCase)) continue;
                        mods.Add(name);
                    }
                    foreach (var session in Directory.EnumerateDirectories(backupDirectory, "session_*", SearchOption.TopDirectoryOnly))
                    {
                        foreach (var modSub in Directory.EnumerateDirectories(session, "*", SearchOption.TopDirectoryOnly))
                        {
                            var name = Path.GetFileName(modSub);
                            if (!string.IsNullOrWhiteSpace(name)) mods.Add(name);
                        }
                    }
                }
            }
            catch { }

            // Include all mods known to Penumbra so entries exist even without backups
            try
            {
                var list = _penumbraIpc.GetModList();
                foreach (var dir in list.Keys)
                {
                    if (!string.IsNullOrWhiteSpace(dir))
                        mods.Add(dir);
                }
            }
            catch { }

            var indexTrace = PerfTrace.Step(_logger, "RefreshAllBackupState index");
            var latestZipByMod = new Dictionary<string, (string file, DateTime created)>(StringComparer.OrdinalIgnoreCase);
            var latestPmpByMod = new Dictionary<string, (string file, DateTime created)>(StringComparer.OrdinalIgnoreCase);
            var modsWithZip = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var modsWithPmp = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var modsInSessions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (!string.IsNullOrWhiteSpace(backupDirectory) && Directory.Exists(backupDirectory))
                {
                    foreach (var dir in Directory.EnumerateDirectories(backupDirectory, "*", SearchOption.TopDirectoryOnly))
                    {
                        var modName = Path.GetFileName(dir);
                        if (string.IsNullOrWhiteSpace(modName)) continue;
                        if (modName.StartsWith("session_", StringComparison.OrdinalIgnoreCase)) continue;
                        try
                        {
                            var latestZip = Directory.EnumerateFiles(dir, "backup_*.zip").OrderByDescending(f => f).FirstOrDefault();
                            if (!string.IsNullOrEmpty(latestZip))
                            {
                                var created = File.GetCreationTimeUtc(latestZip);
                                latestZipByMod[modName] = (System.IO.Path.GetFileName(latestZip), created);
                                modsWithZip.Add(modName);
                            }
                        }
                        catch { }
                        try
                        {
                            var latestPmp = Directory.EnumerateFiles(dir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
                            if (!string.IsNullOrEmpty(latestPmp))
                            {
                                var created = File.GetCreationTimeUtc(latestPmp);
                                latestPmpByMod[modName] = (System.IO.Path.GetFileName(latestPmp), created);
                                modsWithPmp.Add(modName);
                            }
                        }
                        catch { }
                    }
                    foreach (var session in Directory.EnumerateDirectories(backupDirectory, "session_*", SearchOption.TopDirectoryOnly))
                    {
                        try
                        {
                            foreach (var modSub in Directory.EnumerateDirectories(session, "*", SearchOption.TopDirectoryOnly))
                            {
                                var name = Path.GetFileName(modSub);
                                if (string.IsNullOrWhiteSpace(name)) continue;
                                var manifestPath = Path.Combine(modSub, "manifest.json");
                                if (File.Exists(manifestPath))
                                    modsInSessions.Add(name);
                            }
                        }
                        catch { }
                    }
                }
            }
            catch { }
            indexTrace.Dispose();

            var metaTrace = PerfTrace.Step(_logger, "RefreshAllBackupState meta-index");
            var versionByMod = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var authorByMod = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var root = _penumbraIpc.ModDirectory ?? string.Empty;
                foreach (var mod in mods)
                {
                    var absForMeta = string.IsNullOrWhiteSpace(root) ? string.Empty : System.IO.Path.Combine(root, mod);
                    if (string.IsNullOrWhiteSpace(absForMeta) || !Directory.Exists(absForMeta))
                        continue;
                    var metaPath = System.IO.Path.Combine(absForMeta, "meta.json");
                    if (!File.Exists(metaPath))
                        continue;
                    try
                    {
                        using var fs = new FileStream(metaPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
                        using var doc = JsonDocument.Parse(fs);
                        if (doc.RootElement.TryGetProperty("Version", out var vers) && vers.ValueKind == JsonValueKind.String)
                        {
                            var v = vers.GetString() ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(v)) versionByMod[mod] = v;
                        }
                        else if (doc.RootElement.TryGetProperty("FileVersion", out var fvers) && fvers.ValueKind == JsonValueKind.String)
                        {
                            var v = fvers.GetString() ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(v)) versionByMod[mod] = v;
                        }
                        else if (doc.RootElement.TryGetProperty("VersionString", out var vstr) && vstr.ValueKind == JsonValueKind.String)
                        {
                            var v = vstr.GetString() ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(v)) versionByMod[mod] = v;
                        }
                        if (doc.RootElement.TryGetProperty("Author", out var author) && author.ValueKind == JsonValueKind.String)
                        {
                            var a = author.GetString() ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(a)) authorByMod[mod] = a;
                        }
                    }
                    catch { }
                }
            }
            catch { }
            metaTrace.Dispose();

            var concurrency = Math.Min(4, Environment.ProcessorCount);
            using var gate = new SemaphoreSlim(concurrency, concurrency);
            var tasks = new List<Task>(mods.Count);
            foreach (var mod in mods)
            {
                tasks.Add(Task.Run(async () =>
                {
                    await gate.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        await Task.Yield();
                        var sw = Stopwatch.StartNew();
                        var modTrace = PerfTrace.Step(_logger, "RefreshAllBackupState mod " + mod);
                        var hasTex = modsWithZip.Contains(mod) || modsInSessions.Contains(mod);
                        var hasPmp = modsWithPmp.Contains(mod);
                        _modStateService.UpdateBackupFlags(mod, hasTex, hasPmp);
                        var abs = string.Empty;
                        try
                        {
                            var root = _penumbraIpc.ModDirectory ?? string.Empty;
                            if (!string.IsNullOrWhiteSpace(root))
                                abs = System.IO.Path.Combine(root, mod);
                            if (string.IsNullOrWhiteSpace(abs))
                                abs = GetModAbsolutePath(mod) ?? string.Empty;
                        }
                        catch { abs = GetModAbsolutePath(mod) ?? string.Empty; }
                        var rel = string.Empty;
                        try { rel = GetModPenumbraRelativePath(mod); } catch { rel = string.Empty; }
                        var existingForMod = _modStateService.Get(mod);
                        var ver = versionByMod.TryGetValue(mod, out var vv) ? vv : (existingForMod.CurrentVersion ?? string.Empty);
                        var auth = authorByMod.TryGetValue(mod, out var aa) ? aa : (existingForMod.CurrentAuthor ?? string.Empty);
                        _modStateService.UpdateCurrentModInfo(mod, abs, rel, ver, auth);
                        try
                        {
                            var dn = displayByMod.TryGetValue(mod, out var disp) ? disp : string.Empty;
                            if (!string.IsNullOrWhiteSpace(dn))
                                _modStateService.UpdateDisplayAndTags(mod, dn, Array.Empty<string>());
                        }
                        catch { }
                        string zipName = string.Empty, zipVer = string.Empty;
                        DateTime zipCreated = DateTime.MinValue;
                        string pmpName = string.Empty, pmpVer = string.Empty;
                        DateTime pmpCreated = DateTime.MinValue;
                        try
                        {
                            if (latestZipByMod.TryGetValue(mod, out var zi))
                            {
                                zipName = zi.file;
                                zipCreated = zi.created;
                            }
                        }
                        catch { }
                        try
                        {
                            if (latestPmpByMod.TryGetValue(mod, out var pi))
                            {
                                pmpName = pi.file;
                                pmpCreated = pi.created;
                                pmpVer = _modStateService.Get(mod).LatestPmpBackupVersion;
                            }
                        }
                        catch { }
                        _modStateService.UpdateLatestBackupsInfo(mod, zipName, zipVer, zipCreated, pmpName, pmpVer, pmpCreated);
                        var ms = (int)Math.Round(sw.Elapsed.TotalMilliseconds);
                        if (ms > 1000)
                        {
                            try { _logger.LogDebug("RefreshAllBackupState mod {mod} took {ms}ms", mod, ms); } catch { }
                        }
                        modTrace.Dispose();
                    }
                    catch { }
                    finally
                    {
                        try { gate.Release(); } catch { }
                    }
                }));
            }
            try { await Task.WhenAll(tasks).ConfigureAwait(false); } catch { }
            _modStateService.EndBatch();
            trace.Dispose();
        }
        catch { }
    }

    public async Task<bool> CreateFullModBackupAsync(string modFolderName, IProgress<(string,int,int)>? progress, CancellationToken token)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            var modAbs = GetModAbsolutePath(modFolderName);
            if (string.IsNullOrWhiteSpace(backupDirectory) || string.IsNullOrWhiteSpace(modAbs) || !Directory.Exists(modAbs))
            {
                try { _logger.LogDebug("CreateFullModBackup aborted: invalid paths for {mod}", modFolderName); } catch { }
                return false;
            }

            try { Directory.CreateDirectory(backupDirectory); } catch { }
            var modBackupDir = Path.Combine(backupDirectory, modFolderName);
            try { Directory.CreateDirectory(modBackupDir); } catch { }

            // Clean old PMPs to keep latest only
            try
            {
                foreach (var p in Directory.EnumerateFiles(modBackupDir, "mod_backup_*.pmp"))
                {
                    try { File.Delete(p); } catch { }
                }
            }
            catch { }

            var stamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
            var pmpPath = Path.Combine(modBackupDir, $"mod_backup_{stamp}.pmp");
            await s_backupLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                await Task.Run(() =>
                {
                    if (token.IsCancellationRequested) return;
                    ZipFile.CreateFromDirectory(modAbs!, pmpPath, CompressionLevel.Fastest, false);
                }, token).ConfigureAwait(false);
            }
            finally
            {
                try { s_backupLock.Release(); } catch { }
            }

            try { progress?.Report((pmpPath, 1, 1)); } catch { }
            try { _logger.LogDebug("Created manual full mod PMP backup {pmp} for {mod}", pmpPath, modFolderName); } catch { }

            try { _modStateService.UpdateBackupFlags(modFolderName, _modStateService.Get(modFolderName).HasTextureBackup, true); } catch { }
            return true;
        }
        catch (Exception ex)
        {
            try { _logger.LogDebug(ex, "Failed to create manual full mod backup for {mod}", modFolderName); } catch { }
            return false;
        }
    }
    // Restore a specific full-mod PMP backup by replacing the mod directory contents
    public async Task<bool> RestorePmpAsync(string modFolderName, string pmpPath, IProgress<(string, int, int)>? progress, CancellationToken token, bool cleanupBackupsAfterRestore = true, bool deregisterDuringRestore = false)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(modFolderName) || string.IsNullOrWhiteSpace(pmpPath) || !File.Exists(pmpPath))
            {
                _logger.LogDebug("RestorePmp aborted: invalid args or file missing for {mod}, path={path}", modFolderName, pmpPath);
                return false;
            }

            var modAbs = GetModAbsolutePath(modFolderName);
            if (string.IsNullOrWhiteSpace(modAbs))
            {
                _logger.LogDebug("RestorePmp aborted: could not resolve mod absolute path for {mod}. Penumbra ModDirectory missing?", modFolderName);
                return false;
            }

            var tempDir = Path.Combine(Path.GetTempPath(), "ShrinkU", "restore-pmp", Path.GetFileNameWithoutExtension(pmpPath));
            var originalPathInfo = _penumbraIpc.GetModPath(modFolderName);
            try { if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true); } catch { }
            Directory.CreateDirectory(tempDir);

            // Step 1: extract archive
            await Task.Run(() =>
            {
                if (token.IsCancellationRequested) return;
                ZipFile.ExtractToDirectory(pmpPath, tempDir, overwriteFiles: true);
            }, token).ConfigureAwait(false);
            progress?.Report(($"Extracted {Path.GetFileName(pmpPath)}", 1, 2));

            var parentDir = Path.GetDirectoryName(modAbs!) ?? string.Empty;
            if (string.IsNullOrEmpty(parentDir))
                parentDir = _penumbraIpc.ModDirectory ?? string.Empty;
            if (string.IsNullOrEmpty(parentDir))
            {
                _logger.LogDebug("RestorePmp aborted: parent directory not resolved for {mod}. Penumbra ModDirectory is empty.", modFolderName);
                return false;
            }

            var backupOldDir = Path.Combine(parentDir, modFolderName + $".__restore_old__{DateTime.Now:yyyyMMdd_HHmmss}");
            bool movedOld = false;
            bool backupCreated = false;
            if (deregisterDuringRestore)
            {
                try
                {
                    try { await Task.Delay(300, token).ConfigureAwait(false); } catch { }
                    try { _penumbraIpc.ClosePenumbraWindow(); } catch { }
                    try { _penumbraIpc.RemoveModDirectory(modFolderName); } catch { }
                    try { await _penumbraIpc.WaitForModDeletedAsync(modFolderName, 5000).ConfigureAwait(false); } catch { }
                    if (Directory.Exists(modAbs))
                    {
                        Directory.Move(modAbs!, backupOldDir);
                        movedOld = true;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to move existing mod dir {dir}: {error}", modAbs, ex.Message);
                }

                try { Directory.CreateDirectory(modAbs!); } catch { }

                try
                {
                    await Task.Run(() =>
                    {
                        if (token.IsCancellationRequested) return;
                        CopyDirectoryRecursive(tempDir, modAbs!);
                    }, token).ConfigureAwait(false);
                    progress?.Report(($"Restored {modFolderName}", 2, 2));
                }
                catch (Exception ex)
                {
                    _logger.LogError("Failed to restore PMP for {mod}: {error}", modFolderName, ex.Message);
                    try { if (Directory.Exists(modAbs)) Directory.Delete(modAbs, true); } catch { }
                    if (movedOld)
                    {
                        try { Directory.Move(backupOldDir, modAbs!); } catch { }
                    }
                    return false;
                }
            }
            else
            {
                try
                {
                    if (Directory.Exists(modAbs))
                    {
                        try { Directory.CreateDirectory(backupOldDir); } catch { }
                        await Task.Run(() =>
                        {
                            if (token.IsCancellationRequested) return;
                            CopyDirectoryRecursive(modAbs!, backupOldDir);
                        }, token).ConfigureAwait(false);
                        backupCreated = true;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to backup existing mod dir {dir}: {error}", modAbs, ex.Message);
                }

                try
                {
                    try { Directory.CreateDirectory(modAbs!); } catch { }
                    await Task.Run(() =>
                    {
                        if (token.IsCancellationRequested) return;
                        ClearDirectory(modAbs!);
                        CopyDirectoryRecursive(tempDir, modAbs!);
                    }, token).ConfigureAwait(false);
                    progress?.Report(($"Restored {modFolderName}", 2, 2));
                }
                catch (Exception ex)
                {
                    _logger.LogError("Failed to restore PMP for {mod}: {error}", modFolderName, ex.Message);
                    if (backupCreated)
                    {
                        try
                        {
                            await Task.Run(() =>
                            {
                                ClearDirectory(modAbs!);
                                CopyDirectoryRecursive(backupOldDir, modAbs!);
                            }, token).ConfigureAwait(false);
                        }
                        catch { }
                    }
                    return false;
                }
            }

            // Cleanup temp and old backup dir
            try { Directory.Delete(tempDir, true); } catch { }
            if (movedOld || backupCreated)
            {
                try { Directory.Delete(backupOldDir, true); } catch { }
            }

            _logger.LogDebug("Restored full mod from PMP {pmp} to {mod}", pmpPath, modAbs);

            if (cleanupBackupsAfterRestore)
            {
                // After a successful PMP restore, remove normal texture backups and PMP files for this mod to free space
                try
                {
                    var backupDirectory = _configService.Current.BackupFolderPath;
                    if (!string.IsNullOrWhiteSpace(backupDirectory) && Directory.Exists(backupDirectory))
                    {
                        var modBackupDir = Path.Combine(backupDirectory, modFolderName);
                        if (Directory.Exists(modBackupDir))
                        {
                            // Delete all per-mod texture backup zips
                            foreach (var zip in Directory.EnumerateFiles(modBackupDir, "backup_*.zip"))
                            {
                                try
                                {
                                    File.Delete(zip);
                                    _logger.LogDebug("Deleted texture backup zip {zip} for {mod}", zip, modFolderName);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogWarning("Failed to delete texture backup zip {zip} for {mod}: {error}", zip, modFolderName, ex.Message);
                                }
                            }

                            try
                            {
                                var manifest = Path.Combine(modBackupDir, "pmp_converted_manifest.json");
                                if (File.Exists(manifest))
                                    File.Delete(manifest);
                            }
                            catch { }

                            // Delete all PMP archives for this mod
                            foreach (var p in Directory.EnumerateFiles(modBackupDir, "mod_backup_*.pmp"))
                            {
                                try
                                {
                                    File.Delete(p);
                                    _logger.LogDebug("Deleted PMP archive {pmp} for {mod}", p, modFolderName);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogWarning("Failed to delete PMP archive {pmp} for {mod}: {error}", p, modFolderName, ex.Message);
                                }
                            }

                            // Remove mod backup folder if it became empty
                            try
                            {
                                if (!Directory.EnumerateFileSystemEntries(modBackupDir).Any())
                                {
                                    Directory.Delete(modBackupDir, true);
                                    _logger.LogDebug("Deleted empty mod backup folder {dir}", modBackupDir);
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning("Failed to delete empty mod backup folder {dir}: {error}", modBackupDir, ex.Message);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Post-restore cleanup encountered an error for {mod}: {error}", modFolderName, ex.Message);
                }
            }
            if (deregisterDuringRestore)
            {
                try { _penumbraIpc.AddModDirectory(modFolderName); } catch { }
                try { await _penumbraIpc.WaitForModAddedAsync(modFolderName, 5000).ConfigureAwait(false); } catch { }
                try
                {
                    var desiredPath = originalPathInfo.FullPath;
                    if (!string.IsNullOrWhiteSpace(desiredPath))
                    {
                        _penumbraIpc.SetModPath(modFolderName, desiredPath);
                        await _penumbraIpc.WaitForModPathAsync(modFolderName, desiredPath, 5000).ConfigureAwait(false);
                    }
                }
                catch { }
            }
            try { await Task.Delay(400, token).ConfigureAwait(false); } catch { }
            try { _penumbraIpc.NudgeModDetection(modFolderName); } catch { }
            try { await Task.Delay(800, token).ConfigureAwait(false); } catch { }
            if (deregisterDuringRestore)
            {
                try { _penumbraIpc.OpenModInPenumbra(modFolderName, null); } catch { }
            }
            try
            {
                var hasTex = await HasBackupForModAsync(modFolderName).ConfigureAwait(false);
                var hasPmp = await HasPmpBackupForModAsync(modFolderName).ConfigureAwait(false);
                _modStateService.UpdateBackupFlags(modFolderName, hasTex, hasPmp);
            }
            catch { }
            if (deregisterDuringRestore)
            {
                try { _modStateService.UpdateInstalledButNotConverted(modFolderName, true); } catch { }
            }
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("RestorePmp failed for {mod}: unexpected error: {error}", modFolderName, ex.Message);
            return false;
        }
    }

    private static void ClearDirectory(string dir)
    {
        if (!Directory.Exists(dir)) return;
        foreach (var file in Directory.EnumerateFiles(dir, "*", SearchOption.TopDirectoryOnly))
        {
            try { File.Delete(file); } catch { }
        }
        foreach (var sub in Directory.EnumerateDirectories(dir, "*", SearchOption.TopDirectoryOnly))
        {
            try { Directory.Delete(sub, true); } catch { }
        }
    }

    private static void CopyDirectoryRecursive(string sourceDir, string targetDir)
    {
        foreach (var dir in Directory.EnumerateDirectories(sourceDir, "*", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(sourceDir, dir);
            var dest = Path.Combine(targetDir, rel);
            Directory.CreateDirectory(dest);
        }
        foreach (var file in Directory.EnumerateFiles(sourceDir, "*", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(sourceDir, file);
            var dest = Path.Combine(targetDir, rel);
            var destDir = Path.GetDirectoryName(dest);
            if (!string.IsNullOrEmpty(destDir)) Directory.CreateDirectory(destDir);
            File.Copy(file, dest, overwrite: true);
        }
    }

    // Delete old backups for a mod when its version changes
    private void DeleteOldBackupsForMod(string modFolderName, string currentVersion)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || !Directory.Exists(backupDirectory))
                return;

            // Delete outdated per-mod ZIPs
            var modZipDir = Path.Combine(backupDirectory, modFolderName);
            if (Directory.Exists(modZipDir))
            {
                foreach (var zip in Directory.EnumerateFiles(modZipDir, "backup_*.zip"))
                {
                    try
                    {
                        using var za = ZipFile.OpenRead(zip);
                        var manifestEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, "manifest.json", StringComparison.OrdinalIgnoreCase));
                        if (manifestEntry == null)
                            continue;
                        using var ms = new MemoryStream();
                        using (var zs = manifestEntry.Open()) zs.CopyTo(ms);
                        ms.Position = 0;
                        var manifest = JsonSerializer.Deserialize<BackupManifest>(ms.ToArray());
                        if (manifest == null || manifest.Entries.Count == 0)
                            continue;
                        // If any entry has a different version, delete this ZIP
                        var anyDifferent = manifest.Entries.Any(e => !string.Equals(e.ModVersion ?? string.Empty, currentVersion ?? string.Empty, StringComparison.OrdinalIgnoreCase));
                        if (anyDifferent)
                        {
                            try { File.Delete(zip); _logger.LogDebug("Deleted outdated mod backup ZIP {zip}", zip); } catch { }
                        }
                    }
                    catch { }
                }
                // If mod folder becomes empty, remove it
                try
                {
                    if (!Directory.EnumerateFileSystemEntries(modZipDir).Any())
                    {
                        Directory.Delete(modZipDir, true);
                        _logger.LogDebug("Deleted empty mod backup folder {dir}", modZipDir);
                    }
                }
                catch { }
            }

            // Delete outdated session subfolders for this mod
            foreach (var sessionDir in Directory.EnumerateDirectories(backupDirectory, "session_*"))
            {
                try
                {
                    var modSub = Path.Combine(sessionDir, modFolderName);
                    var manifestPath = Path.Combine(modSub, "manifest.json");
                    if (!Directory.Exists(modSub) || !File.Exists(manifestPath))
                        continue;
                    var manifest = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(manifestPath));
                    if (manifest == null || manifest.Entries.Count == 0)
                        continue;
                    var anyDifferent = manifest.Entries.Any(e => !string.Equals(e.ModVersion ?? string.Empty, currentVersion ?? string.Empty, StringComparison.OrdinalIgnoreCase));
                    if (anyDifferent)
                    {
                        try
                        {
                            Directory.Delete(modSub, true);
                            _logger.LogDebug("Deleted outdated mod session folder {dir}", modSub);
                            // If the session becomes empty, remove it too
                            if (!Directory.EnumerateFileSystemEntries(sessionDir).Any())
                            {
                                Directory.Delete(sessionDir, true);
                                _logger.LogDebug("Deleted empty session folder {dir}", sessionDir);
                            }
                        }
                        catch { }
                    }
                }
                catch { }
            }
        }
        catch { }
    }

    public async Task BackupAsync(Dictionary<string, string[]> textures, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        var traceTotal = PerfTrace.Step(_logger, "BackupAsync total");
        // Build index of already backed up files grouped by mod and version
        var existingByModVersion = new Dictionary<string, Dictionary<string, HashSet<string>>>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var traceExisting = PerfTrace.Step(_logger, "BackupAsync build existing index");
            var overview = await GetBackupOverviewAsync().ConfigureAwait(false);
            foreach (var session in overview)
            {
                foreach (var e in session.Entries)
                {
                    var mod = e.ModFolderName;
                    if (string.IsNullOrWhiteSpace(mod) && !string.IsNullOrWhiteSpace(e.PrefixedOriginalPath))
                        mod = ExtractModFolderNameFromPrefixed(e.PrefixedOriginalPath);
                    if (string.IsNullOrWhiteSpace(mod))
                        continue;

                    var version = e.ModVersion ?? string.Empty;

                    var key = !string.IsNullOrEmpty(e.PrefixedOriginalPath) ? e.PrefixedOriginalPath : e.OriginalPath;
                    if (string.IsNullOrWhiteSpace(key))
                        continue;

                    if (!existingByModVersion.TryGetValue(mod, out var byVersion))
                    {
                        byVersion = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                        existingByModVersion[mod] = byVersion;
                    }
                    if (!byVersion.TryGetValue(version, out var set))
                    {
                        set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        byVersion[version] = set;
                    }
                    set.Add(key);
                }
            }
            traceExisting.Dispose();
        }
        catch { }

        var backupDirectory = _configService.Current.BackupFolderPath;
        var timestamp = DateTime.Now;
        var sessionName = $"session_{timestamp:yyyyMMdd_HHmmss}";
        var sessionDir = Path.Combine(backupDirectory, sessionName);

        try
        {
            Directory.CreateDirectory(backupDirectory);
            if (_configService.Current.EnableBackupBeforeConversion)
                Directory.CreateDirectory(sessionDir);
        }
        catch
        {
            // Ignore directory creation issues
        }

        int current = 0;
        // Precompute expected totals: textures to back up + per-mod ZIPs + per-mod PMP archives
        // Build a list of textures that actually need backing up (skip already-backed ones)
        var toBackup = new List<(string source, string modName, string modVersion)>();
        var modsTouched = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var modsTouchedAll = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            foreach (var kvp in textures)
            {
                var source = kvp.Key;
                var prefixed = BuildPrefixedPath(source);
                var modName = ExtractModFolderNameFromPrefixed(prefixed) ?? "_unknown";
                var currentModVersion = GetModVersion(modName) ?? string.Empty;

                // Skip backing up files that have already been backed up for this mod+version
                var currentKey = !string.IsNullOrEmpty(prefixed) ? prefixed : source;
                if (!string.IsNullOrWhiteSpace(modName)
                    && existingByModVersion.TryGetValue(modName, out var byVersion)
                    && byVersion.TryGetValue(currentModVersion, out var set)
                    && set.Contains(currentKey))
                {
                    continue;
                }

                toBackup.Add((source, modName, currentModVersion));
                if (!string.IsNullOrWhiteSpace(modName))
                {
                    modsTouched.Add(modName);
                }
            }
        }
        catch { }

        // Always record all mods represented by the provided texture dictionary
        try
        {
            foreach (var kvp in textures)
            {
                var prefixed = BuildPrefixedPath(kvp.Key);
                var modNameForAll = ExtractModFolderNameFromPrefixed(prefixed);
                if (!string.IsNullOrWhiteSpace(modNameForAll))
                    modsTouchedAll.Add(modNameForAll);
            }
        }
        catch { }

        int zipCount = 0;
        int pmpCount = 0;
        try
        {
            if (_configService.Current.EnableZipCompressionForBackups && _configService.Current.EnableBackupBeforeConversion)
            {
                zipCount = modsTouched.Count;
            }
            if (_configService.Current.EnableFullModBackupBeforeConversion)
            {
                foreach (var mod in modsTouchedAll)
                {
                    PerfStep tracePmp = default;
                    try
                    {
                        tracePmp = PerfTrace.Step(_logger, $"PMP {mod}");
                        var modAbs = GetModAbsolutePath(mod);
                        if (string.IsNullOrWhiteSpace(modAbs) || !Directory.Exists(modAbs))
                            continue;
                        var modBackupDir = Path.Combine(backupDirectory, mod);
                        var currentVersion = _modStateService.Get(mod).CurrentVersion ?? string.Empty;
                        var latestPmp = Directory.Exists(modBackupDir) ? Directory.EnumerateFiles(modBackupDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault() : null;
                        if (string.IsNullOrEmpty(latestPmp))
                        {
                            try { _logger.LogDebug("No existing PMP for {mod}; scheduling creation", mod); } catch { }
                            pmpCount++;
                        }
                        else
                        {
                            var latestVersion = _modStateService.Get(mod).LatestPmpBackupVersion ?? string.Empty;
                            var sameVersion = string.Equals(latestVersion, currentVersion ?? string.Empty, StringComparison.OrdinalIgnoreCase);
                            try { _logger.LogDebug("PMP decision for {mod}: currentVersion={ver}, latestPmp={pmp}, metaVersion={mver}, sameVersion={same}", mod, currentVersion, latestPmp, latestVersion, sameVersion); } catch { }
                            if (!sameVersion)
                                pmpCount++;
                        }
                    }
                    catch { }
                }
            }
        }
        catch { }

        var expectedTotal = (_configService.Current.EnableBackupBeforeConversion ? (toBackup.Count + zipCount) : 0)
            + (_configService.Current.EnableFullModBackupBeforeConversion ? pmpCount : 0);
        if (expectedTotal <= 0)
        {
            // Fallback to textures.Count for total when nothing is scheduled
            expectedTotal = textures.Count;
        }
        var manifest = new BackupManifest { Entries = new List<BackupManifestEntry>() };
        var entriesByMod = new Dictionary<string, List<BackupManifestEntry>>(StringComparer.OrdinalIgnoreCase);
        // Perform backups for the computed list of textures to back up (only when enabled)
        if (_configService.Current.EnableBackupBeforeConversion)
        {
        foreach (var kvp in textures)
        {
            if (token.IsCancellationRequested) break;
            var source = kvp.Key;
            var prefixed = BuildPrefixedPath(source);
            var modName = ExtractModFolderNameFromPrefixed(prefixed) ?? "_unknown";
            var currentModVersion = GetModVersion(modName) ?? string.Empty;
            // Optionally delete old backups when a version change is detected
            try
            {
                if (_configService.Current.DeleteOldBackupsOnVersionChange && !string.IsNullOrWhiteSpace(modName))
                {
                    DeleteOldBackupsForMod(modName, currentModVersion);
                }
            }
            catch { }
            // Skip backing up files that have already been backed up in any previous session
            try
            {
                var currentKey = !string.IsNullOrEmpty(prefixed) ? prefixed : source;
                if (!string.IsNullOrWhiteSpace(modName)
                    && existingByModVersion.TryGetValue(modName, out var byVersion)
                    && byVersion.TryGetValue(currentModVersion, out var set)
                    && set.Contains(currentKey))
                {
                    _logger.LogDebug("Skipping backup for already backed up texture {path}", source);
                    continue;
                }
            }
            catch { }
            var modDirInSession = Path.Combine(sessionDir, modName);
            try { Directory.CreateDirectory(modDirInSession); } catch { }
            // Determine path inside the mod by computing relative path from the mod root
            string modRelativePath = Path.GetFileName(source);
            try
            {
                var modAbsolute = GetModAbsolutePath(modName);
                if (!string.IsNullOrWhiteSpace(modAbsolute))
                {
                    var rel = Path.GetRelativePath(modAbsolute, source);
                    if (!string.IsNullOrWhiteSpace(rel) && !rel.StartsWith("..", StringComparison.Ordinal))
                        modRelativePath = rel.Replace('/', '\\');
                }
            }
            catch { }

            var target = Path.Combine(modDirInSession, modRelativePath);
            PerfStep traceCopy = default;
            try
            {
                traceCopy = PerfTrace.Step(_logger, $"Backup copy {Path.GetFileName(source)}");
                var targetDir = Path.GetDirectoryName(target);
                if (!string.IsNullOrWhiteSpace(targetDir))
                    Directory.CreateDirectory(targetDir);
                File.Copy(source, target, overwrite: true);
                progress?.Report((source, ++current, expectedTotal));
                _logger.LogDebug("Backed up texture {path}", source);
                var entry = new BackupManifestEntry
                {
                    OriginalPath = source,
                    PrefixedOriginalPath = prefixed,
                    BackupFileName = Path.GetFileName(source),
                    OriginalFileName = Path.GetFileName(source),
                    ModRelativePath = modRelativePath,
                    ModFolderName = ExtractModFolderNameFromPrefixed(prefixed),
                    ModVersion = string.IsNullOrWhiteSpace(modName) ? null : currentModVersion,
                    CreatedUtc = DateTime.UtcNow,
                };
                manifest.Entries.Add(entry);
                if (!string.IsNullOrEmpty(modName))
                {
                    if (!entriesByMod.TryGetValue(modName, out var list))
                    {
                        list = new List<BackupManifestEntry>();
                        entriesByMod[modName] = list;
                    }
                    list.Add(entry);
                }
            }
            catch
            {
            }
            finally
            {
                traceCopy.Dispose();
            }
            
        }

        try
        {
            foreach (var m in modsTouchedAll)
            {
                var hasTex = modsTouched.Contains(m);
                var hasPmpNow = await HasPmpBackupForModAsync(m).ConfigureAwait(false);
                _modStateService.UpdateBackupFlags(m, hasTex, hasPmpNow);
            }
        }
        catch { }
        }

        if (_configService.Current.EnableBackupBeforeConversion)
        {
            try
            {
                var manifestPath = Path.Combine(sessionDir, "manifest.json");
                    File.WriteAllText(manifestPath, JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true }));
            }
            catch { }
        }

        // Create per-mod ZIP archives if enabled
        if (_configService.Current.EnableZipCompressionForBackups && _configService.Current.EnableBackupBeforeConversion)
        {
            foreach (var (mod, entries) in entriesByMod)
            {
                PerfStep traceZip = default;
                try
                {
                    traceZip = PerfTrace.Step(_logger, $"Zip {mod}");
                    var modBackupDir = Path.Combine(backupDirectory, mod);
                    try { Directory.CreateDirectory(modBackupDir); } catch { }

                    // Build a manifest limited to this mod
                    var modManifest = new BackupManifest { Entries = entries.Select(e => new BackupManifestEntry
                    {
                        OriginalPath = e.OriginalPath,
                        PrefixedOriginalPath = e.PrefixedOriginalPath,
                        BackupFileName = e.BackupFileName,
                        OriginalFileName = e.OriginalFileName,
                        ModRelativePath = e.ModRelativePath,
                        ModFolderName = e.ModFolderName,
                        ModVersion = e.ModVersion,
                        CreatedUtc = e.CreatedUtc,
                    }).ToList() };

                    // Write manifest into mod subdir of the session so it gets included in the ZIP
                    var modSessionSubdir = Path.Combine(sessionDir, mod);
                try
                {
                    File.WriteAllText(Path.Combine(modSessionSubdir, "manifest.json"), JsonSerializer.Serialize(modManifest, new JsonSerializerOptions { WriteIndented = true }));
                }
                catch { }

                // Include mod's meta.json inside the ZIP so version/author can be read later
                try
                {
                    var modAbsPath = GetModAbsolutePath(mod);
                    if (!string.IsNullOrWhiteSpace(modAbsPath))
                    {
                        var metaPath = Path.Combine(modAbsPath!, "meta.json");
                        if (File.Exists(metaPath))
                        {
                            var targetMetaPath = Path.Combine(modSessionSubdir, "meta.json");
                            try { if (File.Exists(targetMetaPath)) File.Delete(targetMetaPath); } catch { }
                            File.Copy(metaPath, targetMetaPath, overwrite: true);
                        }
                    }
                }
                catch { }

                    var zipPath = Path.Combine(modBackupDir, $"backup_{timestamp:yyyyMMdd_HHmmss}.zip");
                    if (File.Exists(zipPath))
                    {
                        try { File.Delete(zipPath); } catch { }
                    }
                    ZipFile.CreateFromDirectory(modSessionSubdir, zipPath, CompressionLevel.Fastest, includeBaseDirectory: false);
                    _logger.LogDebug("Created mod backup ZIP {zip}", zipPath);
                    try { progress?.Report((zipPath, ++current, expectedTotal)); } catch { }
                    try
                    {
                        var meta = ReadMetaFromZip(zipPath);
                        var created = File.GetCreationTimeUtc(zipPath);
                        _modStateService.UpdateLatestBackupsInfo(mod, System.IO.Path.GetFileName(zipPath), meta?.version ?? string.Empty, created, _modStateService.Get(mod).LatestPmpBackupFileName, _modStateService.Get(mod).LatestPmpBackupVersion, _modStateService.Get(mod).LatestPmpBackupCreatedUtc);
                    }
                    catch { }
                    traceZip.Dispose();
                }
                catch
                {
                    // Ignore zip errors for individual mods
                }
                finally
                {
                    traceZip.Dispose();
                }
            }

            if (_configService.Current.DeleteOriginalBackupsAfterCompression)
            {
                try
                {
                    Directory.Delete(sessionDir, recursive: true);
                }
                catch { }
            }
        }

        // Optionally create full mod PMP archives for touched mods
            if (_configService.Current.EnableFullModBackupBeforeConversion)
            {
                foreach (var mod in modsTouchedAll)
                {
                    PerfStep tracePmp = default;
                    try
                    {
                        tracePmp = PerfTrace.Step(_logger, $"PMP {mod}");
                        var modAbs = GetModAbsolutePath(mod);
                        if (string.IsNullOrWhiteSpace(modAbs) || !Directory.Exists(modAbs))
                            continue;

                        var modBackupDir = Path.Combine(backupDirectory, mod);
                        try { Directory.CreateDirectory(modBackupDir); } catch { }

                        var currentVersion = GetModVersion(mod) ?? string.Empty;
                        var currentAuthor = GetModAuthor(mod) ?? string.Empty;

                        var existingPmp = Directory.EnumerateFiles(modBackupDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
                        bool sameVersion = false;
                        if (!string.IsNullOrEmpty(existingPmp) && File.Exists(existingPmp))
                        {
                            var latestVersion = _modStateService.Get(mod).LatestPmpBackupVersion ?? string.Empty;
                            sameVersion = string.Equals(latestVersion, currentVersion ?? string.Empty, StringComparison.OrdinalIgnoreCase);
                            try { _logger.LogDebug("Existing PMP for {mod}: path={pmp}, metaVersion={mver}, author={mauth}, currentVersion={cver}, sameVersion={same}", mod, existingPmp, latestVersion, currentAuthor, currentVersion, sameVersion); } catch { }
                        }

                        // If same version already backed up, skip; otherwise replace with latest
                        if (sameVersion)
                        {
                            _logger.LogDebug("Skipping PMP creation for {mod}; existing backup matches current version {version}", mod, currentVersion);
                            continue;
                        }

                        // Delete old PMP archives before creating a new one
                        try
                        {
                            foreach (var p in Directory.EnumerateFiles(modBackupDir, "mod_backup_*.pmp"))
                            {
                                try { File.Delete(p); _logger.LogDebug("Deleted outdated PMP {pmp} for {mod}", p, mod); } catch { }
                            }
                        }
                        catch { }

                        var stamp = timestamp.ToString("yyyyMMdd_HHmmss");
                        var pmpPath = Path.Combine(modBackupDir, $"mod_backup_{stamp}.pmp");
                        if (File.Exists(pmpPath))
                        {
                            _logger.LogDebug("Skipping full mod PMP creation for {mod}; target path already exists: {pmp}", mod, pmpPath);
                            continue;
                        }

                        await Task.Run(() =>
                        {
                            if (token.IsCancellationRequested) return;
                            ZipFile.CreateFromDirectory(modAbs!, pmpPath, CompressionLevel.Fastest, includeBaseDirectory: false);
                        }, token).ConfigureAwait(false);
                        _logger.LogDebug("Created full mod backup PMP {pmp}", pmpPath);
                        try { progress?.Report((pmpPath, ++current, expectedTotal)); } catch { }
                        try
                        {
                            var created = File.GetCreationTimeUtc(pmpPath);
                            var currentVersion2 = _modStateService.Get(mod).CurrentVersion ?? string.Empty;
                            _modStateService.UpdateLatestBackupsInfo(mod, _modStateService.Get(mod).LatestZipBackupFileName, _modStateService.Get(mod).LatestZipBackupVersion, _modStateService.Get(mod).LatestZipBackupCreatedUtc, System.IO.Path.GetFileName(pmpPath), currentVersion2, created);
                        }
                        catch { }

                        // Write converted textures manifest (relative paths within the mod)
                        try
                        {
                            var convertedRel = new List<string>();
                            foreach (var kvp in textures)
                            {
                                var source = kvp.Key;
                                var prefixed = BuildPrefixedPath(source);
                                var owner = ExtractModFolderNameFromPrefixed(prefixed);
                                if (!string.Equals(owner, mod, StringComparison.OrdinalIgnoreCase))
                                    continue;
                                var rel = Path.GetRelativePath(modAbs!, source).Replace('\\', '/');
                                if (!string.IsNullOrWhiteSpace(rel) && !rel.StartsWith("..", StringComparison.Ordinal))
                                    convertedRel.Add(rel);
                            }
                            var convertedManifestPath = Path.Combine(modBackupDir, "pmp_converted_manifest.json");
                            try { if (File.Exists(convertedManifestPath)) File.Delete(convertedManifestPath); } catch { }
                            File.WriteAllText(convertedManifestPath, JsonSerializer.Serialize(convertedRel, new JsonSerializerOptions { WriteIndented = true }));
                        }
                        catch { }

                        try { _modStateService.UpdateBackupFlags(mod, _modStateService.Get(mod).HasTextureBackup, true); } catch { }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning("Failed to create full mod backup for {mod}: {error}", mod, ex.Message);
                    }
                    finally
                    {
                        tracePmp.Dispose();
                    }
                    await Task.Yield();
                }
            }
    }

    // Read version and author from a ZIP archive's internal meta.json
    private (string version, string author)? ReadMetaFromZip(string zipPath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(zipPath) || !File.Exists(zipPath))
                return null;
            using var za = ZipFile.OpenRead(zipPath);
            try { _logger.LogDebug("Reading meta.json from ZIP: {zip} (entries={count})", zipPath, za.Entries.Count); } catch { }
            var entry = za.Entries.FirstOrDefault(e => string.Equals(Path.GetFileName(e.FullName), "meta.json", StringComparison.OrdinalIgnoreCase));
            if (entry == null)
            {
                try { _logger.LogDebug("meta.json not found in ZIP: {zip}", zipPath); } catch { }
                return null;
            }
            using var ms = new MemoryStream();
            using (var zs = entry.Open()) zs.CopyTo(ms);
            var bytes = ms.ToArray();
            string content;
            try
            {
                if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
                    content = Encoding.UTF8.GetString(bytes, 3, bytes.Length - 3);
                else if (bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE)
                    content = Encoding.Unicode.GetString(bytes, 2, bytes.Length - 2);
                else if (bytes.Length >= 2 && bytes[0] == 0xFE && bytes[1] == 0xFF)
                    content = Encoding.BigEndianUnicode.GetString(bytes, 2, bytes.Length - 2);
                else if (bytes.Length >= 4 && bytes[0] == 0xFF && bytes[1] == 0xFE && bytes[2] == 0x00 && bytes[3] == 0x00)
                    content = Encoding.UTF32.GetString(bytes, 4, bytes.Length - 4);
                else if (bytes.Length >= 4 && bytes[0] == 0x00 && bytes[1] == 0x00 && bytes[2] == 0xFE && bytes[3] == 0xFF)
                    content = Encoding.GetEncoding(12001).GetString(bytes, 4, bytes.Length - 4);
                else
                    content = Encoding.UTF8.GetString(bytes);
            }
            catch { content = Encoding.UTF8.GetString(bytes); }

            string version = string.Empty;
            string author = string.Empty;
            try
            {
                using var doc = JsonDocument.Parse(content);
                foreach (var prop in doc.RootElement.EnumerateObject())
                {
                    var name = prop.Name;
                    if (string.Equals(name, "Version", StringComparison.OrdinalIgnoreCase) && prop.Value.ValueKind == JsonValueKind.String)
                        version = prop.Value.GetString() ?? version;
                    else if (string.Equals(name, "FileVersion", StringComparison.OrdinalIgnoreCase))
                    {
                        if (prop.Value.ValueKind == JsonValueKind.String)
                            version = prop.Value.GetString() ?? version;
                        else if (prop.Value.ValueKind == JsonValueKind.Number)
                            version = prop.Value.ToString();
                    }
                    else if (string.Equals(name, "VersionString", StringComparison.OrdinalIgnoreCase) && prop.Value.ValueKind == JsonValueKind.String)
                        version = prop.Value.GetString() ?? version;
                    else if (string.Equals(name, "Author", StringComparison.OrdinalIgnoreCase) && prop.Value.ValueKind == JsonValueKind.String)
                        author = prop.Value.GetString() ?? author;
                }
            }
            catch
            {
                try
                {
                    var rxAuthor = new Regex(@"""Author""\s*:\s*""(?<a>.*?)""", RegexOptions.Singleline | RegexOptions.IgnoreCase);
                    var rxVersionStr = new Regex(@"""Version""\s*:\s*""(?<v>.*?)""", RegexOptions.Singleline | RegexOptions.IgnoreCase);
                    var rxVersionNum = new Regex(@"""FileVersion""\s*:\s*(?<n>[-]?[0-9]+(?:\.[0-9]+)?)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
                    var rxVersionAlt = new Regex(@"""VersionString""\s*:\s*""(?<v>.*?)""", RegexOptions.Singleline | RegexOptions.IgnoreCase);
                    var mA = rxAuthor.Match(content ?? string.Empty);
                    var mV = rxVersionStr.Match(content ?? string.Empty);
                    var mVN = rxVersionNum.Match(content ?? string.Empty);
                    var mVA = rxVersionAlt.Match(content ?? string.Empty);
                    if (mA.Success) author = mA.Groups["a"].Value;
                    if (mV.Success) version = mV.Groups["v"].Value;
                    else if (mVN.Success) version = mVN.Groups["n"].Value;
                    else if (mVA.Success) version = mVA.Groups["v"].Value;
                }
                catch { }
            }

            try { _logger.LogDebug("Parsed ZIP meta for {zip}: version={version}, author={author}", zipPath, version, author); } catch { }
            return (version ?? string.Empty, author ?? string.Empty);
        }
        catch (Exception ex) { try { _logger.LogDebug(ex, "Failed reading ZIP meta for {zip}", zipPath); } catch { } return null; }
    }

    public Task<(string version, string author, DateTime createdUtc, string zipFileName)?> GetLatestZipMetaForModAsync(string modFolderName)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (!Directory.Exists(backupDirectory)) return Task.FromResult<(string, string, DateTime, string)?>(null);
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir)) return Task.FromResult<(string, string, DateTime, string)?>(null);
            var latestZip = Directory.EnumerateFiles(modDir, "backup_*.zip").OrderByDescending(f => f).FirstOrDefault();
            if (string.IsNullOrEmpty(latestZip)) return Task.FromResult<(string, string, DateTime, string)?>(null);
            var meta = ReadMetaFromZip(latestZip);
            if (!meta.HasValue) return Task.FromResult<(string, string, DateTime, string)?>(null);
            var created = File.GetCreationTimeUtc(latestZip);
            var name = Path.GetFileName(latestZip) ?? string.Empty;
            return Task.FromResult<(string, string, DateTime, string)?>((meta.Value.version ?? string.Empty, meta.Value.author ?? string.Empty, created, name));
        }
        catch { return Task.FromResult<(string, string, DateTime, string)?>(null); }
    }

    public (string version, string author) GetLiveModMeta(string modFolderName)
    {
        try
        {
            var v = GetModVersion(modFolderName) ?? string.Empty;
            var a = GetModAuthor(modFolderName) ?? string.Empty;
            return (v, a);
        }
        catch { return (string.Empty, string.Empty); }
    }

    public async Task<List<OrphanBackupInfo>> FindOrphanedBackupsAsync()
    {
        var result = new List<OrphanBackupInfo>();
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || !Directory.Exists(backupDirectory))
                return result;
            var mods = await _penumbraIpc.GetAllModFoldersAsync().ConfigureAwait(false);
            var modSet = new HashSet<string>(mods ?? new List<string>(), StringComparer.OrdinalIgnoreCase);
            foreach (var modDir in Directory.EnumerateDirectories(backupDirectory))
            {
                var modName = Path.GetFileName(modDir) ?? string.Empty;
                if (string.IsNullOrWhiteSpace(modName)) continue;
                if (modSet.Contains(modName)) continue;
                var info = new OrphanBackupInfo { ModFolderName = modName };
                try
                {
                    foreach (var file in Directory.EnumerateFiles(modDir, "*", SearchOption.TopDirectoryOnly))
                    {
                        var name = Path.GetFileName(file) ?? string.Empty;
                        var len = 0L;
                        try { len = new FileInfo(file).Length; } catch { }
                        if (name.StartsWith("backup_", StringComparison.OrdinalIgnoreCase) && name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                        {
                            info.ZipCount++;
                            info.TotalBytes += len;
                        }
                        else if (name.StartsWith("mod_backup_", StringComparison.OrdinalIgnoreCase) && name.EndsWith(".pmp", StringComparison.OrdinalIgnoreCase))
                        {
                            info.PmpCount++;
                            info.TotalBytes += len;
                        }
                    }
                    info.LatestPmpPath = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
                    result.Add(info);
                }
                catch { }
            }
        }
        catch { }
        return result;
    }

    public Task<bool> DeleteOrphanBackupsAsync(string modFolderName)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || !Directory.Exists(backupDirectory))
                return Task.FromResult(false);
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir))
                return Task.FromResult(false);
            try { Directory.Delete(modDir, true); } catch { return Task.FromResult(false); }
            return Task.FromResult(true);
        }
        catch { return Task.FromResult(false); }
    }

    public Task<bool> ReinstallModFromLatestPmpAsync(string modFolderName, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || !Directory.Exists(backupDirectory))
                return Task.FromResult(false);
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir))
                return Task.FromResult(false);
            var latestPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
            if (string.IsNullOrEmpty(latestPmp) || !File.Exists(latestPmp))
                return Task.FromResult(false);
            return RestorePmpAsync(modFolderName, latestPmp, progress, token, cleanupBackupsAfterRestore: false, deregisterDuringRestore: true);
        }
        catch { return Task.FromResult(false); }
    }

    public async Task<List<BackupSessionInfo>> GetBackupOverviewAsync()
    {
        await Task.Yield();
        var backupDirectory = _configService.Current.BackupFolderPath;
        var overview = new List<BackupSessionInfo>();
        try
        {
            if (!Directory.Exists(backupDirectory)) return overview;

            // Per-mod ZIPs in subdirectories
            foreach (var modDir in Directory.EnumerateDirectories(backupDirectory).OrderBy(d => d))
            {
                var modName = Path.GetFileName(modDir);
                foreach (var zip in Directory.EnumerateFiles(modDir, "backup_*.zip").OrderByDescending(f => f))
                {
                    try
                    {
                        using var za = ZipFile.OpenRead(zip);
                        var manifestEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, "manifest.json", StringComparison.OrdinalIgnoreCase));
                        if (manifestEntry == null) continue;
                        using var ms = new MemoryStream();
                        using (var zs = manifestEntry.Open()) zs.CopyTo(ms);
                        ms.Position = 0;
                        var manifest = JsonSerializer.Deserialize<BackupManifest>(ms.ToArray());
                        if (manifest == null || manifest.Entries.Count == 0) continue;
                        var sess = new BackupSessionInfo
                        {
                            SourcePath = zip,
                            IsZip = true,
                            DisplayName = $"{modName} - {Path.GetFileNameWithoutExtension(zip)}",
                            CreatedUtc = File.GetCreationTimeUtc(zip),
                        };
                        foreach (var e in manifest.Entries)
                        {
                            sess.Entries.Add(new BackupEntryInfo
                            {
                                BackupFileName = e.BackupFileName,
                                OriginalFileName = e.OriginalFileName,
                                OriginalPath = e.OriginalPath,
                                PrefixedOriginalPath = e.PrefixedOriginalPath,
                                ModRelativePath = e.ModRelativePath,
                                ModFolderName = e.ModFolderName,
                                ModVersion = e.ModVersion,
                                CreatedUtc = e.CreatedUtc,
                            });
                        }
                        overview.Add(sess);
                    }
                    catch { }
                }
            }

            // Session folders
            foreach (var session in Directory.EnumerateDirectories(backupDirectory, "session_*").OrderByDescending(f => f))
            {
                try
                {
                    var manifestPath = Path.Combine(session, "manifest.json");
                    if (!File.Exists(manifestPath)) continue;
                    var manifest = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(manifestPath));
                    if (manifest == null || manifest.Entries.Count == 0) continue;
                    var sess = new BackupSessionInfo
                    {
                        SourcePath = session,
                        IsZip = false,
                        DisplayName = Path.GetFileName(session),
                        CreatedUtc = Directory.GetCreationTimeUtc(session),
                    };
                    foreach (var e in manifest.Entries)
                    {
                        sess.Entries.Add(new BackupEntryInfo
                        {
                            BackupFileName = e.BackupFileName,
                            OriginalFileName = e.OriginalFileName,
                            OriginalPath = e.OriginalPath,
                            PrefixedOriginalPath = e.PrefixedOriginalPath,
                            ModRelativePath = e.ModRelativePath,
                            ModFolderName = e.ModFolderName,
                            ModVersion = e.ModVersion,
                            CreatedUtc = e.CreatedUtc,
                        });
                    }
                    overview.Add(sess);
                }
                catch { }
            }
        }
        catch { }

        return overview;
    }

    /// <summary>
    /// Check whether a specific texture (by original or prefixed path key) has a backup entry for the given mod.
    /// Considers both per-mod ZIP archives and session manifests.
    /// </summary>
    public async Task<bool> HasBackupForTextureAsync(string modFolderName, string textureKey)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(modFolderName) || string.IsNullOrWhiteSpace(textureKey))
                return false;
            var normalized = textureKey.Replace('\\', '/').Trim();
            var set = await GetBackedKeysForModAsync(modFolderName).ConfigureAwait(false);
            if (set.Contains(normalized)) return true;
            try
            {
                var prefixed = BuildPrefixedPath(normalized);
                if (!string.IsNullOrWhiteSpace(prefixed) && set.Contains(prefixed))
                    return true;
            }
            catch { }
            return false;
        }
        catch { return false; }
    }

    /// <summary>
    /// Static helper for tests: checks overview entries for a texture backup belonging to a mod.
    /// </summary>
    public static bool HasBackupForTextureInOverview(List<BackupSessionInfo> overview, string modFolderName, string textureKey)
    {
        if (overview == null || overview.Count == 0) return false;
        if (string.IsNullOrWhiteSpace(modFolderName) || string.IsNullOrWhiteSpace(textureKey)) return false;
        var normalized = textureKey.Replace('\\', '/').Trim();
        foreach (var sess in overview)
        {
            foreach (var e in sess.Entries)
            {
                var mod = e.ModFolderName;
                if (string.IsNullOrWhiteSpace(mod) && !string.IsNullOrWhiteSpace(e.PrefixedOriginalPath))
                    mod = e.PrefixedOriginalPath.Split(new[] {'/', '\\'}, StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
                if (!string.Equals(mod, modFolderName, StringComparison.OrdinalIgnoreCase))
                    continue;
                var key = !string.IsNullOrEmpty(e.PrefixedOriginalPath) ? e.PrefixedOriginalPath : e.OriginalPath;
                key = key?.Replace('\\', '/')?.Trim() ?? string.Empty;
                if (string.Equals(key, normalized, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
        }
        return false;
    }

    // Compute size comparison between backups and current files in Penumbra
    public async Task<BackupSavingsStats> ComputeSavingsAsync()
    {
        var stats = new BackupSavingsStats();
        try
        {
            var overview = await GetBackupOverviewAsync().ConfigureAwait(false);
            // When no ZIP/session entries exist, still include PMP totals below

            // Prefer PMP over ZIP/session per mod: build set of mods that have a PMP
            var modsWithPmp = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (Directory.Exists(backupDirectory))
                {
                    foreach (var modDir in Directory.EnumerateDirectories(backupDirectory))
                    {
                        var modName = Path.GetFileName(modDir);
                        var hasPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").Any();
                        if (hasPmp && !string.IsNullOrWhiteSpace(modName))
                            modsWithPmp.Add(modName);
                    }
                }
            }
            catch { }

            // Prefer latest backup per original path to avoid double-counting
            var processed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var session in overview.OrderByDescending(s => s.CreatedUtc))
            {
                foreach (var entry in session.Entries)
                {
                    var key = string.IsNullOrEmpty(entry.PrefixedOriginalPath) ? entry.OriginalPath : entry.PrefixedOriginalPath;
                    if (string.IsNullOrWhiteSpace(key))
                        continue;
                    if (!processed.Add(key))
                        continue;

                    // Determine mod and skip ZIP/session sizes for mods with PMP
                    var modFolder = entry.ModFolderName;
                    if (string.IsNullOrWhiteSpace(modFolder) && !string.IsNullOrWhiteSpace(entry.PrefixedOriginalPath))
                        modFolder = ExtractModFolderNameFromPrefixed(entry.PrefixedOriginalPath);
                    if (!string.IsNullOrWhiteSpace(modFolder) && modsWithPmp.Contains(modFolder))
                        continue;

                    // Determine current file path (Penumbra-resolved if prefixed)
                    var currentPath = !string.IsNullOrEmpty(entry.PrefixedOriginalPath)
                        ? ResolvePrefixedPath(entry.PrefixedOriginalPath)
                        : entry.OriginalPath;

                    long backupBytes = 0L;
                    try
                    {
                        if (session.IsZip)
                        {
                            using var za = ZipFile.OpenRead(session.SourcePath);
                            // Prefer relative path inside mod
                            System.IO.Compression.ZipArchiveEntry? zipEntry = null;
                            if (!string.IsNullOrWhiteSpace(entry.ModRelativePath))
                            {
                                var normalized = entry.ModRelativePath.Replace('\\', '/');
                                zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, normalized, StringComparison.OrdinalIgnoreCase));
                            }
                            if (zipEntry == null)
                            {
                                // Fallbacks to name-based matching
                                zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.Name, entry.BackupFileName, StringComparison.OrdinalIgnoreCase))
                                          ?? za.Entries.FirstOrDefault(e => string.Equals(e.FullName, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"/{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"\\{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                            }
                            if (zipEntry != null)
                                backupBytes = zipEntry.Length;
                        }
                        else
                        {
                            // Prefer relative path to avoid collisions
                            var relCandidate = !string.IsNullOrWhiteSpace(entry.ModRelativePath)
                                ? Path.Combine(session.SourcePath, entry.ModRelativePath)
                                : string.Empty;
                            if (!string.IsNullOrWhiteSpace(relCandidate) && File.Exists(relCandidate))
                            {
                                var fi = new FileInfo(relCandidate);
                                backupBytes = fi.Length;
                            }
                            else
                            {
                                // Session folder layout may store files directly or under mod subdirectories
                                var direct = Path.Combine(session.SourcePath, entry.BackupFileName);
                                var sub = !string.IsNullOrEmpty(entry.ModFolderName)
                                    ? Path.Combine(session.SourcePath, entry.ModFolderName, entry.BackupFileName)
                                    : direct;
                                var candidate = File.Exists(direct) ? direct : sub;
                                if (File.Exists(candidate))
                                {
                                    var fi = new FileInfo(candidate);
                                    backupBytes = fi.Length;
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Skip backup size errors
                    }

                    long currentBytes = 0L;
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(currentPath) && File.Exists(currentPath))
                        {
                            var fi = new FileInfo(currentPath);
                            currentBytes = fi.Length;
                        }
                        else
                        {
                            stats.MissingCurrentFiles++;
                        }
                    }
                    catch
                    {
                        stats.MissingCurrentFiles++;
                    }

                    if (backupBytes > 0)
                    {
                        stats.OriginalTotalBytes += backupBytes;
                        stats.CurrentTotalBytes += currentBytes;
                        stats.ComparedFiles++;
                    }
                }
            }

            // Add PMP totals for mods with PMP (only consider converted textures when a manifest exists)
            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (Directory.Exists(backupDirectory))
                {
                    foreach (var modName in modsWithPmp)
                    {
                        var modDir = Path.Combine(backupDirectory, modName);
                        var latestPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
                        if (string.IsNullOrEmpty(latestPmp))
                            continue;
                        var stamp = Path.GetFileNameWithoutExtension(latestPmp).Replace("mod_backup_", string.Empty);
                        var convertedManifestPath = Path.Combine(modDir, "pmp_converted_manifest.json");
                        List<string>? convertedRel = null;
                        try
                        {
                            if (File.Exists(convertedManifestPath))
                                convertedRel = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(convertedManifestPath));
                        }
                        catch { }
                        long backupBytes = 0L;
                        try
                        {
                            using var za = ZipFile.OpenRead(latestPmp);
                            foreach (var e in za.Entries)
                            {
                                var name = e.FullName?.Replace('\\', '/');
                                if (string.IsNullOrWhiteSpace(name))
                                    continue;
                                if (name.EndsWith("/", StringComparison.Ordinal))
                                    continue;
                                if (convertedRel != null && convertedRel.Count > 0 && !convertedRel.Contains(name, StringComparer.OrdinalIgnoreCase))
                                    continue;
                                backupBytes += e.Length;
                            }
                        }
                        catch { }
                        long currentBytes = 0L;
                        try
                        {
                            var modAbs = GetModAbsolutePath(modName);
                            if (!string.IsNullOrWhiteSpace(modAbs) && Directory.Exists(modAbs))
                            {
                                foreach (var file in Directory.EnumerateFiles(modAbs!, "*", SearchOption.AllDirectories))
                                {
                                    try
                                    {
                                        if (convertedRel != null && convertedRel.Count > 0)
                                        {
                                            var rel = Path.GetRelativePath(modAbs!, file).Replace('\\', '/');
                                            if (!convertedRel.Contains(rel, StringComparer.OrdinalIgnoreCase))
                                                continue;
                                        }
                                        var fi = new FileInfo(file); currentBytes += fi.Length;
                                    }
                                    catch { }
                                }
                            }
                        }
                        catch { }
                        if (backupBytes > 0)
                        {
                            stats.OriginalTotalBytes += backupBytes;
                            stats.CurrentTotalBytes += currentBytes;
                            stats.ComparedFiles += 1;
                        }
                    }
                }
            }
            catch { }
        }
        catch
        {
            // Swallow to avoid noisy UI; stats remain default
        }
        return stats;
    }

    // Compute per-mod savings stats (uncompressed vs current sizes per mod)
    public async Task<Dictionary<string, ModSavingsStats>> ComputePerModSavingsAsync()
    {
        var result = new Dictionary<string, ModSavingsStats>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var overview = await GetBackupOverviewAsync().ConfigureAwait(false);
            // Even when no ZIP/session entries exist, still compute PMP-based totals below

            // Prefer PMP per mod: build set of mods that have PMP archives
            var modsWithPmp = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (Directory.Exists(backupDirectory))
                {
                    foreach (var modDir in Directory.EnumerateDirectories(backupDirectory))
                    {
                        var modName = Path.GetFileName(modDir);
                        var hasPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").Any();
                        if (hasPmp && !string.IsNullOrWhiteSpace(modName))
                            modsWithPmp.Add(modName);
                    }
                }
            }
            catch { }

            // Track processed keys to avoid double counting across sessions
            var processed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var session in overview.OrderByDescending(s => s.CreatedUtc))
            {
                foreach (var entry in session.Entries)
                {
                    // Key by prefixed path when available to remain stable across relocations
                    var key = string.IsNullOrEmpty(entry.PrefixedOriginalPath) ? entry.OriginalPath : entry.PrefixedOriginalPath;
                    if (string.IsNullOrWhiteSpace(key))
                        continue;
                    if (!processed.Add(key))
                        continue;

                    // Resolve which mod this entry belongs to
                    var modFolder = entry.ModFolderName;
                    if (string.IsNullOrWhiteSpace(modFolder) && !string.IsNullOrWhiteSpace(entry.PrefixedOriginalPath))
                        modFolder = ExtractModFolderNameFromPrefixed(entry.PrefixedOriginalPath);
                    if (string.IsNullOrWhiteSpace(modFolder))
                        continue; // skip entries we cannot attribute to a mod
                    // Skip ZIP/session entries for mods that have PMP
                    if (modsWithPmp.Contains(modFolder))
                        continue;

                    long backupBytes = 0L;
                    try
                    {
                        if (session.IsZip)
                        {
                            using var za = ZipFile.OpenRead(session.SourcePath);
                            System.IO.Compression.ZipArchiveEntry? zipEntry = null;
                            if (!string.IsNullOrWhiteSpace(entry.ModRelativePath))
                            {
                                var normalized = entry.ModRelativePath.Replace('\\', '/');
                                zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, normalized, StringComparison.OrdinalIgnoreCase));
                            }
                            if (zipEntry == null)
                            {
                                zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.Name, entry.BackupFileName, StringComparison.OrdinalIgnoreCase))
                                          ?? za.Entries.FirstOrDefault(e => string.Equals(e.FullName, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"/{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"\\{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                            }
                            if (zipEntry != null)
                                backupBytes = zipEntry.Length;
                        }
                        else
                        {
                            var relCandidate = !string.IsNullOrWhiteSpace(entry.ModRelativePath)
                                ? Path.Combine(session.SourcePath, entry.ModRelativePath)
                                : string.Empty;
                            if (!string.IsNullOrWhiteSpace(relCandidate) && File.Exists(relCandidate))
                            {
                                var fi = new FileInfo(relCandidate);
                                backupBytes = fi.Length;
                            }
                            else
                            {
                                var direct = Path.Combine(session.SourcePath, entry.BackupFileName);
                                var sub = !string.IsNullOrEmpty(entry.ModFolderName)
                                    ? Path.Combine(session.SourcePath, entry.ModFolderName, entry.BackupFileName)
                                    : direct;
                                var candidate = File.Exists(direct) ? direct : sub;
                                if (File.Exists(candidate))
                                {
                                    var fi = new FileInfo(candidate);
                                    backupBytes = fi.Length;
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Skip backup size errors
                    }

                    long currentBytes = 0L;
                    try
                    {
                        var currentPath = !string.IsNullOrEmpty(entry.PrefixedOriginalPath)
                            ? ResolvePrefixedPath(entry.PrefixedOriginalPath)
                            : entry.OriginalPath;
                        if (!string.IsNullOrWhiteSpace(currentPath) && File.Exists(currentPath))
                        {
                            var fi = new FileInfo(currentPath);
                            currentBytes = fi.Length;
                        }
                    }
                    catch
                    {
                        // Ignore current size errors per entry
                    }

                    if (backupBytes > 0)
                    {
                        if (!result.TryGetValue(modFolder, out var modStats))
                        {
                            modStats = new ModSavingsStats();
                            result[modFolder] = modStats;
                        }
                        modStats.OriginalBytes += backupBytes;
                        modStats.CurrentBytes += currentBytes;
                        modStats.ComparedFiles += 1;
                        try { _modStateService.UpdateSavings(modFolder, modStats.OriginalBytes, modStats.CurrentBytes, modStats.ComparedFiles); } catch { }
                    }
                }
            }
            // Include PMP-based per-mod totals (prefer PMP over ZIP/session and filter to converted textures when available)
            try
            {
                var backupDirectory = _configService.Current.BackupFolderPath;
                if (Directory.Exists(backupDirectory))
                {
                    foreach (var modName in modsWithPmp)
                    {
                        var modDir = Path.Combine(backupDirectory, modName);
                        var latestPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
                        if (string.IsNullOrEmpty(latestPmp))
                            continue;
                        var stamp = Path.GetFileNameWithoutExtension(latestPmp).Replace("mod_backup_", string.Empty);
                        var convertedManifestPath = Path.Combine(modDir, "pmp_converted_manifest.json");
                        List<string>? convertedRel = null;
                        try
                        {
                            if (File.Exists(convertedManifestPath))
                                convertedRel = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(convertedManifestPath));
                        }
                        catch { }
                        long backupBytes = 0L;
                        int entryCount = 0;
                        try
                        {
                            using var za = ZipFile.OpenRead(latestPmp);
                            foreach (var e in za.Entries)
                            {
                                var name = e.FullName?.Replace('\\', '/');
                                if (string.IsNullOrWhiteSpace(name))
                                    continue;
                                if (name.EndsWith("/", StringComparison.Ordinal))
                                    continue;
                                if (convertedRel != null && convertedRel.Count > 0 && !convertedRel.Contains(name, StringComparer.OrdinalIgnoreCase))
                                    continue;
                                backupBytes += e.Length;
                                entryCount++;
                            }
                        }
                        catch { }
                        long currentBytes = 0L;
                        try
                        {
                            var modAbs = GetModAbsolutePath(modName);
                            if (!string.IsNullOrWhiteSpace(modAbs) && Directory.Exists(modAbs))
                            {
                                foreach (var file in Directory.EnumerateFiles(modAbs!, "*", SearchOption.AllDirectories))
                                {
                                    try
                                    {
                                        if (convertedRel != null && convertedRel.Count > 0)
                                        {
                                            var rel = Path.GetRelativePath(modAbs!, file).Replace('\\', '/');
                                            if (!convertedRel.Contains(rel, StringComparer.OrdinalIgnoreCase))
                                                continue;
                                        }
                                        var fi = new FileInfo(file); currentBytes += fi.Length; 
                                    }
                                    catch { }
                                }
                            }
                        }
                        catch { }
                        if (!result.TryGetValue(modName!, out var stats))
                            stats = new ModSavingsStats();
                        stats.OriginalBytes += backupBytes;
                        stats.CurrentBytes += currentBytes;
                        stats.ComparedFiles += Math.Max(1, entryCount);
                        result[modName!] = stats;
                        try { _modStateService.UpdateSavings(modName!, stats.OriginalBytes, stats.CurrentBytes, stats.ComparedFiles); } catch { }
                    }
                }
            }
            catch { }
        }
        catch
        {
            // Swallow to keep UI resilient
        }
        return result;
    }

    // Return a map of mod-relative paths to original bytes from the latest backup for a mod
    public async Task<Dictionary<string, long>> GetLatestOriginalSizesForModAsync(string modFolderName)
    {
        await Task.Yield();
        var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (!Directory.Exists(backupDirectory)) return result;

            // Prefer PMP first
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (Directory.Exists(modDir))
            {
                var latestPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault();
                if (!string.IsNullOrEmpty(latestPmp))
                {
                    try
                    {
                        var stamp = Path.GetFileNameWithoutExtension(latestPmp).Replace("mod_backup_", string.Empty);
                        var manifestPath = Path.Combine(modDir, "pmp_converted_manifest.json");
                        List<string>? convertedRel = null;
                        try
                        {
                            if (File.Exists(manifestPath))
                                convertedRel = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(manifestPath));
                        }
                        catch { }
                        using var za = ZipFile.OpenRead(latestPmp);
                        foreach (var e in za.Entries)
                        {
                            if (string.IsNullOrWhiteSpace(e.FullName))
                                continue;
                            var key = e.FullName.Replace('/', '\\');
                            if (key.EndsWith("/", StringComparison.Ordinal) || key.EndsWith("\\", StringComparison.Ordinal))
                                continue;
                            if (convertedRel != null && convertedRel.Count > 0)
                            {
                                var name = e.FullName.Replace('\\', '/');
                                if (!convertedRel.Contains(name, StringComparer.OrdinalIgnoreCase))
                                    continue;
                            }
                            result[key] = e.Length;
                        }
                    }
                    catch { }
                    return result;
                }
                // Prefer latest per-mod zip when no PMP is present
                var latestZip = Directory.EnumerateFiles(modDir, "backup_*.zip").OrderByDescending(f => f).FirstOrDefault();
                if (!string.IsNullOrEmpty(latestZip))
                {
                    try
                    {
                        using var za = ZipFile.OpenRead(latestZip);
                        var manifestEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, "manifest.json", StringComparison.OrdinalIgnoreCase));
                        if (manifestEntry != null)
                        {
                            using var ms = new MemoryStream();
                            using (var zs = manifestEntry.Open()) zs.CopyTo(ms);
                            ms.Position = 0;
                            var manifest = JsonSerializer.Deserialize<BackupManifest>(ms.ToArray());
                            if (manifest != null)
                            {
                                foreach (var e in manifest.Entries)
                                {
                                    System.IO.Compression.ZipArchiveEntry? zipEntry = null;
                                    if (!string.IsNullOrWhiteSpace(e.ModRelativePath))
                                    {
                                        var normalized = e.ModRelativePath.Replace('\\', '/');
                                        zipEntry = za.Entries.FirstOrDefault(z => string.Equals(z.FullName, normalized, StringComparison.OrdinalIgnoreCase));
                                    }
                                    if (zipEntry == null)
                                    {
                                        zipEntry = za.Entries.FirstOrDefault(z => string.Equals(z.Name, e.BackupFileName, StringComparison.OrdinalIgnoreCase))
                                            ?? za.Entries.FirstOrDefault(z => string.Equals(z.FullName, e.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                                        || z.FullName.EndsWith($"/{e.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                                        || z.FullName.EndsWith($"\\{e.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                                    }
                                    if (zipEntry != null)
                                    {
                                        var key = (e.ModRelativePath ?? e.BackupFileName ?? zipEntry.FullName)?.Replace('\\', '/');
                                        if (!string.IsNullOrWhiteSpace(key))
                                            result[key] = zipEntry.Length;
                                    }
                                }
                            }
                        }
                    }
                    catch { }
                    return result;
                }
            }

            // Fallback to latest session containing this mod
            foreach (var sessionDir in Directory.EnumerateDirectories(backupDirectory, "session_*").OrderByDescending(f => f))
            {
                var modSub = Path.Combine(sessionDir, modFolderName);
                var manifestPath = Path.Combine(modSub, "manifest.json");
                if (!Directory.Exists(modSub) || !File.Exists(manifestPath))
                    continue;
                try
                {
                    var manifest = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(manifestPath));
                    if (manifest == null) { break; }
                    foreach (var e in manifest.Entries)
                    {
                        var relPath = e.ModRelativePath;
                        long bytes = 0L;
                        var relCandidate = !string.IsNullOrWhiteSpace(relPath) ? Path.Combine(modSub, relPath) : string.Empty;
                        if (!string.IsNullOrWhiteSpace(relCandidate) && File.Exists(relCandidate))
                        {
                            bytes = new FileInfo(relCandidate).Length;
                        }
                        else if (!string.IsNullOrWhiteSpace(e.BackupFileName))
                        {
                            var direct = Path.Combine(modSub, e.BackupFileName);
                            if (File.Exists(direct))
                                bytes = new FileInfo(direct).Length;
                        }
                        var key = (relPath ?? e.BackupFileName ?? string.Empty).Replace('\\', '/');
                        if (!string.IsNullOrWhiteSpace(key) && bytes > 0)
                            result[key] = bytes;
                    }
                }
                catch { }
                // Use the latest session only
                break;
            }

            // No PMP and no ZIP; attempt session fallbacks above already performed
        }
        catch { }
        return result;
    }

    public Task<HashSet<string>> GetPmpConvertedRelPathsForModAsync(string modFolderName)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (!Directory.Exists(backupDirectory)) return Task.FromResult(set);
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir)) return Task.FromResult(set);
            var manifestPath = Path.Combine(modDir, "pmp_converted_manifest.json");
            if (!File.Exists(manifestPath)) return Task.FromResult(set);
            var list = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(manifestPath)) ?? new List<string>();
            foreach (var s in list)
            {
                if (string.IsNullOrWhiteSpace(s)) continue;
                set.Add(s.Replace('\\', '/'));
            }
        }
        catch { }
        return Task.FromResult(set);
    }

    // Return all backed keys for a mod across all sessions/zips (prefixed or original paths)
    public async Task<HashSet<string>> GetBackedKeysForModAsync(string modFolderName)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var overview = await GetBackupOverviewAsync().ConfigureAwait(false);
            if (overview == null || overview.Count == 0)
                return set;

            foreach (var session in overview)
            {
                foreach (var entry in session.Entries)
                {
                    // Determine mod attribution for the entry
                    var mod = entry.ModFolderName;
                    if (string.IsNullOrWhiteSpace(mod) && !string.IsNullOrWhiteSpace(entry.PrefixedOriginalPath))
                        mod = ExtractModFolderNameFromPrefixed(entry.PrefixedOriginalPath);
                    if (!string.Equals(mod, modFolderName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var key = !string.IsNullOrEmpty(entry.PrefixedOriginalPath) ? entry.PrefixedOriginalPath : entry.OriginalPath;
                    if (!string.IsNullOrWhiteSpace(key))
                        set.Add(key);
                }
            }
        }
        catch { }
        return set;
    }

    public Task<bool> HasBackupForModAsync(string modFolderName)
    {
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (!Directory.Exists(backupDirectory)) return Task.FromResult(false);

            // Check per-mod ZIPs
            var modZipDir = Path.Combine(backupDirectory, modFolderName);
            if (Directory.Exists(modZipDir))
            {
                var hasZip = Directory.EnumerateFiles(modZipDir, "backup_*.zip").Any();
                if (hasZip) return Task.FromResult(true);
                // Prefer PMP: if PMP exists, consider backup present
                var hasPmp = Directory.EnumerateFiles(modZipDir, "mod_backup_*.pmp").Any();
                if (hasPmp) return Task.FromResult(true);
            }

            // Check session folders for mod subdir with manifest
            var sessionDirs = Directory.EnumerateDirectories(backupDirectory, "session_*").OrderByDescending(f => f);
            foreach (var session in sessionDirs)
            {
                var modSub = Path.Combine(session, modFolderName);
                var manifestPath = Path.Combine(modSub, "manifest.json");
                if (Directory.Exists(modSub) && File.Exists(manifestPath))
                    return Task.FromResult(true);
            }
            return Task.FromResult(false);
        }
        catch
        {
            return Task.FromResult(false);
        }
    }

    public async Task<bool> RestoreLatestForModAsync(string modFolderName, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        try
        {
            var trace = PerfTrace.Step(_logger, $"Restore {modFolderName}");
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (!Directory.Exists(backupDirectory)) return false;
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (Directory.Exists(modDir))
            {
                var latestZip = Directory.EnumerateFiles(modDir, "backup_*.zip").OrderByDescending(f => f).FirstOrDefault();
                if (!string.IsNullOrEmpty(latestZip))
                {
                    var zipSuccess = await RestoreFromZipAsync(latestZip, progress, token).ConfigureAwait(false);
                    // If restore from zip succeeded, and mod folder became empty, remove the folder
                    if (zipSuccess)
                    {
                        // After a successful normal texture restore from zip:
                        // Only delete PMP archives if there are no remaining zip backups in the mod folder.
                        try
                        {
                            var anyZipLeft = Directory.Exists(modDir) && Directory.EnumerateFiles(modDir, "backup_*.zip").Any();
                            if (!anyZipLeft && Directory.Exists(modDir))
                            {
                                foreach (var p in Directory.EnumerateFiles(modDir, "mod_backup_*.pmp"))
                                {
                                    try
                                    {
                                        File.Delete(p);
                                        _logger.LogDebug("Deleted PMP archive after normal restore because no zip backups remain: {pmp}", p);
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger.LogWarning("Failed to delete PMP archive {pmp} after normal restore: {error}", p, ex.Message);
                                    }
                                }
                            }
                        }
                        catch { }

                        try
                        {
                            if (Directory.Exists(modDir) && !Directory.EnumerateFileSystemEntries(modDir).Any())
                            {
                                Directory.Delete(modDir, true);
                                _logger.LogDebug("Deleted empty mod backup folder {dir}", modDir);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning("Failed to delete mod backup folder {dir}: {error}", modDir, ex.Message);
                        }
                    }
                    try
                    {
                        var hasTex = await HasBackupForModAsync(modFolderName).ConfigureAwait(false);
                        var hasPmp = await HasPmpBackupForModAsync(modFolderName).ConfigureAwait(false);
                        _modStateService.UpdateBackupFlags(modFolderName, hasTex, hasPmp);
                    }
                    catch { }
                    trace.Dispose();
                    return zipSuccess;
                }
            }

            // Fallback to latest session folder containing this mod
            var sessionDirs = Directory.EnumerateDirectories(backupDirectory, "session_*").OrderByDescending(f => f);
            foreach (var session in sessionDirs)
            {
                var modSub = Path.Combine(session, modFolderName);
                var manifestPath = Path.Combine(modSub, "manifest.json");
                if (Directory.Exists(modSub) && File.Exists(manifestPath))
                {
                    var sessionSuccess = await RestoreFromSessionAsync(modSub, progress, token).ConfigureAwait(false);
                    // After a successful normal texture restore from session:
                    // Only delete PMP archives if there are no remaining zip backups in the mod folder.
                    try
                    {
                        var anyZipLeft = Directory.Exists(modDir) && Directory.EnumerateFiles(modDir, "backup_*.zip").Any();
                        if (!anyZipLeft && Directory.Exists(modDir))
                        {
                            foreach (var p in Directory.EnumerateFiles(modDir, "mod_backup_*.pmp"))
                            {
                                try
                                {
                                    File.Delete(p);
                                    _logger.LogDebug("Deleted PMP archive after normal restore (session) because no zip backups remain: {pmp}", p);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogWarning("Failed to delete PMP archive {pmp} after normal restore (session): {error}", p, ex.Message);
                                }
                            }
                        }
                    }
                    catch { }
                    // After restoring from session, also attempt to remove empty mod folder in root backup directory
                    try
                    {
                        if (Directory.Exists(modDir) && !Directory.EnumerateFileSystemEntries(modDir).Any())
                        {
                            Directory.Delete(modDir, true);
                            _logger.LogDebug("Deleted empty mod backup folder {dir}", modDir);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning("Failed to delete mod backup folder {dir}: {error}", modDir, ex.Message);
                    }
                    try
                    {
                        var hasTex = await HasBackupForModAsync(modFolderName).ConfigureAwait(false);
                        var hasPmp = await HasPmpBackupForModAsync(modFolderName).ConfigureAwait(false);
                        _modStateService.UpdateBackupFlags(modFolderName, hasTex, hasPmp);
                    }
                    catch { }
                    trace.Dispose();
                    return sessionSuccess;
                }
            }
            trace.Dispose();
            return false;
        }
        catch { return false; }
    }

    public async Task RestoreEntryAsync(BackupSessionInfo session, BackupEntryInfo entry, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        try
        {
            var targetPath = !string.IsNullOrEmpty(entry.PrefixedOriginalPath)
                ? ResolvePrefixedPath(entry.PrefixedOriginalPath)
                : entry.OriginalPath;

            if (string.IsNullOrWhiteSpace(targetPath))
            {
                _logger.LogError("Invalid target path for entry {entry}", entry.BackupFileName);
                return;
            }

            // Create target directory
            var targetDir = Path.GetDirectoryName(targetPath);
            if (!string.IsNullOrEmpty(targetDir))
            {
                Directory.CreateDirectory(targetDir);
            }

            long expectedSize = 0;
            string? backupFileToDelete = null;

            if (session.IsZip)
            {
                using var za = ZipFile.OpenRead(session.SourcePath);
                System.IO.Compression.ZipArchiveEntry? zipEntry = null;
                if (!string.IsNullOrWhiteSpace(entry.ModRelativePath))
                {
                    var normalized = entry.ModRelativePath.Replace('\\', '/');
                    zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, normalized, StringComparison.OrdinalIgnoreCase));
                }
                if (zipEntry == null)
                {
                    zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.Name, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                           || string.Equals(e.FullName, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                           || e.FullName.EndsWith($"/{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                           || e.FullName.EndsWith($"\\{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                }
                
                if (zipEntry == null)
                {
                    _logger.LogError("Backup entry not found in ZIP: {file}", entry.BackupFileName);
                    return;
                }

                expectedSize = zipEntry.Length;
                using var zs = zipEntry.Open();
                using var fs = new FileStream(targetPath, FileMode.Create, FileAccess.Write, FileShare.Read);
                await zs.CopyToAsync(fs, token).ConfigureAwait(false);
            }
            else
            {
                // Prefer the stored relative path to avoid collisions
                var backupFile = !string.IsNullOrWhiteSpace(entry.ModRelativePath)
                    ? Path.Combine(session.SourcePath, entry.ModRelativePath)
                    : Path.Combine(session.SourcePath, entry.BackupFileName);

                if (!File.Exists(backupFile))
                {
                    // Fallback to legacy layout under mod subdirectory
                    if (!string.IsNullOrEmpty(entry.ModFolderName))
                    {
                        var modBackupFile = Path.Combine(session.SourcePath, entry.ModFolderName, entry.BackupFileName);
                        if (File.Exists(modBackupFile))
                            backupFile = modBackupFile;
                    }
                }

                if (!File.Exists(backupFile))
                {
                    _logger.LogError("Backup file not found: {file}", backupFile);
                    return;
                }

                var backupInfo = new FileInfo(backupFile);
                expectedSize = backupInfo.Length;
                File.Copy(backupFile, targetPath, overwrite: true);
                backupFileToDelete = backupFile;
            }

            // Verify the restored file
            var verification = await VerifyRestoredFileAsync(targetPath, expectedSize).ConfigureAwait(false);
            if (!verification.Success)
            {
                _logger.LogError("Verification failed for {file}: {error}", targetPath, verification.ErrorMessage);
                
                // Clean up failed restore
                try
                {
                    if (File.Exists(targetPath))
                        File.Delete(targetPath);
                }
                catch (Exception ex)
                {
                    _logger.LogError("Failed to clean up failed restore {file}: {error}", targetPath, ex.Message);
                }
                return;
            }

            // Only delete backup file after successful verification (for non-zip sessions)
            if (!string.IsNullOrEmpty(backupFileToDelete))
            {
                try
                {
                    File.Delete(backupFileToDelete);
                    _logger.LogDebug("Deleted backup file {file}", backupFileToDelete);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to delete backup file {file}: {error}", backupFileToDelete, ex.Message);
                }
            }

            progress?.Report((targetPath, 1, 1));
            _logger.LogDebug("Successfully restored and verified single backup {file} to {path}", entry.BackupFileName, targetPath);
            
            // Defer player redraw until the overall restore completes
        }
        catch (Exception ex)
        {
            _logger.LogError("Failed to restore entry {file}: {error}", entry.BackupFileName, ex.Message);
            throw;
        }
    }

    public async Task RestoreLatestAsync(IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        var backupDirectory = _configService.Current.BackupFolderPath;
        if (!Directory.Exists(backupDirectory)) return;

        // Prefer latest zip; fallback to latest session directory with manifest
        var latestZip = Directory.EnumerateFiles(backupDirectory, "backup_*.zip")
                                 .OrderByDescending(f => f)
                                 .FirstOrDefault();
        if (!string.IsNullOrEmpty(latestZip))
        {
            await RestoreFromZipAsync(latestZip, progress, token).ConfigureAwait(false);
            return;
        }

        var latestSession = Directory.EnumerateDirectories(backupDirectory, "session_*")
                                     .OrderByDescending(f => f)
                                     .FirstOrDefault();
        if (!string.IsNullOrEmpty(latestSession))
        {
            await RestoreFromSessionAsync(latestSession, progress, token).ConfigureAwait(false);
        }
    }

    public async Task<bool> RestoreFromZipAsync(string zipPath, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        var trace = PerfTrace.Step(_logger, $"RestoreFromZip {Path.GetFileName(zipPath)}");
        var tempDir = Path.Combine(Path.GetTempPath(), "ShrinkU", "restore", Path.GetFileNameWithoutExtension(zipPath));
        try
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true);
        }
        catch { }

        Directory.CreateDirectory(tempDir);
        // Offload zip extraction to a background thread and honor cancellation
        await Task.Run(() =>
        {
            if (token.IsCancellationRequested) return;
            ZipFile.ExtractToDirectory(zipPath, tempDir, overwriteFiles: true);
        }, token).ConfigureAwait(false);
        var success = await RestoreFromSessionAsync(tempDir, progress, token).ConfigureAwait(false);
        try
        {
            Directory.Delete(tempDir, true);
        }
        catch { }

        // Remove the zip backup only after a successful restore
        if (success)
        {
            try { File.Delete(zipPath); } catch { }
        }
        trace.Dispose();
        return success;
    }

    // Allow callers (UI) to trigger a redraw after all restores have finished
    public void RedrawPlayer()
    {
        try
        {
            _penumbraIpc.RedrawPlayer();
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to redraw player: {error}", ex.Message);
        }
    }

    public async Task<bool> RestoreFromSessionAsync(string sessionPath, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        var trace = PerfTrace.Step(_logger, $"RestoreFromSession {Path.GetFileName(sessionPath)}");
        var restoredFiles = new List<string>();
        var backupFilesToDelete = new List<string>();
        var hasErrors = false;

        try
        {
            var manifestPath = Path.Combine(sessionPath, "manifest.json");
            if (!File.Exists(manifestPath))
            {
                _logger.LogWarning("Manifest not found at {path}", manifestPath);
                return false;
            }

            var manifest = JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(manifestPath));
            if (manifest == null || manifest.Entries.Count == 0)
            {
                _logger.LogWarning("Empty or invalid manifest at {path}", manifestPath);
                return false;
            }

            _logger.LogDebug("Starting restore of {count} files from session {session}", manifest.Entries.Count, sessionPath);

            int current = 0;
            foreach (var entry in manifest.Entries)
            {
                if (token.IsCancellationRequested)
                {
                    _logger.LogDebug("Restore cancelled by user");
                    break;
                }

                // Prefer the stored relative path inside the session (or mod subdir)
                var backupFile = !string.IsNullOrWhiteSpace(entry.ModRelativePath)
                    ? Path.Combine(sessionPath, entry.ModRelativePath)
                    : Path.Combine(sessionPath, entry.BackupFileName);

                if (!File.Exists(backupFile))
                {
                    // Fallback for legacy manifests
                    if (!string.IsNullOrEmpty(entry.ModFolderName))
                    {
                        var modBackupFile = Path.Combine(sessionPath, entry.ModFolderName, entry.BackupFileName);
                        if (File.Exists(modBackupFile))
                            backupFile = modBackupFile;
                    }
                }

                if (!File.Exists(backupFile))
                {
                    _logger.LogError("Backup file not found: {file}", backupFile);
                    hasErrors = true;
                    continue;
                }

                try
                {
                    var targetPath = !string.IsNullOrEmpty(entry.PrefixedOriginalPath)
                        ? ResolvePrefixedPath(entry.PrefixedOriginalPath)
                        : entry.OriginalPath;

                    if (string.IsNullOrWhiteSpace(targetPath))
                    {
                        _logger.LogError("Invalid target path for entry {entry}", entry.BackupFileName);
                        hasErrors = true;
                        continue;
                    }

                    // Get backup file size for verification
                    var backupInfo = new FileInfo(backupFile);
                    var expectedSize = backupInfo.Length;

                    // Create target directory
                    var targetDir = Path.GetDirectoryName(targetPath);
                    if (!string.IsNullOrEmpty(targetDir))
                    {
                        Directory.CreateDirectory(targetDir);
                    }

                    // Perform the restore off the UI thread and honor cancellation
                    await Task.Run(() =>
                    {
                        if (token.IsCancellationRequested) return;
                        File.Copy(backupFile, targetPath, overwrite: true);
                    }, token).ConfigureAwait(false);
                    restoredFiles.Add(targetPath);

                    // Verify the restored file
                    var verification = await VerifyRestoredFileAsync(targetPath, expectedSize).ConfigureAwait(false);
                    if (!verification.Success)
                    {
                        _logger.LogError("Verification failed for {file}: {error}", targetPath, verification.ErrorMessage);
                        hasErrors = true;
                        continue;
                    }

                    // Mark backup file for deletion only after successful verification
                    backupFilesToDelete.Add(backupFile);

                    progress?.Report((targetPath, ++current, manifest.Entries.Count));
                    _logger.LogDebug("Successfully restored and verified {file}", targetPath);
                }
                catch (Exception ex)
                {
                    _logger.LogError("Failed to restore {file}: {error}", entry.BackupFileName, ex.Message);
                    hasErrors = true;
                }

                // Yield to let UI remain responsive between files
                await Task.Yield();
            }

            // If there were errors, perform rollback
            if (hasErrors)
            {
                _logger.LogError("Restore had errors, performing rollback");
                await RollbackRestoredFilesAsync(restoredFiles).ConfigureAwait(false);
                return false;
            }

            // Only delete backup files if all restores were successful
            foreach (var backupFile in backupFilesToDelete)
            {
                try
                {
                    await Task.Run(() =>
                    {
                        if (token.IsCancellationRequested) return;
                        File.Delete(backupFile);
                    }, token).ConfigureAwait(false);
                    _logger.LogDebug("Deleted backup file {file}", backupFile);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to delete backup file {file}: {error}", backupFile, ex.Message);
                }
            }

            // Clean up manifest and empty directories
            try
            {
                await Task.Run(() =>
                {
                    if (token.IsCancellationRequested) return;
                    File.Delete(manifestPath);
                }, token).ConfigureAwait(false);
                _logger.LogDebug("Deleted manifest {manifest}", manifestPath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to delete manifest {manifest}: {error}", manifestPath, ex.Message);
            }

            try
            {
                if (Directory.Exists(sessionPath) && !Directory.EnumerateFileSystemEntries(sessionPath).Any())
                {
                    await Task.Run(() =>
                    {
                        if (token.IsCancellationRequested) return;
                        Directory.Delete(sessionPath, true);
                    }, token).ConfigureAwait(false);
                    _logger.LogDebug("Deleted empty session directory {dir}", sessionPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to delete session directory {dir}: {error}", sessionPath, ex.Message);
            }

            _logger.LogDebug("Successfully restored {count} files from session", restoredFiles.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError("Critical error during restore: {error}", ex.Message);
            await RollbackRestoredFilesAsync(restoredFiles).ConfigureAwait(false);
            return false;
        }
        trace.Dispose();
        return true;
    }

    public async Task RestoreAllBackupsAsync(IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        var backupDirectory = _configService.Current.BackupFolderPath;
        if (!Directory.Exists(backupDirectory)) return;

        // Restore all ZIP sessions first, then any remaining session folders
        var zipFiles = Directory.EnumerateFiles(backupDirectory, "backup_*.zip")
                                .OrderBy(f => f)
                                .ToList();
        foreach (var zip in zipFiles)
        {
            if (token.IsCancellationRequested) break;
            try
            {
                _ = await RestoreFromZipAsync(zip, progress, token).ConfigureAwait(false);
            }
            catch { }
        }

        var sessionDirs = Directory.EnumerateDirectories(backupDirectory, "session_*")
                                   .OrderBy(f => f)
                                   .ToList();
        foreach (var session in sessionDirs)
        {
            if (token.IsCancellationRequested) break;
            try
            {
                await RestoreFromSessionAsync(session, progress, token).ConfigureAwait(false);
            }
            catch { }
        }
    }

    // Compute SHA256 hash of a file for verification
    private async Task<string> ComputeFileHashAsync(string filePath)
    {
        try
        {
            using var sha256 = SHA256.Create();
            using var stream = File.OpenRead(filePath);
            var hash = await sha256.ComputeHashAsync(stream).ConfigureAwait(false);
            return Convert.ToHexString(hash);
        }
        catch
        {
            return string.Empty;
        }
    }

    // Verify that a restored file matches expected criteria
    private async Task<RestoreVerificationResult> VerifyRestoredFileAsync(string filePath, long expectedSize, string? expectedHash = null)
    {
        var result = new RestoreVerificationResult
        {
            FilePath = filePath,
            ExpectedSize = expectedSize
        };

        try
        {
            result.FileExists = File.Exists(filePath);
            if (!result.FileExists)
            {
                result.ErrorMessage = "File does not exist after restore";
                return result;
            }

            var fileInfo = new FileInfo(filePath);
            result.ActualSize = fileInfo.Length;
            result.SizeMatches = result.ActualSize == expectedSize;

            if (!result.SizeMatches)
            {
                result.ErrorMessage = $"Size mismatch: expected {expectedSize} bytes, got {result.ActualSize} bytes";
                return result;
            }

            // Optional hash verification if provided
            if (!string.IsNullOrEmpty(expectedHash))
            {
                var actualHash = await ComputeFileHashAsync(filePath).ConfigureAwait(false);
                if (!string.Equals(actualHash, expectedHash, StringComparison.OrdinalIgnoreCase))
                {
                    result.ErrorMessage = $"Hash mismatch: expected {expectedHash}, got {actualHash}";
                    return result;
                }
            }

            result.Success = true;
        }
        catch (Exception ex)
        {
            result.ErrorMessage = $"Verification failed: {ex.Message}";
        }

        return result;
    }

    // Rollback a list of restored files
    private async Task RollbackRestoredFilesAsync(List<string> restoredFiles)
    {
        foreach (var file in restoredFiles)
        {
            try
            {
                if (File.Exists(file))
                {
                    File.Delete(file);
                    _logger.LogDebug("Rolled back restored file {file}", file);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Failed to rollback file {file}: {error}", file, ex.Message);
            }
        }
        await Task.CompletedTask;
    }
    // Compute savings for a single mod fast (prefers PMP/ZIP of that mod; avoids global scans)
    public async Task<ModSavingsStats> ComputeSavingsForModAsync(string modFolderName)
    {
        var stats = new ModSavingsStats();
        try
        {
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (string.IsNullOrWhiteSpace(backupDirectory) || string.IsNullOrWhiteSpace(modFolderName))
                return stats;
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (!Directory.Exists(modDir))
                return stats;

            // Prefer latest PMP for accurate original sizes
            string? latestPmp = null;
            try { latestPmp = Directory.EnumerateFiles(modDir, "mod_backup_*.pmp").OrderByDescending(f => f).FirstOrDefault(); } catch { }
            List<string>? convertedRel = null;
            try
            {
                var convertedManifestPath = Path.Combine(modDir, "pmp_converted_manifest.json");
                if (File.Exists(convertedManifestPath))
                    convertedRel = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(convertedManifestPath));
            }
            catch { }

            if (!string.IsNullOrEmpty(latestPmp) && File.Exists(latestPmp))
            {
                try
                {
                    using var za = ZipFile.OpenRead(latestPmp);
                    foreach (var e in za.Entries)
                    {
                        var name = e.FullName?.Replace('\\', '/');
                        if (string.IsNullOrWhiteSpace(name) || name.EndsWith("/", StringComparison.Ordinal))
                            continue;
                        if (convertedRel != null && convertedRel.Count > 0 && !convertedRel.Contains(name, StringComparer.OrdinalIgnoreCase))
                            continue;
                        stats.OriginalBytes += e.Length;
                        stats.ComparedFiles += 1;
                    }
                }
                catch { }
            }
            else
            {
                // Fallback: use latest ZIP for the mod
                string? latestZip = null;
                try { latestZip = Directory.EnumerateFiles(modDir, "backup_*.zip").OrderByDescending(f => f).FirstOrDefault(); } catch { }
                if (!string.IsNullOrEmpty(latestZip) && File.Exists(latestZip))
                {
                    try
                    {
                        using var za = ZipFile.OpenRead(latestZip);
                        foreach (var e in za.Entries)
                        {
                            var name = e.FullName?.Replace('\\', '/');
                            if (string.IsNullOrWhiteSpace(name) || name.EndsWith("/", StringComparison.Ordinal))
                                continue;
                            stats.OriginalBytes += e.Length;
                            stats.ComparedFiles += 1;
                        }
                    }
                    catch { }
                }
            }

            // Compute current bytes from live mod folder
            try
            {
                var modAbs = GetModAbsolutePath(modFolderName);
                if (!string.IsNullOrWhiteSpace(modAbs) && Directory.Exists(modAbs))
                {
                    foreach (var file in Directory.EnumerateFiles(modAbs!, "*", SearchOption.AllDirectories))
                    {
                        try
                        {
                            if (convertedRel != null && convertedRel.Count > 0)
                            {
                                var rel = Path.GetRelativePath(modAbs!, file).Replace('\\', '/');
                                if (!convertedRel.Contains(rel, StringComparer.OrdinalIgnoreCase))
                                    continue;
                            }
                            var fi = new FileInfo(file);
                            stats.CurrentBytes += fi.Length;
                        }
                        catch { }
                    }
                }
            }
            catch { }

            try { _modStateService.UpdateSavings(modFolderName, stats.OriginalBytes, stats.CurrentBytes, stats.ComparedFiles); } catch { }
        }
        catch { }
        return stats;
    }
}
