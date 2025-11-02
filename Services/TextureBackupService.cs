using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace ShrinkU.Services;

public sealed class TextureBackupService
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly PenumbraIpc _penumbraIpc;

    public TextureBackupService(ILogger logger, ShrinkUConfigService configService, PenumbraIpc penumbraIpc)
    {
        _logger = logger;
        _configService = configService;
        _penumbraIpc = penumbraIpc;
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
        public string? ModFolderName { get; set; }
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
        public string? ModFolderName { get; set; }
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

    // Resolve absolute path to a mod directory from its folder name
    public string? GetModAbsolutePath(string modFolder)
    {
        try
        {
            var root = _penumbraIpc.ModDirectory;
            if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(modFolder))
                return null;
            return Path.Combine(root, modFolder);
        }
        catch
        {
            return null;
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

    private static string? ExtractModFolderName(string prefixedPath)
    {
        try
        {
            var p = prefixedPath;
            if (!p.StartsWith("{penumbra}", StringComparison.OrdinalIgnoreCase))
                return null;
            p = p.Substring("{penumbra}".Length);
            p = p.TrimStart('\\', '/');
            var idx = p.IndexOfAny(new[] { '\\', '/' });
            if (idx <= 0)
                return null;
            return p.Substring(0, idx);
        }
        catch
        {
            return null;
        }
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

    public async Task BackupAsync(Dictionary<string, string[]> textures, IProgress<(string, int, int)>? progress, CancellationToken token)
    {
        var backupDirectory = _configService.Current.BackupFolderPath;
        var timestamp = DateTime.Now;
        var sessionName = $"session_{timestamp:yyyyMMdd_HHmmss}";
        var sessionDir = Path.Combine(backupDirectory, sessionName);

        try
        {
            Directory.CreateDirectory(backupDirectory);
            Directory.CreateDirectory(sessionDir);
        }
        catch
        {
            // Ignore directory creation issues
        }

        int current = 0;
        var manifest = new BackupManifest { Entries = new List<BackupManifestEntry>() };
        var entriesByMod = new Dictionary<string, List<BackupManifestEntry>>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in textures)
        {
            if (token.IsCancellationRequested) break;
            var source = kvp.Key;
            var prefixed = BuildPrefixedPath(source);
            var modName = ExtractModFolderName(prefixed) ?? "_unknown";
            var modDirInSession = Path.Combine(sessionDir, modName);
            try { Directory.CreateDirectory(modDirInSession); } catch { }
            var target = Path.Combine(modDirInSession, Path.GetFileName(source));
            try
            {
                File.Copy(source, target, overwrite: true);
                progress?.Report((source, ++current, textures.Count));
                _logger.LogDebug("Backed up texture {path}", source);
                var entry = new BackupManifestEntry
                {
                    OriginalPath = source,
                    PrefixedOriginalPath = prefixed,
                    BackupFileName = Path.GetFileName(source),
                    OriginalFileName = Path.GetFileName(source),
                    ModFolderName = ExtractModFolderName(prefixed),
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
                // Ignore backup errors to keep conversion flowing
            }
            await Task.Yield();
        }

        // Write manifest for the session
        try
        {
            var manifestPath = Path.Combine(sessionDir, "manifest.json");
            File.WriteAllText(manifestPath, JsonSerializer.Serialize(manifest));
        }
        catch { }

        // Create per-mod ZIP archives if enabled
        if (_configService.Current.EnableZipCompressionForBackups)
        {
            foreach (var (mod, entries) in entriesByMod)
            {
                try
                {
                    var modBackupDir = Path.Combine(backupDirectory, mod);
                    try { Directory.CreateDirectory(modBackupDir); } catch { }

                    // Build a manifest limited to this mod
                    var modManifest = new BackupManifest { Entries = entries.Select(e => new BackupManifestEntry
                    {
                        OriginalPath = e.OriginalPath,
                        PrefixedOriginalPath = e.PrefixedOriginalPath,
                        BackupFileName = e.BackupFileName,
                        OriginalFileName = e.OriginalFileName,
                        ModFolderName = e.ModFolderName,
                        CreatedUtc = e.CreatedUtc,
                    }).ToList() };

                    // Write manifest into mod subdir of the session so it gets included in the ZIP
                    var modSessionSubdir = Path.Combine(sessionDir, mod);
                    try
                    {
                        File.WriteAllText(Path.Combine(modSessionSubdir, "manifest.json"), JsonSerializer.Serialize(modManifest));
                    }
                    catch { }

                    var zipPath = Path.Combine(modBackupDir, $"backup_{timestamp:yyyyMMdd_HHmmss}.zip");
                    if (File.Exists(zipPath))
                    {
                        try { File.Delete(zipPath); } catch { }
                    }
                    ZipFile.CreateFromDirectory(modSessionSubdir, zipPath);
                    _logger.LogDebug("Created mod backup ZIP {zip}", zipPath);
                }
                catch
                {
                    // Ignore zip errors for individual mods
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
    }

    public async Task<List<BackupSessionInfo>> GetBackupOverviewAsync()
    {
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
                                ModFolderName = e.ModFolderName,
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
                            ModFolderName = e.ModFolderName,
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

    // Compute size comparison between backups and current files in Penumbra
    public async Task<BackupSavingsStats> ComputeSavingsAsync()
    {
        var stats = new BackupSavingsStats();
        try
        {
            var overview = await GetBackupOverviewAsync().ConfigureAwait(false);
            if (overview.Count == 0)
                return stats;

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
                            // Try simple name match first
                            var zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.Name, entry.BackupFileName, StringComparison.OrdinalIgnoreCase));
                            if (zipEntry == null)
                            {
                                // Fallback: full name match when backups are inside a folder
                                zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"/{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"\\{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                            }
                            if (zipEntry != null)
                                backupBytes = zipEntry.Length;
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
            if (overview.Count == 0)
                return result;

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
                        modFolder = ExtractModFolderName(entry.PrefixedOriginalPath);
                    if (string.IsNullOrWhiteSpace(modFolder))
                        continue; // skip entries we cannot attribute to a mod

                    long backupBytes = 0L;
                    try
                    {
                        if (session.IsZip)
                        {
                            using var za = ZipFile.OpenRead(session.SourcePath);
                            var zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.Name, entry.BackupFileName, StringComparison.OrdinalIgnoreCase));
                            if (zipEntry == null)
                            {
                                zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.FullName, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"/{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                                        || e.FullName.EndsWith($"\\{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                            }
                            if (zipEntry != null)
                                backupBytes = zipEntry.Length;
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
                    }
                }
            }
        }
        catch
        {
            // Swallow to keep UI resilient
        }
        return result;
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
            var backupDirectory = _configService.Current.BackupFolderPath;
            if (!Directory.Exists(backupDirectory)) return false;
            var modDir = Path.Combine(backupDirectory, modFolderName);
            if (Directory.Exists(modDir))
            {
                var latestZip = Directory.EnumerateFiles(modDir, "backup_*.zip").OrderByDescending(f => f).FirstOrDefault();
                if (!string.IsNullOrEmpty(latestZip))
                {
                    var zipSuccess = await RestoreFromZipAsync(latestZip, progress, token).ConfigureAwait(false);
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
                    return sessionSuccess;
                }
            }
        }
        catch { return false; }
        return false;
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
                var zipEntry = za.Entries.FirstOrDefault(e => string.Equals(e.Name, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                           || string.Equals(e.FullName, entry.BackupFileName, StringComparison.OrdinalIgnoreCase)
                                                           || e.FullName.EndsWith($"/{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase)
                                                           || e.FullName.EndsWith($"\\{entry.BackupFileName}", StringComparison.OrdinalIgnoreCase));
                
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
                var backupFile = Path.Combine(session.SourcePath, entry.BackupFileName);
                if (!string.IsNullOrEmpty(entry.ModFolderName))
                {
                    var modBackupFile = Path.Combine(session.SourcePath, entry.ModFolderName, entry.BackupFileName);
                    if (File.Exists(modBackupFile))
                        backupFile = modBackupFile;
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

                var backupFile = Path.Combine(sessionPath, entry.BackupFileName);
                if (!string.IsNullOrEmpty(entry.ModFolderName))
                {
                    var modBackupFile = Path.Combine(sessionPath, entry.ModFolderName, entry.BackupFileName);
                    if (File.Exists(modBackupFile))
                        backupFile = modBackupFile;
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
}