using Dalamud.Plugin;
using Microsoft.Extensions.Logging;
using Penumbra.Api.Enums;
using Penumbra.Api.Helpers;
using Penumbra.Api.IpcSubscribers;
using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;

namespace ShrinkU.Services;

public sealed class PenumbraIpc : IDisposable
{
    private readonly IDalamudPluginInterface _pi;
    private readonly ILogger _logger;

    private readonly GetEnabledState _penumbraEnabled;
    private readonly GetModDirectory _penumbraResolveModDir;
    private readonly ConvertTextureFile _penumbraConvertTextureFile;
    private readonly RedrawObject _penumbraRedraw;
    private readonly GetPlayerResourcePaths _penumbraGetPlayerResourcePaths;
    private readonly ResolvePlayerPathsAsync _penumbraResolvePlayerPaths;
    private readonly GetCollections _penumbraGetCollections;
    private readonly GetCollection _penumbraGetCollection;
    private readonly GetAllModSettings _penumbraGetAllModSettings;
    private readonly GetModList _penumbraGetModList;
    private readonly GetModPath _penumbraGetModPath;
    private readonly Penumbra.Api.IpcSubscribers.SetModPath? _penumbraSetModPath;
    private readonly OpenMainWindow _penumbraOpenMainWindow;
    private readonly CloseMainWindow _penumbraCloseMainWindow;
    private readonly Penumbra.Api.IpcSubscribers.AddMod _penumbraAddMod;
    private readonly object? _penumbraDeleteMod;

    // Event subscribers (disposed with plugin lifetime)
    private readonly IDisposable? _subModAdded;
    private readonly IDisposable? _subModDeleted;
    private readonly IDisposable? _subModMoved;
    private readonly IDisposable? _subModSettingChanged;
    private readonly IDisposable? _subEnabledChange;
    private readonly IDisposable? _subInitialized;
    private readonly IDisposable? _subDisposed;
    private readonly IDisposable? _subGameObjectRedrawn;
    private CancellationTokenSource? _pathWatchCts;
    private Task? _pathWatchTask;
    private Dictionary<string, string> _lastPaths = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _modSettingLogLock = new();
    private CancellationTokenSource? _modSettingLogDebounceCts;
    private int _modSettingLogBurstCount = 0;
    private ModSettingChange _modSettingLogLastChange = default;
    private string _modSettingLogLastDir = string.Empty;

    public PenumbraIpc(IDalamudPluginInterface pi, ILogger logger)
    {
        _pi = pi;
        _logger = logger;

        _penumbraEnabled = new GetEnabledState(pi);
        _penumbraResolveModDir = new GetModDirectory(pi);
        _penumbraConvertTextureFile = new ConvertTextureFile(pi);
        _penumbraRedraw = new RedrawObject(pi);
        _penumbraGetPlayerResourcePaths = new GetPlayerResourcePaths(pi);
        _penumbraResolvePlayerPaths = new ResolvePlayerPathsAsync(pi);
        _penumbraGetCollections = new GetCollections(pi);
        _penumbraGetCollection = new GetCollection(pi);
        _penumbraGetAllModSettings = new GetAllModSettings(pi);
        _penumbraGetModList = new GetModList(pi);
        _penumbraGetModPath = new GetModPath(pi);
        _penumbraOpenMainWindow = new OpenMainWindow(pi);
        _penumbraCloseMainWindow = new CloseMainWindow(pi);
        _penumbraAddMod = new Penumbra.Api.IpcSubscribers.AddMod(pi);
        try { _penumbraSetModPath = new Penumbra.Api.IpcSubscribers.SetModPath(pi); } catch { _penumbraSetModPath = null; }
        try
        {
            var t = Type.GetType("Penumbra.Api.IpcSubscribers.DeleteMod, Penumbra.Api")
                    ?? Type.GetType("Penumbra.Api.IpcSubscribers.RemoveMod, Penumbra.Api");
            _penumbraDeleteMod = t != null ? Activator.CreateInstance(t, pi) : null;
        }
        catch { _penumbraDeleteMod = null; }

        APIAvailable = CheckApi();

        try
        {
            var mvid = typeof(PenumbraIpc).Module?.ModuleVersionId.ToString() ?? "unknown";
            _logger.LogDebug("PenumbraIpc initialized: DIAG-v3 mvid={mvid} instance={id}", mvid, GetHashCode());
        }
        catch { }

        // Subscribe to Penumbra broadcast events for mod and state changes
        try
        {
            _subModAdded = Penumbra.Api.IpcSubscribers.ModAdded.Subscriber(pi, dir =>
            {
                _logger.LogDebug("Penumbra mod added: {dir}", dir);
                ModAdded?.Invoke(dir);
                _logger.LogDebug("Penumbra ModsChanged broadcast: source=ModAdded for {dir}", dir);
                ModsChanged?.Invoke();
            });

            _subModDeleted = Penumbra.Api.IpcSubscribers.ModDeleted.Subscriber(pi, dir =>
            {
                _logger.LogDebug("Penumbra mod deleted: {dir}", dir);
                ModDeleted?.Invoke(dir);
                _logger.LogDebug("Penumbra ModsChanged broadcast: source=ModDeleted for {dir}", dir);
                ModsChanged?.Invoke();
            });

            _subModMoved = Penumbra.Api.IpcSubscribers.ModMoved.Subscriber(pi, (oldDir, newDir) =>
            {
                _logger.LogDebug("Penumbra mod moved: {oldDir} -> {newDir}", oldDir, newDir);
                ModMoved?.Invoke(oldDir, newDir);
                // Moving a mod changes its hierarchical path; broadcast ModsChanged so consumers refresh paths
                _logger.LogDebug("Penumbra ModsChanged broadcast: source=ModMoved for {oldDir} -> {newDir}", oldDir, newDir);
                ModsChanged?.Invoke();
            });

            _subModSettingChanged = Penumbra.Api.IpcSubscribers.ModSettingChanged.Subscriber(pi,
                (change, collectionId, modDir, inherited) =>
                {
                    // Coalesce logs for rapid cascaded ModSettingChanged events from a single toggle.
                    lock (_modSettingLogLock)
                    {
                        try { _modSettingLogDebounceCts?.Cancel(); } catch { }
                        try { _modSettingLogDebounceCts?.Dispose(); } catch { }
                        _modSettingLogDebounceCts = new CancellationTokenSource();
                        var token = _modSettingLogDebounceCts.Token;
                        _modSettingLogBurstCount++;
                        _modSettingLogLastChange = change;
                        _modSettingLogLastDir = modDir;
                        Task.Run(async () =>
                        {
                            try
                            {
                                await Task.Delay(300, token).ConfigureAwait(false);
                                if (token.IsCancellationRequested)
                                    return;
                                var count = _modSettingLogBurstCount;
                                var lastChange = _modSettingLogLastChange;
                                var lastDir = _modSettingLogLastDir;
                                _modSettingLogBurstCount = 0;
                                _logger.LogDebug("Penumbra mod setting changed: {change} for {modDir} x{count}", lastChange, lastDir, count);
                            }
                            catch (TaskCanceledException) { }
                            catch { }
                        });
                    }
                    // Forward specific change details to consumers on every event to preserve behavior.
                    ModSettingChanged?.Invoke(change, collectionId, modDir, inherited);
                });

            _subEnabledChange = Penumbra.Api.IpcSubscribers.EnabledChange.Subscriber(pi, enabled =>
            {
                _logger.LogDebug("Penumbra enabled state changed: {enabled}", enabled);
                PenumbraEnabledChanged?.Invoke(enabled);
                // Re-check API availability and mod root when the plugin state changes.
                APIAvailable = CheckApi();
                _logger.LogDebug("Penumbra enabled change processed; no ModsChanged broadcast");
            });

            _subInitialized = Penumbra.Api.IpcSubscribers.Initialized.Subscriber(pi, () =>
            {
                _logger.LogDebug("Penumbra API initialized");
                APIAvailable = CheckApi();
                _logger.LogDebug("Penumbra ModsChanged broadcast: source=Initialized");
                ModsChanged?.Invoke();
                // Start path watcher when API is ready
                StartPathWatcher();
            });

            _subDisposed = Penumbra.Api.IpcSubscribers.Disposed.Subscriber(pi, () =>
            {
                _logger.LogDebug("Penumbra API disposed");
                APIAvailable = false;
                _logger.LogDebug("Penumbra ModsChanged broadcast: source=Disposed");
                ModsChanged?.Invoke();
                // Stop path watcher when API is disposed
                StopPathWatcher();
            });

            // Subscribe to redraws to detect changes of currently used player resources (gear swaps, materials, etc.)
            _subGameObjectRedrawn = Penumbra.Api.IpcSubscribers.GameObjectRedrawn.Subscriber(pi, (ptr, index) =>
            {
                try
                {
                    _logger.LogDebug("Penumbra object redrawn: index={index}", index);
                    try { RawGameObjectRedrawn?.Invoke(ptr, index); } catch { }
                    try { PlayerResourcesChanged?.Invoke(); } catch { }
                }
                catch { }
            });
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to subscribe to Penumbra events");
        }

        // If API is already available at construction, start the path watcher.
        if (APIAvailable)
        {
            StartPathWatcher();
        }
    }

    public void Dispose()
    {
        try { _subModAdded?.Dispose(); } catch { }
        try { _subModDeleted?.Dispose(); } catch { }
        try { _subModMoved?.Dispose(); } catch { }
        try { _subModSettingChanged?.Dispose(); } catch { }
        try { _subEnabledChange?.Dispose(); } catch { }
        try { _subInitialized?.Dispose(); } catch { }
        try { _subDisposed?.Dispose(); } catch { }
        try { _subGameObjectRedrawn?.Dispose(); } catch { }
        StopPathWatcher();
        _logger.LogDebug("Penumbra IPC subscribers disposed");
    }

    public bool APIAvailable { get; private set; }
    public string? ModDirectory { get; private set; }

    // Event surface for consumers
    public event Action? ModsChanged;
    public event Action<string>? ModAdded;
    public event Action<string>? ModDeleted;
    public event Action<string, string>? ModMoved;
    public event Action<bool>? PenumbraEnabledChanged;
    public event Action<ModSettingChange, Guid, string, bool>? ModSettingChanged;
    public event Action<IntPtr, int>? RawGameObjectRedrawn; // low-level event for consumers needing object info
    public event Action? PlayerResourcesChanged; // high-level signal for UI to refresh used-only set

    private bool CheckApi()
    {
        try
        {
            var enabled = _penumbraEnabled.Invoke();
            ModDirectory = _penumbraResolveModDir.Invoke();
            _logger.LogDebug("Penumbra API enabled: {enabled}, ModDir: {moddir}", enabled, ModDirectory ?? "<null>");
            return enabled;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Penumbra API not available");
            return false;
        }
    }

    // Start a lightweight background watcher to detect mod path changes initiated from Penumbra's UI.
    private void StartPathWatcher()
    {
        try
        {
            StopPathWatcher();
            _pathWatchCts = new CancellationTokenSource();
            var token = _pathWatchCts.Token;
            _pathWatchTask = Task.Run(async () =>
            {
                // Initial snapshot
                try
                {
                    _lastPaths = await GetModPathsAsync().ConfigureAwait(false);
                }
                catch { _lastPaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); }

                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(2000, token).ConfigureAwait(false);
                        if (token.IsCancellationRequested)
                            break;

                        var current = await GetModPathsAsync().ConfigureAwait(false);
                        bool anyChange = false;

                        // Detect moved mods
                        foreach (var kv in current)
                        {
                            var dir = kv.Key;
                            var path = kv.Value ?? string.Empty;
                            if (_lastPaths.TryGetValue(dir, out var oldPath))
                            {
                                if (!string.Equals(oldPath, path, StringComparison.Ordinal))
                                {
                                    anyChange = true;
                                    _logger.LogDebug("Penumbra mod path changed (watcher): {dir} : {old} -> {new}", dir, oldPath, path);
                                }
                            }
                            else
                            {
                                // Newly discovered mod
                                anyChange = true;
                                _logger.LogDebug("Penumbra mod path discovered (watcher): {dir} : {new}", dir, path);
                            }
                        }

                        // Detect removed mods from snapshot
                        foreach (var dir in _lastPaths.Keys)
                        {
                            if (!current.ContainsKey(dir))
                            {
                                anyChange = true;
                                _logger.LogDebug("Penumbra mod path missing (watcher): {dir} previously {old}", dir, _lastPaths[dir]);
                            }
                        }

                        if (anyChange)
                        {
                            _lastPaths = current;
                            // Broadcast a single ModsChanged to refresh UI
                            _logger.LogDebug("Penumbra ModsChanged broadcast: source=PathWatcher");
                            try { ModsChanged?.Invoke(); } catch { }
                        }
                    }
                    catch (TaskCanceledException) { }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Path watcher iteration failed");
                    }
                }
            }, token);
            _logger.LogDebug("Penumbra path watcher started");
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to start path watcher");
        }
    }

    private void StopPathWatcher()
    {
        try
        {
            _pathWatchCts?.Cancel();
            _pathWatchCts?.Dispose();
            _pathWatchCts = null;
        }
        catch { }
        _pathWatchTask = null;
        _logger.LogDebug("Penumbra path watcher stopped");
    }

    public async Task ConvertTextureFilesAsync(ILogger logger, Dictionary<string, string[]> textures, IProgress<(string, int)> progress, CancellationToken token, bool redrawAfter = true)
    {
        if (!APIAvailable)
        {
            _logger.LogDebug("Penumbra API unavailable; skipping conversion");
            return;
        }

        int current = 0;
        foreach (var kvp in textures)
        {
            if (token.IsCancellationRequested)
                break;

            var source = kvp.Key;
            progress.Report((source, ++current));

            try
            {
                var t = _penumbraConvertTextureFile.Invoke(source, source, TextureType.Bc7Tex, mipMaps: true);
                await t.ConfigureAwait(false);

                if (t.IsCompletedSuccessfully && kvp.Value.Any())
                {
                    foreach (var dup in kvp.Value)
                    {
                        if (token.IsCancellationRequested)
                            break;
                        try
                        {
                            await Task.Run(() => File.Copy(source, dup, overwrite: true), token).ConfigureAwait(false);
                            _logger.LogDebug("Migrated duplicate {dup}", dup);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogDebug(ex, "Failed to copy duplicate {dup}", dup);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Conversion failed for {source}", source);
            }

            // Yield control to keep overall UI responsive during long conversions
            await Task.Yield();
        }

        if (redrawAfter)
        {
            try
            {
                _penumbraRedraw.Invoke(0, setting: RedrawType.Redraw);
            }
            catch { }
        }
    }

    public Task<Dictionary<string, string[]>> ScanModTexturesAsync()
    {
        return Task.Run(() =>
        {
            var result = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
            try
            {
                if (string.IsNullOrWhiteSpace(ModDirectory) || !Directory.Exists(ModDirectory))
                    return result;

                foreach (var file in Directory.EnumerateFiles(ModDirectory!, "*.*", SearchOption.AllDirectories)
                                               .Where(f => f.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
                                                        || f.EndsWith(".dds", StringComparison.OrdinalIgnoreCase)))
                {
                    // Initial pass: collect all texture files; non-BC7 filtering can be added
                    // via header inspection in a later iteration.
                    result[file] = Array.Empty<string>();
                }
            }
            catch
            {
                // Ignore scan errors
            }
            return result;
        });
    }

    public Task<Dictionary<string, List<string>>> ScanModTexturesGroupedAsync()
    {
        return Task.Run(() =>
        {
            var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            try
            {
                if (string.IsNullOrWhiteSpace(ModDirectory) || !Directory.Exists(ModDirectory))
                    return result;

                var root = Path.GetFullPath(ModDirectory!);
                foreach (var file in Directory.EnumerateFiles(root, "*.*", SearchOption.AllDirectories)
                                             .Where(f => f.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
                                                      || f.EndsWith(".dds", StringComparison.OrdinalIgnoreCase)))
                {
                    // Derive mod folder name from the first directory segment under the mod root
                    var rel = Path.GetRelativePath(root, file);
                    var parts = rel.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                    var modName = parts.Length > 1 ? parts[0] : "<unknown>";
                    if (!result.TryGetValue(modName, out var list))
                    {
                        list = new List<string>();
                        result[modName] = list;
                    }
                    list.Add(file);
                }
            }
            catch
            {
                // Ignore scan errors
            }
            return result;
        });
    }

    public Task<Dictionary<string, string>> GetModDisplayNamesAsync()
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            if (string.IsNullOrWhiteSpace(ModDirectory) || !Directory.Exists(ModDirectory))
                return Task.FromResult(map);

            var root = Path.GetFullPath(ModDirectory!);
            foreach (var dir in Directory.EnumerateDirectories(root, "*", SearchOption.TopDirectoryOnly))
            {
                var folderName = Path.GetFileName(dir);
                var metaPath = Path.Combine(dir, "meta.json");
                if (!File.Exists(metaPath))
                {
                    map[folderName] = folderName;
                    continue;
                }

                try
                {
                    using var s = File.OpenRead(metaPath);
                    using var doc = JsonDocument.Parse(s);
                    if (doc.RootElement.TryGetProperty("Name", out var nameProp) && nameProp.ValueKind == JsonValueKind.String)
                    {
                        var display = nameProp.GetString() ?? folderName;
                        map[folderName] = display;
                    }
                    else
                    {
                        map[folderName] = folderName;
                    }
                }
                catch
                {
                    map[folderName] = folderName;
                }
            }
        }
        catch
        {
            // Ignore errors
        }
        return Task.FromResult(map);
    }

    public Dictionary<string, string> GetModList()
    {
        try
        {
            return _penumbraGetModList.Invoke();
        }
        catch
        {
            return new Dictionary<string, string>();
        }
    }

    public bool ModExists(string modDirectory)
    {
        try
        {
            var list = _penumbraGetModList.Invoke();
            return list.ContainsKey(modDirectory);
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> WaitForModDeletedAsync(string modDirectory, int timeoutMs = 5000)
    {
        var stop = DateTime.UtcNow.AddMilliseconds(Math.Max(100, timeoutMs));
        while (DateTime.UtcNow < stop)
        {
            if (!ModExists(modDirectory))
                return true;
            try { await Task.Delay(100).ConfigureAwait(false); } catch { }
        }
        return !ModExists(modDirectory);
    }

    public async Task<bool> WaitForModAddedAsync(string modDirectory, int timeoutMs = 5000)
    {
        var stop = DateTime.UtcNow.AddMilliseconds(Math.Max(100, timeoutMs));
        while (DateTime.UtcNow < stop)
        {
            if (ModExists(modDirectory))
                return true;
            try { await Task.Delay(100).ConfigureAwait(false); } catch { }
        }
        return ModExists(modDirectory);
    }

    public async Task<bool> WaitForModPathAsync(string modDirectory, string desiredFullPath, int timeoutMs = 5000)
    {
        if (string.IsNullOrWhiteSpace(desiredFullPath))
            return true;
        var stop = DateTime.UtcNow.AddMilliseconds(Math.Max(100, timeoutMs));
        while (DateTime.UtcNow < stop)
        {
            var (_, fullPath, _, _) = GetModPath(modDirectory);
            if (string.Equals(fullPath ?? string.Empty, desiredFullPath ?? string.Empty, StringComparison.Ordinal))
                return true;
            try { await Task.Delay(100).ConfigureAwait(false); } catch { }
        }
        var (_, finalPath, _, _) = GetModPath(modDirectory);
        return string.Equals(finalPath ?? string.Empty, desiredFullPath ?? string.Empty, StringComparison.Ordinal);
    }

    // Retrieve Penumbra's hierarchical mod paths (category folders) for each mod directory.
    public Task<Dictionary<string, string>> GetModPathsAsync()
    {
        var paths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (!APIAvailable)
            return Task.FromResult(paths);

        try
        {
            var list = _penumbraGetModList.Invoke();
            foreach (var kv in list)
            {
                var modDir = kv.Key;
                try
                {
                    var tuple = _penumbraGetModPath.Invoke(modDir, "");
                    var fullPath = tuple.Item2 ?? string.Empty;
                    paths[modDir] = fullPath;
                }
                catch
                {
                    // If IPC fails for a specific mod, fall back to empty path
                    paths[modDir] = string.Empty;
                }
            }
        }
        catch
        {
            // If IPC is unavailable, return empty map
        }

        return Task.FromResult(paths);
    }

    // Get all collections and their names.
    public Task<Dictionary<Guid, string>> GetCollectionsAsync()
    {
        var result = new Dictionary<Guid, string>();
        if (!APIAvailable)
            return Task.FromResult(result);

        try
        {
            result = _penumbraGetCollections.Invoke();
        }
        catch
        {
            // Ignore IPC errors
        }
        return Task.FromResult(result);
    }

    // Get the currently effective collection.
    public Task<(Guid Id, string Name)?> GetCurrentCollectionAsync()
    {
        (Guid, string)? current = null;
        if (!APIAvailable)
            return Task.FromResult(current);

        try
        {
            current = _penumbraGetCollection.Invoke(ApiCollectionType.Current);
        }
        catch
        {
            // Ignore IPC errors
        }
        return Task.FromResult(current);
    }

    // Get enabled state (and priority/inheritance) for all mods in a collection.
    public Task<Dictionary<string, (bool Enabled, int Priority, bool Inherited, bool Temporary)>> GetAllModEnabledStatesAsync(Guid collectionId)
    {
        var result = new Dictionary<string, (bool, int, bool, bool)>(StringComparer.OrdinalIgnoreCase);
        if (!APIAvailable)
            return Task.FromResult(result);

        try
        {
            var tuple = _penumbraGetAllModSettings.Invoke(collectionId, ignoreInheritance: false, ignoreTemporary: false, key: 0);
            var dict = tuple.Item2;
            if (dict != null)
            {
                foreach (var kv in dict)
                {
                    var modDir = kv.Key;
                    var (enabled, priority, _, inherited, temporary) = kv.Value;
                    result[modDir] = (enabled, priority, inherited, temporary);
                }
            }
        }
        catch
        {
            // Ignore IPC errors
        }

        return Task.FromResult(result);
    }

    public Task<List<string>> GetAllModFoldersAsync()
    {
        var folders = new List<string>();
        try
        {
            // Prefer Penumbra IPC's authoritative mod list to avoid transient/non-mod directories.
            if (APIAvailable)
            {
                try
                {
                    var modList = _penumbraGetModList.Invoke();
                    foreach (var kv in modList)
                    {
                        var modDir = kv.Key;
                        if (!string.IsNullOrWhiteSpace(modDir))
                            folders.Add(modDir);
                    }
                    return Task.FromResult(folders);
                }
                catch
                {
                    // Fall through to filesystem enumeration if IPC fails
                }
            }

            if (string.IsNullOrWhiteSpace(ModDirectory) || !Directory.Exists(ModDirectory))
                return Task.FromResult(folders);

            var root = Path.GetFullPath(ModDirectory!);
            foreach (var dir in Directory.EnumerateDirectories(root, "*", SearchOption.TopDirectoryOnly))
            {
                var folderName = Path.GetFileName(dir);
                // Skip hidden/temporary dot-prefixed directories that are not actual mods
                if (!string.IsNullOrEmpty(folderName) && !folderName.StartsWith(".", StringComparison.Ordinal))
                    folders.Add(folderName);
            }
        }
        catch
        {
            // Ignore errors
        }
        return Task.FromResult(folders);
    }

    public Task<Dictionary<string, List<string>>> GetModTagsAsync()
    {
        var tagsByMod = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        try
        {
            if (string.IsNullOrWhiteSpace(ModDirectory) || !Directory.Exists(ModDirectory))
                return Task.FromResult(tagsByMod);

            var root = Path.GetFullPath(ModDirectory!);
            foreach (var dir in Directory.EnumerateDirectories(root, "*", SearchOption.TopDirectoryOnly))
            {
                var folderName = Path.GetFileName(dir);
                var metaPath = Path.Combine(dir, "meta.json");
                var list = new List<string>();
                if (File.Exists(metaPath))
                {
                    try
                    {
                        using var s = File.OpenRead(metaPath);
                        using var doc = JsonDocument.Parse(s);
                        if (doc.RootElement.TryGetProperty("ModTags", out var tagsProp) && tagsProp.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var e in tagsProp.EnumerateArray())
                            {
                                if (e.ValueKind == JsonValueKind.String)
                                {
                                    var tag = e.GetString();
                                    if (!string.IsNullOrWhiteSpace(tag))
                                        list.Add(tag);
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Ignore malformed meta.json
                    }
                }
                tagsByMod[folderName] = list;
            }
        }
        catch
        {
            // Ignore errors
        }
        return Task.FromResult(tagsByMod);
    }

    public void RedrawPlayer()
    {
        if (!APIAvailable)
            return;
        try
        {
            _penumbraRedraw.Invoke(0, setting: RedrawType.Redraw);
        }
        catch
        {
            // Ignore redraw errors
        }
    }

    public void OpenModInPenumbra(string modDirectory, string modName)
    {
        if (!APIAvailable)
            return;
        try
        {
            _penumbraOpenMainWindow.Invoke(TabType.Mods, modDirectory ?? string.Empty, modName ?? string.Empty);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to open Penumbra window for mod {modDirectory}", modDirectory);
        }
    }

    public void ClosePenumbraWindow()
    {
        if (!APIAvailable)
            return;
        try
        {
            _penumbraCloseMainWindow.Invoke();
        }
        catch { }
    }

    public bool AddModDirectory(string modFolderName)
    {
        try
        {
            if (!APIAvailable)
                return false;
            if (string.IsNullOrWhiteSpace(modFolderName))
                return false;
            var result = _penumbraAddMod.Invoke(modFolderName);
            try { _logger.LogDebug("Penumbra AddMod result for {mod}: {result}", modFolderName, result); } catch { }
            return result == Penumbra.Api.Enums.PenumbraApiEc.Success;
        }
        catch
        {
            return false;
        }
    }

    public bool RemoveModDirectory(string modFolderName)
    {
        try
        {
            if (!APIAvailable)
                return false;
            if (string.IsNullOrWhiteSpace(modFolderName))
                return false;
            if (_penumbraDeleteMod == null)
                return false;
            var t = _penumbraDeleteMod.GetType();
            var m = t.GetMethod("Invoke");
            if (m == null)
                return false;
            var result = m.Invoke(_penumbraDeleteMod, new object[] { modFolderName, string.Empty });
            try { _logger.LogDebug("Penumbra DeleteMod result for {mod}: {result}", modFolderName, result); } catch { }
            return result != null && result.ToString() == Penumbra.Api.Enums.PenumbraApiEc.Success.ToString();
        }
        catch
        {
            return false;
        }
    }

    public (Penumbra.Api.Enums.PenumbraApiEc, string FullPath, bool FullDefault, bool NameDefault) GetModPath(string modDirectory)
    {
        try
        {
            var tuple = _penumbraGetModPath.Invoke(modDirectory, "");
            return tuple;
        }
        catch
        {
            return (Penumbra.Api.Enums.PenumbraApiEc.ModMissing, string.Empty, true, true);
        }
    }

    public bool SetModPath(string modDirectory, string fullPath)
    {
        try
        {
            if (_penumbraSetModPath == null)
                return false;
            var ec = _penumbraSetModPath.Invoke(modDirectory, fullPath, "");
            return ec == Penumbra.Api.Enums.PenumbraApiEc.Success;
        }
        catch
        {
            return false;
        }
    }

    public void NudgeModDetection(string modFolderName)
    {
        try
        {
            if (!APIAvailable)
                return;
            var root = ModDirectory ?? string.Empty;
            if (string.IsNullOrWhiteSpace(root) || string.IsNullOrWhiteSpace(modFolderName))
                return;
            var modPath = System.IO.Path.Combine(root, modFolderName);
            if (!Directory.Exists(modPath))
                return;
            var metaPath = System.IO.Path.Combine(modPath, "meta.json");
            if (File.Exists(metaPath))
            {
                try
                {
                    var bytes = File.ReadAllBytes(metaPath);
                    File.WriteAllBytes(metaPath, bytes);
                    try { File.SetLastWriteTimeUtc(metaPath, DateTime.UtcNow); } catch { }
                }
                catch
                {
                }
            }
            
            try { Directory.SetLastWriteTimeUtc(modPath, DateTime.UtcNow); } catch { }
            
            try { var _ = GetModPathsAsync(); } catch { }
            try { ModsChanged?.Invoke(); } catch { }
        }
        catch
        {
        }
    }

    public async Task<HashSet<string>> GetCurrentlyUsedTextureModPathsAsync()
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!APIAvailable || string.IsNullOrWhiteSpace(ModDirectory) || !Directory.Exists(ModDirectory))
            return result;

        try
        {
            var root = Path.GetFullPath(ModDirectory!);
            var playerResources = _penumbraGetPlayerResourcePaths.Invoke();
            if (playerResources == null || playerResources.Count == 0)
                return result;

            var gamePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in playerResources)
            {
                var dict = kv.Value;
                if (dict == null) continue;
                foreach (var set in dict.Values)
                {
                    foreach (var path in set)
                    {
                        if (path.EndsWith(".tex", StringComparison.OrdinalIgnoreCase)
                         || path.EndsWith(".dds", StringComparison.OrdinalIgnoreCase))
                        {
                            gamePaths.Add(path);
                        }
                    }
                }
            }

            if (gamePaths.Count == 0)
                return result;

            var tuple = await _penumbraResolvePlayerPaths.Invoke(gamePaths.ToArray(), Array.Empty<string>()).ConfigureAwait(false);
            var resolved = tuple.Item1;
            foreach (var p in resolved)
            {
                if (string.IsNullOrWhiteSpace(p))
                    continue;
                var full = Path.GetFullPath(p);
                if (full.StartsWith(root, StringComparison.OrdinalIgnoreCase) && File.Exists(full))
                {
                    result.Add(full);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to get currently used texture paths from Penumbra");
        }

        return result;
    }
}