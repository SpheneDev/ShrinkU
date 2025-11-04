using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.IO;
using Dalamud.Plugin;
using Dalamud.Plugin.Ipc;
using Microsoft.Extensions.Logging;
using ShrinkU.Services;

namespace ShrinkU.Interop;

public sealed class ShrinkUIpc : IDisposable
{
    private readonly IDalamudPluginInterface _pi;
    private readonly ILogger _logger;
    private readonly TextureBackupService _backupService;
    private readonly PenumbraIpc _penumbraIpc;

    private readonly List<IDisposable> _providers = new();

    public ShrinkUIpc(IDalamudPluginInterface pi, ILogger logger, TextureBackupService backupService, PenumbraIpc penumbraIpc)
    {
        _pi = pi;
        _logger = logger;
        _backupService = backupService;
        _penumbraIpc = penumbraIpc;

        RegisterProviders();
        try { _logger.LogDebug("ShrinkU IPC providers registered"); } catch { }
    }

    private void RegisterProviders()
    {
        // Get list of currently used texture paths under Penumbra's mod directory (absolute paths)
        TryRegisterFuncProvider<string[]>("ShrinkU.GetCurrentlyUsedTextureModPaths", () =>
        {
            try
            {
                var set = _penumbraIpc.GetCurrentlyUsedTextureModPathsAsync().GetAwaiter().GetResult();
                return set.ToArray();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC GetCurrentlyUsedTextureModPaths failed");
                return Array.Empty<string>();
            }
        });

        // Resolve mod folder name from an absolute path under Penumbra's mod root
        TryRegisterFuncProvider<string, string?>("ShrinkU.GetModFolderForPath", (path) =>
        {
            try
            {
                var modRoot = _penumbraIpc.ModDirectory ?? string.Empty;
                if (string.IsNullOrWhiteSpace(path) || string.IsNullOrWhiteSpace(modRoot))
                    return null;
                var full = Path.GetFullPath(path);
                var root = Path.GetFullPath(modRoot);
                if (!full.StartsWith(root, StringComparison.OrdinalIgnoreCase))
                    return null;
                var rel = Path.GetRelativePath(root, full).Replace('/', '\\');
                if (string.IsNullOrWhiteSpace(rel))
                    return null;
                var idx = rel.IndexOf('\\');
                if (idx < 0)
                    return rel;
                return rel.Substring(0, idx);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC GetModFolderForPath failed for {path}", path);
                return null;
            }
        });

        // Check if backups exist for a given mod folder
        TryRegisterFuncProvider<string, bool>("ShrinkU.HasBackupForMod", (modFolder) =>
        {
            try
            {
                return _backupService.HasBackupForModAsync(modFolder).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC HasBackupForMod failed for {mod}", modFolder);
                return false;
            }
        });

        // Restore latest backup for a given mod folder
        TryRegisterFuncProvider<string, bool>("ShrinkU.RestoreLatestForMod", (modFolder) =>
        {
            try
            {
                return _backupService.RestoreLatestForModAsync(modFolder, progress: null, token: default).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC RestoreLatestForMod failed for {mod}", modFolder);
                return false;
            }
        });

        // Return a JSON overview of backup sessions (entries include mod names, original filenames, relative paths)
        TryRegisterFuncProvider<string>("ShrinkU.GetBackupOverviewJson", () =>
        {
            try
            {
                var overview = _backupService.GetBackupOverviewAsync().GetAwaiter().GetResult();
                var json = JsonSerializer.Serialize(overview);
                return json;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC GetBackupOverviewJson failed");
                return "[]";
            }
        });
    }

    private void TryRegisterFuncProvider<TRet>(string label, Func<TRet> func)
    {
        try
        {
            var provider = _pi.GetIpcProvider<TRet>(label);
            provider.RegisterFunc(func);
            _providers.Add(new DisposableUnregister(() => provider.UnregisterFunc()));
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to register IPC provider {label}", label);
        }
    }

    private void TryRegisterFuncProvider<T1, TRet>(string label, Func<T1, TRet> func)
    {
        try
        {
            var provider = _pi.GetIpcProvider<T1, TRet>(label);
            provider.RegisterFunc(func);
            _providers.Add(new DisposableUnregister(() => provider.UnregisterFunc()));
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to register IPC provider {label}", label);
        }
    }

    public void Dispose()
    {
        foreach (var p in _providers)
        {
            try { p.Dispose(); } catch { }
        }
        _providers.Clear();
        try { _logger.LogDebug("ShrinkU IPC providers disposed"); } catch { }
    }

    private sealed class DisposableUnregister : IDisposable
    {
        private readonly Action _unregister;
        public DisposableUnregister(Action unregister) => _unregister = unregister;
        public void Dispose()
        {
            try { _unregister(); } catch { }
        }
    }
}