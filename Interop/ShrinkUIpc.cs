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
    private readonly TextureConversionService _conversionService;
    private readonly ShrinkU.Configuration.ShrinkUConfigService _configService;
    private readonly PenumbraIpc _penumbraIpc;

    private readonly List<IDisposable> _providers = new();

    public ShrinkUIpc(IDalamudPluginInterface pi, ILogger logger, TextureBackupService backupService, PenumbraIpc penumbraIpc, ShrinkU.Configuration.ShrinkUConfigService configService, TextureConversionService conversionService)
    {
        _pi = pi;
        _logger = logger;
        _backupService = backupService;
        _penumbraIpc = penumbraIpc;
        _configService = configService;
        _conversionService = conversionService;

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
                var success = _backupService.RestoreLatestForModAsync(modFolder, progress: null, token: default).GetAwaiter().GetResult();
                if (success)
                {
                    try
                    {
                        if (_configService.Current.ExternalConvertedMods.Remove(modFolder))
                            _configService.Save();
                    }
                    catch { }
                    try { _conversionService.NotifyExternalTextureChange("ipc-restore-latest-for-mod"); } catch { }
                }
                return success;
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

        // Allow external controllers to set automatic handling and controller name
        TryRegisterFuncProvider<(string controllerName, bool handled), bool>("ShrinkU.SetAutomaticController", payload =>
        {
            try
            {
                var (controllerName, handled) = payload;
                var name = controllerName ?? string.Empty;
                _configService.Current.AutomaticControllerName = handled ? name : string.Empty;
                // Maintain legacy flag for Sphene-specific checks
                _configService.Current.AutomaticHandledBySphene = handled && name.Equals("Sphene", StringComparison.OrdinalIgnoreCase);
                _configService.Save();
                _logger.LogDebug("Automatic controller set via IPC: handled={handled}, name={name}", handled, name);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC SetAutomaticController failed: {name}", payload.controllerName);
                return false;
            }
        });

        // Trigger an automatic conversion run in the background without opening UI
        TryRegisterFuncProvider<bool>("ShrinkU.StartAutomaticConversion", () =>
        {
            try
            {
                if (_configService.Current.TextureProcessingMode != ShrinkU.Configuration.TextureProcessingMode.Automatic)
                {
                    _logger.LogDebug("Automatic conversion requested but mode is not Automatic");
                    return false;
                }

                // Fetch candidates synchronously and schedule conversion in background
                var candidates = _conversionService.GetAutomaticCandidateTexturesAsync().GetAwaiter().GetResult();
                if (candidates == null || candidates.Count == 0)
                {
                    _logger.LogDebug("Automatic conversion requested but no candidate textures were found");
                    return false;
                }

                _ = _conversionService.StartConversionAsync(candidates).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        var ex = t.Exception?.GetBaseException();
                        _logger.LogDebug(ex, "Background automatic conversion failed");
                    }
                    else
                    {
                        try
                        {
                            // Persist external conversion markers per mod for indicator to survive restarts
                            var now = DateTime.UtcNow;
                            foreach (var mod in candidates.Keys)
                            {
                                try
                                {
                                    _configService.Current.ExternalConvertedMods[mod] = new ShrinkU.Configuration.ExternalChangeMarker
                                    {
                                        Reason = "ipc-auto-conversion-complete",
                                        AtUtc = now,
                                    };
                                }
                                catch { }
                            }
                            _configService.Save();
                        }
                        catch { }
                        try { _conversionService.NotifyExternalTextureChange("ipc-auto-conversion-complete"); } catch { }
                        _logger.LogDebug("Background automatic conversion completed");
                    }
                });
                _logger.LogDebug("Background automatic conversion started");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "IPC StartAutomaticConversion failed");
                return false;
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