using Dalamud.Interface.Windowing;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;
using Dalamud.Game.Command;
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Reflection;
using System.IO;
using ShrinkU.Services;
using ShrinkU.UI;
using ShrinkU.Configuration;
using ShrinkU.Interop;
using System.Security.Principal;

namespace ShrinkU;

public sealed class Plugin : IDalamudPlugin
{
    private readonly IDalamudPluginInterface _pluginInterface = null!;
    private readonly ILogger _logger;
    private readonly WindowSystem _windowSystem = new("ShrinkU");
    private readonly ICommandManager _commandManager;

    private readonly ShrinkUConfigService _configService;
    private readonly PenumbraIpc _penumbraIpc;
    private readonly TextureBackupService _backupService;
    private readonly TextureConversionService _conversionService;
    private readonly ModStateService _modStateService;
    private readonly ConversionUI _conversionUi;
    private readonly SettingsUI _settingsUi;
    private readonly FirstRunSetupUI _firstRunUi;
    private readonly ShrinkU.Interop.ShrinkUIpc _shrinkuIpc;
    private readonly ChangelogService _changelogService;
    private readonly ReleaseChangelogUI _releaseChangelogUi;
    private readonly DebugTraceService _debugTrace;
    private readonly DebugUI _debugUi;
    private System.Threading.CancellationTokenSource? _initRefreshCts;

    public Plugin(IDalamudPluginInterface pluginInterface, IFramework framework, ICommandManager commandManager, IPluginLog pluginLog)
    {
        _pluginInterface = pluginInterface ?? throw new ArgumentNullException(nameof(pluginInterface));
        _logger = new PluginLogger(pluginLog);
        _commandManager = commandManager;

        // Initialize configuration and services
        _configService = new ShrinkUConfigService(pluginInterface, _logger);
        _penumbraIpc = new PenumbraIpc(pluginInterface, _logger);
        _debugTrace = new DebugTraceService(1000);
        _modStateService = new ModStateService(_logger, _configService, _debugTrace);
        _backupService = new TextureBackupService(_logger, _configService, _penumbraIpc, _modStateService);
        _conversionService = new TextureConversionService(_logger, _penumbraIpc, _backupService, _configService, _modStateService);
        var cacheService = new ConversionCacheService(_logger, _configService, _backupService, _modStateService);
        _shrinkuIpc = new ShrinkU.Interop.ShrinkUIpc(pluginInterface, _logger, _backupService, _penumbraIpc, _configService, _conversionService);

        // Ensure standalone ShrinkU is not marked as controlled by Sphene
        try
        {
            if (_configService.Current.AutomaticHandledBySphene || !string.IsNullOrWhiteSpace(_configService.Current.AutomaticControllerName))
            {
                _logger.LogDebug("Resetting automatic controller flags in standalone ShrinkU");
                _configService.Current.AutomaticHandledBySphene = false;
                _configService.Current.AutomaticControllerName = string.Empty;
                _configService.Save();
            }
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to reset automatic controller flags"); }

        // Initialize changelog service and UI before wiring callbacks
        _changelogService = new ChangelogService(_logger, new System.Net.Http.HttpClient(), _configService);
        _releaseChangelogUi = new ReleaseChangelogUI(_pluginInterface, _logger, _configService, _changelogService);

        // Create UI windows and register
        _debugUi = new DebugUI(_logger, _configService, _debugTrace);
        _settingsUi = new SettingsUI(_logger, _configService, _conversionService, _backupService, () => _releaseChangelogUi.IsOpen = true, _debugTrace, () => _debugUi.IsOpen = true);
        _conversionUi = new ConversionUI(_logger, _configService, _conversionService, _backupService, () => _settingsUi.IsOpen = true, _modStateService, cacheService, _debugTrace);
        _firstRunUi = new FirstRunSetupUI(_logger, _configService)
        {
            OnCompleted = () =>
            {
                try
                {
                    _logger.LogDebug("First run completed callback invoked");
                    _conversionUi.IsOpen = true;
                }
                catch (Exception ex) { _logger.LogError(ex, "Failed to open conversion UI after first run"); }
            }
        };
        _windowSystem.AddWindow(_conversionUi);
        _windowSystem.AddWindow(_settingsUi);
        _windowSystem.AddWindow(_firstRunUi);
        _windowSystem.AddWindow(_releaseChangelogUi);
        _windowSystem.AddWindow(_debugUi);

        _pluginInterface.UiBuilder.Draw += DrawUi;
        _pluginInterface.UiBuilder.OpenConfigUi += OpenConfigUi;
        _pluginInterface.UiBuilder.OpenMainUi += OpenMainUi;

        // Register chat command to open ShrinkU
        try
        {
            _commandManager.AddHandler("/shrinku", new CommandInfo(OnCommand)
            {
                HelpMessage = "Open ShrinkU window. Optional: /shrinku config"
            });
            _logger.LogDebug("Registered /shrinku command handler");
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to register /shrinku command handler"); }

        try
        {
            _conversionService.SetEnabled(true);
            var asm = typeof(Plugin).Assembly;
            var ver = asm?.GetName()?.Version?.ToString() ?? "unknown";
            var loc = asm?.Location ?? "unknown";
            var ts = File.Exists(loc) ? File.GetLastWriteTimeUtc(loc).ToString("O") : "unknown";
            var mvid = typeof(Plugin).Module?.ModuleVersionId.ToString() ?? "unknown";
            _logger.LogDebug($"ShrinkU plugin initialized: DIAG-v3 version={ver} builtUtc={ts} mvid={mvid}");
            _logger.LogDebug($"Assembly location: {loc}");
            try
            {
                var baseDir = AppContext.BaseDirectory ?? string.Empty;
                var curDir = Environment.CurrentDirectory ?? string.Empty;
                var cfgDir = _pluginInterface?.ConfigDirectory?.FullName ?? string.Empty;
                _logger.LogDebug($"AppContext.BaseDirectory: {baseDir}");
                _logger.LogDebug($"Environment.CurrentDirectory: {curDir}");
                _logger.LogDebug($"Plugin ConfigDirectory: {cfgDir}");
            }
            catch (Exception ex) { _logger.LogDebug(ex, "Failed to log plugin environment details"); }
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to log plugin diagnostics"); }

        var startupErrors = RunStandaloneStartupChecks();
        try { if (startupErrors.Count > 0) _conversionUi.SetStartupDependencyErrors(startupErrors); } catch { }

        // Open setup guide on first run, otherwise open main window
        try
        {
            if (!_configService.Current.FirstRunCompleted)
            {
                _firstRunUi.IsOpen = true;
                _logger.LogDebug("First run detected: opening setup guide window");
            }
            else
            {
                var currentVer = typeof(Plugin).Assembly?.GetName()?.Version?.ToString() ?? string.Empty;
                var lastSeen = _configService.Current.LastSeenReleaseChangelogVersion ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(currentVer) && !string.Equals(currentVer, lastSeen, StringComparison.OrdinalIgnoreCase))
                {
                    _releaseChangelogUi.IsOpen = true;
                    _logger.LogDebug("Opening ShrinkU release notes window: current={cur} lastSeen={last}", currentVer, lastSeen);
                }
                try { _conversionUi.SetStartupRefreshInProgress(true); }
                catch (Exception ex) { _logger.LogDebug(ex, "Failed to set startup refresh flag"); }
                try
                {
                    _initRefreshCts?.Cancel();
                    _initRefreshCts?.Dispose();
                    _initRefreshCts = new System.Threading.CancellationTokenSource();
                    var token = _initRefreshCts.Token;
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            
                            await _backupService.RefreshAllBackupStateAsync().ConfigureAwait(false);
                            var threads = Math.Max(1, _configService.Current.MaxStartupThreads);
                            await _conversionService.RunInitialParallelUpdateAsync(threads, token).ConfigureAwait(false);
                            await _conversionService.UpdateAllModUsedTextureFilesAsync().ConfigureAwait(false);
                            
                            try { _modStateService.Save(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to save mod state"); }
                        }
                        catch (Exception ex) { _logger.LogError(ex, "Initial refresh failed"); }
                        finally
                        {
                            try { _conversionUi.SetStartupRefreshInProgress(false); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to clear startup refresh flag"); }
                        }
                    }, token);
                }
                catch (Exception ex) { _logger.LogError(ex, "Failed to start initial refresh task"); }
            }
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to determine initial UI state"); }
    }

    public void Dispose()
    {
        try { _modStateService.SetSavingEnabled(false); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to disable mod state saving during dispose"); }
        _windowSystem.RemoveAllWindows();
        _pluginInterface.UiBuilder.Draw -= DrawUi;
        _pluginInterface.UiBuilder.OpenConfigUi -= OpenConfigUi;
        _pluginInterface.UiBuilder.OpenMainUi -= OpenMainUi;
        try { _initRefreshCts?.Cancel(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to cancel init refresh CTS"); }
        try { _initRefreshCts?.Dispose(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to dispose init refresh CTS"); }
        _initRefreshCts = null;
        try { _conversionUi.ShutdownBackgroundWork(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to shutdown conversion UI background work"); }
        try { _conversionUi.Dispose(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to dispose conversion UI"); }
        try { _conversionService.Dispose(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to dispose conversion service"); }
        try { _shrinkuIpc.Dispose(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to dispose ShrinkU IPC"); }
        try { _logger.LogDebug("ShrinkU plugin disposing: releasing Penumbra subscriptions (DIAG-v3)"); _penumbraIpc.Dispose(); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to dispose Penumbra IPC"); }
        try { _commandManager.RemoveHandler("/shrinku"); } catch (Exception ex) { _logger.LogDebug(ex, "Failed to remove /shrinku command handler"); }
    }

    private void DrawUi()
    {
        _windowSystem.Draw();
    }

    private void OpenConfigUi()
    {
        if (!_configService.Current.FirstRunCompleted)
        {
            _firstRunUi.IsOpen = true;
            return;
        }
        _settingsUi.IsOpen = true;
    }

    private void OpenMainUi()
    {
        if (!_configService.Current.FirstRunCompleted)
        {
            _firstRunUi.IsOpen = true;
            return;
        }
        _conversionUi.IsOpen = true;
    }

    private void OnCommand(string command, string args)
    {
        // Support optional argument to open settings
        if (!string.IsNullOrWhiteSpace(args) && args.Trim().Equals("config", StringComparison.OrdinalIgnoreCase))
        {
            if (!_configService.Current.FirstRunCompleted)
            {
                _firstRunUi.IsOpen = true;
                return;
            }
            _settingsUi.IsOpen = true;
            return;
        }

        if (!string.IsNullOrWhiteSpace(args) && args.Trim().Equals("bench", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                var threads = Math.Max(1, _configService.Current.MaxStartupThreads);
                _ = Task.Run(async () => { try { await _conversionService.RunStartupBenchmarkAsync(threads, System.Threading.CancellationToken.None).ConfigureAwait(false); } catch (Exception ex) { _logger.LogError(ex, "Startup benchmark failed"); } });
            }
            catch (Exception ex) { _logger.LogError(ex, "Failed to invoke startup benchmark"); }
            return;
        }

        if (!_configService.Current.FirstRunCompleted)
        {
            _firstRunUi.IsOpen = true;
            return;
        }
        _conversionUi.IsOpen = true;
    }

    private System.Collections.Generic.List<string> RunStandaloneStartupChecks()
    {
        var errors = new System.Collections.Generic.List<string>();
        try
        {
            try
            {
                var isAdmin = false;
                try
                {
                    var id = WindowsIdentity.GetCurrent();
                    if (id != null)
                    {
                        var p = new WindowsPrincipal(id);
                        isAdmin = p.IsInRole(WindowsBuiltInRole.Administrator);
                    }
                }
                catch { }
                _logger.LogDebug("Startup privilege level: admin={admin}", isAdmin);
            }
            catch (Exception ex) { _logger.LogDebug(ex, "Privilege check failed"); }

            try
            {
                if (!_penumbraIpc.APIAvailable)
                {
                    errors.Add("Penumbra API nicht verfügbar. Bitte Penumbra aktivieren.");
                    _logger.LogError("Penumbra API unavailable at startup");
                }
                var modDir = _penumbraIpc.ModDirectory ?? string.Empty;
                if (string.IsNullOrWhiteSpace(modDir) || !System.IO.Directory.Exists(modDir))
                {
                    errors.Add("Penumbra Mod-Verzeichnis nicht gefunden.");
                    _logger.LogError("Penumbra mod directory missing: {dir}", modDir);
                }
            }
            catch (Exception ex) { _logger.LogError(ex, "Penumbra dependency check failed"); }

            try
            {
                var cfgDir = _pluginInterface.ConfigDirectory?.FullName ?? string.Empty;
                if (string.IsNullOrWhiteSpace(cfgDir) || !System.IO.Directory.Exists(cfgDir))
                {
                    errors.Add("Konfigurationsordner nicht verfügbar.");
                    _logger.LogError("Config directory unavailable: {dir}", cfgDir);
                }
                else
                {
                    var testFile = System.IO.Path.Combine(cfgDir, ".shrinku_cfg_write_test.tmp");
                    try
                    {
                        System.IO.File.WriteAllText(testFile, "ok");
                        System.IO.File.Delete(testFile);
                    }
                    catch (Exception ex)
                    {
                        errors.Add("Konfigurationsordner nicht beschreibbar.");
                        _logger.LogError(ex, "Config directory not writable: {dir}", cfgDir);
                    }
                }
            }
            catch (Exception ex) { _logger.LogError(ex, "Config directory check failed"); }

            try
            {
                var backup = _configService.Current.BackupFolderPath ?? string.Empty;
                if (string.IsNullOrWhiteSpace(backup))
                {
                    errors.Add("Backup-Ordner nicht konfiguriert.");
                    _logger.LogError("Backup folder path not configured");
                }
                else
                {
                    try { System.IO.Directory.CreateDirectory(backup); } catch { }
                    if (!System.IO.Directory.Exists(backup))
                    {
                        errors.Add("Backup-Ordner existiert nicht.");
                        _logger.LogError("Backup folder does not exist: {path}", backup);
                    }
                    else
                    {
                        var testFile = System.IO.Path.Combine(backup, ".shrinku_backup_write_test.tmp");
                        try
                        {
                            System.IO.File.WriteAllText(testFile, "ok");
                            System.IO.File.Delete(testFile);
                        }
                        catch (Exception ex)
                        {
                            errors.Add("Backup-Ordner nicht beschreibbar.");
                            _logger.LogError(ex, "Backup folder not writable: {path}", backup);
                        }
                    }
                }
            }
            catch (Exception ex) { _logger.LogError(ex, "Backup directory check failed"); }
        }
        catch (Exception ex) { _logger.LogError(ex, "Startup checks failed"); }
        return errors;
    }
}
