using Dalamud.Interface.Windowing;
using Dalamud.Plugin;
using Dalamud.Plugin.Services;
using Dalamud.Game.Command;
using System;
using Microsoft.Extensions.Logging;
using System.Reflection;
using System.IO;
using ShrinkU.Services;
using ShrinkU.UI;
using ShrinkU.Configuration;
using ShrinkU.Interop;

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
    private readonly ConversionUI _conversionUi;
    private readonly SettingsUI _settingsUi;
    private readonly FirstRunSetupUI _firstRunUi;
    private readonly ShrinkU.Interop.ShrinkUIpc _shrinkuIpc;
    private readonly ChangelogService _changelogService;
    private readonly ReleaseChangelogUI _releaseChangelogUi;

    public Plugin(IDalamudPluginInterface pluginInterface, IFramework framework, ICommandManager commandManager, IPluginLog pluginLog)
    {
        _pluginInterface = pluginInterface ?? throw new ArgumentNullException(nameof(pluginInterface));
        _logger = new PluginLogger(pluginLog);
        _commandManager = commandManager;

        // Initialize configuration and services
        _configService = new ShrinkUConfigService(pluginInterface, _logger);
        _penumbraIpc = new PenumbraIpc(pluginInterface, _logger);
        var modStateService = new ModStateService(_logger, _configService);
        _backupService = new TextureBackupService(_logger, _configService, _penumbraIpc, modStateService);
        _conversionService = new TextureConversionService(_logger, _penumbraIpc, _backupService, _configService, modStateService);
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
        catch { }

        // Initialize changelog service and UI before wiring callbacks
        _changelogService = new ChangelogService(_logger, new System.Net.Http.HttpClient(), _configService);
        _releaseChangelogUi = new ReleaseChangelogUI(_pluginInterface, _logger, _configService, _changelogService);

        // Create UI windows and register
        _settingsUi = new SettingsUI(_logger, _configService, _conversionService, _backupService, () => _releaseChangelogUi.IsOpen = true);
        _conversionUi = new ConversionUI(_logger, _configService, _conversionService, _backupService, () => _settingsUi.IsOpen = true, modStateService);
        _firstRunUi = new FirstRunSetupUI(_logger, _configService)
        {
            OnCompleted = () =>
            {
                try
                {
                    _logger.LogDebug("First run completed callback invoked");
                    _conversionUi.IsOpen = true;
                }
                catch { }
            }
        };
        _windowSystem.AddWindow(_conversionUi);
        _windowSystem.AddWindow(_settingsUi);
        _windowSystem.AddWindow(_firstRunUi);
        _windowSystem.AddWindow(_releaseChangelogUi);

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
        catch { }

        try
        {
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
            catch { }
        }
        catch { }

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
                // Standalone ShrinkU: show changelog after update automatically
                var currentVer = typeof(Plugin).Assembly?.GetName()?.Version?.ToString() ?? string.Empty;
                var lastSeen = _configService.Current.LastSeenReleaseChangelogVersion ?? string.Empty;
                if (!string.IsNullOrWhiteSpace(currentVer) && !string.Equals(currentVer, lastSeen, StringComparison.OrdinalIgnoreCase))
                {
                    _releaseChangelogUi.IsOpen = true;
                    _logger.LogDebug("Opening ShrinkU release notes window: current={cur} lastSeen={last}", currentVer, lastSeen);
                }
            }
        }
        catch { }
    }

    public void Dispose()
    {
        _windowSystem.RemoveAllWindows();
        _pluginInterface.UiBuilder.Draw -= DrawUi;
        _pluginInterface.UiBuilder.OpenConfigUi -= OpenConfigUi;
        _pluginInterface.UiBuilder.OpenMainUi -= OpenMainUi;
        try { _conversionUi.Dispose(); } catch { }
        try { _conversionService.Dispose(); } catch { }
        try { _shrinkuIpc.Dispose(); } catch { }
        try { _logger.LogDebug("ShrinkU plugin disposing: releasing Penumbra subscriptions (DIAG-v3)"); _penumbraIpc.Dispose(); } catch { }
        try { _commandManager.RemoveHandler("/shrinku"); } catch { }
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

        if (!_configService.Current.FirstRunCompleted)
        {
            _firstRunUi.IsOpen = true;
            return;
        }
        _conversionUi.IsOpen = true;
    }
}