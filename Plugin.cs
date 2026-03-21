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
using Dalamud.Bindings.ImGui;

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
    private readonly PenumbraFolderWatcherService _penumbraFolderWatcher;
    private readonly BackupFolderWatcherService _backupFolderWatcher;
    private readonly ConversionUI _conversionUi;
    private readonly SettingsUI _settingsUi;
    private readonly FirstRunSetupUI _firstRunUi;
    private readonly ShrinkU.Interop.ShrinkUIpc _shrinkuIpc;
    private readonly ChangelogService _changelogService;
    private readonly ReleaseChangelogUI _releaseChangelogUi;
    private readonly DebugTraceService _debugTrace;
    private readonly DebugUI _debugUi;
    private readonly StartupProgressUI _startupProgressUi;
    private readonly PenumbraExtensionService _penumbraExtension;
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
        _penumbraFolderWatcher = new PenumbraFolderWatcherService(_logger, _penumbraIpc, _modStateService);
        _backupFolderWatcher = new BackupFolderWatcherService(_logger, _configService, _backupService);
        var cacheService = new ConversionCacheService(_logger, _configService, _backupService, _modStateService);
        _shrinkuIpc = new ShrinkU.Interop.ShrinkUIpc(pluginInterface, _logger, _backupService, _penumbraIpc, _configService, _conversionService, _modStateService);
        _penumbraFolderWatcher.OnFolderChanged += reason =>
        {
            try { _conversionService.NotifyExternalTextureChange($"penumbra-folder-{reason}"); } catch { }
        };

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

        try
        {
            if (!_configService.Current.EnableFullModBackupBeforeConversion)
            {
                _configService.Current.EnableFullModBackupBeforeConversion = true;
                _configService.Current.EnableBackupBeforeConversion = false;
                _configService.Current.EnableZipCompressionForBackups = false;
                _configService.Current.DeleteOriginalBackupsAfterCompression = false;
                _configService.Save();
                _logger.LogDebug("Enforced default: EnableFullModBackupBeforeConversion=true (standalone)");
            }
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to enforce PMP backup default"); }

        // Initialize changelog service and UI before wiring callbacks
        _changelogService = new ChangelogService(_logger, new System.Net.Http.HttpClient(), _configService);
        _releaseChangelogUi = new ReleaseChangelogUI(_pluginInterface, _logger, _configService, _changelogService);

        // Create UI windows and register
        _debugUi = new DebugUI(_logger, _configService, _debugTrace, _penumbraFolderWatcher, _backupFolderWatcher, _conversionService);
        _settingsUi = new SettingsUI(_logger, _configService, _conversionService, _backupService, _modStateService, () => _releaseChangelogUi.OpenLatest(), _debugTrace, () => _debugUi.IsOpen = true);
        _conversionUi = new ConversionUI(_logger, _configService, _conversionService, _backupService, () => _settingsUi.IsOpen = true, _modStateService, cacheService, _debugTrace);
        _penumbraExtension = new PenumbraExtensionService(_penumbraIpc, _conversionUi, _logger);
        _startupProgressUi = new StartupProgressUI(_logger, _configService, _conversionService, _backupService);
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
        _windowSystem.AddWindow(_settingsUi);
        _windowSystem.AddWindow(_conversionUi);
        _windowSystem.AddWindow(_firstRunUi);
        _windowSystem.AddWindow(_releaseChangelogUi);
        _windowSystem.AddWindow(_startupProgressUi);
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
            _logger.LogInformation($"ShrinkU plugin initialized");
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
            catch (Exception ex) { _logger.LogInformation(ex, "Failed to log plugin environment details"); }
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
                _logger.LogInformation("First run detected: opening setup guide window");
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
                
                // Start background refresh with improved error handling and timeout
                try { _conversionUi.SetStartupRefreshInProgress(true); }
                catch (Exception ex) { _logger.LogDebug(ex, "Failed to set startup refresh flag"); }
                
                _initRefreshCts?.Cancel();
                _initRefreshCts?.Dispose();
                _initRefreshCts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(_configService.Current.StartupMaxDurationSeconds));
                var token = _initRefreshCts.Token;
                
                try { _startupProgressUi.ResetAll(); _startupProgressUi.IsOpen = true; } catch { }
                
                _ = Task.Run(async () =>
                {
                    try
                    {
                        try { _startupProgressUi.SetStep(1); } catch { }

                        var backupStopwatch = System.Diagnostics.Stopwatch.StartNew();
                        bool backupUnchanged = false;
                        try { backupUnchanged = _backupService.IsBackupFolderFingerprintUnchanged(); } catch { }
                        if (!backupUnchanged)
                        {
                            await _backupService.RefreshAllBackupStateAsync().ConfigureAwait(false);
                            backupStopwatch.Stop();
                            _logger.LogInformation("Backup state refresh completed in {ms}ms", backupStopwatch.ElapsedMilliseconds);
                        }
                        else
                        {
                            backupStopwatch.Stop();
                            _logger.LogInformation("Startup fast path: skipping RefreshAllBackupState (backups unchanged)");
                        }
                        
                        try { _startupProgressUi.MarkBackupDone(); _startupProgressUi.SetStep(2); } catch { }
                        
                        bool skipHeavy = false;
                        try
                        {
                            var folderTimeoutSeconds = Math.Max(3, Math.Min(120, _configService.Current.StartupFolderTimeoutSeconds));
                            await _penumbraFolderWatcher.WaitForInitialScanAsync(TimeSpan.FromSeconds(folderTimeoutSeconds), token).ConfigureAwait(false);
                            skipHeavy = _penumbraFolderWatcher.IsStartupSnapshotUnchanged();
                        }
                        catch { }

                        if (!skipHeavy)
                        {
                            var populateStopwatch = System.Diagnostics.Stopwatch.StartNew();
                            await _backupService.PopulateMissingOriginalBytesAsync(token).ConfigureAwait(false);
                            populateStopwatch.Stop();
                            _logger.LogInformation("Missing original bytes populated in {ms}ms", populateStopwatch.ElapsedMilliseconds);
                        }
                        else
                        {
                            _logger.LogInformation("Startup fast path: skipping PopulateMissingOriginalBytes (Penumbra unchanged)");
                        }
                        
                        try { _startupProgressUi.SetStep(3); } catch { }

                        if (!skipHeavy)
                        {
                            var threads = Math.Max(1, _configService.Current.MaxStartupThreads);
                            _logger.LogInformation("Starting parallel conversion update with {threads} threads", threads);
                            var conversionStopwatch = System.Diagnostics.Stopwatch.StartNew();
                            await _conversionService.RunInitialParallelUpdateAsync(threads, token).ConfigureAwait(false);
                            conversionStopwatch.Stop();
                            _logger.LogInformation("Parallel conversion update completed in {ms}ms", conversionStopwatch.ElapsedMilliseconds);
                        }
                        else
                        {
                            _logger.LogInformation("Startup fast path: Penumbra folder unchanged, using persisted mod state");
                        }
                        
                        try { _startupProgressUi.SetStep(4); } catch { }
                        
                        var updateStopwatch = System.Diagnostics.Stopwatch.StartNew();
                        await _conversionService.UpdateAllModUsedTextureFilesAsync().ConfigureAwait(false);
                        updateStopwatch.Stop();
                        _logger.LogInformation("Used texture files update completed in {ms}ms", updateStopwatch.ElapsedMilliseconds);
                        
                        try { _startupProgressUi.MarkUsedDone(); _startupProgressUi.SetStep(5); } catch { }

                        try { _modStateService.Save(); _logger.LogDebug("Mod state saved successfully"); } 
                        catch (Exception ex) { _logger.LogInformation(ex, "Failed to save mod state"); }
                        
                        try { _startupProgressUi.MarkSaveDone(); } catch { }
                        if (!skipHeavy)
                        {
                            try { _conversionUi.TriggerStartupRescan(); _logger.LogDebug("Startup rescan triggered"); }
                            catch (Exception ex) { _logger.LogInformation(ex, "Failed to trigger startup rescan"); }
                        }
                        
                        _logger.LogInformation("ShrinkU startup refresh completed successfully");
                    }
                    catch (OperationCanceledException ex) when (token.IsCancellationRequested)
                    {
                        _logger.LogWarning(ex, "Startup refresh cancelled due to timeout");
                    }
                    catch (Exception ex) { _logger.LogError(ex, "Initial refresh failed"); }
                    finally
                    {
                        try { _conversionUi.SetStartupRefreshInProgress(false); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to clear startup refresh flag"); }
                        try { _startupProgressUi.IsOpen = false; } catch { }
                        _initRefreshCts?.Dispose();
                        _initRefreshCts = null;
                    }
                }, token);
            }
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to determine initial UI state"); }
    }

    public void Dispose()
    {
        try { _modStateService.SetSavingEnabled(false); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to disable mod state saving during dispose"); }
        _windowSystem.RemoveAllWindows();
        _pluginInterface.UiBuilder.Draw -= DrawUi;
        _pluginInterface.UiBuilder.OpenConfigUi -= OpenConfigUi;
        _pluginInterface.UiBuilder.OpenMainUi -= OpenMainUi;
        
        // Cancel and dispose init refresh cancellation token source
        try { _initRefreshCts?.Cancel(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to cancel init refresh CTS"); }
        try { _initRefreshCts?.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose init refresh CTS"); }
        _initRefreshCts = null;
        
        // Shutdown background work and dispose services in reverse order of creation
        try { _conversionUi.ShutdownBackgroundWork(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to shutdown conversion UI background work"); }
        try { _conversionUi.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose conversion UI"); }
        try { _settingsUi.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose settings UI"); }
        try { _conversionService.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose conversion service"); }
        try { _backupFolderWatcher.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose backup folder watcher"); }
        try { _penumbraFolderWatcher.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose Penumbra folder watcher"); }
        try { _penumbraExtension.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose Penumbra extension"); }
        try { _shrinkuIpc.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose ShrinkU IPC"); }
        try { _penumbraIpc.Dispose(); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to dispose Penumbra IPC"); }
        try { _commandManager.RemoveHandler("/shrinku"); } catch (Exception ex) { _logger.LogInformation(ex, "Failed to remove /shrinku command handler"); }
        
        _logger.LogInformation("ShrinkU plugin disposed successfully");
    }

    private void DrawUi()
    {
        var style = ImGui.GetStyle();
        var windowBg = style.Colors[(int)ImGuiCol.WindowBg];
        var childBg = style.Colors[(int)ImGuiCol.ChildBg];
        var popupBg = style.Colors[(int)ImGuiCol.PopupBg];
        windowBg.W = 1f;
        childBg.W = 1f;
        popupBg.W = 1f;
        ImGui.PushStyleColor(ImGuiCol.WindowBg, windowBg);
        ImGui.PushStyleColor(ImGuiCol.ChildBg, childBg);
        ImGui.PushStyleColor(ImGuiCol.PopupBg, popupBg);
        ImGui.PushStyleVar(ImGuiStyleVar.Alpha, 1f);
        _windowSystem.Draw();
        ImGui.PopStyleVar();
        ImGui.PopStyleColor(3);
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
                _logger.LogInformation("Startup privilege level: admin={admin}", isAdmin);
            }
            catch (Exception ex) { _logger.LogInformation(ex, "Privilege check failed"); }

            try
            {
                if (!_penumbraIpc.APIAvailable)
                {
                    errors.Add("Penumbra API not available. Please enable Penumbra.");
                    _logger.LogError("Penumbra API unavailable at startup");
                }
                var modDir = _penumbraIpc.ModDirectory ?? string.Empty;
                if (string.IsNullOrWhiteSpace(modDir) || !System.IO.Directory.Exists(modDir))
                {
                    errors.Add("Penumbra mod directory not found.");
                    _logger.LogError("Penumbra mod directory missing: {dir}", modDir);
                }
            }
            catch (Exception ex) { _logger.LogError(ex, "Penumbra dependency check failed"); }

            try
            {
                var cfgDir = _pluginInterface.ConfigDirectory?.FullName ?? string.Empty;
                if (string.IsNullOrWhiteSpace(cfgDir) || !System.IO.Directory.Exists(cfgDir))
                {
                    errors.Add("Configuration folder not available.");
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
                        errors.Add("Configuration folder is not writable.");
                        _logger.LogWarning(ex, "Config directory not writable: {dir}", cfgDir);
                    }
                }
            }
            catch (Exception ex) { _logger.LogError(ex, "Config directory check failed"); }

            try
            {
                var backup = _configService.Current.BackupFolderPath ?? string.Empty;
                if (string.IsNullOrWhiteSpace(backup))
                {
                    errors.Add("Backup folder not configured.");
                    _logger.LogError("Backup folder path not configured");
                }
                else
                {
                    try { System.IO.Directory.CreateDirectory(backup); } catch { }
                    if (!System.IO.Directory.Exists(backup))
                    {
                        errors.Add("Backup folder does not exist.");
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
                        errors.Add("Backup folder is not writable.");
                        _logger.LogWarning(ex, "Backup folder not writable: {path}", backup);
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
