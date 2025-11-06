using Dalamud.Plugin.Services;
using Microsoft.Extensions.Logging;
using System;

namespace ShrinkU.Interop;

public sealed class PluginLogger : ILogger
{
    private readonly IPluginLog _pluginLog;
    private readonly string _category;

    public PluginLogger(IPluginLog pluginLog, string category = "ShrinkU")
    {
        _pluginLog = pluginLog;
        _category = category;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        // Guard against missing plugin log implementation to prevent exceptions breaking UI flows
        if (_pluginLog == null)
            return;
        var message = formatter(state, exception);
        switch (logLevel)
        {
            case LogLevel.Trace:
                _pluginLog.Verbose(message);
                break;
            case LogLevel.Debug:
                _pluginLog.Debug(message);
                break;
            case LogLevel.Information:
                _pluginLog.Information(message);
                break;
            case LogLevel.Warning:
                _pluginLog.Warning(message);
                break;
            case LogLevel.Error:
            case LogLevel.Critical:
                _pluginLog.Error(message);
                break;
            case LogLevel.None:
            default:
                break;
        }
    }
}