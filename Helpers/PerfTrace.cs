using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ShrinkU.Helpers;

public readonly struct PerfStep : IDisposable
{
    private readonly ILogger _logger;
    private readonly string _name;
    private readonly Stopwatch _sw;

    public PerfStep(ILogger logger, string name)
    {
        _logger = logger;
        _name = name;
        _sw = Stopwatch.StartNew();
    }

    public void Dispose()
    {
        _sw.Stop();
        try { _logger.LogDebug("[TRACE] {name} took {ms} ms", _name, (int)_sw.Elapsed.TotalMilliseconds); } catch { }
    }
}

public static class PerfTrace
{
    public static PerfStep Step(ILogger logger, string name) => new PerfStep(logger, name);
}

