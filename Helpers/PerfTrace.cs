using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

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
        var elapsedMs = (int)_sw.Elapsed.TotalMilliseconds;
        PerfTrace.Record(_name, elapsedMs);
        try { _logger.LogTrace("{name} took {ms} ms", _name, (int)_sw.Elapsed.TotalMilliseconds); } catch { }
    }
}

public static class PerfTrace
{
    private sealed class PerfMetric
    {
        public long TotalCount;
        public long TotalMs;
        public int MaxMs;
        public int LastMs;
        public DateTime LastUtc = DateTime.MinValue;
        public Queue<(DateTime atUtc, int ms)> Recent = new();
    }

    private static readonly object s_lock = new();
    private static readonly Dictionary<string, PerfMetric> s_metrics = new(StringComparer.OrdinalIgnoreCase);

    internal static void Record(string name, int elapsedMs)
    {
        if (string.IsNullOrWhiteSpace(name))
            return;
        var now = DateTime.UtcNow;
        var ms = Math.Max(0, elapsedMs);
        lock (s_lock)
        {
            if (!s_metrics.TryGetValue(name, out var metric))
            {
                metric = new PerfMetric();
                s_metrics[name] = metric;
            }

            metric.TotalCount++;
            metric.TotalMs += ms;
            metric.LastMs = ms;
            metric.LastUtc = now;
            if (ms > metric.MaxMs)
                metric.MaxMs = ms;
            metric.Recent.Enqueue((now, ms));
            PruneRecent(metric, now, TimeSpan.FromMinutes(5));
        }
    }

    public static PerfTraceSummary GetSummary(TimeSpan? window = null, int top = 8)
    {
        var actualWindow = window.GetValueOrDefault(TimeSpan.FromSeconds(60));
        if (actualWindow <= TimeSpan.Zero)
            actualWindow = TimeSpan.FromSeconds(60);
        var now = DateTime.UtcNow;
        var cutoff = now - actualWindow;
        var rows = new List<PerfTraceOperationStat>();
        long totalWindowMs = 0;
        int totalWindowCount = 0;

        lock (s_lock)
        {
            foreach (var kv in s_metrics)
            {
                var metric = kv.Value;
                PruneRecent(metric, now, TimeSpan.FromMinutes(5));
                int windowCount = 0;
                long windowMs = 0;
                foreach (var entry in metric.Recent)
                {
                    if (entry.atUtc < cutoff)
                        continue;
                    windowCount++;
                    windowMs += entry.ms;
                }

                if (windowCount <= 0)
                    continue;

                totalWindowCount += windowCount;
                totalWindowMs += windowMs;
                rows.Add(new PerfTraceOperationStat
                {
                    Name = kv.Key,
                    WindowCount = windowCount,
                    WindowTotalMs = windowMs,
                    MaxMs = metric.MaxMs,
                    LastMs = metric.LastMs,
                    LastUtc = metric.LastUtc,
                });
            }
        }

        var topRows = rows
            .OrderByDescending(static r => r.WindowTotalMs)
            .ThenBy(static r => r.Name, StringComparer.OrdinalIgnoreCase)
            .Take(Math.Max(1, top))
            .ToList();
        var utilization = (totalWindowMs / Math.Max(1d, actualWindow.TotalMilliseconds)) * 100d;

        return new PerfTraceSummary
        {
            GeneratedUtc = now,
            WindowSeconds = actualWindow.TotalSeconds,
            WindowOperationCount = totalWindowCount,
            WindowTotalMs = totalWindowMs,
            WindowUtilizationPercent = utilization,
            TopOperations = topRows,
        };
    }

    private static void PruneRecent(PerfMetric metric, DateTime nowUtc, TimeSpan keep)
    {
        var cutoff = nowUtc - keep;
        while (metric.Recent.Count > 0 && metric.Recent.Peek().atUtc < cutoff)
        {
            metric.Recent.Dequeue();
        }
    }

    public static PerfStep Step(ILogger logger, string name) => new PerfStep(logger, name);
}

public sealed class PerfTraceSummary
{
    public DateTime GeneratedUtc { get; set; } = DateTime.MinValue;
    public double WindowSeconds { get; set; } = 0;
    public int WindowOperationCount { get; set; } = 0;
    public long WindowTotalMs { get; set; } = 0;
    public double WindowUtilizationPercent { get; set; } = 0;
    public List<PerfTraceOperationStat> TopOperations { get; set; } = new();
}

public sealed class PerfTraceOperationStat
{
    public string Name { get; set; } = string.Empty;
    public int WindowCount { get; set; } = 0;
    public long WindowTotalMs { get; set; } = 0;
    public int MaxMs { get; set; } = 0;
    public int LastMs { get; set; } = 0;
    public DateTime LastUtc { get; set; } = DateTime.MinValue;
}
