using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace ShrinkU.Services;

public sealed class DebugTraceService
{
    private readonly int _maxEntries;
    private readonly ConcurrentQueue<(DateTime atUtc, string message)> _modState;
    private readonly ConcurrentQueue<(DateTime atUtc, string message)> _ui;
    private readonly ConcurrentQueue<(DateTime atUtc, string message)> _actions;

    public DebugTraceService(int maxEntries = 1000)
    {
        _maxEntries = maxEntries;
        _modState = new ConcurrentQueue<(DateTime, string)>();
        _ui = new ConcurrentQueue<(DateTime, string)>();
        _actions = new ConcurrentQueue<(DateTime, string)>();
    }

    public void AddModState(string message)
    {
        _modState.Enqueue((DateTime.UtcNow, message ?? string.Empty));
        Trim(_modState);
    }

    public void AddUi(string message)
    {
        _ui.Enqueue((DateTime.UtcNow, message ?? string.Empty));
        Trim(_ui);
    }

    public void AddAction(string message)
    {
        _actions.Enqueue((DateTime.UtcNow, message ?? string.Empty));
        Trim(_actions);
    }

    public IReadOnlyList<(DateTime atUtc, string message)> SnapshotModState()
    {
        var list = new List<(DateTime, string)>();
        foreach (var e in _modState)
            list.Add(e);
        return list;
    }

    public IReadOnlyList<(DateTime atUtc, string message)> SnapshotUi()
    {
        var list = new List<(DateTime, string)>();
        foreach (var e in _ui)
            list.Add(e);
        return list;
    }

    public IReadOnlyList<(DateTime atUtc, string message)> SnapshotActions()
    {
        var list = new List<(DateTime, string)>();
        foreach (var e in _actions)
            list.Add(e);
        return list;
    }

    public void Clear()
    {
        while (_modState.TryDequeue(out _)) { }
        while (_ui.TryDequeue(out _)) { }
        while (_actions.TryDequeue(out _)) { }
    }

    private void Trim(ConcurrentQueue<(DateTime atUtc, string message)> q)
    {
        while (q.Count > _maxEntries && q.TryDequeue(out _)) { }
    }
}