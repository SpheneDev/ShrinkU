using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Services;
using System;
using System.Numerics;

namespace ShrinkU.UI;

public sealed class StartupProgressUI : Window
{
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly TextureConversionService _conversionService;
    private readonly TextureBackupService _backupService;

    private volatile int _stepIndex = 0;
    private volatile int _populateDone = 0;
    private volatile int _populateTotal = 0;
    private volatile int _populateEta = 0;
    private volatile int _initialDone = 0;
    private volatile int _initialTotal = 0;
    private volatile int _initialEta = 0;
    private volatile bool _backupDone = false;
    private volatile bool _populateDoneFlag = false;
    private volatile bool _initialDoneFlag = false;
    private volatile bool _usedDone = false;
    private volatile bool _saveDone = false;

    public StartupProgressUI(ILogger logger, ShrinkUConfigService configService, TextureConversionService conversionService, TextureBackupService backupService)
        : base("ShrinkU Startup###ShrinkUStartupProgressUI")
    {
        _logger = logger;
        _configService = configService;
        _conversionService = conversionService;
        _backupService = backupService;
        SizeConstraints = new WindowSizeConstraints { MinimumSize = new Vector2(520, 240), MaximumSize = new Vector2(1920, 1080) };
        _conversionService.OnStartupProgress += p => { _initialDone = p.processed; _initialTotal = p.total; _initialEta = p.etaSeconds; if (_initialDone >= _initialTotal && _initialTotal > 0) _initialDoneFlag = true; };
        _backupService.OnPopulateOriginalBytesProgress += p => { _populateDone = p.processed; _populateTotal = p.total; _populateEta = p.etaSeconds; if (_populateDone >= _populateTotal && _populateTotal > 0) _populateDoneFlag = true; };
    }

    public void SetStep(int index) { _stepIndex = index; }
    public void MarkBackupDone() { _backupDone = true; }
    public void MarkUsedDone() { _usedDone = true; }
    public void MarkSaveDone() { _saveDone = true; }
    public void ResetAll()
    {
        _stepIndex = 0;
        _populateDone = 0;
        _populateTotal = 0;
        _populateEta = 0;
        _initialDone = 0;
        _initialTotal = 0;
        _initialEta = 0;
        _backupDone = false;
        _populateDoneFlag = false;
        _initialDoneFlag = false;
        _usedDone = false;
        _saveDone = false;
    }

    public override void Draw()
    {
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Initialization");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);

        var totalWeight = 0f;
        var doneWeight = 0f;

        totalWeight += 1f; doneWeight += _backupDone ? 1f : 0f;
        totalWeight += 1f; doneWeight += _populateTotal > 0 ? Math.Min(1f, Math.Max(0f, (float)_populateDone / Math.Max(1, _populateTotal))) : (_populateDoneFlag ? 1f : 0f);
        totalWeight += 1f; doneWeight += _initialTotal > 0 ? Math.Min(1f, Math.Max(0f, (float)_initialDone / Math.Max(1, _initialTotal))) : (_initialDoneFlag ? 1f : 0f);
        totalWeight += 1f; doneWeight += _usedDone ? 1f : 0f;
        totalWeight += 1f; doneWeight += _saveDone ? 1f : 0f;

        var overall = totalWeight > 0f ? Math.Min(1f, Math.Max(0f, doneWeight / totalWeight)) : 0f;
        ImGui.ProgressBar(overall, new Vector2(-1, 16), $"{(int)Math.Round(overall * 100)}% overall");
        ImGui.Spacing();

        DrawStep("Backup Refresh", _backupDone, 0, 0, _stepIndex >= 1);
        DrawStep("OriginalBytes Population", _populateDoneFlag, _populateTotal, _populateDone, _stepIndex >= 2, _populateEta);
        DrawStep("Initial Mod Update", _initialDoneFlag, _initialTotal, _initialDone, _stepIndex >= 3, _initialEta);
        DrawStep("Used Textures Update", _usedDone, 0, 0, _stepIndex >= 4);
        DrawStep("Save", _saveDone, 0, 0, _stepIndex >= 5);
    }

    private void DrawStep(string title, bool done, int total, int doneCount, bool active, int etaSeconds = 0)
    {
        var status = done ? "done" : (active ? "running" : "pending");
        ImGui.TextColored(ShrinkUColors.Accent, title);
        ImGui.SameLine();
        ImGui.Text($"– {status}");
        if (total > 0)
        {
            var pct = Math.Min(1f, Math.Max(0f, (float)doneCount / Math.Max(1, total)));
            ImGui.ProgressBar(pct, new Vector2(-1, 12), $"{doneCount}/{total} • {(int)Math.Round(pct * 100)}% • ETA {etaSeconds}s");
        }
        else
        {
            if (done)
                ImGui.ProgressBar(1f, new Vector2(-1, 12), "100%");
            else
                ImGui.ProgressBar(active ? 0.5f : 0f, new Vector2(-1, 12), active ? "running" : "");
        }
        ImGui.Spacing();
    }
}
