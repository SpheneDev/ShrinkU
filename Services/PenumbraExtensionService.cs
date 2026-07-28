using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.UI;
using System;
using System.Numerics;

namespace ShrinkU.Services;

public class PenumbraExtensionService : IDisposable
{
    private readonly PenumbraIpc _ipc;
    private readonly ConversionUI _ui;
    private readonly ShrinkUConfigService _configService;
    private readonly ILogger _logger;

    public PenumbraExtensionService(PenumbraIpc ipc, ConversionUI ui, ShrinkUConfigService configService, ILogger logger)
    {
        _ipc = ipc;
        _ui = ui;
        _configService = configService;
        _logger = logger;
        _ipc.PostEnabledDraw += OnPostEnabledDraw;
        _logger.LogDebug("PenumbraExtensionService initialized");
    }

    private void OnPostEnabledDraw(string modDir)
    {
        if (string.IsNullOrEmpty(modDir)) return;
        if (!_configService.Current.ShowPenumbraButton) return;

        try
        {
            ImGui.PushID("shrinku-penumbra-" + modDir);
            ImGui.Spacing();
            if (ImGui.TreeNodeEx("ShrinkU", ImGuiTreeNodeFlags.None))
            {
                ImGui.PushStyleVar(ImGuiStyleVar.FramePadding, new Vector2(10f, 3f));
                if (ImGui.Button("Open ShrinkU"))
                {
                    _ui.OpenForMod(modDir);
                }
                ImGui.PopStyleVar();
                if (ImGui.IsItemHovered())
                {
                    ImGui.SetTooltip("Open ShrinkU to manage this mod.");
                }
                ImGui.TreePop();
            }
            ImGui.PopID();
            ImGui.Spacing();
        }
        catch (Exception ex)
        {
            // Suppress errors to avoid crashing Penumbra UI
             _logger.LogTrace(ex, "Error drawing ShrinkU button in Penumbra");
        }
    }

    public void Dispose()
    {
        _ipc.PostEnabledDraw -= OnPostEnabledDraw;
    }
}
