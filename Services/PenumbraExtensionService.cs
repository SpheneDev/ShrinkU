using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using Dalamud.Interface.Utility;
using Microsoft.Extensions.Logging;
using ShrinkU.UI;
using System;
using System.Numerics;

namespace ShrinkU.Services;

public class PenumbraExtensionService : IDisposable
{
    private readonly PenumbraIpc _ipc;
    private readonly ConversionUI _ui;
    private readonly ILogger _logger;

    public PenumbraExtensionService(PenumbraIpc ipc, ConversionUI ui, ILogger logger)
    {
        _ipc = ipc;
        _ui = ui;
        _logger = logger;
        _ipc.PostEnabledDraw += OnPostEnabledDraw;
        _logger.LogDebug("PenumbraExtensionService initialized");
    }

    private void OnPostEnabledDraw(string modDir)
    {
        if (string.IsNullOrEmpty(modDir)) return;

        try
            {
                // Add some spacing to separate from the Enabled checkbox
                ImGui.Dummy(new Vector2(0, 5));

                // Full width but standard height
                if (ImGui.Button("Open ShrinkU", new Vector2(-1, 0)))
                {
                     _ui.OpenForMod(modDir);
                }
                if (ImGui.IsItemHovered())
            {
                 ImGui.SetTooltip("Open ShrinkU to manage this mod.");
            }
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
