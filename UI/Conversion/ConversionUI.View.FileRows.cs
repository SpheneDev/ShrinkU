using Dalamud.Bindings.ImGui;
using System;
using System.Collections.Generic;
using System.IO;
using System.Numerics;
using ShrinkU.Configuration;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawFileFlatRow_ViewImpl(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        var mod = row.Mod;
        var file = row.File;
        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var baseName = Path.GetFileName(file);
        ImGui.TextUnformatted(baseName);
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip(file);

        ImGui.TableSetColumnIndex(2);
        var hasBackupForMod = GetOrQueryModBackup(mod);
        var fileSize = GetCachedOrComputeSize(file);
        if (!hasBackupForMod)
        {
            DrawRightAlignedTextColored("-", _compressedTextColor);
        }
        else
        {
            if (fileSize > 0)
                DrawRightAlignedSizeColored(fileSize, _compressedTextColor);
            else
                ImGui.TextUnformatted("");
        }

        ImGui.TableSetColumnIndex(3);
        if (!hasBackupForMod)
        {
            if (fileSize > 0)
                DrawRightAlignedSize(fileSize);
            else
                ImGui.TextUnformatted("");
        }
        else
        {
            long originalSize = 0;
            if (_modPaths.TryGetValue(mod, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
            {
                try
                {
                    var rel = Path.GetRelativePath(modRoot, file).Replace('\\', '/');
                    if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null)
                    {
                        map.TryGetValue(rel, out originalSize);
                    }
                }
                catch { }
            }
            if (originalSize > 0)
                DrawRightAlignedSize(originalSize);
            else
                ImGui.TextUnformatted("");
        }

        ImGui.TableSetColumnIndex(4);
    }
}