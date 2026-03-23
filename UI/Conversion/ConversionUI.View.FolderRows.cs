using Dalamud.Bindings.ImGui;
using System;
using System.Collections.Generic;
using System.Numerics;
using System.Linq;
using Dalamud.Interface;
using Dalamud.Interface.Utility.Raii;
using ShrinkU.Configuration;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawFolderFlatRow_ViewImpl(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        var fullPath = row.FolderPath;
        var child = row.Node;
        ImGui.TableSetColumnIndex(0);
        var folderCellMin = ImGui.GetCursorScreenPos();
        var folderCellWidth = ImGui.GetColumnWidth();
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var catDefaultOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
        ImGui.SetNextItemOpen(catDefaultOpen, ImGuiCond.Always);
        ImGui.PushStyleColor(ImGuiCol.Header, Vector4.Zero);
        ImGui.PushStyleColor(ImGuiCol.HeaderHovered, Vector4.Zero);
        ImGui.PushStyleColor(ImGuiCol.HeaderActive, Vector4.Zero);
        var catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
        ImGui.PopStyleColor(3);
        var rowHeight = FixedFlatRowHeight;
        var cellPaddingY = ImGui.GetStyle().CellPadding.Y;
        var folderHoverMin = new Vector2(folderCellMin.X, folderCellMin.Y - cellPaddingY);
        var folderHoverMax = new Vector2(folderCellMin.X + folderCellWidth, folderCellMin.Y - cellPaddingY + rowHeight);
        var folderHovered = ImGui.IsMouseHoveringRect(folderHoverMin, folderHoverMax, true);
        var folderSelected = IsFolderFullySelected(child, visibleByMod);
        if (folderHovered)
        {
            var hoverColor = folderSelected ? new Vector4(0.28f, 0.49f, 0.84f, 0.52f) : new Vector4(0.30f, 0.34f, 0.40f, 0.34f);
            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32(hoverColor));
        }
        var catToggled = ImGui.IsItemToggledOpen();
        if (catToggled)
        {
            if (catOpen) _expandedFolders.Add(fullPath);
            else _expandedFolders.Remove(fullPath);
        }
        ImGui.SameLine();
        var folderColor = new Vector4(0.70f, 0.80f, 1.00f, 1f);
        ImGui.PushFont(UiBuilder.IconFont);
        ImGui.TextColored(folderColor, (catOpen ? FontAwesomeIcon.FolderOpen : FontAwesomeIcon.Folder).ToIconString());
        ImGui.PopFont();
        ImGui.SameLine();
        if (!_folderCountsCache.TryGetValue(fullPath, out var cvals))
            cvals = (0, 0, 0, 0);
        ImGui.TextColored(folderColor, $"{child.Name} (mods {cvals.modsConverted}/{cvals.modsTotal}, textures {cvals.texturesConverted}/{cvals.texturesTotal})");

        ImGui.TableSetColumnIndex(2);
        var fSig = string.Concat(_flatRowsSig, "|", _perModSavingsRevision.ToString());
        if (!string.Equals(_folderSizeCacheSig, fSig, StringComparison.Ordinal))
        {
            _folderSizeCache.Clear();
            _folderSizeCacheSig = fSig;
        }
        if (!_folderSizeCache.TryGetValue(fullPath, out var cached))
        {
            long orig = 0;
            long comp = 0;
            foreach (var m in child.Mods)
            {
                List<string>? filesForMod = null;
                if (_scannedByMod.TryGetValue(m, out var allFiles) && allFiles != null && allFiles.Count > 0)
                    filesForMod = allFiles;
                else if (visibleByMod.TryGetValue(m, out var visFiles) && visFiles != null)
                    filesForMod = visFiles;
                var totalAll = GetTotalTexturesForMod(m, filesForMod);
                if (totalAll <= 0) continue;
                var modOrig = GetOrQueryModOriginalTotal(m);
                if (modOrig > 0)
                    orig += modOrig;
                var hasBackupM = GetOrQueryModBackup(m);
                if (!hasBackupM) continue;
                var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                if (snap.TryGetValue(m, out var st) && st != null && st.CurrentBytes > 0 && st.ComparedFiles > 0 && !st.InstalledButNotConverted)
                    comp += st.CurrentBytes;
                else if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null && stats.CurrentBytes > 0 && stats.ComparedFiles > 0)
                {
                    var stx = snap.TryGetValue(m, out var s3) ? s3 : null;
                    if (!(stx != null && stx.InstalledButNotConverted))
                        comp += stats.CurrentBytes;
                }
            }
            cached = (orig, comp);
            _folderSizeCache[fullPath] = cached;
        }
        if (cached.orig > 0)
            DrawRightAlignedSize(cached.orig);
        else
            ImGui.TextUnformatted("");

        ImGui.TableSetColumnIndex(1);
        if (cached.comp > 0)
        {
            var color = cached.comp > cached.orig ? ShrinkUColors.WarningLight : _compressedTextColor;
            DrawRightAlignedSizeColored(cached.comp, color);
        }
        else
        {
            DrawRightAlignedTextColored("-", _compressedTextColor);
        }

        ImGui.TableSetColumnIndex(3);
        var folderMods = CollectModsRecursive(child)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        using (var _dDeleteFolder = ImRaii.Disabled(ActionsDisabled() || folderMods.Count == 0))
        {
            ImGui.PushFont(UiBuilder.IconFont);
            if (ImGui.Button($"{FontAwesomeIcon.Trash.ToIconString()}##delete-folder-{fullPath}", new Vector2(24, 0)))
            {
                if (ImGui.GetIO().KeyCtrl)
                    TryDeleteSelectedEntriesAndBackups(folderMods, "delete-folder-ctrl");
                else
                    SetStatus("Hold CTRL while clicking folder trash to delete all mod entries in this folder.");
            }
            ImGui.PopFont();
        }
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (folderMods.Count == 0)
                ImGui.SetTooltip("No mods found in this folder.");
            else
                ImGui.SetTooltip("Hold CTRL and click to delete all mod entries and backups in this folder.");
        }
    }
}
