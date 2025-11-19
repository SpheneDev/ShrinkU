using Dalamud.Bindings.ImGui;
using System;
using System.Collections.Generic;
using System.Numerics;
using System.Linq;
using Dalamud.Interface;
using ShrinkU.Configuration;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawFolderFlatRow_ViewImpl(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        ImGui.TableSetColumnIndex(0);
        var fullPath = row.FolderPath;
        var child = row.Node;
        var folderFiles = CollectFilesRecursive(child, visibleByMod);
        bool folderSelected = (folderFiles.Count > 0 && folderFiles.All(f => _selectedTextures.Contains(f)))
            || (folderFiles.Count == 0 && child.Mods.Count > 0 && child.Mods.All(m => _selectedEmptyMods.Contains(m)));
        ImGui.BeginDisabled(folderFiles.Count == 0 && child.Mods.Count == 0);
        if (ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
        {
            if (folderSelected)
            {
                var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.TryGetValue(mod, out var files))
                        continue;
                    var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                    var hasBackup = GetOrQueryModBackup(mod);
                    var excluded = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
                    var convertedAll = 0;
                    var totalAll = files.Count;
                    var disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
                    if (!disableCheckbox)
                    {
                        foreach (var f in files)
                            _selectedTextures.Add(f);
                    }
                    else if (!isOrphan)
                    {
                        _selectedEmptyMods.Add(mod);
                    }
                }
            }
            else
            {
                foreach (var f in folderFiles) _selectedTextures.Remove(f);
                foreach (var mod in child.Mods) _selectedEmptyMods.Remove(mod);
            }
        }
        ImGui.EndDisabled();
        if (folderFiles.Count == 0 && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip("No selectable files in this folder (filtered or excluded).");
        else
            ShowTooltip("Select or deselect all files in this folder.");

        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var catDefaultOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
        ImGui.SetNextItemOpen(catDefaultOpen, ImGuiCond.Always);
        var catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
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

        ImGui.TableSetColumnIndex(3);
        if (!string.Equals(_folderSizeCacheSig, _flatRowsSig, StringComparison.Ordinal))
        {
            _folderSizeCache.Clear();
            _folderSizeCacheSig = _flatRowsSig;
        }
        if (!_folderSizeCache.TryGetValue(fullPath, out var cached))
        {
            long orig = 0;
            long comp = 0;
            foreach (var m in child.Mods)
            {
                var modOrig = GetOrQueryModOriginalTotal(m);
                if (modOrig > 0)
                    orig += modOrig;
                var hasBackupM = GetOrQueryModBackup(m);
                if (!hasBackupM) continue;
                if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null && stats.CurrentBytes > 0)
                    comp += stats.CurrentBytes;
            }
            cached = (orig, comp);
            _folderSizeCache[fullPath] = cached;
        }
        if (cached.orig > 0)
            DrawRightAlignedSize(cached.orig);
        else
            ImGui.TextUnformatted("");

        ImGui.TableSetColumnIndex(2);
        if (cached.comp > 0)
        {
            var color = cached.comp > cached.orig ? ShrinkUColors.WarningLight : _compressedTextColor;
            DrawRightAlignedSizeColored(cached.comp, color);
        }
        else
        {
            DrawRightAlignedTextColored("-", _compressedTextColor);
        }
    }
}