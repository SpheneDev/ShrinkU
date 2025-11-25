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
        var hasSelectable = HasSelectableFiles(child, visibleByMod);
        bool folderSelected = IsFolderFullySelected(child, visibleByMod);
        ImGui.BeginDisabled(!hasSelectable && child.Mods.Count == 0);
        if (ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
        {
            if (folderSelected)
            {
                var filesAll = CollectFilesRecursive(child, visibleByMod);
                for (int i = 0; i < filesAll.Count; i++)
                    _selectedTextures.Add(filesAll[i]);
                var stack = new Stack<TableCatNode>();
                stack.Push(child);
                while (stack.Count > 0)
                {
                    var cur = stack.Pop();
                    foreach (var mod in cur.Mods)
                    {
                        List<string>? src = null;
                        if (_scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
                            src = all;
                        else if (visibleByMod.TryGetValue(mod, out var vis) && vis != null)
                            src = vis;
                        var cnt = src?.Count ?? 0;
                        if (cnt > 0)
                        {
                            _selectedCountByMod[mod] = cnt;
                        }
                        else
                        {
                            _selectedEmptyMods.Add(mod);
                            _selectedCountByMod[mod] = 0;
                        }
                    }
                    foreach (var ch in cur.Children.Values)
                        stack.Push(ch);
                }
            }
            else
            {
                var folderFiles = CollectFilesRecursive(child, visibleByMod);
                for (int i = 0; i < folderFiles.Count; i++)
                    _selectedTextures.Remove(folderFiles[i]);
                var stack = new Stack<TableCatNode>();
                stack.Push(child);
                while (stack.Count > 0)
                {
                    var cur = stack.Pop();
                    foreach (var mod in cur.Mods)
                    {
                        _selectedEmptyMods.Remove(mod);
                        if (visibleByMod.ContainsKey(mod) || _scannedByMod.ContainsKey(mod))
                            _selectedCountByMod[mod] = 0;
                    }
                    foreach (var ch in cur.Children.Values)
                        stack.Push(ch);
                }
            }
        }
        ImGui.EndDisabled();
        if (!hasSelectable && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
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
