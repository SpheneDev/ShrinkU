using Dalamud.Bindings.ImGui;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Logging;
using Dalamud.Interface;
using ShrinkU.Configuration;
using ShrinkU.Services;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawCategoryTableNode_ViewImpl(TableCatNode node, Dictionary<string, List<string>> visibleByMod, ref int idx, string pathPrefix, int depth = 0, int clipStart = 0, int clipEnd = int.MaxValue)
    {
        const float indentStep = 16f;
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            bool drawFolderRow = idx >= clipStart && idx < clipEnd;
            if (drawFolderRow)
            {
                ImGui.TableNextRow();
                ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                ImGui.TableSetColumnIndex(0);
            }
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            bool hasSelectable = false;
            if (drawFolderRow)
            {
                hasSelectable = HasSelectableFiles(child, visibleByMod);
            }
            bool folderSelected = false;
            if (drawFolderRow)
            {
                folderSelected = IsFolderFullySelected(child, visibleByMod);
            }
            if (drawFolderRow) ImGui.BeginDisabled(!hasSelectable && child.Mods.Count == 0);
            if (drawFolderRow && ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
            {
                if (folderSelected)
                {
                    var filesAll = CollectFilesRecursive(child, visibleByMod);
                    for (int i = 0; i < filesAll.Count; i++)
                        _selectedTextures.Add(filesAll[i]);
                    var stackSel = new Stack<TableCatNode>();
                    stackSel.Push(child);
                    while (stackSel.Count > 0)
                    {
                        var cur = stackSel.Pop();
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
                            stackSel.Push(ch);
                    }
                }
                else
                {
                    var folderFiles2 = CollectFilesRecursive(child, visibleByMod);
                    for (int i = 0; i < folderFiles2.Count; i++)
                        _selectedTextures.Remove(folderFiles2[i]);
                    var stackDes = new Stack<TableCatNode>();
                    stackDes.Push(child);
                    while (stackDes.Count > 0)
                    {
                        var cur = stackDes.Pop();
                        foreach (var mod in cur.Mods)
                        {
                            _selectedEmptyMods.Remove(mod);
                            if (visibleByMod.ContainsKey(mod) || _scannedByMod.ContainsKey(mod))
                                _selectedCountByMod[mod] = 0;
                        }
                        foreach (var ch in cur.Children.Values)
                            stackDes.Push(ch);
                    }
                }
            }
            if (drawFolderRow) ImGui.EndDisabled();
            if (drawFolderRow && !hasSelectable && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                ImGui.SetTooltip("No selectable files in this folder (filtered or excluded).");
            else if (drawFolderRow)
                ShowTooltip("Select or deselect all files in this folder.");
            idx++;

            var catDefaultOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
            bool catOpen;
            if (drawFolderRow)
            {
                ImGui.TableSetColumnIndex(1);
                ImGui.SetCursorPosX(ImGui.GetCursorPosX() + depth * indentStep);
                ImGui.SetNextItemOpen(catDefaultOpen, ImGuiCond.Always);
                catOpen = ImGui.TreeNodeEx($"##cat-{fullPath}", ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.FramePadding | ImGuiTreeNodeFlags.NoTreePushOnOpen);
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
                ImGui.TextColored(folderColor, $"{name} (mods {cvals.modsConverted}/{cvals.modsTotal}, textures {cvals.texturesConverted}/{cvals.texturesTotal})");
                ImGui.TableSetColumnIndex(3);
                if (!_folderSizeCache.TryGetValue(fullPath, out var sizePair))
                    sizePair = (0, 0);
                if (sizePair.orig > 0)
                    DrawRightAlignedSize(sizePair.orig);
                else
                    ImGui.TextUnformatted("");
                ImGui.TableSetColumnIndex(2);
                if (sizePair.comp > 0)
                {
                    var color = sizePair.comp > sizePair.orig ? ShrinkUColors.WarningLight : _compressedTextColor;
                    DrawRightAlignedSizeColored(sizePair.comp, color);
                }
                else
                {
                    DrawRightAlignedTextColored("-", _compressedTextColor);
                }
            }
            else
            {
                catOpen = catDefaultOpen;
            }

            if (catOpen)
            {
                DrawCategoryTableNode(child, visibleByMod, ref idx, fullPath, depth + 1, clipStart, clipEnd);

                foreach (var mod in child.Mods)
                {
                    if (!visibleByMod.TryGetValue(mod, out var files))
                        continue;

                    bool drawModRow = idx >= clipStart && idx < clipEnd;
                    if (drawModRow)
                    {
                        ImGui.TableNextRow();
                        ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                    }
                    var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                    var hasBackup = GetOrQueryModBackup(mod);
                    var excluded = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
                    var isNonConvertible = files.Count == 0;

                    if (drawModRow)
                    {
                        ImGui.TableSetColumnIndex(1);
                        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (depth + 1) * indentStep);
                    }
                    var nodeFlags = ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.FramePadding;
                    if (!_configService.Current.ShowModFilesInOverview)
                        nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
                    int totalAll = 0;
                    int convertedAll = 0;
                    string header;
                    if (drawModRow)
                    {
                        totalAll = GetTotalTexturesForMod(mod, files);
                        if (hasBackup && totalAll > 0)
                        {
                            if (_cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
                            else
                            {
                                var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                                if (snap.TryGetValue(mod, out var ms) && ms != null && ms.ComparedFiles > 0)
                                    convertedAll = Math.Min(ms.ComparedFiles, totalAll);
                            }
                        }
                        // Re-evaluate non-convertible based on all textures, not current filter
                        isNonConvertible = totalAll <= 0;
                        header = hasBackup
                            ? $"{ResolveModDisplayName(mod)} ({convertedAll}/{totalAll})"
                            : $"{ResolveModDisplayName(mod)} ({totalAll})";
                    }
                    else
                    {
                        header = ResolveModDisplayName(mod);
                    }
                    bool open = _expandedMods.Contains(mod);
                    if (drawModRow)
                    {
                        ImGui.SetNextItemOpen(open, ImGuiCond.Always);
                        ImGui.PushFont(UiBuilder.IconFont);
                        var iconColor = _modEnabledStates.TryGetValue(mod, out var stIcon) && stIcon.Enabled ? new Vector4(0.40f, 0.85f, 0.40f, 1f) : new Vector4(1f, 1f, 1f, 1f);
                        ImGui.TextColored(iconColor, FontAwesomeIcon.Cube.ToIconString());
                        ImGui.PopFont();
                        ImGui.SameLine();
                    ImGui.AlignTextToFramePadding();
                    open = ImGui.TreeNodeEx($"{header}##mod-{mod}", nodeFlags);
                    var modToggled = ImGui.IsItemToggledOpen();
                    if (modToggled)
                    {
                        if (open) _expandedMods.Add(mod);
                        else _expandedMods.Remove(mod);
                    }
                    }
                    var headerHoveredTree = ImGui.IsItemHovered();

                    if (drawModRow) ImGui.TableSetColumnIndex(0);
                    bool modSelected = (files.Count == 0)
                        ? _selectedEmptyMods.Contains(mod)
                        : ((_selectedCountByMod.TryGetValue(mod, out var sc) ? sc : 0) >= totalAll);
                    var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                    var disableCheckbox = false;
                    if (drawModRow)
                        disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
                    string disableReason = string.Empty;
                    if (disableCheckbox)
                    {
                        if (excluded)
                            disableReason = "Mod excluded by tags.";
                        else if (automaticMode && !isOrphan && (convertedAll >= totalAll))
                            disableReason = "All textures already converted.";
                        else if (!isNonConvertible && files.Count == 0)
                            disableReason = string.Empty;
                        else if (isNonConvertible)
                            disableReason = "Mod has no textures.";
                    }
                    if (drawModRow) ImGui.BeginDisabled(disableCheckbox);
                    if (drawModRow && ImGui.Checkbox($"##modsel-{mod}", ref modSelected))
                    {
                        if (isNonConvertible)
                        {
                            if (modSelected) _selectedEmptyMods.Add(mod); else _selectedEmptyMods.Remove(mod);
                        }
                        else
                        {
                            if (modSelected)
                            {
                                List<string>? allFilesForMod = null;
                                if (_scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
                                    allFilesForMod = all;
                                else
                                    allFilesForMod = files;
                                foreach (var f in allFilesForMod) _selectedTextures.Add(f);
                                _selectedCountByMod[mod] = totalAll;
                            }
                            else
                            {
                                if (_scannedByMod.TryGetValue(mod, out var allRem) && allRem != null && allRem.Count > 0)
                                    foreach (var f in allRem) _selectedTextures.Remove(f);
                                foreach (var f in files) _selectedTextures.Remove(f);
                                _selectedCountByMod[mod] = 0;
                            }
                        }
                    }
                    if (drawModRow) ImGui.EndDisabled();
                    if (drawModRow && disableCheckbox && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        ImGui.SetTooltip(string.IsNullOrEmpty(disableReason) ? "Selection disabled." : disableReason);
                    else if (drawModRow)
                        ShowTooltip("Toggle selection for all files in this mod.");
                    if (drawModRow && headerHoveredTree)
                    {
                        bool hasPmp = false;
                        long origBytesTip = 0;
                        long compBytesTip = 0;
                        int comparedFilesTip = 0;
                        var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                        if (snap.TryGetValue(mod, out var ms) && ms != null)
                        {
                            hasPmp = ms.HasPmpBackup;
                            origBytesTip = ms.OriginalBytes;
                            compBytesTip = ms.CurrentBytes;
                            comparedFilesTip = ms.ComparedFiles;
                        }
                        else
                        {
                            hasPmp = GetOrQueryModPmp(mod);
                            origBytesTip = GetOrQueryModOriginalTotal(mod);
                            if (!isOrphan && _cachedPerModSavings.TryGetValue(mod, out var tipStats) && tipStats != null)
                                compBytesTip = Math.Max(0, tipStats.CurrentBytes);
                        }

                        float reductionPctTip = 0f;
                        if (origBytesTip > 0 && compBytesTip > 0)
                            reductionPctTip = MathF.Max(0f, (float)(origBytesTip - compBytesTip) / origBytesTip * 100f);

                        ImGui.BeginTooltip();
                        ImGui.TextUnformatted($"{ResolveModDisplayName(mod)}");
                        ImGui.Separator();
                        ImGui.TextUnformatted($"Textures: {(hasBackup ? convertedAll : 0)}/{totalAll} converted");
                        if (comparedFilesTip > 0)
                            ImGui.TextUnformatted($"Compared: {comparedFilesTip}");
                        ImGui.TextUnformatted($"Uncompressed: {FormatSize(origBytesTip)}");
                        var compText = compBytesTip > 0 ? $"{FormatSize(compBytesTip)} ({reductionPctTip:0.00}% reduction)" : "-";
                        ImGui.TextUnformatted($"Compressed: {compText}");
                        var texturesEnabled = _configService.Current.EnableBackupBeforeConversion;
                        var pmpEnabled = _configService.Current.EnableFullModBackupBeforeConversion;
                        string backupText = (hasBackup && hasPmp && texturesEnabled && pmpEnabled) ? "Tex, PMP" : (hasPmp ? "PMP" : (hasBackup ? "Textures" : "None"));
                        ImGui.TextUnformatted($"Backups: {backupText}");

                        string ver = string.Empty;
                        string auth = string.Empty;
                        if (snap.TryGetValue(mod, out var ms2) && ms2 != null)
                        {
                            ver = ms2.CurrentVersion ?? string.Empty;
                            auth = ms2.CurrentAuthor ?? string.Empty;
                        }
                        if (string.IsNullOrWhiteSpace(ver) || string.IsNullOrWhiteSpace(auth))
                        {
                            var meta = GetOrQueryModPmpMeta(mod);
                            if (meta.HasValue)
                            {
                                if (string.IsNullOrWhiteSpace(ver)) ver = meta.Value.version;
                                if (string.IsNullOrWhiteSpace(auth)) auth = meta.Value.author;
                            }
                        }
                        if (string.IsNullOrWhiteSpace(ver) || string.IsNullOrWhiteSpace(auth))
                        {
                            var zmeta = GetOrQueryModZipMeta(mod);
                            if (zmeta.HasValue)
                            {
                                if (string.IsNullOrWhiteSpace(ver)) ver = zmeta.Value.version;
                                if (string.IsNullOrWhiteSpace(auth)) auth = zmeta.Value.author;
                            }
                        }
                        if (!string.IsNullOrWhiteSpace(ver)) ImGui.TextUnformatted($"Version: {ver}");
                        if (!string.IsNullOrWhiteSpace(auth)) ImGui.TextUnformatted($"Author: {auth}");
                        var enabledState = _modEnabledStates.TryGetValue(mod, out var st) ? (st.Enabled ? "Enabled" : "Disabled") : "Disabled";
                        ImGui.TextUnformatted($"State: {enabledState}");
                        ImGui.EndTooltip();
                    }
                    var hasPersistent = _configService.Current.ExternalConvertedMods.ContainsKey(mod);
                    var showExternal = ((DateTime.UtcNow - _lastExternalChangeAt).TotalSeconds < 30 && !string.IsNullOrEmpty(_lastExternalChangeReason)) || hasPersistent;
                    if (showExternal && drawModRow)
                    {
                        ImGui.SameLine();
                        ImGui.PushFont(UiBuilder.IconFont);
                        ImGui.TextColored(new Vector4(0.70f, 0.85f, 1.00f, 1f), FontAwesomeIcon.Plug.ToIconString());
                        ImGui.PopFont();
                        if (ImGui.IsItemHovered())
                        {
                            string reason = _lastExternalChangeReason;
                            if (hasPersistent)
                            {
                                try
                                {
                                    if (_configService.Current.ExternalConvertedMods.TryGetValue(mod, out var marker) && marker != null)
                                        reason = marker.Reason ?? reason;
                                }
                                catch { }
                            }
                            var tip = !string.IsNullOrWhiteSpace(reason) && reason.Equals("ipc-auto-conversion-complete", StringComparison.OrdinalIgnoreCase)
                                ? "Automatic via Sphene"
                                : string.IsNullOrWhiteSpace(reason) ? "External conversion detected" : "External conversion detected: " + reason;
                            ImGui.SetTooltip(tip);
                        }
                    }
                    if (drawModRow) ImGui.SameLine();
                    if (drawModRow && IsModInefficient(mod))
                    {
                        ImGui.PushFont(UiBuilder.IconFont);
                        ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
                        ImGui.PopFont();
                        if (ImGui.IsItemHovered())
                            ImGui.SetTooltip("This mod becomes larger after conversion");
                        ImGui.SameLine();
                    }
                    if (drawModRow && hasBackup && _cachedPerModSavings.TryGetValue(mod, out var noteStats1) && noteStats1 != null && noteStats1.OriginalBytes > 0 && noteStats1.CurrentBytes > noteStats1.OriginalBytes)
                    {
                        ImGui.PushFont(UiBuilder.IconFont);
                        ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
                        ImGui.PopFont();
                        if (ImGui.IsItemHovered())
                            ImGui.SetTooltip("This mod is smaller when not converted");
                        ImGui.SameLine();
                    }

                    if (drawModRow) ImGui.TableSetColumnIndex(3);
                    _cachedPerModSavings.TryGetValue(mod, out var modStats);
                    var stateSnap = _modStateSnapshot ?? _modStateService.Snapshot();
                    stateSnap.TryGetValue(mod, out var modState);
                    long modOriginalBytes = modState != null && modState.ComparedFiles > 0 ? modState.OriginalBytes : GetOrQueryModOriginalTotal(mod);
                    var hideStatsForNoTextures = totalAll == 0;
                    if (drawModRow && hideStatsForNoTextures)
                        ImGui.TextUnformatted("");
                    else if (drawModRow)
                        DrawRightAlignedSize(modOriginalBytes);

                    if (drawModRow) ImGui.TableSetColumnIndex(2);
                    long modCurrentBytes = 0;
                    var hideCompressed = modState != null && modState.InstalledButNotConverted;
                    if (!isOrphan && hasBackup && modState != null && modState.CurrentBytes > 0 && !hideCompressed)
                        modCurrentBytes = modState.CurrentBytes;
                    if (drawModRow && hideStatsForNoTextures)
                        ImGui.TextUnformatted("");
                    else if (drawModRow && modCurrentBytes <= 0)
                        DrawRightAlignedTextColored("-", _compressedTextColor);
                    else if (drawModRow)
                    {
                        var color = modCurrentBytes > modOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
                        DrawRightAlignedSizeColored(modCurrentBytes, color);
                    }

                    if (drawModRow) ImGui.TableSetColumnIndex(4);
                    if (drawModRow) ImGui.TextUnformatted("");
                    
                    if (drawModRow) ImGui.EndDisabled();
                    idx++;

                    if (open)
                    {
                        if (_configService.Current.ShowModFilesInOverview)
                        {
                            foreach (var file in files)
                            {
                                bool drawFileRow = idx >= clipStart && idx < clipEnd;
                                if (drawFileRow)
                                {
                                    ImGui.TableNextRow();
                                    ImGui.TableSetColumnIndex(0);
                                }

                                if (drawFileRow)
                                {
                                    ImGui.TableSetColumnIndex(1);
                                    ImGui.SetCursorPosX(ImGui.GetCursorPosX() + (depth + 2) * indentStep);
                                    var baseName = Path.GetFileName(file);
                                    ImGui.TextUnformatted(baseName);
                                    if (ImGui.IsItemHovered())
                                        ImGui.SetTooltip(file);
                                }

                                if (drawFileRow) ImGui.TableSetColumnIndex(2);
                                var hasBackupForMod = GetOrQueryModBackup(mod);
                                var fileSize = GetCachedOrComputeSize(file);
                                if (drawFileRow && !hasBackupForMod)
                                {
                                    DrawRightAlignedTextColored("-", _compressedTextColor);
                                }
                                else if (drawFileRow)
                                {
                                    if (fileSize > 0)
                                        DrawRightAlignedSizeColored(fileSize, _compressedTextColor);
                                    else
                                        ImGui.TextUnformatted("");
                                }

                                if (drawFileRow) ImGui.TableSetColumnIndex(3);
                                if (!hasBackupForMod && drawFileRow)
                                {
                                    if (fileSize > 0)
                                        DrawRightAlignedSize(fileSize);
                                    else
                                        ImGui.TextUnformatted("");
                                }
                                else if (drawFileRow)
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

                                if (drawFileRow) ImGui.TableSetColumnIndex(4);
                                idx++;
                            }
                        }
                    }
                }
            }
        }
    }
}
