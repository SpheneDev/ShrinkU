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
            List<string>? folderFiles = null;
            bool hasSelectable = false;
            if (drawFolderRow)
            {
                hasSelectable = HasSelectableFiles(child, visibleByMod);
            }
            bool folderSelected = false;
            if (drawFolderRow)
            {
                folderFiles = CollectFilesRecursive(child, visibleByMod);
                var hasFiles = folderFiles != null && folderFiles.Count > 0;
                folderSelected = (hasFiles && folderFiles.All(f => _selectedTextures.Contains(f)))
                    || (!hasFiles && child.Mods.Count > 0 && child.Mods.All(m => _selectedEmptyMods.Contains(m)));
            }
            if (drawFolderRow) ImGui.BeginDisabled(!hasSelectable && child.Mods.Count == 0);
            if (drawFolderRow && ImGui.Checkbox($"##cat-sel-{fullPath}", ref folderSelected))
            {
                if (folderSelected)
                {
                    folderFiles = CollectFilesRecursive(child, visibleByMod);
                    var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                    foreach (var mod in child.Mods)
                    {
                        if (!visibleByMod.TryGetValue(mod, out var files))
                            continue;
                        var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                        var totalAll = _scannedByMod.TryGetValue(mod, out var allModFiles2) && allModFiles2 != null ? allModFiles2.Count : files.Count;
                        var convertedAll = 0;
                        var hasBackupM = GetOrQueryModBackup(mod);
                        if (hasBackupM && totalAll > 0)
                        {
                            if (_modPaths.TryGetValue(mod, out var modRoot2) && !string.IsNullOrWhiteSpace(modRoot2))
                            {
                                if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map2) && map2 != null && map2.Count > 0 && allModFiles2 != null)
                                {
                                    foreach (var f in allModFiles2)
                                    {
                                        try
                                        {
                                            var rel = Path.GetRelativePath(modRoot2, f).Replace('\\', '/');
                                            if (map2.ContainsKey(rel)) convertedAll++;
                                        }
                                        catch { }
                                    }
                                }
                            }
                            if (convertedAll == 0 && _cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
                        }
                        var disableCheckbox = automaticMode && !isOrphan && (convertedAll >= totalAll);
                        if (totalAll == 0)
                        {
                            _selectedEmptyMods.Add(mod);
                            continue;
                        }
                        if (!disableCheckbox)
                            foreach (var f in files) _selectedTextures.Add(f);
                    }
                }
                else
                {
                    folderFiles = CollectFilesRecursive(child, visibleByMod);
                    foreach (var f in folderFiles) _selectedTextures.Remove(f);
                    foreach (var mod in child.Mods) _selectedEmptyMods.Remove(mod);
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
                var totalModsInFolder = CountModsRecursive(child);
                var convertedModsInFolder = CountConvertedModsRecursive(child);
                var totalTexturesInFolder = CountTexturesRecursive(child);
                var convertedTexturesInFolder = CountConvertedTexturesRecursive(child);
                ImGui.TextColored(folderColor, $"{name} (mods {convertedModsInFolder}/{totalModsInFolder}, textures {convertedTexturesInFolder}/{totalTexturesInFolder})");
                ImGui.TableSetColumnIndex(3);
                long folderOriginalBytes = 0;
                foreach (var m in child.Mods)
                {
                    var modOrig = GetOrQueryModOriginalTotal(m);
                    if (modOrig > 0)
                        folderOriginalBytes += modOrig;
                }
                if (folderOriginalBytes > 0)
                    DrawRightAlignedSize(folderOriginalBytes);
                else
                    ImGui.TextUnformatted("");
                ImGui.TableSetColumnIndex(2);
                long folderCompressedBytes = 0;
                foreach (var m in child.Mods)
                {
                    var hasBackupM = GetOrQueryModBackup(m);
                    if (!hasBackupM) continue;
                    if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null && stats.CurrentBytes > 0)
                        folderCompressedBytes += stats.CurrentBytes;
                }
                if (folderCompressedBytes > 0)
                {
                    var color = folderCompressedBytes > folderOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
                    DrawRightAlignedSizeColored(folderCompressedBytes, color);
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
                    var nodeFlags = ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.FramePadding;
                    if (!_configService.Current.ShowModFilesInOverview)
                        nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
                    int totalAll = 0;
                    int convertedAll = 0;
                    string header;
                    if (drawModRow)
                    {
                        totalAll = _scannedByMod.TryGetValue(mod, out var allModFiles2) && allModFiles2 != null ? allModFiles2.Count : files.Count;
                        if (hasBackup && totalAll > 0)
                        {
                            if (_cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
                            else
                            {
                                var snap = _modStateService.Snapshot();
                                if (snap.TryGetValue(mod, out var ms) && ms != null && ms.ComparedFiles > 0)
                                    convertedAll = Math.Min(ms.ComparedFiles, totalAll);
                            }
                        }
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
                        open = ImGui.TreeNodeEx($"##mod-{mod}", nodeFlags);
                    }
                    var headerHoveredTree = ImGui.IsItemHovered();
                    if (drawModRow && ImGui.BeginPopupContextItem($"modctx-{mod}"))
                    {
                        if (ImGui.MenuItem("Open in Penumbra"))
                        {
                            var display = ResolveModDisplayName(mod);
                            _conversionService.OpenModInPenumbra(mod, display);
                        }
                        if (ImGui.MenuItem("Backup Mod (PMP)"))
                        {
                            _running = true;
                            ResetBothProgress();
                            SetStatus($"Creating full mod backup (PMP) for {mod}");
                            var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                            _ = _backupService.CreateFullModBackupAsync(mod, progress, CancellationToken.None)
                                .ContinueWith(t =>
                                {
                                    var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                    _uiThreadActions.Enqueue(() => { SetStatus(success ? $"Backup completed for {mod}" : $"Backup failed for {mod}"); });
                                    try { RefreshModState(mod, "manual-pmp-backup"); } catch { }
                                    TriggerMetricsRefresh();
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                        {
                                            bool any = bt.Result;
                                            try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                            _cacheService.SetModHasBackup(mod, any);
                                            _modsWithPmpCache[mod] = true;
                                        }
                                    });
                                    _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                                }, TaskScheduler.Default);
                        }
                        if (ImGui.MenuItem("Open Backup Folder"))
                        {
                            try
                            {
                                var path = _configService.Current.BackupFolderPath;
                                if (!string.IsNullOrWhiteSpace(path))
                                {
                                    var modDir = Path.Combine(path, mod);
                                    try { Directory.CreateDirectory(modDir); } catch { }
                                    try { Process.Start(new ProcessStartInfo("explorer.exe", modDir) { UseShellExecute = true }); } catch { }
                                }
                            }
                            catch { }
                        }
                        ImGui.EndPopup();
                    }
                    if (drawModRow)
                    {
                        ImGui.SameLine();
                        ImGui.AlignTextToFramePadding();
                        ImGui.PushFont(UiBuilder.IconFont);
                    }
                    if (_modEnabledStates.TryGetValue(mod, out var stIcon))
                    {
                        var iconColor = stIcon.Enabled ? new Vector4(0.40f, 0.85f, 0.40f, 1f) : new Vector4(1f, 1f, 1f, 1f);
                        if (drawModRow) ImGui.TextColored(iconColor, FontAwesomeIcon.Cube.ToIconString());
                    }
                    else
                    {
                        if (drawModRow) ImGui.TextUnformatted(FontAwesomeIcon.Cube.ToIconString());
                    }
                    if (drawModRow) ImGui.PopFont();
                    if (drawModRow)
                    {
                        ImGui.SameLine();
                        ImGui.TextUnformatted(header);
                    }

                    if (drawModRow) ImGui.TableSetColumnIndex(0);
                    bool modSelected = isNonConvertible
                        ? _selectedEmptyMods.Contains(mod)
                        : files.All(f => _selectedTextures.Contains(f));
                    var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                    var disableCheckbox = false;
                    if (drawModRow)
                        disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
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
                                foreach (var f in files) _selectedTextures.Add(f);
                            else
                                foreach (var f in files) _selectedTextures.Remove(f);
                        }
                    }
                    if (drawModRow) ImGui.EndDisabled();
                    if (drawModRow && disableCheckbox && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        ImGui.SetTooltip(automaticMode ? "Automatic mode: mod cannot be selected for conversion." : "Mod excluded by tags");
                    else if (drawModRow)
                        ShowTooltip("Toggle selection for all files in this mod.");
                    if (drawModRow && headerHoveredTree)
                    {
                        bool hasPmp = GetOrQueryModPmp(mod);
                        long origBytesTip = GetOrQueryModOriginalTotal(mod);
                        long compBytesTip = 0;
                        if (!isOrphan && _cachedPerModSavings.TryGetValue(mod, out var tipStats) && tipStats != null)
                            compBytesTip = Math.Max(0, tipStats.CurrentBytes);

                        float reductionPctTip = 0f;
                        if (origBytesTip > 0 && compBytesTip > 0)
                            reductionPctTip = MathF.Max(0f, (float)(origBytesTip - compBytesTip) / origBytesTip * 100f);

                        ImGui.BeginTooltip();
                        ImGui.TextUnformatted($"{ResolveModDisplayName(mod)}");
                        ImGui.Separator();
                        ImGui.TextUnformatted($"Textures: {(hasBackup ? convertedAll : 0)}/{totalAll} converted");
                        ImGui.TextUnformatted($"Uncompressed: {FormatSize(origBytesTip)}");
                        var compText = compBytesTip > 0 ? $"{FormatSize(compBytesTip)} ({reductionPctTip:0.00}% reduction)" : "-";
                        ImGui.TextUnformatted($"Compressed: {compText}");
                        var texturesEnabled = _configService.Current.EnableBackupBeforeConversion;
                        var pmpEnabled = _configService.Current.EnableFullModBackupBeforeConversion;
                        string backupText = (hasBackup && hasPmp && texturesEnabled && pmpEnabled) ? "Tex, PMP" : (hasPmp ? "PMP" : (hasBackup ? "Textures" : "None"));
                        ImGui.TextUnformatted($"Backups: {backupText}");

                        string ver = string.Empty;
                        string auth = string.Empty;
                        if (hasPmp)
                        {
                            var meta = GetOrQueryModPmpMeta(mod);
                            if (meta.HasValue)
                            {
                                ver = meta.Value.version;
                                auth = meta.Value.author;
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
                        if (string.IsNullOrWhiteSpace(ver) || string.IsNullOrWhiteSpace(auth))
                        {
                            var live = _backupService.GetLiveModMeta(mod);
                            if (string.IsNullOrWhiteSpace(ver)) ver = live.version;
                            if (string.IsNullOrWhiteSpace(auth)) auth = live.author;
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
                    var stateSnap = _modStateService.Snapshot();
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
                    if (drawModRow) ImGui.BeginDisabled(ActionsDisabled());
                    if (hasBackup || files.Count > 0 || isOrphan)
                    {
                        var hasTexBackup = GetOrQueryModTextureBackup(mod);
                        var hasPmpBackup = GetOrQueryModPmp(mod);
                        bool doInstall = isOrphan;
                        bool doRestore = false;
                        bool doReinstall = false;
                        bool doConvert = false;
                        if (doInstall)
                        {
                            doRestore = false;
                            doConvert = false;
                        }
                        else
                        {
                            var anyBackup = hasBackup || hasTexBackup || hasPmpBackup;
                            if (isNonConvertible)
                            {
                                doConvert = false;
                                if (hasPmpBackup && anyBackup)
                                {
                                    doReinstall = true;
                                    doRestore = false;
                                }
                                else
                                {
                                    doRestore = anyBackup;
                                }
                            }
                            else
                            {
                                doRestore = anyBackup;
                                doConvert = !anyBackup;
                                if (modState != null && modState.InstalledButNotConverted)
                                {
                                    doConvert = true;
                                    doRestore = false;
                                }
                            }
                        }
                        var actionLabel = doInstall
                            ? $"Install##install-{mod}"
                            : (doReinstall ? $"Reinstall##reinstall-{mod}" : (doRestore ? $"Restore##restore-{mod}" : $"Convert##convert-{mod}"));

                        if (drawModRow)
                        {
                            if (doInstall)
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                            else if (doRestore)
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                            else if (doReinstall)
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ReinstallButton);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ReinstallButtonHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ReinstallButtonActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                            else
                            {
                                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ConvertButton);
                                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ConvertButtonHovered);
                                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ConvertButtonActive);
                                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                            }
                        }

                        var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                        var restoreDisabledByAuto = autoMode && (doRestore || doReinstall);
                        bool canInstall = true;
                        if (doInstall)
                        {
                            try
                            {
                                var info = _orphaned.FirstOrDefault(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                                canInstall = info != null && !string.IsNullOrWhiteSpace(info.LatestPmpPath);
                            }
                            catch { canInstall = false; }
                        }
                        if (drawModRow) ImGui.BeginDisabled(excluded || (!isOrphan && restoreDisabledByAuto) || (isOrphan && !canInstall) || ActionsDisabled());
                        if (drawModRow && ImGui.Button(actionLabel, new Vector2(60, 0)))
                        {
                            if (doInstall)
                            {
                                _running = true;
                                ResetBothProgress();
                                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                                _ = _backupService.ReinstallModFromLatestPmpAsync(mod, progress, CancellationToken.None)
                                    .ContinueWith(t =>
                                    {
                                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                        _uiThreadActions.Enqueue(() =>
                                        {
                                            SetStatus(success ? $"Install completed for {mod}" : $"Install failed for {mod}");
                                            _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                            _needsUIRefresh = true;
                                            ClearModCaches(mod);
                                        });
                                        try { _conversionService.OpenModInPenumbra(mod, null); } catch { }
                                        try
                                        {
                                            var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                                            if (autoMode)
                                            {
                                                _ = _conversionService.StartAutomaticConversionForModWithDelayAsync(mod, 2000);
                                            }
                                        }
                                        catch { }
                                        try { _configService.Current.ExternalConvertedMods.Remove(mod); _configService.Save(); } catch { }
                                        try { RefreshModState(mod, "orphan-install"); } catch { }
                                        try { TriggerMetricsRefresh(); } catch { }
                                        _running = false;
                                        _needsUIRefresh = true;
                                    }, TaskScheduler.Default);
                            }
                            else if (doReinstall)
                            {
                                _running = true;
                                ResetBothProgress();
                                _reinstallInProgress = true;
                                _reinstallMod = mod;
                                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                                _ = _backupService.ReinstallModFromLatestPmpAsync(mod, progress, CancellationToken.None)
                                    .ContinueWith(t =>
                                    {
                                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                        _uiThreadActions.Enqueue(() =>
                                        {
                                            SetStatus(success ? $"Reinstall completed for {mod}" : $"Reinstall failed for {mod}");
                                            _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                            _needsUIRefresh = true;
                                            ClearModCaches(mod);
                                            RefreshModState(mod, "reinstall-completed");
                                            Task.Run(async () =>
                                            {
                                                try
                                                {
                                                    await Task.Delay(1200).ConfigureAwait(false);
                                                    _uiThreadActions.Enqueue(() =>
                                                    {
                                                        RefreshScanResults(true, "reinstall-completed");
                                                    });
                                                }
                                                catch { }
                                            });
                                        });
                                        Task.Run(async () =>
                                        {
                                            try
                                            {
                                                await Task.Delay(1200).ConfigureAwait(false);
                                                _conversionService.OpenModInPenumbra(mod, null);
                                            }
                                            catch { }
                                        });
                                        try { RefreshModState(mod, "reinstall-folder-view"); } catch { }
                                        try { TriggerMetricsRefresh(); } catch { }
                                        _reinstallInProgress = false;
                                        _reinstallMod = string.Empty;
                                        ResetBothProgress();
                                        _running = false;
                                    }, TaskScheduler.Default);
                            }
                            else if (doRestore)
                            {
                                var hasPmpForClick = GetOrQueryModPmp(mod);
                                if (hasPmpForClick && _configService.Current.PreferPmpRestoreWhenAvailable)
                                {
                                    TryStartPmpRestoreNewest(mod, "restore-pmp-folder-view", true, false, false, true, false);
                                }
                                else if (hasPmpForClick)
                                {
                                    ImGui.OpenPopup($"restorectx-{mod}");
                                }
                                else
                                {
                                    _running = true;
                                    _modsWithBackupCache.TryRemove(mod, out _);
                                    ResetBothProgress();
                                    _currentRestoreMod = mod;
                                    var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                                    _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                        .ContinueWith(t => {
                                            var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                            try { _backupService.RedrawPlayer(); } catch { }
                                            _logger.LogDebug("Restore completed (mod button in folder view)");
                                            RefreshModState(mod, "restore-folder-view");
                                            TriggerMetricsRefresh();
                                            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                                            _perModSavingsTask.ContinueWith(ps =>
                                            {
                                                if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                                                {
                                                    _cachedPerModSavings = ps.Result;
                                                    _needsUIRefresh = true;
                                                }
                                            }, TaskScheduler.Default);
                                            _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                            {
                                                if (bt.Status == TaskStatus.RanToCompletion)
                                                {
                                                    bool any = bt.Result;
                                                    try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                                    _cacheService.SetModHasBackup(mod, any);
                                                }
                                            });
                                            _running = false;
                                            ResetRestoreProgress();
                                        });
                                }
                            }
                            else
                            {
                                var allFilesForMod = files;
                                try
                                {
                                    if (_configService.Current.IncludeHiddenModTexturesOnConvert && _scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
                                        allFilesForMod = all;
                                }
                                catch { }
                                var toConvert = BuildToConvert(allFilesForMod);
                                ResetConversionProgress();
                                ResetRestoreProgress();
                                _ = _conversionService.StartConversionAsync(toConvert)
                                    .ContinueWith(_ =>
                                    {
                                        _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                        {
                                            if (bt.Status == TaskStatus.RanToCompletion)
                                            {
                                                bool any = bt.Result;
                                                try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                                _cacheService.SetModHasBackup(mod, any);
                                            }
                                        });
                                        _running = false;
                                        ResetRestoreProgress();
                                    });
                            }
                        }
                        if (drawModRow) ImGui.PopStyleColor(1);
                        if (drawModRow && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                        {
                            if (excluded)
                                ImGui.SetTooltip("Mod excluded by tags");
                            else if (restoreDisabledByAuto)
                                ImGui.SetTooltip("Automatic mode active: restoring is disabled.");
                            else
                            {
                                if (doInstall)
                                    ShowTooltip("Install mod from PMP backup.");
                                else if (doReinstall)
                                    ShowTooltip("Reinstall mod from PMP backup.");
                                else if (doRestore)
                                    ShowTooltip("Restore backups for this mod.");
                                else
                                {
                                    var msg = _configService.Current.IncludeHiddenModTexturesOnConvert
                                        ? "Convert all textures for this mod."
                                        : "Convert all visible textures for this mod.";
                                    ShowTooltip(msg);
                                }
                            }
                        }
                        var hasPmp = GetOrQueryModPmp(mod);
                        if (drawModRow && doRestore && hasPmp && !automaticMode)
                        {
                            if (ImGui.BeginPopupContextItem($"restorectx-{mod}"))
                            {
                                ImGui.OpenPopup($"restorectx-{mod}");
                                ImGui.EndPopup();
                            }
                            if (ImGui.BeginPopup($"restorectx-{mod}"))
                            {
                                var hasTexBk = GetOrQueryModTextureBackup(mod);
                                ImGui.BeginDisabled(!hasTexBk);
                                if (ImGui.MenuItem("Restore textures"))
                                {
                                    _running = true;
                                    _modsWithBackupCache.TryRemove(mod, out _);
                                    ResetConversionProgress();
                                    ResetRestoreProgress();
                                    _currentRestoreMod = mod;
                                    SetStatus($"Restore requested for {mod}");
                                    var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                                    _logger.LogInformation("Restore requested for {mod}", mod);
                                    _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                                        .ContinueWith(t => {
                                            var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                            try { _backupService.RedrawPlayer(); } catch { }
                                            _logger.LogDebug(success
                                                ? "Restore completed successfully (context menu, folder view)"
                                                : "Restore failed or aborted (context menu, folder view)");
                                            _logger.LogInformation(success
                                                ? "Restore completed for {mod}"
                                                : "Restore failed for {mod}", mod);
                                            _uiThreadActions.Enqueue(() => { SetStatus(success ? $"Restore completed for {mod}" : $"Restore failed for {mod}"); });
                                            RefreshModState(mod, "restore-folder-view-context");
                                            TriggerMetricsRefresh();
                                            _perModSavingsTask = _backupService.ComputePerModSavingsAsync();
                                            _perModSavingsTask.ContinueWith(ps =>
                                            {
                                                if (ps.Status == TaskStatus.RanToCompletion && ps.Result != null)
                                                {
                                                    _cachedPerModSavings = ps.Result;
                                                    _needsUIRefresh = true;
                                                }
                                            }, TaskScheduler.Default);
                                            _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                            {
                                                if (bt.Status == TaskStatus.RanToCompletion)
                                                {
                                                    bool any = bt.Result;
                                                    try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                                    _cacheService.SetModHasBackup(mod, any);
                                                }
                                            });
                                            _running = false;
                                            ImGui.CloseCurrentPopup();
                                        });
                                }
                                ImGui.EndDisabled();
                                if (!hasTexBk) ShowTooltip("No texture backups available for this mod.");
                                if (ImGui.MenuItem("Restore PMP"))
                                {
                                    TryStartPmpRestoreNewest(mod, "pmp-restore-folder-view-context-newest", false, true, false, false, true);
                                }
                                ImGui.EndPopup();
                            }
                        }
                        if (drawModRow) ImGui.PopStyleColor(3);
                        if (drawModRow) ImGui.EndDisabled();
                    }
                    if (drawModRow && isNonConvertible && !hasBackup)
                    {
                        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                        var backupLabel = $"Backup##backup-{mod}";
                        ImGui.BeginDisabled(isOrphan || ActionsDisabled());
                        if (ImGui.Button(backupLabel, new Vector2(60, 0)))
                        {
                            _running = true;
                            ResetBothProgress();
                            SetStatus($"Creating full mod backup (PMP) for {mod}");
                            var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
                            _ = _backupService.CreateFullModBackupAsync(mod, progress, CancellationToken.None)
                                .ContinueWith(t =>
                                {
                                    var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                    _uiThreadActions.Enqueue(() => { SetStatus(success ? $"Backup completed for {mod}" : $"Backup failed for {mod}"); });
                                    try { RefreshModState(mod, "manual-pmp-backup-button"); } catch { }
                                    TriggerMetricsRefresh();
                                    _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                    {
                                        if (bt.Status == TaskStatus.RanToCompletion)
                                        {
                                            bool any = bt.Result;
                                            try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                                            _cacheService.SetModHasBackup(mod, any);
                                            _modsWithPmpCache[mod] = true;
                                        }
                                    });
                                    _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                                }, TaskScheduler.Default);
                        }
                        ImGui.EndDisabled();
                        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                            ShowTooltip("Create a full mod backup (PMP).");
                        ImGui.PopStyleColor(4);
                    }
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