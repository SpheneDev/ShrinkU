using Dalamud.Bindings.ImGui;
using Dalamud.Interface;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private IReadOnlyDictionary<string, ShrinkU.Services.ModStateEntry>? _modStateSnapshot;
    private string _fileSizeWarmupSig = string.Empty;
    private string _visibleByModSig = string.Empty;
    private Dictionary<string, List<string>> _visibleByMod = new(StringComparer.OrdinalIgnoreCase);
    private void DrawOverview_ViewImpl()
    {
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Scanned Files Overview");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);
        ImGui.SetNextItemWidth(186f);
        ImGui.InputTextWithHint("##scanFilter", "Filter by file or mod", ref _scanFilter, 128);

        ImGui.SameLine();
        var h = ImGui.GetFrameHeight();
        var w = h + ImGui.GetStyle().ItemInnerSpacing.X * 2f;
        ImGui.PushFont(UiBuilder.IconFont);
        if (_filterPenumbraUsedOnly)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        }
        ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
        var usedOnlyClicked = ImGui.Button(FontAwesomeIcon.Eye.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterPenumbraUsedOnly)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (usedOnlyClicked)
        {
            _filterPenumbraUsedOnly = !_filterPenumbraUsedOnly;
            _configService.Current.FilterPenumbraUsedOnly = _filterPenumbraUsedOnly;
            _configService.Save();
            RequestUiRefresh("used-only-toggle");
            if (_filterPenumbraUsedOnly && _penumbraUsedFiles.Count == 0 && !_loadingPenumbraUsed)
            {
                _loadingPenumbraUsed = true;
                _ = _conversionService.GetUsedModTexturePathsAsync().ContinueWith(t =>
                {
                    if (t.Status == TaskStatus.RanToCompletion && t.Result != null)
                    {
                        _penumbraUsedFiles = t.Result;
                    }
                    _loadingPenumbraUsed = false;
                    RequestUiRefresh("used-only-toggle-loaded");
                });
            }
        }

        ImGui.SameLine();
        ImGui.PushFont(UiBuilder.IconFont);
        if (_filterNonConvertibleMods)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        }
        ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
        var hideNonConvertibleClicked = ImGui.Button(FontAwesomeIcon.Ban.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterNonConvertibleMods)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (hideNonConvertibleClicked)
        {
            _filterNonConvertibleMods = !_filterNonConvertibleMods;
            _configService.Current.FilterNonConvertibleMods = _filterNonConvertibleMods;
            _configService.Save();
        }

        ImGui.SameLine();
        ImGui.PushFont(UiBuilder.IconFont);
        if (_filterInefficientMods)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        }
        ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
        var hideInefficientClicked = ImGui.Button(FontAwesomeIcon.ExclamationTriangle.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterInefficientMods)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (hideInefficientClicked)
        {
            _filterInefficientMods = !_filterInefficientMods;
            _configService.Current.HideInefficientMods = _filterInefficientMods;
            _configService.Save();
        }

        if (!_selectedCollectionId.HasValue)
        {
            _ = _conversionService.GetCurrentCollectionAsync().ContinueWith(cc =>
            {
                if (cc.Status == TaskStatus.RanToCompletion && cc.Result != null)
                {
                    _selectedCollectionId = cc.Result?.Id;
                    if (_selectedCollectionId.HasValue)
                    {
                        _ = _conversionService.GetAllModEnabledStatesAsync(_selectedCollectionId.Value).ContinueWith(es =>
                        {
                            if (es.Status == TaskStatus.RanToCompletion && es.Result != null)
                                _modEnabledStates = es.Result;
                        });
                    }
                }
            });
        }
        if (_scannedByMod.Count == 0)
        {
            // Proceed with empty file lists; rows will use mod_state counts
        }

        DrawScannedFilesTable();
    }

    private void DrawScannedFilesTable_ViewImpl()
    {
        var snapForSig = _modStateService.Snapshot();
        var usedCountSig = 0;
        foreach (var kv in snapForSig)
        {
            if (kv.Value != null && kv.Value.UsedTextureFiles != null)
                usedCountSig += kv.Value.UsedTextureFiles.Count;
        }
        var liveUsedCountSig = _filterPenumbraUsedOnly ? _penumbraUsedFiles.Count : 0;
        var visibleSig = string.Concat(_scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0", "|", _orphaned.Count.ToString(), "|", _scannedByMod.Count.ToString(), "|", usedCountSig.ToString(), "|", liveUsedCountSig.ToString());
        if (!string.Equals(visibleSig, _visibleByModSig, StringComparison.Ordinal))
        {
            _visibleByMod.Clear();
            var snap = snapForSig;
            var sourceKeys = _scannedByMod.Count > 0
                ? _scannedByMod.Keys.Union(snap.Keys, StringComparer.OrdinalIgnoreCase).ToList()
                : snap.Keys.ToList();
            foreach (var mod in sourceKeys)
            {
                List<string> files = null;
                if (_scannedByMod.TryGetValue(mod, out var list) && list != null)
                    files = list;
                else
                    files = _modStateService.ReadDetailTextures(mod);
                if (IsModExcludedByTags(mod))
                    continue;
                var displayName = ResolveModDisplayName(mod);
                var filtered = string.IsNullOrEmpty(_scanFilter)
                    ? files
                    : files.Where(f => Path.GetFileName(f).IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                    || mod.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                    || displayName.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0).ToList();
                if (_filterPenumbraUsedOnly)
                {
                    if (_penumbraUsedFiles.Count > 0)
                    {
                        var usedGlobal = new HashSet<string>(_penumbraUsedFiles.Select(p => (p ?? string.Empty).Replace('/', '\\')), StringComparer.OrdinalIgnoreCase);
                        filtered = filtered.Where(f => usedGlobal.Contains((f ?? string.Empty).Replace('/', '\\'))).ToList();
                    }
                    else
                    {
                        List<string> usedList = null;
                        if (snap.TryGetValue(mod, out var eUsed) && eUsed != null && eUsed.UsedTextureFiles != null)
                            usedList = eUsed.UsedTextureFiles;
                        else
                            usedList = _modStateService.ReadDetailUsed(mod);
                        var usedByMod = new HashSet<string>((usedList ?? new List<string>()).Select(p => (p ?? string.Empty).Replace('/', '\\')), StringComparer.OrdinalIgnoreCase);
                        if (usedByMod.Count > 0)
                            filtered = filtered.Where(f => usedByMod.Contains((f ?? string.Empty).Replace('/', '\\'))).ToList();
                        else
                            filtered = new List<string>();
                    }
                }

                var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                if (_filterNonConvertibleMods && files.Count == 0 && !isOrphan)
                    continue;

                if (_filterInefficientMods && IsModInefficient(mod))
                    continue;

                var modMatchesFilter = string.IsNullOrEmpty(_scanFilter)
                                       || displayName.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                       || mod.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0;
                var include = _filterPenumbraUsedOnly ? filtered.Count > 0 : (filtered.Count > 0 || modMatchesFilter);
                if (include)
                    _visibleByMod[mod] = filtered;
            }
            foreach (var o in _orphaned)
            {
                var name = o.ModFolderName;
                if (!string.IsNullOrWhiteSpace(name) && !_visibleByMod.ContainsKey(name))
                    _visibleByMod[name] = new List<string>();
            }
            _visibleByModSig = visibleSig;
            _modStateSnapshot = snap;
            _modPaths = snap.ToDictionary(
                kv => kv.Key,
                kv => {
                    var folder = kv.Value?.PenumbraRelativePath ?? string.Empty;
                    var leaf = kv.Value?.RelativeModName ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(folder)) return leaf ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(leaf)) return folder ?? string.Empty;
                    return string.Concat(folder, "/", leaf);
                },
                StringComparer.OrdinalIgnoreCase);
            _modDisplayNames = snap.ToDictionary(kv => kv.Key, kv => kv.Value?.DisplayName ?? string.Empty, StringComparer.OrdinalIgnoreCase);
            _selectedCountByMod.Clear();
            foreach (var (mod, files) in _visibleByMod)
            {
                int c = 0;
                if (files != null && files.Count > 0)
                {
                    for (int i = 0; i < files.Count; i++)
                        if (_selectedTextures.Contains(files[i])) c++;
                }
                _selectedCountByMod[mod] = c;
            }
            var warmSig = string.Concat(_visibleByModSig, "|", _visibleByMod.Count.ToString());
            if (!string.Equals(warmSig, _fileSizeWarmupSig, StringComparison.Ordinal))
            {
                try
                {
                    var allFiles = _visibleByMod.Values.SelectMany(v => v).Take(2000).ToList();
                    _cacheService.WarmupFileSizeCache(allFiles);
                    _fileSizeWarmupSig = warmSig;
                }
                catch { }
            }
        }
        var visibleByMod = _visibleByMod;

        var mods = visibleByMod.Keys.Where(m => !string.Equals(m, "mod_state", StringComparison.OrdinalIgnoreCase)).ToList();
        if (_scanSortKind == ScanSortKind.ModName)
            mods = (_scanSortAsc ? mods.OrderBy(m => ResolveModDisplayName(m)) : mods.OrderByDescending(m => ResolveModDisplayName(m))).ToList();
        else
        {
            foreach (var k in mods.ToList())
            {
                var sorted = _scanSortAsc
                    ? visibleByMod[k].OrderBy(f => Path.GetFileName(f)).ToList()
                    : visibleByMod[k].OrderByDescending(f => Path.GetFileName(f)).ToList();
                visibleByMod[k] = sorted;
            }
        }

        TableCatNode? root = null;

        var selectAllClicked = ImGui.Button("Select All");
        if (selectAllClicked)
        {
            foreach (var kv in visibleByMod)
            {
                var mod = kv.Key;
                var hasBackup = GetOrQueryModBackup(mod);
                var excluded = !hasBackup && IsModExcludedByTags(mod);
                foreach (var f in kv.Value)
                {
                    if (!excluded || hasBackup)
                        _selectedTextures.Add(f);
                }
                _selectedCountByMod[mod] = excluded && !hasBackup ? 0 : kv.Value.Count;
            }
        }
        ImGui.SameLine();
        var clearAllClicked = ImGui.Button("Clear All");
        if (clearAllClicked)
        {
            foreach (var kv in visibleByMod)
                foreach (var f in kv.Value)
                    _selectedTextures.Remove(f);
            foreach (var mod in visibleByMod.Keys)
                _selectedCountByMod[mod] = 0;
        }

        ImGui.SameLine();
        var expandAllClicked = ImGui.Button("Expand All");
        if (expandAllClicked)
        {
            foreach (var m in visibleByMod.Keys)
                _expandedMods.Add(m);
            TableCatNode? rootExpand = root;
            if (rootExpand == null && _modPaths.Count > 0)
                rootExpand = BuildTableCategoryTree(mods);
            if (rootExpand != null)
            {
                var allFolders = new List<string>();
                CollectFolderPaths(rootExpand, string.Empty, allFolders);
                foreach (var fp in allFolders)
                    _expandedFolders.Add(fp);
            }
        }
        ImGui.SameLine();
        var collapseAllClicked = ImGui.Button("Collapse All");
        if (collapseAllClicked)
        {
            _expandedMods.Clear();
            _expandedFolders.Clear();
        }

        if (_filterPenumbraUsedOnly)
        {
            ImGui.SameLine();
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.InfoCircle.ToIconString());
            var iconHovered = ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled);
            ImGui.PopFont();
            if (iconHovered)
                ImGui.SetTooltip("Only mods currently used by Penumbra are shown. Disable Used-Only to see all mods.");

            ImGui.SameLine();
            ImGui.TextUnformatted("Penumbra Used-Only active");
        }

        float availY = ImGui.GetContentRegionAvail().Y;
        float frameH = ImGui.GetFrameHeight();
        float reserveH = (frameH * 2) + ImGui.GetStyle().ItemSpacing.Y * 6;
        float childH = MathF.Max(150f, availY - reserveH);
        ImGui.BeginChild("ScannedFilesTableRegion", new Vector2(0, childH), false, ImGuiWindowFlags.None);

        var flags = ImGuiTableFlags.BordersOuter | ImGuiTableFlags.BordersV | ImGuiTableFlags.Resizable | ImGuiTableFlags.ScrollY | ImGuiTableFlags.RowBg;
        if (ImGui.BeginTable("ScannedFilesTable", 5, flags))
        {
            ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, _scannedFirstColWidth);
            ImGui.TableSetupColumn("Mod", ImGuiTableColumnFlags.WidthStretch);
            ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed, _scannedCompressedColWidth);
            ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, _scannedActionColWidth);
            ImGui.TableSetupScrollFreeze(0, 1);
            ImGui.TableHeadersRow();
            _zebraRowIndex = 0;

            var expandedFoldersSig = string.Join(",", _expandedFolders.OrderBy(s => s, StringComparer.Ordinal));
            var expandedModsSig = string.Join(",", _expandedMods.OrderBy(s => s, StringComparer.OrdinalIgnoreCase));
            var sig = string.Concat(expandedFoldersSig, "|", expandedModsSig, "|", visibleByMod.Count.ToString(), "|", (_configService.Current.ShowModFilesInOverview ? "1" : "0"), "|", _scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0", "|", _modPathsSig);
            if (!string.Equals(sig, _flatRowsSig, StringComparison.Ordinal))
            {
                root = BuildTableCategoryTree(mods);
                _flatRows.Clear();
                BuildFlatRows(root, visibleByMod, string.Empty, 0);
                _cachedTotalRows = _flatRows.Count;
                _flatRowsSig = sig;
                _folderSizeCache.Clear();
                _folderSizeCacheSig = _flatRowsSig;
                _folderCountsCache.Clear();
                _folderCountsCacheSig = _flatRowsSig;
                BuildFolderCountsCache(root, visibleByMod, string.Empty);
            }
            var clipper = ImGui.ImGuiListClipper();
                clipper.Begin(_cachedTotalRows);
                while (clipper.Step())
                {
                    for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; i++)
                    {
                        var row = _flatRows[i];
                        ImGui.TableNextRow();
                        ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
                        if (row.Kind == FlatRowKind.Folder)
                        {
                            DrawFolderFlatRow(row, visibleByMod);
                        }
                        else if (row.Kind == FlatRowKind.Mod)
                        {
                            DrawModFlatRow(row, visibleByMod);
                        }
                        else
                        {
                            DrawFileFlatRow(row, visibleByMod);
                        }
                    }
                }
                clipper.End();
            
            ImGui.TableSetColumnIndex(0);
            var currentFirstWidth = ImGui.GetColumnWidth();
            if (MathF.Abs(currentFirstWidth - _scannedFirstColWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _scannedFirstColWidth = currentFirstWidth;
                _configService.Current.ScannedFilesFirstColWidth = currentFirstWidth;
                _configService.Save();
                _logger.LogDebug($"Saved first column width: {currentFirstWidth}px");
            }

            ImGui.TableSetColumnIndex(1);
            var currentFileWidth = ImGui.GetColumnWidth();
            _scannedFileColWidth = currentFileWidth;

            ImGui.TableSetColumnIndex(2);
            var prevCompressedWidth = _scannedCompressedColWidth;
            var currentCompressedWidth = ImGui.GetColumnWidth();
            _scannedCompressedColWidth = currentCompressedWidth;

            ImGui.TableSetColumnIndex(3);
            var prevUncompressedWidth = _scannedSizeColWidth;
            var currentUncompressedWidth = ImGui.GetColumnWidth();
            _scannedSizeColWidth = currentUncompressedWidth;
            if (MathF.Abs(currentUncompressedWidth - prevUncompressedWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _configService.Current.ScannedFilesSizeColWidth = currentUncompressedWidth;
                _configService.Save();
                _logger.LogDebug($"Saved size column width: {currentUncompressedWidth}px");
            }

            ImGui.TableSetColumnIndex(4);
            var prevActionWidth = _scannedActionColWidth;
            var currentActionWidth = ImGui.GetColumnWidth();
            _scannedActionColWidth = currentActionWidth;
            if (MathF.Abs(currentActionWidth - prevActionWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _configService.Current.ScannedFilesActionColWidth = currentActionWidth;
                _configService.Save();
                _logger.LogDebug($"Saved action column width: {currentActionWidth}px");
            }

            ImGui.EndTable();
        }
        ImGui.EndChild();

        ImGui.Separator();

        bool showFiles = _configService.Current.ShowModFilesInOverview;
        int visibleModsWithTextures = 0;
        foreach (var m in mods)
        {
            if (visibleByMod.TryGetValue(m, out var files) && files != null && files.Count > 0)
                visibleModsWithTextures++;
        }
        var sigFooter = string.Concat(showFiles ? "1" : "0", "|", visibleModsWithTextures.ToString(), "|", _perModSavingsRevision.ToString(), "|", _scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0");

        if (_footerTotalsDirty || !string.Equals(sigFooter, _footerTotalsSignature, StringComparison.Ordinal))
        {
            long totalUncompressedCalc = 0;
            long totalCompressedCalc = 0;
            long savedBytesCalc = 0;
            foreach (var m in mods)
            {
                if (!visibleByMod.TryGetValue(m, out var files) || files == null || files.Count == 0)
                    continue;

                long modOrig = 0;
                long modCur = 0;

                if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null)
                {
                    if (stats.OriginalBytes > 0) modOrig = stats.OriginalBytes;
                    if (stats.CurrentBytes > 0) modCur = stats.CurrentBytes;
                }
                if (modOrig <= 0)
                {
                    var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                    if (snap.TryGetValue(m, out var st) && st != null && st.ComparedFiles > 0)
                        modOrig = st.OriginalBytes;
                    else
                        modOrig = GetOrQueryModOriginalTotal(m);
                }

                if (modOrig > 0) totalUncompressedCalc += modOrig;
                if (modCur > 0)
                {
                    totalCompressedCalc += modCur;
                    if (modOrig > 0) savedBytesCalc += Math.Max(0, modOrig - modCur);
                }
            }
            _footerTotalUncompressed = totalUncompressedCalc;
            _footerTotalCompressed = totalCompressedCalc;
            _footerTotalSaved = savedBytesCalc;
            _footerTotalsSignature = sigFooter;
            _footerTotalsDirty = false;
        }

        long totalUncompressed = _footerTotalUncompressed;
        long totalCompressed = _footerTotalCompressed;
        long savedBytes = _footerTotalSaved;

        var footerFlags = ImGuiTableFlags.BordersOuter | ImGuiTableFlags.BordersV | ImGuiTableFlags.RowBg | ImGuiTableFlags.SizingFixedFit;
        if (ImGui.BeginTable("ScannedFilesTotals", 5, footerFlags))
        {
            ImGui.TableSetupColumn("", ImGuiTableColumnFlags.WidthFixed, _scannedFirstColWidth);
            ImGui.TableSetupColumn("Mod", ImGuiTableColumnFlags.WidthFixed, _scannedFileColWidth);
            ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed, _scannedCompressedColWidth);
            ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, _scannedActionColWidth);

            ImGui.TableNextRow();
            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
            ImGui.TableSetColumnIndex(1);
            var reduction = totalUncompressed > 0
                ? MathF.Max(0f, (float)savedBytes / totalUncompressed * 100f)
                : 0f;
            ImGui.TextUnformatted($"Total saved ({reduction.ToString("0.00")}%)");
            ImGui.TableSetColumnIndex(2);
            if (totalCompressed > 0)
            {
                var color = (totalUncompressed > 0 && totalCompressed > totalUncompressed)
                    ? ShrinkUColors.WarningLight
                    : _compressedTextColor;
                DrawRightAlignedSizeColored(totalCompressed, color);
            }
            else
                DrawRightAlignedTextColored("-", _compressedTextColor);
            ImGui.TableSetColumnIndex(3);
            if (totalUncompressed > 0)
                DrawRightAlignedSize(totalUncompressed);
            else
                ImGui.TextUnformatted("");
            ImGui.TableSetColumnIndex(4);
            ImGui.TextUnformatted("");

            ImGui.EndTable();
        }

        ImGui.Spacing();
        var (convertableMods, restorableMods) = GetSelectedModStates();
        bool hasConvertableMods = convertableMods > 0;
        bool hasRestorableMods = restorableMods > 0;
        bool hasOnlyRestorableMods = hasRestorableMods && !hasConvertableMods;

        ImGui.BeginDisabled(ActionsDisabled() || hasOnlyRestorableMods || !hasConvertableMods);
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        if (ImGui.Button("Backup and Convert"))
        {
            _running = true;
            var toConvert = GetConvertableTextures();
            ResetBothProgress();
            var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
            var selectedEmptyMods = _selectedEmptyMods.Where(m => !GetOrQueryModBackup(m)).ToList();
            var backupTasks = new List<Task<bool>>();
            foreach (var m in selectedEmptyMods)
            {
                try { backupTasks.Add(_backupService.CreateFullModBackupAsync(m, progress, CancellationToken.None)); }
                catch (Exception ex) { _logger.LogError(ex, "Queue backup task failed for {mod}", m); }
            }
            if (toConvert.Count == 0 && backupTasks.Count == 0)
            {
                var modsToConvert = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var (mod, count) in _selectedCountByMod)
                {
                    if (count > 0 && !GetOrQueryModBackup(mod) && !IsModExcludedByTags(mod))
                        modsToConvert.Add(mod);
                }
                foreach (var m in _selectedEmptyMods)
                {
                    if (!GetOrQueryModBackup(m) && !IsModExcludedByTags(m))
                        modsToConvert.Add(m);
                }
                if (modsToConvert.Count == 0)
                {
                    SetStatus("Nothing selected to backup or convert.");
                    _running = false;
                    ResetBothProgress();
                }
                else
                {
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            foreach (var m in modsToConvert)
                            {
                                var list = await _conversionService.GetModTextureFilesAsync(m).ConfigureAwait(false);
                                if (list != null && list.Count > 0)
                                {
                                    foreach (var f in list)
                                        toConvert[f] = Array.Empty<string>();
                                    _uiThreadActions.Enqueue(() => { _scannedByMod[m] = list; _needsUIRefresh = true; });
                                }
                                await Task.Yield();
                            }
                            if (toConvert.Count == 0)
                            {
                                _uiThreadActions.Enqueue(() => { SetStatus("Nothing selected to convert."); _running = false; ResetBothProgress(); });
                            }
                            else
                            {
                                await _conversionService.StartConversionAsync(toConvert).ConfigureAwait(false);
                                _uiThreadActions.Enqueue(() => { _running = false; });
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "On-demand scan for selected mods failed");
                            _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                        }
                    });
                }
            }
            else
            {
                if (backupTasks.Count > 0)
                {
                    var modsQueue = selectedEmptyMods.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                    _ = Task.Run(async () =>
                    {
                        foreach (var m in modsQueue)
                        {
                            try { await _backupService.CreateFullModBackupAsync(m, progress, CancellationToken.None).ConfigureAwait(false); }
                            catch (Exception ex) { _logger.LogError(ex, "Backup task failed for {mod}", m); }
                            await Task.Yield();
                        }
                        _uiThreadActions.Enqueue(() => { TriggerMetricsRefresh(); if (toConvert.Count == 0) { _running = false; ResetBothProgress(); } });
                    });
                }
                if (toConvert.Count > 0)
                {
                    _ = _conversionService.StartConversionAsync(toConvert)
                        .ContinueWith(_ => { _uiThreadActions.Enqueue(() => { _running = false; }); }, TaskScheduler.Default);
                }
            }
        }
        ImGui.EndDisabled();
        ImGui.PopStyleColor(4);

        ImGui.SameLine();

        var restorableModsForAction = GetRestorableModsForCurrentSelection();
        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var restorableFiltered = automaticMode
            ? restorableModsForAction.Where(m =>
                {
                    var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                    if (snap.TryGetValue(m, out var ms) && ms != null && ms.UsedTextureFiles != null && ms.UsedTextureFiles.Count > 0)
                        return false;
                    return true;
                })
                .ToList()
            : restorableModsForAction;
        bool canRestore = restorableFiltered.Count > 0;
        bool someSkippedByAuto = automaticMode && restorableFiltered.Count < restorableModsForAction.Count;

        ImGui.BeginDisabled(ActionsDisabled() || !canRestore);
        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        if (ImGui.Button("Restore Backups"))
        {
            _running = true;
            ResetBothProgress();
            var progress = new Progress<(string, int, int)>(e =>
            {
                _currentTexture = e.Item1;
                _backupIndex = e.Item2;
                _backupTotal = e.Item3;
                _currentRestoreModIndex = e.Item2;
                _currentRestoreModTotal = e.Item3;
            });
            _restoreCancellationTokenSource?.Dispose();
            _restoreCancellationTokenSource = new CancellationTokenSource();
            var restoreToken = _restoreCancellationTokenSource.Token;

            _ = Task.Run(async () =>
            {
                foreach (var mod in restorableFiltered)
                {
                    try
                    {
                        _currentRestoreMod = mod;
                        _currentRestoreModIndex = 0;
                        _currentRestoreModTotal = 0;
                        var preferPmp = _configService.Current.PreferPmpRestoreWhenAvailable;
                        var hasPmp = false;
                        try { hasPmp = _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); }
                        catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", mod); }
                        if (preferPmp && hasPmp)
                        {
                            var latestPmp = _backupService.GetPmpBackupsForModAsync(mod).GetAwaiter().GetResult().FirstOrDefault();
                            if (!string.IsNullOrEmpty(latestPmp))
                            {
                                await _backupService.RestorePmpAsync(mod, latestPmp, progress, restoreToken);
                            }
                            else
                            {
                                await _backupService.RestoreLatestForModAsync(mod, progress, restoreToken);
                            }
                        }
                        else
                        {
                            await _backupService.RestoreLatestForModAsync(mod, progress, restoreToken);
                        }
                    }
                    catch (Exception ex) { _logger.LogError(ex, "Bulk restore failed for {mod}", mod); }
                }
            }).ContinueWith(_ =>
            {
                try { _backupService.RedrawPlayer(); }
                catch (Exception ex) { _logger.LogError(ex, "RedrawPlayer after bulk restore failed"); }
                _logger.LogDebug("Restore completed (bulk action)");
                foreach (var m in restorableFiltered)
                {
                    try { RefreshModState(m, "restore-bulk"); }
                    catch (Exception ex) { _logger.LogError(ex, "RefreshModState after bulk restore failed for {mod}", m); }
                }
                TriggerMetricsRefresh();
                _ = Task.Run(async () =>
                {
                    foreach (var m in restorableFiltered)
                    {
                        try { await _backupService.ComputeSavingsForModAsync(m).ConfigureAwait(false); } catch { }
                        await Task.Yield();
                    }
                    _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                });
                foreach (var m in restorableFiltered)
                {
                    try { bool _r; _modsWithPmpCache.TryRemove(m, out _r); }
                    catch (Exception ex) { _logger.LogError(ex, "TryRemove _modsWithPmpCache failed for {mod}", m); }
                    try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(m, out _rm); }
                    catch (Exception ex) { _logger.LogError(ex, "TryRemove _modsPmpMetaCache failed for {mod}", m); }
                    _ = _backupService.HasBackupForModAsync(m).ContinueWith(bt =>
                    {
                        if (bt.Status == TaskStatus.RanToCompletion)
                        {
                            bool any = bt.Result;
                            try { any = any || _backupService.HasPmpBackupForModAsync(m).GetAwaiter().GetResult(); }
                            catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", m); }
                            _cacheService.SetModHasBackup(m, any);
                            try { var hasPmpNow = _backupService.HasPmpBackupForModAsync(m).GetAwaiter().GetResult(); _cacheService.SetModHasPmp(m, hasPmpNow); }
                            catch (Exception ex) { _logger.LogError(ex, "SetModHasPmp failed for {mod}", m); }
                        }
                    });
                }
                try
                {
                    int removed = 0;
                    foreach (var m in restorableFiltered)
                    {
                        if (_configService.Current.ExternalConvertedMods.Remove(m))
                            removed++;
                    }
                    if (removed > 0)
                        _configService.Save();
                }
                catch (Exception ex) { _logger.LogError(ex, "Update ExternalConvertedMods after bulk restore failed"); }
                _uiThreadActions.Enqueue(() =>
                {
                    _running = false;
                    _restoreCancellationTokenSource?.Dispose();
                    _restoreCancellationTokenSource = null;
                    _currentRestoreMod = string.Empty;
                    _currentRestoreModIndex = 0;
                    _currentRestoreModTotal = 0;
                });
            });
        }
        ImGui.PopStyleColor(4);
        ImGui.EndDisabled();

        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (!canRestore)
                ImGui.SetTooltip("No selected mods have backups to restore.");
            else if (someSkippedByAuto)
                ImGui.SetTooltip("Automatic mode: skipping mods with currently used textures.");
        }
    }
}
