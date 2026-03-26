using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Utility.Raii;
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
    private string _visibleByModFileSortSig = string.Empty;
    private Dictionary<string, List<string>> _visibleByMod = new(StringComparer.OrdinalIgnoreCase);
    private void DrawOverview_ViewImpl()
    {
        ImGui.SetWindowFontScale(1.15f);
        ImGui.TextColored(ShrinkUColors.Accent, "Scanned Files Overview");
        ImGui.Dummy(new Vector2(0, 6f));
        ImGui.SetWindowFontScale(1.0f);
        ImGui.SetNextItemWidth(186f);
        var filterChanged = ImGui.InputTextWithHint("##scanFilter", "Filter by file or mod", ref _scanFilter, 128);
        _scanFilter = (_scanFilter ?? string.Empty).Trim();
        if (filterChanged)
            RequestUiRefresh("scan-filter-changed");

        var h = ImGui.GetFrameHeight();
        var w = h + ImGui.GetStyle().ItemInnerSpacing.X * 2f;

        if (!string.IsNullOrEmpty(_scanFilter))
        {
            ImGui.SameLine();
            ImGui.PushFont(UiBuilder.IconFont);
            // Red button for clear action
            ImGui.PushStyleColor(ImGuiCol.Button, new Vector4(0.6f, 0.2f, 0.2f, 1f));
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, new Vector4(0.8f, 0.3f, 0.3f, 1f));
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, new Vector4(0.4f, 0.1f, 0.1f, 1f));
            ImGui.PushStyleVar(ImGuiStyleVar.ButtonTextAlign, new Vector2(0.9f, 0.5f));
            if (ImGui.Button(FontAwesomeIcon.Times.ToIconString(), new Vector2(w, h)))
            {
                _scanFilter = string.Empty;
                RequestUiRefresh("scan-filter-cleared");
            }
            ImGui.PopStyleColor(3);
            ImGui.PopFont();
            ImGui.PopStyleVar();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("Clear filter");
        }

        ImGui.SameLine();
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
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip("Show only mods used by the current Penumbra collection.");
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
        var hideNonConvertibleClicked = ImGui.Button(FontAwesomeIcon.Image.ToIconString(), new Vector2(w, h));
        ImGui.PopStyleVar();
        if (_filterNonConvertibleMods)
        {
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
            ImGui.PopStyleColor();
        }
        ImGui.PopFont();
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip("Hide mods without textures.");
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
        if (ImGui.IsItemHovered())
            ImGui.SetTooltip("Hide inefficient mods (larger after conversion).");
        if (hideInefficientClicked)
        {
            _filterInefficientMods = !_filterInefficientMods;
            _configService.Current.HideInefficientMods = _filterInefficientMods;
            _configService.Save();
        }

        EnsureCollectionEnabledStatesFresh();
        if (_scannedByMod.Count == 0)
        {
            // Proceed with empty file lists; rows will use mod_state counts
        }

        DrawScannedFilesTable();
    }

    private void DrawPenumbraDebugSection()
    {
        if (!ImGui.CollapsingHeader("Penumbra Mod Debug", ImGuiTreeNodeFlags.None))
            return;

        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var k in _scannedByMod.Keys)
            if (!string.IsNullOrWhiteSpace(k) && !string.Equals(k, "mod_state", StringComparison.OrdinalIgnoreCase))
                keys.Add(k);
        foreach (var k in _modPaths.Keys)
            if (!string.IsNullOrWhiteSpace(k) && !string.Equals(k, "mod_state", StringComparison.OrdinalIgnoreCase))
                keys.Add(k);
        var snap = _modStateSnapshot ?? _modStateService.Snapshot();
        foreach (var k in snap.Keys)
            if (!string.IsNullOrWhiteSpace(k) && !string.Equals(k, "mod_state", StringComparison.OrdinalIgnoreCase))
                keys.Add(k);
        var mods = keys.OrderBy(x => ResolveModDisplayName(x), StringComparer.OrdinalIgnoreCase).ToList();
        if (mods.Count > 0 && string.IsNullOrWhiteSpace(_debugPenumbraSelectedMod))
            SelectDebugPenumbraMod(mods[0]);

        ImGui.SetNextItemWidth(360f);
        var selectedIdx = Math.Max(0, mods.FindIndex(m => string.Equals(m, _debugPenumbraSelectedMod, StringComparison.OrdinalIgnoreCase)));
        var preview = mods.Count > 0 ? $"{ResolveModDisplayName(mods[selectedIdx])} [{mods[selectedIdx]}]" : "<no mods>";
        if (ImGui.BeginCombo("Mod", preview))
        {
            for (int i = 0; i < mods.Count; i++)
            {
                var mod = mods[i];
                var label = $"{ResolveModDisplayName(mod)} [{mod}]";
                var selected = string.Equals(mod, _debugPenumbraSelectedMod, StringComparison.OrdinalIgnoreCase);
                if (ImGui.Selectable(label, selected))
                    SelectDebugPenumbraMod(mod);
                if (selected)
                    ImGui.SetItemDefaultFocus();
            }
            ImGui.EndCombo();
        }

        ImGui.SameLine();
        if (ImGui.Button("Refresh Debug Data"))
        {
            if (!string.IsNullOrWhiteSpace(_debugPenumbraSelectedMod))
                SelectDebugPenumbraMod(_debugPenumbraSelectedMod, true);
        }

        if (_debugPenumbraLoading)
        {
            ImGui.TextUnformatted("Loading data from Penumbra API...");
            return;
        }
        if (!string.IsNullOrWhiteSpace(_debugPenumbraError))
        {
            ImGui.TextColored(ShrinkUColors.WarningLight, _debugPenumbraError);
            return;
        }
        var d = _debugPenumbraSnapshot;
        if (d == null)
            return;

        ImGui.Separator();
        ImGui.TextUnformatted("[API] API Available: " + (d.ApiAvailable ? "Yes" : "No"));
        ImGui.TextUnformatted("[INPUT] Selected ModDir: " + d.ModDirectory);
        ImGui.TextUnformatted("[API] Display Name: " + (string.IsNullOrWhiteSpace(d.DisplayName) ? "-" : d.DisplayName));
        ImGui.TextUnformatted("[API] Penumbra Root: " + (string.IsNullOrWhiteSpace(d.PenumbraModRoot) ? "-" : d.PenumbraModRoot));
        ImGui.TextUnformatted("[API] Penumbra Path: " + (string.IsNullOrWhiteSpace(d.PenumbraPath) ? "-" : d.PenumbraPath));
        ImGui.TextUnformatted("[CACHE] ShrinkU UI Path: " + (string.IsNullOrWhiteSpace(d.UiMappedPath) ? "-" : d.UiMappedPath));
        ImGui.TextUnformatted("[STATE] ModState Relative Path: " + (string.IsNullOrWhiteSpace(d.StateRelativePath) ? "-" : d.StateRelativePath));
        ImGui.TextUnformatted("[STATE] ModState Absolute Path: " + (string.IsNullOrWhiteSpace(d.StateAbsolutePath) ? "-" : d.StateAbsolutePath));
        ImGui.TextUnformatted("[API] Collection: " + (d.CollectionId.HasValue ? d.CollectionName : "-"));
        if (d.CollectionId.HasValue)
            ImGui.TextUnformatted("[API] Collection ID: " + d.CollectionId.Value);
        ImGui.TextUnformatted("[API] Enabled: " + (d.Enabled.HasValue ? (d.Enabled.Value ? "Yes" : "No") : "-"));
        ImGui.TextUnformatted("[API] Priority: " + (d.Priority.HasValue ? d.Priority.Value.ToString() : "-"));
        ImGui.TextUnformatted("[API] Inherited: " + (d.Inherited.HasValue ? (d.Inherited.Value ? "Yes" : "No") : "-"));
        ImGui.TextUnformatted("[API] Temporary: " + (d.Temporary.HasValue ? (d.Temporary.Value ? "Yes" : "No") : "-"));
        var metadataPrefix = string.Equals(d.MetadataSource, "penumbra-ipc-modlist-adapter", StringComparison.OrdinalIgnoreCase)
            ? "[API]"
            : (string.Equals(d.MetadataSource, "meta.json-fallback", StringComparison.OrdinalIgnoreCase) ? "[FALLBACK/meta.json]" : "[UNKNOWN]");
        ImGui.TextUnformatted(metadataPrefix + " Version: " + (string.IsNullOrWhiteSpace(d.Version) ? "-" : d.Version));
        ImGui.TextUnformatted(metadataPrefix + " Author: " + (string.IsNullOrWhiteSpace(d.Author) ? "-" : d.Author));
        ImGui.TextUnformatted(metadataPrefix + " Website: " + (string.IsNullOrWhiteSpace(d.Website) ? "-" : d.Website));
        ImGui.TextUnformatted("[STATE] Used by Penumbra: " + (d.UsedByPenumbra ? "Yes" : "No") + $" ({d.UsedTextureCount})");
        ImGui.TextWrapped(metadataPrefix + " Description: " + (string.IsNullOrWhiteSpace(d.Description) ? "-" : d.Description));
        ImGui.TextWrapped("[API] Tags: " + (d.Tags.Count > 0 ? string.Join(", ", d.Tags) : "-"));
        ImGui.TextUnformatted("[STATE] Texture count: " + d.StateTextureCount);
        ImGui.TextUnformatted("[STATE] Compared files: " + d.StateComparedFiles);
        ImGui.TextUnformatted("[STATE] Needs rescan: " + (d.StateNeedsRescan ? "Yes" : "No"));
        ImGui.TextUnformatted("[STATE] Last known write UTC: " + (d.StateLastKnownWriteUtc == DateTime.MinValue ? "-" : d.StateLastKnownWriteUtc.ToString("O")));
        ImGui.TextUnformatted("[STATE] Last scan UTC: " + (d.StateLastScanUtc == DateTime.MinValue ? "-" : d.StateLastScanUtc.ToString("O")));
        ImGui.TextUnformatted("[SCAN] Scanned texture files in current UI session: " + d.ScannedTextureCount);
        var pathAligned = string.Equals(NormalizeModPathValue(d.PenumbraPath), NormalizeModPathValue(d.UiMappedPath), StringComparison.OrdinalIgnoreCase);
        ImGui.TextUnformatted("[DERIVED] Path alignment (Penumbra vs ShrinkU cache): " + (pathAligned ? "OK" : "Mismatch"));
        ImGui.TextUnformatted("[DERIVED] API has path for mod: " + (d.PenumbraPathFromApi ? "Yes" : "No"));
        ImGui.TextUnformatted("[DERIVED] Metadata source: " + (string.IsNullOrWhiteSpace(d.MetadataSource) ? "-" : d.MetadataSource));
        ImGui.TextUnformatted("[META] Loaded UTC: " + d.LoadedAtUtc.ToString("O"));
        ImGui.Separator();
        ImGui.TextUnformatted("Source legend:");
        ImGui.TextUnformatted("[API] from Penumbra IPC/API calls");
        ImGui.TextUnformatted("[FALLBACK/meta.json] only used when API metadata is unavailable");
        ImGui.TextUnformatted("[STATE] from ShrinkU mod_state cache");
        ImGui.TextUnformatted("[CACHE] from ShrinkU in-memory UI cache");
        ImGui.TextUnformatted("[SCAN] from current session texture scan");
        ImGui.TextUnformatted("[DERIVED] computed from other fields");
        ImGui.TextUnformatted("[META] debug timing/info");
    }

    private void DrawScannedFilesTable_ViewImpl()
    {
        RefreshLivePenumbraMods(false, "overview-poll");
        if (!_orphanScanInFlight)
        {
            if (_orphaned.Count == 0 || _lastOrphanScanUtc == DateTime.MinValue || (DateTime.UtcNow - _lastOrphanScanUtc) > TimeSpan.FromSeconds(45))
                ScheduleOrphanScan("view-poll", false);
        }
        var snapForSig = _modStateSnapshot ?? _modStateService.Snapshot();
        var modCountSig = snapForSig.Count;
        var usedCountSig = 0;
        var totalTexturesSig = 0;
        foreach (var kv in snapForSig)
        {
            if (kv.Value != null)
            {
                usedCountSig += Math.Max(0, kv.Value.UsedTextureCount);
                totalTexturesSig += Math.Max(0, kv.Value.TotalTextures);
            }
        }
        var liveUsedCountSig = _filterPenumbraUsedOnly ? _penumbraUsedFiles.Count : 0;
        var visibleSig = string.Concat(
            _scanFilter, "|",
            _filterPenumbraUsedOnly ? "1" : "0", "|",
            _filterNonConvertibleMods ? "1" : "0", "|",
            _filterInefficientMods ? "1" : "0", "|",
            _orphaned.Count.ToString(), "|",
            _scannedByMod.Count.ToString(), "|",
            modCountSig.ToString(), "|",
            totalTexturesSig.ToString(), "|",
            usedCountSig.ToString(), "|",
            liveUsedCountSig.ToString());
        if (!string.Equals(visibleSig, _visibleByModSig, StringComparison.Ordinal))
        {
            _visibleByMod.Clear();
            var snap = snapForSig;
            var staleUncategorizedWithoutBackup = new List<string>();
            HashSet<string>? usedGlobal = null;
            if (_filterPenumbraUsedOnly && _penumbraUsedFiles.Count > 0)
                usedGlobal = new HashSet<string>(_penumbraUsedFiles.Select(p => (p ?? string.Empty).Replace('/', '\\')), StringComparer.OrdinalIgnoreCase);
            var orphanedMods = _orphaned.Count > 0
                ? new HashSet<string>(_orphaned.Select(x => x.ModFolderName ?? string.Empty), StringComparer.OrdinalIgnoreCase)
                : null;
            var sourceKeys = _scannedByMod.Count > 0
                ? _scannedByMod.Keys.Union(snap.Keys, StringComparer.OrdinalIgnoreCase).ToList()
                : snap.Keys.ToList();
            foreach (var mod in sourceKeys)
            {
                if (string.IsNullOrWhiteSpace(mod) || string.Equals(mod, "mod_state", StringComparison.OrdinalIgnoreCase))
                    continue;
                var files = (_scannedByMod.TryGetValue(mod, out var list) && list != null)
                    ? list
                    : ((snap.TryGetValue(mod, out var eFiles) && eFiles != null && eFiles.TextureFiles != null && eFiles.TextureFiles.Count > 0)
                        ? eFiles.TextureFiles
                        : (_modStateService.ReadDetailTextures(mod) ?? new List<string>()));
                if (IsModExcludedByTags(mod))
                    continue;
                var displayName = ResolveModDisplayName(mod);
                var filtered = string.IsNullOrEmpty(_scanFilter)
                    ? files
                    : files.Where(f => Path.GetFileName(f).IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0).ToList();
                if (_filterPenumbraUsedOnly)
                {
                    if (usedGlobal != null)
                    {
                        filtered = filtered.Where(f => usedGlobal.Contains((f ?? string.Empty).Replace('/', '\\'))).ToList();
                    }
                    else
                    {
                        filtered = new List<string>();
                    }
                }

                var isOrphan = orphanedMods != null && orphanedMods.Contains(mod);
                var hasPath = _modPaths.ContainsKey(mod);
                var hasBackupAny = GetOrQueryModBackup(mod) || GetOrQueryModTextureBackup(mod) || GetOrQueryModPmp(mod);
                var existsInPenumbra = _livePenumbraMods.Contains(mod);
                var isMissingUncategorizedNoBackup = _hasLivePenumbraModsSnapshot
                    && !isOrphan
                    && !hasPath
                    && !existsInPenumbra
                    && !hasBackupAny
                    && (files == null || files.Count == 0);
                if (isMissingUncategorizedNoBackup)
                {
                    staleUncategorizedWithoutBackup.Add(mod);
                    continue;
                }
                var totalTexturesForMod = GetTotalTexturesForMod(mod, files);
                if (_filterNonConvertibleMods && totalTexturesForMod == 0 && !isOrphan)
                    continue;

                if (_filterInefficientMods && IsModInefficient(mod))
                    continue;

                var modMatchesFilter = string.IsNullOrEmpty(_scanFilter)
                                       || displayName.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                                       || mod.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0;
                var include = string.IsNullOrEmpty(_scanFilter)
                    ? (_filterPenumbraUsedOnly ? filtered.Count > 0 : true)
                    : (_filterPenumbraUsedOnly ? filtered.Count > 0 : (modMatchesFilter || filtered.Count > 0));
                if (include)
                    _visibleByMod[mod] = filtered;
            }
            if (!_filterPenumbraUsedOnly)
            {
                foreach (var o in _orphaned)
                {
                    var name = o.ModFolderName;
                    if (string.IsNullOrWhiteSpace(name) || _visibleByMod.ContainsKey(name))
                        continue;
                    if (string.IsNullOrEmpty(_scanFilter))
                    {
                        _visibleByMod[name] = new List<string>();
                    }
                    else
                    {
                        var disp = ResolveModDisplayName(name);
                        if (name.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0
                            || disp.IndexOf(_scanFilter, StringComparison.OrdinalIgnoreCase) >= 0)
                            _visibleByMod[name] = new List<string>();
                    }
                }
            }
            if (staleUncategorizedWithoutBackup.Count > 0)
                QueuePruneNonexistentUncategorizedMods(staleUncategorizedWithoutBackup, "prune-uncategorized-missing");
            _visibleByModSig = visibleSig;
            _modStateSnapshot = snap;
            if (_modPaths.Count == 0 && !_loadingModPaths)
                RefreshModPathsFromPenumbra("overview-rebuild");
            _modDisplayNames = snap.ToDictionary(kv => kv.Key, kv => kv.Value?.DisplayName ?? string.Empty, StringComparer.OrdinalIgnoreCase);
            _selectedCountByMod.Clear();
            var sourceKeysCount = _scannedByMod.Count > 0
                ? _scannedByMod.Keys.Union(_visibleByMod.Keys, StringComparer.OrdinalIgnoreCase).ToList()
                : _visibleByMod.Keys.ToList();
            foreach (var mod in sourceKeysCount)
            {
                if (string.IsNullOrWhiteSpace(mod) || string.Equals(mod, "mod_state", StringComparison.OrdinalIgnoreCase))
                    continue;
                var allFiles = GetAllFilesForModDisplay(mod, _visibleByMod.TryGetValue(mod, out var vis) ? vis : null);
                int c = 0;
                if (allFiles != null && allFiles.Count > 0)
                {
                    for (int i = 0; i < allFiles.Count; i++)
                        if (_selectedTextures.Contains(allFiles[i])) c++;
                }
                _selectedCountByMod[mod] = c;
            }
            var warmSig = string.Concat(_visibleByModSig, "|", _visibleByMod.Count.ToString());
            if (!string.Equals(warmSig, _fileSizeWarmupSig, StringComparison.Ordinal))
            {
                try
                {
                    var modsSeed = _visibleByMod.Keys.Where(m => !string.IsNullOrWhiteSpace(m)).ToList();
                    var allFiles = _visibleByMod.Values.SelectMany(v => v).Take(2000).ToList();
                    _ = Task.Run(() =>
                    {
                        try
                        {
                            for (int i = 0; i < modsSeed.Count; i++)
                            {
                                var sizes = _modStateService.ReadDetailTextureSizes(modsSeed[i]);
                                if (sizes.Count > 0)
                                    _cacheService.SeedFileSizes(sizes);
                            }
                            _cacheService.WarmupFileSizeCache(allFiles);
                        }
                        catch { }
                    });
                    _fileSizeWarmupSig = warmSig;
                }
                catch { }
            }
        }
        var visibleByMod = _visibleByMod;

        var mods = visibleByMod.Keys.Where(m => !string.IsNullOrWhiteSpace(m) && !string.Equals(m, "mod_state", StringComparison.OrdinalIgnoreCase)).ToList();
        if (_scanSortKind == ScanSortKind.ModName)
            mods = (_scanSortAsc ? mods.OrderBy(m => ResolveModDisplayName(m)) : mods.OrderByDescending(m => ResolveModDisplayName(m))).ToList();
        else
        {
            var fileSortSig = string.Concat(_visibleByModSig, "|", _scanSortAsc ? "1" : "0", "|", mods.Count.ToString());
            if (!string.Equals(fileSortSig, _visibleByModFileSortSig, StringComparison.Ordinal))
            {
                foreach (var k in mods.ToList())
                {
                    var sorted = _scanSortAsc
                        ? visibleByMod[k].OrderBy(f => Path.GetFileName(f)).ToList()
                        : visibleByMod[k].OrderByDescending(f => Path.GetFileName(f)).ToList();
                    visibleByMod[k] = sorted;
                }
                _visibleByModFileSortSig = fileSortSig;
            }
        }

        TableCatNode? root = null;

        var selectAllClicked = ImGui.Button("Select All");
        ImGui.SameLine();
        ImGui.PushFont(UiBuilder.IconFont);
        var selectAllMenuClicked = ImGui.Button(FontAwesomeIcon.CaretDown.ToIconString());
        ImGui.PopFont();
        if (selectAllMenuClicked) ImGui.OpenPopup("select_all_popup");
        if (selectAllClicked)
        {
            ClearAllModSelections();
            foreach (var mod in mods)
            {
                var files = GetAllFilesForModDisplay(mod, visibleByMod.TryGetValue(mod, out var v) ? v : null);
                var count = files?.Count ?? 0;
                if (count > 0)
                {
                    foreach (var f in files!)
                        _selectedTextures.Add(f);
                    _selectedCountByMod[mod] = count;
                }
                else
                {
                    _selectedEmptyMods.Add(mod);
                    _selectedCountByMod[mod] = 0;
                }
            }
            if (_modSelectionOrder.Count > 0)
                _modSelectionAnchor = _modSelectionOrder[0];
        }
        if (ImGui.BeginPopup("select_all_popup"))
        {
            if (ImGui.MenuItem("Mods that can be converted"))
            {
                var snapPopup = _modStateService.Snapshot();
                var sourceKeys = mods;
                foreach (var mod in sourceKeys)
                {
                    var caps = EvaluateModCapabilities(mod);
                    if (caps.Excluded) continue;
                    if (caps.HasAnyBackup) continue;
                    if (!caps.CanConvert) continue;
                    var files = visibleByMod.TryGetValue(mod, out var v) && v != null ? v : new List<string>();
                    List<string>? allFilesForMod = null;
                    if (_scannedByMod.TryGetValue(mod, out var allFiles) && allFiles != null && allFiles.Count > 0)
                        allFilesForMod = allFiles;
                    else
                        allFilesForMod = files;
                    var totalAll = allFilesForMod.Count;
                    if (totalAll > 0)
                    {
                        for (int i = 0; i < totalAll; i++) _selectedTextures.Add(allFilesForMod[i]);
                        _selectedCountByMod[mod] = totalAll;
                        _selectedEmptyMods.Remove(mod);
                    }
                }
            }
            if (ImGui.MenuItem("Mods without textures (backup)"))
            {
                var snapPopup2 = _modStateService.Snapshot();
                var sourceKeys2 = _scannedByMod.Count > 0
                    ? _scannedByMod.Keys.Union(snapPopup2.Keys, StringComparer.OrdinalIgnoreCase).ToList()
                    : snapPopup2.Keys.ToList();
                foreach (var mod in sourceKeys2)
                {
                    if (IsModExcludedByTags(mod)) continue;
                    var noTextures = false;
                    if (snapPopup2.TryGetValue(mod, out var ms) && ms != null)
                        noTextures = ms.TotalTextures <= 0;
                    else if (_scannedByMod.TryGetValue(mod, out var all) && all != null)
                        noTextures = all.Count == 0;
                    if (!noTextures) continue;
                    var hasTexBackup = GetOrQueryModTextureBackup(mod);
                    var hasPmpBackup = GetOrQueryModPmp(mod);
                    var hasBackupAny = GetOrQueryModBackup(mod) || hasTexBackup || hasPmpBackup;
                    if (hasBackupAny) continue;
                    _selectedEmptyMods.Add(mod);
                    _selectedCountByMod[mod] = 0;
                }
            }
            ImGui.EndPopup();
        }
        ImGui.SameLine();
        var clearAllClicked = ImGui.Button("Clear All");
        if (clearAllClicked)
        {
            ClearAllModSelections();
            _modSelectionAnchor = string.Empty;
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
        if (ImGui.BeginTable("ScannedFilesTable", 4, flags))
        {
            ImGui.TableSetupColumn("Mod", ImGuiTableColumnFlags.WidthStretch | ImGuiTableColumnFlags.NoResize);
            ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed | ImGuiTableColumnFlags.NoResize, _scannedCompressedColWidth);
            ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed | ImGuiTableColumnFlags.NoResize, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed, FixedActionColumnWidth);
            ImGui.TableSetupScrollFreeze(0, 1);
            ImGui.TableHeadersRow();
            _zebraRowIndex = 0;

            var expandedFoldersSig = string.Join(",", _expandedFolders.OrderBy(s => s, StringComparer.Ordinal));
            var expandedModsSig = string.Join(",", _expandedMods.OrderBy(s => s, StringComparer.OrdinalIgnoreCase));
            var sig = string.Concat(expandedFoldersSig, "|", expandedModsSig, "|", visibleByMod.Count.ToString(), "|", (_configService.Current.ShowModFilesInOverview ? "1" : "0"), "|", _scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0", "|", _modPathsSig, "|", _orphanRevision.ToString());
            if (!string.Equals(sig, _flatRowsSig, StringComparison.Ordinal))
            {
                root = BuildTableCategoryTree(mods);
                _flatRows.Clear();
                BuildFlatRows(root, visibleByMod, string.Empty, 0);
                _cachedTotalRows = _flatRows.Count;
                _flatRowsSig = sig;
                _folderSizeCache.Clear();
                _folderSizeCacheSig = string.Concat(_flatRowsSig, "|", _perModSavingsRevision.ToString());
                _folderCountsCache.Clear();
                _folderCountsCacheSig = string.Concat(_flatRowsSig, "|", _perModSavingsRevision.ToString());
                BuildFolderCountsCache(root, visibleByMod, string.Empty);
            }
            _modSelectionOrder.Clear();
            _modSelectionOrder.AddRange(_flatRows.Where(r => r.Kind == FlatRowKind.Mod).Select(r => r.Mod));
            var clipper = ImGui.ImGuiListClipper();
                clipper.Begin(_cachedTotalRows);
                while (clipper.Step())
                {
                    for (int i = clipper.DisplayStart; i < clipper.DisplayEnd; i++)
                    {
                        var row = _flatRows[i];
                        ImGui.TableNextRow(ImGuiTableRowFlags.None, FixedFlatRowHeight);
                        var rowColor = (_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor;
                        var selectedHighlight = false;
                        if (row.Kind == FlatRowKind.Mod)
                            selectedHighlight = IsModSelected(row.Mod, visibleByMod);
                        else if (row.Kind == FlatRowKind.Folder)
                            selectedHighlight = IsFolderFullySelected(row.Node, visibleByMod);
                        if (selectedHighlight)
                            rowColor = new Vector4(0.23f, 0.40f, 0.72f, 0.38f);
                        ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32(rowColor));
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
            var currentFileWidth = ImGui.GetColumnWidth();
            _scannedFileColWidth = currentFileWidth;

            ImGui.TableSetColumnIndex(1);
            var prevCompressedWidth = _scannedCompressedColWidth;
            var currentCompressedWidth = ImGui.GetColumnWidth();
            _scannedCompressedColWidth = currentCompressedWidth;

            ImGui.TableSetColumnIndex(2);
            var prevUncompressedWidth = _scannedSizeColWidth;
            var currentUncompressedWidth = ImGui.GetColumnWidth();
            _scannedSizeColWidth = currentUncompressedWidth;
            if (MathF.Abs(currentUncompressedWidth - prevUncompressedWidth) > 0.5f && ImGui.IsMouseReleased(ImGuiMouseButton.Left))
            {
                _configService.Current.ScannedFilesSizeColWidth = currentUncompressedWidth;
                _configService.Save();
                _logger.LogDebug($"Saved size column width: {currentUncompressedWidth}px");
            }

            ImGui.TableSetColumnIndex(3);

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
        var sigFooter = string.Concat(showFiles ? "1" : "0", "|", visibleModsWithTextures.ToString(), "|", visibleByMod.Count.ToString(), "|", _perModSavingsRevision.ToString(), "|", _scanFilter, "|", _filterPenumbraUsedOnly ? "1" : "0", "|", _filterNonConvertibleMods ? "1" : "0", "|", _filterInefficientMods ? "1" : "0");

        if (_footerTotalsDirty || !string.Equals(sigFooter, _footerTotalsSignature, StringComparison.Ordinal))
        {
            long totalUncompressedCalc = 0;
            long totalCompressedCalc = 0;
            long savedBytesCalc = 0;
            var snap = _modStateService.Snapshot();
            foreach (var m in mods)
            {
                if (!visibleByMod.TryGetValue(m, out var files) || files == null)
                    continue;

                long modOrig = 0;
                long modCur = 0;
                var totalAll = GetTotalTexturesForMod(m, files);
                if (totalAll <= 0)
                    continue;
                if (snap.TryGetValue(m, out var st) && st != null && st.OriginalBytes > 0)
                    modOrig = st.OriginalBytes;
                else
                    modOrig = GetOrQueryModOriginalTotal(m);

                // Prefer snapshot for current size to match mod rows; fall back to cached savings
                if (snap.TryGetValue(m, out var st2) && st2 != null && st2.CurrentBytes > 0 && st2.ComparedFiles > 0 && !st2.InstalledButNotConverted)
                    modCur = st2.CurrentBytes;
                else if (_cachedPerModSavings.TryGetValue(m, out var stats) && stats != null && stats.CurrentBytes > 0 && stats.ComparedFiles > 0)
                {
                    var stx = snap.TryGetValue(m, out var s3) ? s3 : null;
                    if (!(stx != null && stx.InstalledButNotConverted))
                        modCur = stats.CurrentBytes;
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
        if (ImGui.BeginTable("ScannedFilesTotals", 4, footerFlags))
        {
            ImGui.TableSetupColumn("Mod", ImGuiTableColumnFlags.WidthFixed, _scannedFileColWidth);
            ImGui.TableSetupColumn("Compressed", ImGuiTableColumnFlags.WidthFixed, _scannedCompressedColWidth);
            ImGui.TableSetupColumn("Uncompressed", ImGuiTableColumnFlags.WidthFixed, _scannedSizeColWidth);
            ImGui.TableSetupColumn("Action", ImGuiTableColumnFlags.WidthFixed | ImGuiTableColumnFlags.NoResize, FixedActionColumnWidth);

            ImGui.TableNextRow();
            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32((_zebraRowIndex++ % 2 == 0) ? _zebraEvenColor : _zebraOddColor));
            ImGui.TableSetColumnIndex(0);
            var reduction = totalUncompressed > 0
                ? MathF.Max(0f, (1f - (float)totalCompressed / totalUncompressed) * 100f)
                : 0f;
            ImGui.TextUnformatted($"Total saved ({reduction.ToString("0.00")}%)");
            ImGui.TableSetColumnIndex(1);
            if (totalCompressed > 0)
            {
                var color = (totalUncompressed > 0 && totalCompressed > totalUncompressed)
                    ? ShrinkUColors.WarningLight
                    : _compressedTextColor;
                DrawRightAlignedSizeColored(totalCompressed, color);
            }
            else
                DrawRightAlignedTextColored("-", _compressedTextColor);
            ImGui.TableSetColumnIndex(2);
            if (totalUncompressed > 0)
                DrawRightAlignedSize(totalUncompressed);
            else
                ImGui.TextUnformatted("");
            ImGui.TableSetColumnIndex(3);
            ImGui.TextUnformatted("");

            ImGui.EndTable();
        }

        ImGui.Spacing();
        var (convertableMods, restorableMods) = GetSelectedModStates();
        bool hasConvertableMods = convertableMods > 0;
        bool hasRestorableMods = restorableMods > 0;
        bool hasOnlyRestorableMods = hasRestorableMods && !hasConvertableMods;

        
        var selectedEmptyModsBtn = _selectedEmptyMods.Where(m => !IsModExcludedByTags(m)).ToList();
        var modsWithSelectionsBtn = _selectedCountByMod.Where(kv => kv.Value > 0 && !IsModExcludedByTags(kv.Key)).Select(kv => kv.Key).ToList();
        var selectedModsAll = new HashSet<string>(modsWithSelectionsBtn, StringComparer.OrdinalIgnoreCase);
        foreach (var m in selectedEmptyModsBtn) selectedModsAll.Add(m);
        var convertibleSelectedMods = new List<string>();
        var nonConvertibleSelectedMods = new List<string>();
        var reinstallSelectedMods = new List<string>();
        var convertibleSelectedModsNoBackup = new List<string>();
        var snapGating = _modStateService.Snapshot();
        foreach (var m in selectedModsAll)
        {
            var hasAny = false;
            if (snapGating.TryGetValue(m, out var ms) && ms != null && ms.TotalTextures > 0)
                hasAny = true;
            else if (_scannedByMod.TryGetValue(m, out var all) && all != null && all.Count > 0)
                hasAny = true;
            if (hasAny)
                convertibleSelectedMods.Add(m);
            else
            {
                var isOrphanSel = _orphaned.Any(x => string.Equals(x.ModFolderName, m, StringComparison.OrdinalIgnoreCase));
                var hasPmpSel = GetOrQueryModPmp(m);
                var visFilesSel = visibleByMod.TryGetValue(m, out var vsel) && vsel != null ? vsel : new List<string>();
                var totalAllSel = GetTotalTexturesForMod(m, visFilesSel);
                var isReinstallSel = hasPmpSel && !isOrphanSel && totalAllSel == 0;
                if (isReinstallSel)
                    reinstallSelectedMods.Add(m);
                else
                    nonConvertibleSelectedMods.Add(m);
            }
        }
        foreach (var m in convertibleSelectedMods)
        {
            var hasAnyBackup = GetOrQueryModBackup(m) || GetOrQueryModTextureBackup(m) || GetOrQueryModPmp(m);
            if (!hasAnyBackup)
                convertibleSelectedModsNoBackup.Add(m);
        }
        var deletableSelectedMods = selectedModsAll
            .Where(m => GetOrQueryModBackup(m) || GetOrQueryModTextureBackup(m) || GetOrQueryModPmp(m))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        using (var _d = ImRaii.Disabled(ActionsDisabled() || nonConvertibleSelectedMods.Count == 0 || reinstallSelectedMods.Count > 0))
        if (ImGui.Button("Backup"))
        {
            _running = true;
            ResetBothProgress();
            var progress = new Progress<(string,int,int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; });
            var modsQueue = nonConvertibleSelectedMods.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            _ = Task.Run(async () =>
            {
                foreach (var m in modsQueue)
                {
                    try { await _backupService.CreateFullModBackupAsync(m, progress, CancellationToken.None).ConfigureAwait(false); }
                    catch (Exception ex) { _logger.LogError(ex, "Backup task failed for {mod}", m); }
                    await Task.Yield();
                }
                foreach (var m in modsQueue)
                {
                    try { await _backupService.ComputeSavingsForModAsync(m).ConfigureAwait(false); } catch { }
                    await Task.Yield();
                }
                _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; });
            }).ContinueWith(_ =>
            {
                try { _backupService.RedrawPlayer(); } catch { }
                _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
            }, TaskScheduler.Default);
        }
        
        ImGui.PopStyleColor(4);

        ImGui.SameLine();

        ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
        ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
        ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
        ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
        using (var _d2 = ImRaii.Disabled(ActionsDisabled() || (convertibleSelectedModsNoBackup.Count == 0 && GetConvertableTextures().Count == 0)))
        if (ImGui.Button("Convert"))
        {
            _running = true;
            ResetConversionProgress();
            _bulkStartedAt = DateTime.UtcNow;
            _bulkBackedUpMods.Clear();
            var toConvert = GetConvertableTextures();
            var modsToConvert = new HashSet<string>(convertibleSelectedModsNoBackup, StringComparer.OrdinalIgnoreCase);
            _uiThreadActions.Enqueue(() => { _totalMods = modsToConvert.Count; _currentModIndex = 0; _currentModTotalFiles = 0; _needsUIRefresh = true; });
            _ = Task.Run(async () =>
            {
                try
                {
                    foreach (var m in modsToConvert)
                    {
                        List<string>? list = null;
                        if (_scannedByMod.TryGetValue(m, out var all) && all != null && all.Count > 0)
                            list = all;
                        if (list == null)
                        {
                            var fetched = await _conversionService.GetModTextureFilesAsync(m).ConfigureAwait(false);
                            if (fetched != null && fetched.Count > 0)
                            {
                                list = fetched;
                                _uiThreadActions.Enqueue(() => { _scannedByMod[m] = fetched; _needsUIRefresh = true; });
                            }
                        }
                        if (list != null)
                        {
                            foreach (var f in list)
                                toConvert[f] = Array.Empty<string>();
                        }
                        await Task.Yield();
                    }
                    try { _modsTouchedLastRun.Clear(); } catch { }
                    await _conversionService.StartConversionAsync(toConvert).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Convert task failed");
                }
                finally
                {
                    _uiThreadActions.Enqueue(() => { _running = false; });
                }
            });
        }
        
        ImGui.PopStyleColor(4);

        ImGui.SameLine();

        var restorableModsForAction = GetRestorableModsForCurrentSelection();
        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var restorableFiltered = automaticMode
            ? restorableModsForAction.Where(m =>
                {
                    var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                    if (snap.TryGetValue(m, out var ms) && ms != null && (ms.UsedTextureCount > 0 || (ms.UsedTextureFiles != null && ms.UsedTextureFiles.Count > 0)))
                        return false;
                    return true;
                })
                .ToList()
            : restorableModsForAction;
        bool canRestore = restorableFiltered.Count > 0;
        bool someSkippedByAuto = automaticMode && restorableFiltered.Count < restorableModsForAction.Count;

        using (var _d3 = ImRaii.Disabled(ActionsDisabled() || !canRestore))
        {
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
                _bulkStartedAt = DateTime.UtcNow;
                _restoreModsTotal = restorableFiltered.Count;
                _restoreModsDone = 0;

                _ = Task.Run(async () =>
                {
                    foreach (var mod in restorableFiltered)
                    {
                        try
                        {
                            _uiThreadActions.Enqueue(() => { 
                                _restoreModsDone++; 
                                _currentRestoreMod = mod; 
                                _currentModStartedAt = DateTime.UtcNow;
                            });
                            _currentRestoreMod = mod;
                            _currentRestoreModIndex = 0;
                            _currentRestoreModTotal = 0;
                            var hasPmp = false;
                            try { hasPmp = _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); }
                            catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", mod); }
                            if (hasPmp)
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
                    RefreshScanResults(true, "restore-bulk-completed");
                    foreach (var m in restorableFiltered)
                    {
                        try { RefreshModState(m, "restore-bulk"); }
                        catch (Exception ex) { _logger.LogError(ex, "RefreshModState after bulk restore failed for {mod}", m); }
                    }
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
                        foreach (var m in restorableFiltered)
                        {
                            try { _modStateService.UpdateExternalChange(m, null); } catch { }
                        }
                    }
                    catch (Exception ex) { _logger.LogError(ex, "Update external change after bulk restore failed"); }
                    _uiThreadActions.Enqueue(() =>
                    {
                        _running = false;
                        _restoreCancellationTokenSource?.Dispose();
                        _restoreCancellationTokenSource = null;
                        _currentRestoreMod = string.Empty;
                        _currentRestoreModIndex = 0;
                        _currentRestoreModTotal = 0;
                        _restoreModsDone = 0;
                        _restoreModsTotal = 0;
                    });

                    try
                    {
                        var threads = Math.Max(1, _configService.Current.MaxStartupThreads);
                        _uiThreadActions.Enqueue(() => { SetStartupRefreshInProgress(true); });
                        _ = Task.Run(async () =>
                        {
                            try { await _conversionService.RunInitialParallelUpdateAsync(threads, CancellationToken.None).ConfigureAwait(false); } catch { }
                            _uiThreadActions.Enqueue(() => { SetStartupRefreshInProgress(false); _needsUIRefresh = true; });
                        });
                    }
                    catch { }
                });
            }
            ImGui.PopStyleColor(4);
        }
        

        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (!canRestore)
                ImGui.SetTooltip("No selected mods have backups to restore.");
            else if (someSkippedByAuto)
                ImGui.SetTooltip("Automatic mode: skipping mods with currently used textures.");
        }

        ImGui.SameLine();
        using (var _dDeleteSelected = ImRaii.Disabled(ActionsDisabled() || deletableSelectedMods.Count == 0))
        {
            ImGui.PushFont(UiBuilder.IconFont);
            if (ImGui.Button($"{FontAwesomeIcon.Trash.ToIconString()}##delete-selected-mods", new Vector2(32, 0)))
            {
                if (ImGui.GetIO().KeyCtrl)
                    TryDeleteSelectedEntriesAndBackups(deletableSelectedMods, "delete-selected-ctrl");
                else
                    SetStatus("Hold CTRL while clicking trash to delete selected entries and backups.");
            }
            ImGui.PopFont();
        }
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
        {
            if (deletableSelectedMods.Count == 0)
                ImGui.SetTooltip("No selected mods with backups to delete.");
            else
                ImGui.SetTooltip("Hold CTRL and click to delete all selected entries with backups.");
        }
    }
}
