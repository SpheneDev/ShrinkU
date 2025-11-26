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
using Dalamud.Interface.Utility.Raii;
using ShrinkU.Configuration;
using ShrinkU.Services;
namespace ShrinkU.UI;
public sealed partial class ConversionUI
{
    private void DrawModFlatRow_ViewImpl(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        var mod = row.Mod;
        var files = visibleByMod.TryGetValue(mod, out var list) ? list : new List<string>();
        var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
        var hasBackup = GetOrQueryModBackup(mod);
        bool excludedByTags = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
        var excluded = excludedByTags || (_configService.Current.ExcludedMods != null && _configService.Current.ExcludedMods.Contains(mod));
        try
        {
            var prev = _excludeTraceState.TryGetValue(mod, out var p) ? p : (bool?)null;
            if (!prev.HasValue || prev.Value != excluded)
            {
                _excludeTraceState[mod] = excluded;
                var cnt = _configService.Current.ExcludedMods?.Count ?? 0;
                _logger.LogDebug("[TRACE-EXCLUDE-SPHENE] Row excluded eval: mod={mod} now={now} exCount={count}", mod, excluded, cnt);
            }
        }
        catch { }
        int totalAll = GetTotalTexturesForMod(mod, files);
        int convertedAll = 0;
        if (totalAll > 0)
        {
            if (_cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
            else if (_modStateSnapshot != null && _modStateSnapshot.TryGetValue(mod, out var ms) && ms != null && ms.ComparedFiles > 0)
                convertedAll = Math.Min(ms.ComparedFiles, totalAll);
        }
        ShrinkU.Services.ModStateEntry? modState = null;
        if (_modStateSnapshot != null)
            _modStateSnapshot.TryGetValue(mod, out modState);
        if (modState != null && modState.InstalledButNotConverted)
            convertedAll = 0;
        var isNonConvertible = totalAll == 0;

        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var nodeFlags = ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.FramePadding;
        if (!_configService.Current.ShowModFilesInOverview)
            nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
        string header;
        if (hasBackup)
            header = ShrinkU.Helpers.ConversionUiHelpers.FormatHeader(ResolveModDisplayName(mod), convertedAll, totalAll);
        else
            header = string.Concat(ResolveModDisplayName(mod), " (", totalAll.ToString(), ")");
        var open = _expandedMods.Contains(mod);
        ImGui.SetNextItemOpen(open, ImGuiCond.Always);
        ImGui.PushFont(UiBuilder.IconFont);
        Vector4 iconColor = new Vector4(1f, 1f, 1f, 1f);
        if (_modStateSnapshot != null && _modStateSnapshot.TryGetValue(mod, out var msEnabledPre) && msEnabledPre != null && msEnabledPre.Enabled)
            iconColor = new Vector4(0.40f, 0.85f, 0.40f, 1f);
        if (excluded)
            iconColor = new Vector4(0.85f, 0.60f, 0.20f, 1f);
        ImGui.TextColored(iconColor, FontAwesomeIcon.Cube.ToIconString());
        ImGui.PopFont();
        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        if (excluded)
            header = string.Concat(header, " [Excluded]");
        open = ImGui.TreeNodeEx($"{header}##mod-{mod}", nodeFlags);
        var modToggled = ImGui.IsItemToggledOpen();
        if (modToggled)
        {
            if (open) _expandedMods.Add(mod);
            else _expandedMods.Remove(mod);
        }
        var headerHoveredTree = ImGui.IsItemHovered();
        if (ImGui.BeginPopupContextItem($"modctx-{mod}"))
        {
            if (ImGui.MenuItem("Open in Penumbra"))
            {
                try { _conversionService.OpenModInPenumbra(mod, null); }
                catch (Exception ex) { _logger.LogError(ex, "OpenModInPenumbra failed for {mod}", mod); }
            }
            if (ImGui.MenuItem("Open Penumbra mod folder"))
            {
                var targetMod = mod;
                _ = Task.Run(() =>
                {
                    try
                    {
                        string? abs = null;
                        var snap = _modStateSnapshot ?? _modStateService.Snapshot();
                        if (snap.TryGetValue(targetMod, out var ms) && ms != null && !string.IsNullOrWhiteSpace(ms.ModAbsolutePath))
                            abs = ms.ModAbsolutePath;
                        else
                            abs = _backupService.GetModAbsolutePath(targetMod);
                        if (!string.IsNullOrWhiteSpace(abs) && Directory.Exists(abs))
                            Process.Start(new ProcessStartInfo { FileName = "explorer.exe", Arguments = abs, UseShellExecute = true });
                    }
                    catch (Exception ex) { _logger.LogError(ex, "Open mod folder failed for {mod}", targetMod); }
                });
            }
            if (ImGui.MenuItem("Open Shrinku backup folder"))
            {
                var targetMod2 = mod;
                _ = Task.Run(() =>
                {
                    try
                    {
                        var root = _configService.Current.BackupFolderPath;
                        var dir = string.IsNullOrWhiteSpace(root) ? string.Empty : Path.Combine(root, targetMod2);
                        if (!string.IsNullOrWhiteSpace(dir) && Directory.Exists(dir))
                            Process.Start(new ProcessStartInfo { FileName = "explorer.exe", Arguments = dir, UseShellExecute = true });
                    }
                    catch (Exception ex) { _logger.LogError(ex, "Open backup folder failed for {mod}", targetMod2); }
                });
            }
            var isExcluded = _configService.Current.ExcludedMods != null && _configService.Current.ExcludedMods.Contains(mod);
            if (!isExcluded)
            {
                if (ImGui.MenuItem("Exclude from conversion"))
                {
                    try { _configService.Current.ExcludedMods ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase); _configService.Current.ExcludedMods.Add(mod); _configService.Save(); }
                    catch (Exception ex) { _logger.LogError(ex, "Failed to exclude mod {mod}", mod); }
                }
            }
            else
            {
                if (ImGui.MenuItem("Include in conversion"))
                {
                    try { _configService.Current.ExcludedMods ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase); _ = _configService.Current.ExcludedMods.Remove(mod); _configService.Save(); }
                    catch (Exception ex) { _logger.LogError(ex, "Failed to include mod {mod}", mod); }
                }
            }
            var hasTexBk = GetOrQueryModTextureBackup(mod);
            using (var _d = ImRaii.Disabled(!hasTexBk))
            {
            if (ImGui.MenuItem("Restore textures"))
            {
                _running = true;
                _modsWithBackupCache.TryRemove(mod, out _);
                ResetBothProgress();
                _currentRestoreMod = mod;
                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                    .ContinueWith(t => {
                        var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                        try { _backupService.RedrawPlayer(); }
                        catch (Exception ex) { _logger.LogError(ex, "RedrawPlayer after flat restore failed for {mod}", mod); }
                        RefreshModState(mod, "restore-flat-context");
                        _ = _backupService.ComputeSavingsForModAsync(mod).ContinueWith(ps =>
                        {
                            if (ps.Status == TaskStatus.RanToCompletion)
                            {
                                try { _cachedPerModSavings[mod] = ps.Result; } catch { }
                                _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                            }
                        }, TaskScheduler.Default);
                        _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                        {
                            if (bt.Status == TaskStatus.RanToCompletion)
                            {
                                bool any = bt.Result;
                                try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); }
                                catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", mod); }
                                _cacheService.SetModHasBackup(mod, any);
                            }
                        });
                        _uiThreadActions.Enqueue(() => { _running = false; });
                    });
            }
            }
            if (!hasTexBk) ShowTooltip("No texture backups available for this mod.");
            if (ImGui.MenuItem("Restore PMP"))
            {
                TryStartPmpRestoreNewest(mod, "pmp-restore-flat-context-newest", false, true, false, false, true);
            }
            ImGui.EndPopup();
        }
        

        ImGui.TableSetColumnIndex(0);
        bool modSelected = isNonConvertible
            ? _selectedEmptyMods.Contains(mod)
            : ((_selectedCountByMod.TryGetValue(mod, out var sc) ? sc : 0) >= totalAll);
        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
        string disableReason = string.Empty;
        if (disableCheckbox)
        {
            if (excluded)
                disableReason = "Mod excluded.";
            else if (automaticMode && !isOrphan && (convertedAll >= totalAll))
                disableReason = "All textures already converted.";
            else if (!isNonConvertible && files.Count == 0)
                disableReason = string.Empty;
            else if (isNonConvertible)
                disableReason = "Mod has no textures.";
        }
        ImGui.BeginDisabled(disableCheckbox);
        if (ImGui.Checkbox($"##modsel-{mod}", ref modSelected))
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
        ImGui.EndDisabled();
        if (disableCheckbox && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip(string.IsNullOrEmpty(disableReason) ? "Selection disabled." : disableReason);
        else
            ShowTooltip("Toggle selection for all files in this mod.");

        if (headerHoveredTree)
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
            var showConvertedCountTip = hasBackup && !(modState != null && modState.InstalledButNotConverted);
            ImGui.TextUnformatted($"Textures: {(showConvertedCountTip ? convertedAll : 0)}/{totalAll} converted");
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
            ImGui.EndTooltip();
        }

        ImGui.SameLine();
        if (IsModInefficient(mod))
        {
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("This mod becomes larger after conversion");
            ImGui.SameLine();
        }
        if (hasBackup && _cachedPerModSavings.TryGetValue(mod, out var noteStats1) && noteStats1 != null && noteStats1.OriginalBytes > 0 && noteStats1.CurrentBytes > noteStats1.OriginalBytes)
        {
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("This mod is smaller when not converted");
            ImGui.SameLine();
        }

        ImGui.TableSetColumnIndex(3);
        _cachedPerModSavings.TryGetValue(mod, out var modStats);
        long modOriginalBytes = modState != null && modState.OriginalBytes > 0 ? modState.OriginalBytes : GetOrQueryModOriginalTotal(mod);
        var hideStatsForNoTextures = totalAll == 0;
        if (hideStatsForNoTextures)
            ImGui.TextUnformatted("");
        else
            DrawRightAlignedSize(modOriginalBytes);

        ImGui.TableSetColumnIndex(2);
        long modCurrentBytes = 0;
        var hideCompressed = modState != null && modState.InstalledButNotConverted;
        if (!isOrphan && hasBackup && modState != null && modState.CurrentBytes > 0 && !hideCompressed)
            modCurrentBytes = modState.CurrentBytes;
        if (hideStatsForNoTextures)
            ImGui.TextUnformatted("");
        else if (modCurrentBytes <= 0)
            DrawRightAlignedTextColored("-", _compressedTextColor);
        else
        {
            var color = modCurrentBytes > modOriginalBytes ? ShrinkUColors.WarningLight : _compressedTextColor;
            DrawRightAlignedSizeColored(modCurrentBytes, color);
        }

        ImGui.TableSetColumnIndex(4);
        var hasTexBackup = GetOrQueryModTextureBackup(mod);
        var hasPmpBackup = GetOrQueryModPmp(mod);
        var anyBackup = hasBackup || hasTexBackup || hasPmpBackup;
        if (totalAll > 0)
        {
            var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
            var showRestore = anyBackup && !(modState != null && modState.InstalledButNotConverted);
            if (!showRestore)
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ConvertButton);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ConvertButtonHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ConvertButtonActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                var convertDisabled = excluded || (hasBackup && (convertedAll >= totalAll));
                using (var _d = ImRaii.Disabled(ActionsDisabled() || convertDisabled))
                {
                if (ImGui.Button($"Convert##convert-{mod}", new Vector2(60, 0)))
                {
                    List<string>? allFilesForMod = null;
                    if (_scannedByMod.TryGetValue(mod, out var all) && all != null && all.Count > 0)
                        allFilesForMod = all;
                    ResetConversionProgress();
                    ResetRestoreProgress();
                    _ = Task.Run(async () =>
                    {
                        var all = allFilesForMod;
                        if (all == null || all.Count == 0)
                        {
                            try
                            {
                                var scanned = await _conversionService.GetModTextureFilesAsync(mod).ConfigureAwait(false);
                                all = scanned ?? new List<string>();
                                _uiThreadActions.Enqueue(() => { _scannedByMod[mod] = all; _needsUIRefresh = true; });
                            }
                            catch (Exception ex) { _logger.LogError(ex, "GetModTextureFilesAsync failed for {mod}", mod); }
                        }
                        var toConvert = BuildToConvert(all ?? new List<string>());
                        await _conversionService.StartConversionAsync(toConvert).ConfigureAwait(false);
                    });
                }
                if (convertDisabled && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                    ShowTooltip(convertedAll >= totalAll ? "All textures already converted." : (excluded ? "Mod excluded by tags." : "Processing in progress."));
                ImGui.PopStyleColor(4);
                if (!convertDisabled) ShowTooltip("Convert all textures for this mod.");
                }
            }
            else
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                var restoreDisabledByAuto = autoMode && !isOrphan;
                using (var _d = ImRaii.Disabled(ActionsDisabled() || excluded || restoreDisabledByAuto))
                {
                if (ImGui.Button($"Restore##restore-{mod}", new Vector2(60, 0)))
                {
                    var hasPmpForClick = GetOrQueryModPmp(mod);
                    if (hasPmpForClick)
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
                                try { _backupService.RedrawPlayer(); }
                                catch (Exception ex) { _logger.LogError(ex, "RedrawPlayer after reinstall failed for {mod}", mod); }
                                RefreshModState(mod, "restore-flat");
                                _ = _backupService.ComputeSavingsForModAsync(mod).ContinueWith(ps =>
                                {
                                    if (ps.Status == TaskStatus.RanToCompletion)
                                    {
                                        try { _cachedPerModSavings[mod] = ps.Result; } catch { }
                                        _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                                    }
                                }, TaskScheduler.Default);
                                _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                                {
                                    if (bt.Status == TaskStatus.RanToCompletion)
                                    {
                                        bool any = bt.Result;
                                        try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); }
                                        catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", mod); }
                                        _cacheService.SetModHasBackup(mod, any);
                                    }
                                });
                                _uiThreadActions.Enqueue(() => { _running = false; });
                            });
                    }
                }
                if ((excluded || restoreDisabledByAuto) && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                    ShowTooltip(excluded ? "Mod excluded by tags." : "Automatic mode: restore disabled for installed mods.");
                ImGui.PopStyleColor(4);
                if (!(excluded || restoreDisabledByAuto))
                    ShowTooltip("Restore backups for this mod.");
                }
            }
        }

        if (totalAll == 0)
        {
            if (anyBackup)
            {
                if (hasPmpBackup && !isOrphan)
                {
                    ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.ReinstallButton);
                    ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.ReinstallButtonHovered);
                    ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.ReinstallButtonActive);
                    ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                    using (var _d = ImRaii.Disabled(ActionsDisabled()))
                    {
                    if (ImGui.Button($"Reinstall##reinstall-{mod}", new Vector2(60, 0)))
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
                                    SetStatus(success ? $"Reinstall completed for {mod}" : $"Reinstall failed for {mod}");
                                    _disableActionsUntilUtc = DateTime.UtcNow.AddSeconds(2);
                                    _needsUIRefresh = true;
                                    ClearModCaches(mod);
                                    RefreshModState(mod, "reinstall-completed-flat");
                                });
                                ResetBothProgress();
                                _uiThreadActions.Enqueue(() => { _running = false; });
                            }, TaskScheduler.Default);
                    }
                    ShowTooltip("Reinstall mod from PMP backup.");
                    }
                    ImGui.PopStyleColor(4);
                }
                else if (!isOrphan)
                {
                    ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.RestoreButton);
                    ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.RestoreButtonHovered);
                    ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.RestoreButtonActive);
                    ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                    using (var _d = ImRaii.Disabled(ActionsDisabled()))
                    {
                    if (ImGui.Button($"Restore##restore-{mod}", new Vector2(60, 0)))
                    {
                        _running = true;
                        _modsWithBackupCache.TryRemove(mod, out _);
                        ResetBothProgress();
                        _currentRestoreMod = mod;
                        var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                        _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
                            .ContinueWith(t => {
                                var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                                try { _backupService.RedrawPlayer(); }
                                catch (Exception ex) { _logger.LogError(ex, "RedrawPlayer after reinstall failed for {mod}", mod); }
                                RefreshModState(mod, "restore-flat-nonconvertible");
                                _ = _backupService.ComputeSavingsForModAsync(mod).ContinueWith(ps =>
                                {
                                    if (ps.Status == TaskStatus.RanToCompletion)
                                    {
                                        try { _cachedPerModSavings[mod] = ps.Result; } catch { }
                                        _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                                    }
                                }, TaskScheduler.Default);
                                _uiThreadActions.Enqueue(() => { _running = false; });
                            });
                    }
                    ShowTooltip("Restore textures from backup.");
                    }
                    ImGui.PopStyleColor(4);
                }
            }
            else
            {
                ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
                ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
                ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
                ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
                var backupLabel = $"Backup##backup-{mod}";
                using (var _d = ImRaii.Disabled(isOrphan || ActionsDisabled()))
                {
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
                            try { RefreshModState(mod, "manual-pmp-backup-button"); }
                            catch (Exception ex) { _logger.LogError(ex, "RefreshModState after manual PMP backup failed for {mod}", mod); }
                            _ = _backupService.ComputeSavingsForModAsync(mod).ContinueWith(ps =>
                            {
                                if (ps.Status == TaskStatus.RanToCompletion)
                                {
                                    try { _cachedPerModSavings[mod] = ps.Result; } catch { }
                                    _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                                }
                            }, TaskScheduler.Default);
                            _ = _backupService.HasBackupForModAsync(mod).ContinueWith(bt =>
                            {
                                if (bt.Status == TaskStatus.RanToCompletion)
                                {
                                    bool any = bt.Result;
                                    try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); }
                                    catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", mod); }
                                    _cacheService.SetModHasBackup(mod, any);
                                    _modsWithPmpCache[mod] = true;
                                }
                            });
                            _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                        }, TaskScheduler.Default);
                }
                if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                    ShowTooltip("Create a full mod backup (PMP).");
                ImGui.PopStyleColor(4);
                }
            }
        }

        var canInstall = false;
        if (isOrphan && hasPmpBackup)
        {
            try
            {
                var info = _orphaned.FirstOrDefault(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
                canInstall = info != null && !string.IsNullOrWhiteSpace(info.LatestPmpPath);
            }
            catch (Exception ex) { _logger.LogError(ex, "Resolve install capability failed for {mod}", mod); canInstall = false; }
        }
        if (isOrphan && hasPmpBackup)
        {
            ImGui.PushStyleColor(ImGuiCol.Button, ShrinkUColors.Accent);
            ImGui.PushStyleColor(ImGuiCol.ButtonHovered, ShrinkUColors.AccentHovered);
            ImGui.PushStyleColor(ImGuiCol.ButtonActive, ShrinkUColors.AccentActive);
            ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ButtonTextOnAccent);
            using (var _d = ImRaii.Disabled(ActionsDisabled() || !canInstall))
            if (ImGui.Button($"Install##install-{mod}", new Vector2(60, 0)))
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
                        try
                        {
                            var am = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
                            if (am)
                                _ = _conversionService.StartAutomaticConversionForModWithDelayAsync(mod, 2000);
                        }
                        catch (Exception ex) { _logger.LogError(ex, "StartAutomaticConversionForModWithDelayAsync failed for {mod}", mod); }
                        try { _modStateService.UpdateExternalChange(mod, null); }
                        catch (Exception ex) { _logger.LogError(ex, "Update external change after install failed for {mod}", mod); }
                        try { RefreshModState(mod, "orphan-install-flat"); }
                        catch (Exception ex) { _logger.LogError(ex, "RefreshModState after install failed for {mod}", mod); }
                        try
                        {
                            var orphans = _backupService.FindOrphanedBackupsAsync().GetAwaiter().GetResult();
                            _uiThreadActions.Enqueue(() =>
                            {
                                _orphaned = orphans ?? new List<ShrinkU.Services.TextureBackupService.OrphanBackupInfo>();
                                _orphanRevision++;
                                _needsUIRefresh = true;
                                RequestUiRefresh("orphan-refresh-after-install");
                            });
                        }
                        catch (Exception ex) { _logger.LogError(ex, "FindOrphanedBackupsAsync after install failed for {mod}", mod); }
                        _ = _backupService.ComputeSavingsForModAsync(mod).ContinueWith(ps =>
                        {
                            if (ps.Status == TaskStatus.RanToCompletion)
                            {
                                try { _cachedPerModSavings[mod] = ps.Result; } catch { }
                                _uiThreadActions.Enqueue(() => { _perModSavingsRevision++; _footerTotalsDirty = true; _needsUIRefresh = true; });
                            }
                        }, TaskScheduler.Default);
                        _uiThreadActions.Enqueue(() => { _running = false; _needsUIRefresh = true; });
                    }, TaskScheduler.Default);
            }
            ShowTooltip("Install mod from PMP backup if Penumbra removed it.");
            ImGui.PopStyleColor(4);
            ImGui.SameLine();
        }

        // No backup creation button for non-convertible mods without backups
    }
}
