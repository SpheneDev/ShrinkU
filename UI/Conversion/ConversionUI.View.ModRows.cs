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

        ImGui.TableSetColumnIndex(0);
        var modCellMin = ImGui.GetCursorScreenPos();
        var modCellWidth = ImGui.GetColumnWidth();
        var cellPaddingY = ImGui.GetStyle().CellPadding.Y;
        var rowContentHeight = MathF.Max(0f, FixedFlatRowHeight - (cellPaddingY * 2f));
        var textLineHeight = ImGui.GetTextLineHeight();
        const float modRowVisualNudgeY = 1.5f;
        var centeredTextOffsetY = MathF.Max(0f, (rowContentHeight - textLineHeight) * 0.5f) + modRowVisualNudgeY;
        var modRowCursor = ImGui.GetCursorPos();
        ImGui.SetCursorPos(new Vector2(modRowCursor.X + row.Depth * 16f, modRowCursor.Y + centeredTextOffsetY));
        var nodeFlags = ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.SpanFullWidth;
        if (!_configService.Current.ShowModFilesInOverview)
            nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
        string header;
        if (hasBackup)
            header = ShrinkU.Helpers.ConversionUiHelpers.FormatHeader(ResolveModDisplayName(mod), convertedAll, totalAll);
        else
            header = string.Concat(ResolveModDisplayName(mod), " (", totalAll.ToString(), ")");
        var open = _expandedMods.Contains(mod);
        ImGui.SetNextItemOpen(open, ImGuiCond.Always);
        if (excluded)
            header = string.Concat(header, " [Excluded]");
        var iconCursorPos = ImGui.GetCursorPos();
        var iconCellWidth = MathF.Max(16f, ImGui.GetFrameHeight() - 2f);
        var iconText = FontAwesomeIcon.Cube.ToIconString();
        ImGui.SetCursorPosX(iconCursorPos.X + iconCellWidth + ImGui.GetStyle().ItemInnerSpacing.X);
        ImGui.PushStyleColor(ImGuiCol.Header, Vector4.Zero);
        ImGui.PushStyleColor(ImGuiCol.HeaderHovered, Vector4.Zero);
        ImGui.PushStyleColor(ImGuiCol.HeaderActive, Vector4.Zero);
        open = ImGui.TreeNodeEx($"{header}##mod-{mod}", nodeFlags);
        ImGui.PopStyleColor(3);
        var modHeaderRightX = ImGui.GetItemRectMax().X;
        var modToggled = ImGui.IsItemToggledOpen();
        var modHeaderClicked = ImGui.IsItemClicked(ImGuiMouseButton.Left);
        if (modToggled)
        {
            if (open) _expandedMods.Add(mod);
            else _expandedMods.Remove(mod);
        }
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
                StartLatestTextureRestoreForMod(mod, "restore-flat-context", "RedrawPlayer after flat restore failed");
            }
            }
            if (!hasTexBk) ShowTooltip("No texture backups available for this mod.");
            if (ImGui.MenuItem("Restore PMP"))
            {
                TryStartPmpRestoreNewest(mod, "pmp-restore-flat-context-newest", false, true, false, false, true);
            }
            ImGui.EndPopup();
        }

        var cursorAfterHeader = ImGui.GetCursorPos();
        var modUsedByPenumbra = IsModUsedByPenumbra(mod, modState);
        ImGui.SetCursorPos(cursorAfterHeader);
        var eyeHovered = DrawPenumbraUsedIndicator(mod, modState, totalAll);
        var cursorAfterEye = ImGui.GetCursorPos();
        var warningIconY = modCellMin.Y + centeredTextOffsetY;
        var warningIconX = modHeaderRightX + ImGui.GetStyle().ItemSpacing.X;
        if (modUsedByPenumbra)
            warningIconX = MathF.Max(warningIconX, ImGui.GetItemRectMax().X + ImGui.GetStyle().ItemSpacing.X);
        if (IsModInefficient(mod))
        {
            ImGui.SetCursorScreenPos(new Vector2(warningIconX, warningIconY));
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("This mod becomes larger after conversion");
            warningIconX = ImGui.GetItemRectMax().X + ImGui.GetStyle().ItemSpacing.X;
        }
        if (hasBackup && _cachedPerModSavings.TryGetValue(mod, out var noteStats1) && noteStats1 != null && noteStats1.OriginalBytes > 0 && noteStats1.CurrentBytes > noteStats1.OriginalBytes)
        {
            ImGui.SetCursorScreenPos(new Vector2(warningIconX, warningIconY));
            ImGui.PushFont(UiBuilder.IconFont);
            ImGui.TextColored(ShrinkUColors.WarningLight, FontAwesomeIcon.ExclamationTriangle.ToIconString());
            ImGui.PopFont();
            if (ImGui.IsItemHovered())
                ImGui.SetTooltip("This mod is smaller when not converted");
        }
        ImGui.SetCursorPos(iconCursorPos);
        ImGui.PushFont(UiBuilder.IconFont);
        Vector4 iconColor = new Vector4(1f, 1f, 1f, 1f);
        if (_modEnabledStates.TryGetValue(mod, out var stEnabledPre) && stEnabledPre.Enabled)
            iconColor = new Vector4(0.40f, 0.85f, 0.40f, 1f);
        if (excluded)
            iconColor = new Vector4(0.85f, 0.60f, 0.20f, 1f);
        ImGui.TextColored(iconColor, iconText);
        ImGui.PopFont();
        ImGui.SetCursorPos(cursorAfterEye);
        

        bool modSelected = IsModSelected(mod, visibleByMod);
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
        var anyPopupOpen = ImGui.IsPopupOpen(string.Empty, ImGuiPopupFlags.AnyPopup);
        var rowHeight = FixedFlatRowHeight;
        var modCellHoverMin = new Vector2(modCellMin.X, modCellMin.Y - cellPaddingY);
        var modCellHoverMax = new Vector2(modCellMin.X + modCellWidth, modCellMin.Y - cellPaddingY + rowHeight);
        var hoveredModCell = ImGui.IsMouseHoveringRect(modCellHoverMin, modCellHoverMax, true) && !anyPopupOpen;
        if (hoveredModCell)
        {
            var hoverColor = modSelected ? new Vector4(0.28f, 0.49f, 0.84f, 0.52f) : new Vector4(0.30f, 0.34f, 0.40f, 0.34f);
            ImGui.TableSetBgColor(ImGuiTableBgTarget.RowBg0, ImGui.GetColorU32(hoverColor));
        }
        if (hoveredModCell && !eyeHovered)
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
            ImGui.TextUnformatted($"Penumbra ModDir: {mod}");
            ImGui.TextUnformatted($"Used by Penumbra: {(IsModUsedByPenumbra(mod, modState) ? "Yes" : "No")}");
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
            ImGui.Separator();
            if (disableCheckbox)
                ImGui.TextUnformatted(string.IsNullOrEmpty(disableReason) ? "Selection disabled." : disableReason);
            else
                ImGui.TextUnformatted("Selection: click row, CTRL for toggle, SHIFT for range.");
            ImGui.EndTooltip();
        }

        ImGui.TableSetColumnIndex(2);
        var col2Cursor = ImGui.GetCursorPos();
        ImGui.SetCursorPosY(col2Cursor.Y + centeredTextOffsetY);
        long modOriginalBytes = modState != null && modState.OriginalBytes > 0 ? modState.OriginalBytes : GetOrQueryModOriginalTotal(mod);
        var hideStatsForNoTextures = totalAll == 0;
        if (hideStatsForNoTextures)
            ImGui.TextUnformatted("");
        else
            DrawRightAlignedSize(modOriginalBytes);

        ImGui.TableSetColumnIndex(1);
        var col1Cursor = ImGui.GetCursorPos();
        ImGui.SetCursorPosY(col1Cursor.Y + centeredTextOffsetY);
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

        ImGui.TableSetColumnIndex(3);
        var hasTexBackup = GetOrQueryModTextureBackup(mod);
        var hasPmpBackup = GetOrQueryModPmp(mod);
        var anyBackup = hasBackup || hasTexBackup || hasPmpBackup;
        using (var _dDelete = ImRaii.Disabled(ActionsDisabled() || !anyBackup))
        {
            ImGui.PushFont(UiBuilder.IconFont);
            if (ImGui.Button($"{FontAwesomeIcon.Trash.ToIconString()}##delete-icon-{mod}", new Vector2(24, 0)))
            {
                if (ImGui.GetIO().KeyCtrl)
                    TryDeleteModEntryAndBackups(mod, "delete-entry-ctrl");
                else
                    SetStatus("Hold CTRL while clicking delete to remove entry and backups.");
            }
            ImGui.PopFont();
        }
        if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ShowTooltip(anyBackup ? "Hold CTRL and click to delete entry and backups." : "Delete is only available when backups exist.");
        ImGui.SameLine();
        if (totalAll > 0 && !isOrphan)
        {
            var autoMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
            var showRestore = anyBackup && !(modState != null && modState.InstalledButNotConverted);
            if (!showRestore)
            {
                PushButtonColors(ShrinkUColors.ConvertButton, ShrinkUColors.ConvertButtonHovered, ShrinkUColors.ConvertButtonActive, ShrinkUColors.ButtonTextOnAccent);
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
                PopButtonColors();
                if (!convertDisabled) ShowTooltip("Convert all textures for this mod.");
                }
            }
            else
            {
                var defaultTextColor = ImGui.GetStyle().Colors[(int)ImGuiCol.Text];
                PushButtonColors(ShrinkUColors.RestoreButton, ShrinkUColors.RestoreButtonHovered, ShrinkUColors.RestoreButtonActive, ShrinkUColors.ButtonTextOnAccent);
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
                        StartLatestTextureRestoreForMod(mod, "restore-flat", "RedrawPlayer after reinstall failed");
                    }
                }
                if ((excluded || restoreDisabledByAuto) && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                    ShowTooltip(excluded ? "Mod excluded by tags." : "Automatic mode: restore disabled for installed mods.");
                PopButtonColors();
                if (!(excluded || restoreDisabledByAuto))
                    ShowTooltip("Restore backups for this mod.");
                }
                if (ImGui.BeginPopup($"restorectx-{mod}"))
                {
                    using var _popupText = ImRaii.PushColor(ImGuiCol.Text, defaultTextColor);
                    var hasTexBk = GetOrQueryModTextureBackup(mod);
                    using (var _d2 = ImRaii.Disabled(!hasTexBk))
                    {
                        if (ImGui.MenuItem("Restore textures"))
                        {
                            StartLatestTextureRestoreForMod(mod, "restore-flat-context", "RedrawPlayer after flat restore failed");
                        }
                    }
                    if (ImGui.MenuItem("Restore PMP"))
                    {
                        TryStartPmpRestoreNewest(mod, "pmp-restore-flat-context-newest", false, true, false, false, true);
                    }
                    ImGui.EndPopup();
                }
            }
        }

        if (totalAll == 0)
        {
            if (anyBackup)
            {
                if (hasPmpBackup && !isOrphan)
                {
                    PushButtonColors(ShrinkUColors.ReinstallButton, ShrinkUColors.ReinstallButtonHovered, ShrinkUColors.ReinstallButtonActive, ShrinkUColors.ButtonTextOnAccent);
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
                    PopButtonColors();
                }
                else if (!isOrphan)
                {
                    PushButtonColors(ShrinkUColors.RestoreButton, ShrinkUColors.RestoreButtonHovered, ShrinkUColors.RestoreButtonActive, ShrinkUColors.ButtonTextOnAccent);
                    using (var _d = ImRaii.Disabled(ActionsDisabled()))
                    {
                    if (ImGui.Button($"Restore##restore-{mod}", new Vector2(60, 0)))
                    {
                        StartLatestTextureRestoreForMod(mod, "restore-flat-nonconvertible", "RedrawPlayer after reinstall failed");
                    }
                    ShowTooltip("Restore textures from backup.");
                    }
                    PopButtonColors();
                }
            }
            else
            {
                PushButtonColors(ShrinkUColors.Accent, ShrinkUColors.AccentHovered, ShrinkUColors.AccentActive, ShrinkUColors.ButtonTextOnAccent);
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
                            _ = _backupService.HasBackupForModAsync(mod).ContinueWith(async bt =>
                            {
                                if (bt.Status == TaskStatus.RanToCompletion)
                                {
                                    bool any = bt.Result;
                                    try 
                                    { 
                                        var hasPmp = await _backupService.HasPmpBackupForModAsync(mod).ConfigureAwait(false);
                                        any = any || hasPmp;
                                    }
                                    catch (Exception ex) { _logger.LogError(ex, "HasPmpBackup check failed for {mod}", mod); }
                                    _cacheService.SetModHasBackup(mod, any);
                                    _modsWithPmpCache[mod] = true;
                                }
                            }, TaskScheduler.Default);
                            _uiThreadActions.Enqueue(() => { _running = false; ResetBothProgress(); });
                        }, TaskScheduler.Default);
                }
                if (ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
                    ShowTooltip("Create a full mod backup (PMP).");
                PopButtonColors();
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
            PushButtonColors(ShrinkUColors.Accent, ShrinkUColors.AccentHovered, ShrinkUColors.AccentActive, ShrinkUColors.ButtonTextOnAccent);
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
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                var orphans = await _backupService.FindOrphanedBackupsAsync().ConfigureAwait(false);
                                _uiThreadActions.Enqueue(() =>
                                {
                                    _orphaned = orphans ?? new List<ShrinkU.Services.TextureBackupService.OrphanBackupInfo>();
                                    _orphanRevision++;
                                    _needsUIRefresh = true;
                                    RequestUiRefresh("orphan-refresh-after-install");
                                });
                            }
                            catch (Exception ex) { _logger.LogError(ex, "FindOrphanedBackupsAsync after install failed for {mod}", mod); }
                        });
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
            PopButtonColors();
        }

        var clickedInModCell = ImGui.IsMouseClicked(ImGuiMouseButton.Left) && hoveredModCell;
        if (modHeaderClicked || clickedInModCell)
            HandleModRowClickSelection(mod, disableCheckbox, visibleByMod);

        // No backup creation button for non-convertible mods without backups
    }
}
