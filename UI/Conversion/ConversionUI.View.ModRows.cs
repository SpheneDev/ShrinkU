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
    private void DrawModFlatRow_ViewImpl(FlatRow row, Dictionary<string, List<string>> visibleByMod)
    {
        var mod = row.Mod;
        var files = visibleByMod.TryGetValue(mod, out var list) ? list : new List<string>();
        var isOrphan = _orphaned.Any(x => string.Equals(x.ModFolderName, mod, StringComparison.OrdinalIgnoreCase));
        var hasBackup = GetOrQueryModBackup(mod);
        var excluded = (!hasBackup && !isOrphan && IsModExcludedByTags(mod));
        var isNonConvertible = files.Count == 0;

        ImGui.TableSetColumnIndex(1);
        ImGui.SetCursorPosX(ImGui.GetCursorPosX() + row.Depth * 16f);
        var nodeFlags = ImGuiTreeNodeFlags.SpanFullWidth | ImGuiTreeNodeFlags.NoTreePushOnOpen | ImGuiTreeNodeFlags.FramePadding;
        if (!_configService.Current.ShowModFilesInOverview)
            nodeFlags |= ImGuiTreeNodeFlags.Bullet | ImGuiTreeNodeFlags.Leaf;
        int totalAll = files.Count;
        int convertedAll = 0;
        if (hasBackup && totalAll > 0)
        {
            if (_cachedPerModSavings.TryGetValue(mod, out var s2) && s2 != null && s2.ComparedFiles > 0)
                convertedAll = Math.Min(s2.ComparedFiles, totalAll);
            else
            {
                if (_modStateSnapshot != null && _modStateSnapshot.TryGetValue(mod, out var ms) && ms != null && ms.ComparedFiles > 0)
                    convertedAll = Math.Min(ms.ComparedFiles, totalAll);
            }
        }
        var header = hasBackup ? $"{ResolveModDisplayName(mod)} ({convertedAll}/{totalAll})" : $"{ResolveModDisplayName(mod)} ({totalAll})";
        var open = _expandedMods.Contains(mod);
        ImGui.SetNextItemOpen(open, ImGuiCond.Always);
        open = ImGui.TreeNodeEx($"##mod-{mod}", nodeFlags);
        var headerHoveredTree = ImGui.IsItemHovered();
        if (ImGui.BeginPopupContextItem($"modctx-{mod}"))
        {
            if (ImGui.MenuItem("Open in Penumbra"))
            {
                try { _conversionService.OpenModInPenumbra(mod, null); } catch { }
            }
            var hasTexBk = GetOrQueryModTextureBackup(mod);
            ImGui.BeginDisabled(!hasTexBk);
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
                        try { _backupService.RedrawPlayer(); } catch { }
                        RefreshModState(mod, "restore-flat-context");
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
                    });
            }
            ImGui.EndDisabled();
            if (!hasTexBk) ShowTooltip("No texture backups available for this mod.");
            if (ImGui.MenuItem("Restore PMP"))
            {
                TryStartPmpRestoreNewest(mod, "pmp-restore-flat-context-newest", false, true, false, false, true);
            }
            ImGui.EndPopup();
        }
        ImGui.SameLine();
        ImGui.AlignTextToFramePadding();
        ImGui.PushFont(UiBuilder.IconFont);
        if (_modEnabledStates.TryGetValue(mod, out var stIcon))
        {
            var iconColor = stIcon.Enabled ? new Vector4(0.40f, 0.85f, 0.40f, 1f) : new Vector4(1f, 1f, 1f, 1f);
            ImGui.TextColored(iconColor, FontAwesomeIcon.Cube.ToIconString());
        }
        else
        {
            ImGui.TextUnformatted(FontAwesomeIcon.Cube.ToIconString());
        }
        ImGui.PopFont();
        ImGui.SameLine();
        ImGui.TextUnformatted(header);

        ImGui.TableSetColumnIndex(0);
        bool modSelected = isNonConvertible
            ? _selectedEmptyMods.Contains(mod)
            : ((_selectedCountByMod.TryGetValue(mod, out var sc) ? sc : 0) >= files.Count);
        var automaticMode = _configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic;
        var disableCheckbox = excluded || (automaticMode && !isOrphan && (convertedAll >= totalAll));
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
                    foreach (var f in files) _selectedTextures.Add(f);
                    _selectedCountByMod[mod] = files.Count;
                }
                else
                {
                    foreach (var f in files) _selectedTextures.Remove(f);
                    _selectedCountByMod[mod] = 0;
                }
            }
        }
        ImGui.EndDisabled();
        if (disableCheckbox && ImGui.IsItemHovered(ImGuiHoveredFlags.AllowWhenDisabled))
            ImGui.SetTooltip(automaticMode ? "Automatic mode: mod cannot be selected for conversion." : "Mod excluded by tags");
        else
            ShowTooltip("Toggle selection for all files in this mod.");

        if (headerHoveredTree)
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
        ShrinkU.Services.ModStateEntry? modState = null;
        if (_modStateSnapshot != null)
            _modStateSnapshot.TryGetValue(mod, out modState);
        long modOriginalBytes = modState != null && modState.ComparedFiles > 0 ? modState.OriginalBytes : GetOrQueryModOriginalTotal(mod);
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
        ImGui.BeginDisabled(ActionsDisabled());
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
            ImGui.BeginDisabled(excluded || (!isOrphan && restoreDisabledByAuto) || (isOrphan && !canInstall) || ActionsDisabled());
            if (ImGui.Button(actionLabel, new Vector2(60, 0)))
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
                            try { RefreshModState(mod, "orphan-install-flat"); } catch { }
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
                                RefreshModState(mod, "reinstall-completed-flat");
                            });
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
                        TryStartPmpRestoreNewest(mod, "restore-pmp-flat", true, false, false, true, false);
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
                                RefreshModState(mod, "restore-flat");
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
                    _ = _conversionService.StartConversionAsync(toConvert);
                }
            }
            ImGui.PopStyleColor(4);
            ImGui.EndDisabled();
        }
    }
}