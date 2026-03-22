using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Dalamud.Bindings.ImGui;
using ShrinkU.Configuration;
using ShrinkU.Services;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private void TryStartPmpRestoreNewest(string mod, string refreshReason, bool removeCacheBefore, bool setCompletionStatus, bool resetConversionAfter, bool resetRestoreAfter, bool closePopup)
    {
        if (_configService.Current.TextureProcessingMode == TextureProcessingMode.Automatic)
        {
            SetStatus("Automatic mode active: restoring is disabled.");
            return;
        }
        TraceAction(refreshReason, nameof(TryStartPmpRestoreNewest), mod);
        List<string>? pmpFiles = null;
        try { pmpFiles = _backupService.GetPmpBackupsForModAsync(mod).GetAwaiter().GetResult(); } catch { }
        if (pmpFiles == null || pmpFiles.Count == 0)
        {
            SetStatus($"No .pmp backups found for {mod}");
            return;
        }
        var latest = pmpFiles.OrderByDescending(f => f).First();
        var display = Path.GetFileName(latest);

        if (removeCacheBefore)
        {
            ClearModCaches(mod);
        }

        _running = true;
        ResetBothProgress();
        _currentRestoreMod = mod;
        SetStatus($"PMP restore requested for {mod}: {display}");
        var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
        _ = _backupService.RestorePmpAsync(mod, latest, progress, CancellationToken.None)
            .ContinueWith(t =>
            {
                var success = t.Status == TaskStatus.RanToCompletion && t.Result;
                TraceAction(refreshReason, "RestorePmpAsync.completed", success ? latest : string.Empty);
                try { _backupService.RedrawPlayer(); } catch { }
                if (setCompletionStatus)
                {
                    _uiThreadActions.Enqueue(() => { SetStatus(success ? $"PMP restore completed for {mod}: {display}" : $"PMP restore failed for {mod}: {display}"); });
                }
                RefreshScanResults(true, refreshReason);
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
                        try { any = any || _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); } catch { }
                        _cacheService.SetModHasBackup(mod, any);
                        try { bool _r; _modsWithPmpCache.TryRemove(mod, out _r); } catch { }
                        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
                        try { var hasPmpNow = _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); _cacheService.SetModHasPmp(mod, hasPmpNow); } catch { }
                    }
                });
                _uiThreadActions.Enqueue(() =>
                {
                    _running = false;
                    if (resetConversionAfter) ResetConversionProgress();
                    if (resetRestoreAfter) ResetRestoreProgress();
                    if (closePopup) ImGui.CloseCurrentPopup();
                    _modStateSnapshot = _modStateService.Snapshot();
                    _needsUIRefresh = true;
                });
            });
    }

    private void OpenDeleteEntryAndBackupsConfirm(string mod)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return;
        _pendingDeleteMod = mod;
        _openDeleteConfirmPopup = true;
    }

    private void DrawDeleteEntryAndBackupsConfirmPopup()
    {
        if (ImGui.BeginPopupModal("Delete entry and backups?###shrinku-delete-entry", ImGuiWindowFlags.AlwaysAutoResize))
        {
            var mod = _pendingDeleteMod ?? string.Empty;
            ImGui.TextWrapped($"Delete entry and all backups for \"{mod}\"?");
            ImGui.TextWrapped("This removes the ShrinkU state entry and backup folders for this mod.");
            if (ImGui.Button("Delete", new System.Numerics.Vector2(120, 0)))
            {
                var target = _pendingDeleteMod;
                _pendingDeleteMod = string.Empty;
                if (!string.IsNullOrWhiteSpace(target))
                    TryDeleteModEntryAndBackups(target, "delete-entry-confirmed");
                ImGui.CloseCurrentPopup();
            }
            ImGui.SameLine();
            if (ImGui.Button("Cancel", new System.Numerics.Vector2(120, 0)))
            {
                _pendingDeleteMod = string.Empty;
                ImGui.CloseCurrentPopup();
            }
            ImGui.EndPopup();
        }
    }

    private void RemoveModFromUiState(string mod)
    {
        try { _selectedEmptyMods.Remove(mod); } catch { }
        try { _selectedCountByMod.Remove(mod); } catch { }
        try { _modDisplayNames.Remove(mod); } catch { }
        try { _modTags.Remove(mod); } catch { }
        try { _modEnabledStates.Remove(mod); } catch { }
        try { _modPathsStable.Remove(mod); } catch { }
        try { _modPaths.Remove(mod); } catch { }
        try { _visibleByMod.Remove(mod); } catch { }
        try { _livePenumbraMods.Remove(mod); } catch { }

        if (_scannedByMod.TryGetValue(mod, out var files) && files != null)
        {
            foreach (var f in files)
            {
                try { _texturesToConvert.Remove(f); } catch { }
                try { _fileOwnerMod.Remove(f); } catch { }
                try { _selectedTextures.Remove(f); } catch { }
            }
        }

        try { _scannedByMod.Remove(mod); } catch { }
        ClearModCaches(mod);
        try { _modsWithBackupCache.TryRemove(mod, out _); } catch { }
        try { bool _r; _modsWithPmpCache.TryRemove(mod, out _r); } catch { }
        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
        try { (string version, string author, DateTime createdUtc, string zipFileName) _rz; _modsZipMetaCache.TryRemove(mod, out _rz); } catch { }
    }

    private void TryDeleteModEntryAndBackups(string mod, string origin)
    {
        if (string.IsNullOrWhiteSpace(mod))
            return;

        if (ActionsDisabled())
        {
            SetStatus("Actions are currently disabled.");
            return;
        }

        _running = true;
        SetStatus($"Deleting entry and backups for {mod}");
        _ = Task.Run(async () =>
        {
            TextureBackupService.DeleteBackupsResult? deleteResult = null;
            try
            {
                deleteResult = await _backupService.DeleteBackupsForModAsync(mod).ConfigureAwait(false);
            }
            catch { }

            try { _modStateService.RemoveEntry(mod); } catch { }

            _uiThreadActions.Enqueue(() =>
            {
                RemoveModFromUiState(mod);
                _modStateSnapshot = _modStateService.Snapshot();
                _footerTotalsDirty = true;
                _needsUIRefresh = true;
            });

            ScheduleOrphanScan(origin, true);
            var deletedAnything = deleteResult != null && deleteResult.DeletedAnything;
            var success = deleteResult == null || deleteResult.Success;
            var status = success
                ? (deletedAnything ? $"Entry and backups deleted for {mod}" : $"Entry deleted for {mod} (no backups found)")
                : $"Entry deleted for {mod}, but backup cleanup had issues";

            _uiThreadActions.Enqueue(() =>
            {
                SetStatus(status);
                _running = false;
            });
        });
    }

    private void QueuePruneNonexistentUncategorizedMods(List<string> mods, string origin)
    {
        if (mods == null || mods.Count == 0)
            return;

        var queue = new List<string>();
        lock (_staleEntryPruneInFlight)
        {
            foreach (var mod in mods)
            {
                if (string.IsNullOrWhiteSpace(mod))
                    continue;
                if (_staleEntryPruneInFlight.Contains(mod))
                    continue;
                _staleEntryPruneInFlight.Add(mod);
                queue.Add(mod);
            }
        }

        if (queue.Count == 0)
            return;

        _ = Task.Run(() =>
        {
            foreach (var mod in queue)
            {
                try { _modStateService.RemoveEntry(mod); } catch { }
                _uiThreadActions.Enqueue(() =>
                {
                    RemoveModFromUiState(mod);
                    _modStateSnapshot = _modStateService.Snapshot();
                    _footerTotalsDirty = true;
                    _needsUIRefresh = true;
                    RequestUiRefresh(string.Concat(origin, "-", mod));
                });
                lock (_staleEntryPruneInFlight)
                {
                    _staleEntryPruneInFlight.Remove(mod);
                }
            }
        });
    }
}
