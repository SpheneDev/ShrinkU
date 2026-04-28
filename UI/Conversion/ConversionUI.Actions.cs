using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Dalamud.Bindings.ImGui;
using Microsoft.Extensions.Logging;
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

        _ = Task.Run(async () =>
        {
            try
            {
                List<string>? pmpFiles = null;
                try { pmpFiles = await _backupService.GetPmpBackupsForModAsync(mod).ConfigureAwait(false); } catch { }
                if (pmpFiles == null || pmpFiles.Count == 0)
                {
                    _uiThreadActions.Enqueue(() => SetStatus($"No .pmp backups found for {mod}"));
                    return;
                }
                var latest = pmpFiles.OrderByDescending(f => f).First();
                var display = Path.GetFileName(latest);

                if (removeCacheBefore)
                {
                    _uiThreadActions.Enqueue(() => ClearModCaches(mod));
                }

                _uiThreadActions.Enqueue(() =>
                {
                    _running = true;
                    ResetBothProgress();
                    _currentRestoreMod = mod;
                    SetStatus($"PMP restore requested for {mod}: {display}");
                });

                var progress = new Progress<(string, int, int)>(e => { _currentTexture = e.Item1; _backupIndex = e.Item2; _backupTotal = e.Item3; _currentRestoreModIndex = e.Item2; _currentRestoreModTotal = e.Item3; });
                var success = await _backupService.RestorePmpAsync(mod, latest, progress, CancellationToken.None).ConfigureAwait(false);
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
                        catch { }
                        _cacheService.SetModHasBackup(mod, any);
                        try { bool _r; _modsWithPmpCache.TryRemove(mod, out _r); } catch { }
                        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
                        try 
                        { 
                            var hasPmpNow = await _backupService.HasPmpBackupForModAsync(mod).ConfigureAwait(false);
                            _cacheService.SetModHasPmp(mod, hasPmpNow);
                        } 
                        catch { }
                    }
                }, TaskScheduler.Default);
                _uiThreadActions.Enqueue(() =>
                {
                    _running = false;
                    if (resetConversionAfter) ResetConversionProgress();
                    if (resetRestoreAfter) ResetRestoreProgress();
                    if (closePopup) ImGui.CloseCurrentPopup();
                    _modStateSnapshot = _modStateService.Snapshot();
                    _needsUIRefresh = true;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "TryStartPmpRestoreNewest failed for {mod}", mod);
            }
        });
    }

    private void StartLatestTextureRestoreForMod(string mod, string refreshReason, string redrawLogContext)
    {
        _running = true;
        _modsWithBackupCache.TryRemove(mod, out _);
        ResetBothProgress();
        _currentRestoreMod = mod;
        var progress = new Progress<(string, int, int)>(e =>
        {
            _currentTexture = e.Item1;
            _backupIndex = e.Item2;
            _backupTotal = e.Item3;
            _currentRestoreModIndex = e.Item2;
            _currentRestoreModTotal = e.Item3;
        });
        _ = _backupService.RestoreLatestForModAsync(mod, progress, CancellationToken.None)
            .ContinueWith(t =>
            {
                try { _backupService.RedrawPlayer(); }
                catch (Exception ex) { _logger.LogError(ex, "{context} for {mod}", redrawLogContext, mod); }
                RefreshModState(mod, refreshReason);
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
                    }
                }, TaskScheduler.Default);
                _uiThreadActions.Enqueue(() => { _running = false; });
            });
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

    private void TryDeleteSelectedEntriesAndBackups(IReadOnlyCollection<string> mods, string origin)
    {
        if (mods == null || mods.Count == 0)
            return;

        if (ActionsDisabled())
        {
            SetStatus("Actions are currently disabled.");
            return;
        }

        var targets = mods
            .Where(m => !string.IsNullOrWhiteSpace(m))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (targets.Count == 0)
            return;

        _running = true;
        SetStatus($"Deleting {targets.Count} selected entries and backups");
        _ = Task.Run(async () =>
        {
            var deleted = 0;
            var issues = 0;
            foreach (var mod in targets)
            {
                TextureBackupService.DeleteBackupsResult? deleteResult = null;
                try
                {
                    deleteResult = await _backupService.DeleteBackupsForModAsync(mod).ConfigureAwait(false);
                }
                catch
                {
                    issues++;
                }

                try { _modStateService.RemoveEntry(mod); } catch { issues++; }

                var targetMod = mod;
                _uiThreadActions.Enqueue(() =>
                {
                    RemoveModFromUiState(targetMod);
                    _modStateSnapshot = _modStateService.Snapshot();
                    _footerTotalsDirty = true;
                    _needsUIRefresh = true;
                });

                if (deleteResult != null && !deleteResult.Success)
                    issues++;
                deleted++;
            }

            ScheduleOrphanScan(origin, true);
            var status = issues > 0
                ? $"Deleted {deleted} selected entries with {issues} issue(s)"
                : $"Deleted {deleted} selected entries and backups";
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
