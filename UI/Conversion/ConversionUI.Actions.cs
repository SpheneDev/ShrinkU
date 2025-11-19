using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Dalamud.Bindings.ImGui;
using ShrinkU.Configuration;

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
                try { _backupService.RedrawPlayer(); } catch { }
                if (setCompletionStatus)
                {
                    _uiThreadActions.Enqueue(() => { SetStatus(success ? $"PMP restore completed for {mod}: {display}" : $"PMP restore failed for {mod}: {display}"); });
                }
                RefreshScanResults(true, refreshReason);
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
                        try { bool _r; _modsWithPmpCache.TryRemove(mod, out _r); } catch { }
                        try { (string version, string author, DateTime createdUtc, string pmpFileName) _rm; _modsPmpMetaCache.TryRemove(mod, out _rm); } catch { }
                        try { var hasPmpNow = _backupService.HasPmpBackupForModAsync(mod).GetAwaiter().GetResult(); _cacheService.SetModHasPmp(mod, hasPmpNow); } catch { }
                    }
                });
                _running = false;
                if (resetConversionAfter) ResetConversionProgress();
                if (resetRestoreAfter) ResetRestoreProgress();
                if (closePopup) ImGui.CloseCurrentPopup();
            });
    }
}