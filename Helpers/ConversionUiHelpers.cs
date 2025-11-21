using System;

namespace ShrinkU.Helpers
{
    public static class ConversionUiHelpers
    {
        public static string FormatHeader(string displayName, int converted, int total)
        {
            if (converted < 0) converted = 0;
            if (total < 0) total = 0;
            if (converted > total) converted = total;
            return string.Concat(displayName, " (", converted.ToString(), "/", total.ToString(), ")");
        }

        public static (bool convert, bool restore, bool install, bool backup, bool convertDisabled, bool restoreDisabled) DecideActions(
            bool hasTextures,
            int converted,
            int total,
            bool hasAnyBackup,
            bool hasPmpBackup,
            bool isOrphan,
            bool excluded,
            bool automaticMode)
        {
            var convertVisible = hasTextures;
            var convertDisabled = excluded || (total <= 0) || (converted >= total);
            var restoreVisible = hasTextures || hasAnyBackup;
            var restoreDisabled = !hasAnyBackup || excluded || isOrphan || (automaticMode && !isOrphan);
            var installVisible = !hasTextures && hasAnyBackup && hasPmpBackup && isOrphan;
            var backupVisible = !hasTextures && !hasAnyBackup;
            return (convertVisible, restoreVisible, installVisible, backupVisible, convertDisabled, restoreDisabled);
        }
    }
}
