using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace ShrinkU.UI;

public sealed partial class ConversionUI
{
    private sealed class TableCatNode
    {
        public string Name { get; }
        public Dictionary<string, TableCatNode> Children { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<string> Mods { get; } = new();
        public TableCatNode(string name) => Name = name;
    }

    private enum FlatRowKind { Folder, Mod, File }
    private sealed class FlatRow
    {
        public FlatRowKind Kind;
        public TableCatNode Node = null!;
        public string FolderPath = string.Empty;
        public string Mod = string.Empty;
        public string File = string.Empty;
        public int Depth;
    }

    private TableCatNode BuildTableCategoryTree(IEnumerable<string> mods)
    {
        var root = new TableCatNode("/");
        foreach (var mod in mods)
        {
            if (!_modPaths.TryGetValue(mod, out var fullPath) || string.IsNullOrWhiteSpace(fullPath))
            {
                if (!root.Children.TryGetValue("(Uncategorized)", out var unc))
                    root.Children["(Uncategorized)"] = unc = new TableCatNode("(Uncategorized)");
                unc.Mods.Add(mod);
                continue;
            }

            string folderOnly;
            try
            {
                var fpNorm = fullPath.Replace('\\', '/');
                if (_modDisplayNames.TryGetValue(mod, out var dn) && !string.IsNullOrWhiteSpace(dn))
                {
                    var dnNorm = dn.Replace('\\', '/');
                    if (fpNorm.EndsWith(dnNorm, StringComparison.Ordinal))
                        folderOnly = fpNorm.Substring(0, fpNorm.Length - dnNorm.Length).TrimEnd('/');
                    else
                        folderOnly = fpNorm;
                }
                else
                {
                    folderOnly = fpNorm;
                }
            }
            catch
            {
                folderOnly = fullPath.Replace('\\', '/');
            }

            var parts = folderOnly.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length == 0)
            {
                if (!root.Children.TryGetValue("(Uncategorized)", out var unc2))
                    root.Children["(Uncategorized)"] = unc2 = new TableCatNode("(Uncategorized)");
                unc2.Mods.Add(mod);
                continue;
            }

            var cursor = root;
            for (var i = 0; i < parts.Length; i++)
            {
                var seg = parts[i];
                if (!cursor.Children.TryGetValue(seg, out var next))
                {
                    next = new TableCatNode(seg);
                    cursor.Children[seg] = next;
                }
                cursor = next;
            }
            cursor.Mods.Add(mod);
        }
        return root;
    }

    private IEnumerable<KeyValuePair<string, TableCatNode>> OrderedChildrenPairs(TableCatNode node)
    {
        return node.Children
            .OrderBy(kv => kv.Key.Equals("(Uncategorized)", StringComparison.OrdinalIgnoreCase) ? 1 : 0)
            .ThenBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase);
    }

    private void CollectFolderPaths(TableCatNode node, string prefix, List<string> output)
    {
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var full = string.IsNullOrEmpty(prefix) ? name : $"{prefix}/{name}";
            output.Add(full);
            CollectFolderPaths(child, full, output);
        }
    }

    private bool HasSelectableFiles(TableCatNode node, Dictionary<string, List<string>> visibleByMod)
    {
        foreach (var mod in node.Mods)
        {
            if (visibleByMod.TryGetValue(mod, out var files) && files != null && files.Count > 0)
                return true;
        }
        foreach (var child in node.Children.Values)
        {
            if (HasSelectableFiles(child, visibleByMod))
                return true;
        }
        return false;
    }

    private void BuildFlatRows(TableCatNode node, Dictionary<string, List<string>> visibleByMod, string pathPrefix, int depth)
    {
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            _flatRows.Add(new FlatRow { Kind = FlatRowKind.Folder, Node = child, FolderPath = fullPath, Depth = depth });
            var catOpen = _filterPenumbraUsedOnly || _expandedFolders.Contains(fullPath);
            if (!catOpen)
                continue;
            BuildFlatRows(child, visibleByMod, fullPath, depth + 1);
            foreach (var mod in child.Mods)
            {
                if (!visibleByMod.ContainsKey(mod))
                    continue;
                _flatRows.Add(new FlatRow { Kind = FlatRowKind.Mod, Node = child, Mod = mod, Depth = depth + 1 });
                if (_configService.Current.ShowModFilesInOverview && _expandedMods.Contains(mod))
                {
                    var files = visibleByMod[mod];
                    if (files != null)
                    {
                        for (int i = 0; i < files.Count; i++)
                            _flatRows.Add(new FlatRow { Kind = FlatRowKind.File, Node = child, Mod = mod, File = files[i], Depth = depth + 2 });
                    }
                }
            }
        }
    }

    private void BuildFolderCountsCache(TableCatNode node, Dictionary<string, List<string>> visibleByMod, string pathPrefix)
    {
        foreach (var (name, child) in OrderedChildrenPairs(node))
        {
            var fullPath = string.IsNullOrEmpty(pathPrefix) ? name : $"{pathPrefix}/{name}";
            int modsTotal = 0, modsConverted = 0, texturesTotal = 0, texturesConverted = 0;
            var stack = new Stack<TableCatNode>();
            stack.Push(child);
            while (stack.Count > 0)
            {
                var cur = stack.Pop();
                modsTotal += cur.Mods.Count;
                foreach (var m in cur.Mods)
                {
                    if (_scannedByMod.TryGetValue(m, out var files) && files != null)
                        texturesTotal += files.Count;
                    if (_cachedPerModSavings.TryGetValue(m, out var s) && s != null && s.ComparedFiles > 0)
                        texturesConverted += s.ComparedFiles;
                    else
                    {
                        var snap = _modStateService.Snapshot();
                        if (snap.TryGetValue(m, out var st) && st != null && st.ComparedFiles > 0)
                            texturesConverted += st.ComparedFiles;
                    }
                    if (GetOrQueryModBackup(m))
                        modsConverted++;
                }
                foreach (var ch in cur.Children.Values)
                    stack.Push(ch);
            }
            _folderCountsCache[fullPath] = (modsTotal, modsConverted, texturesTotal, Math.Min(texturesConverted, texturesTotal));
            BuildFolderCountsCache(child, visibleByMod, fullPath);
        }
    }

    private int CountModsRecursive(TableCatNode node)
    {
        var count = node.Mods.Count;
        foreach (var child in node.Children.Values)
            count += CountModsRecursive(child);
        return count;
    }

    private int CountConvertedModsRecursive(TableCatNode node)
    {
        var count = 0;
        foreach (var mod in node.Mods)
        {
            var hasBackup = GetOrQueryModBackup(mod);
            if (hasBackup)
                count++;
        }
        foreach (var child in node.Children.Values)
            count += CountConvertedModsRecursive(child);
        return count;
    }

    private int CountTexturesRecursive(TableCatNode node)
    {
        var count = 0;
        foreach (var mod in node.Mods)
        {
            if (_scannedByMod.TryGetValue(mod, out var files) && files != null)
                count += files.Count;
        }
        foreach (var child in node.Children.Values)
            count += CountTexturesRecursive(child);
        return count;
    }

    private int CountConvertedTexturesRecursive(TableCatNode node)
    {
        var count = 0;
        foreach (var mod in node.Mods)
        {
            var hasBackup = GetOrQueryModBackup(mod);
            if (!hasBackup)
                continue;
            if (_scannedByMod.TryGetValue(mod, out var files) && files != null && files.Count > 0)
            {
                _ = GetOrQueryModOriginalTotal(mod);
                if (_modPaths.TryGetValue(mod, out var modRoot) && !string.IsNullOrWhiteSpace(modRoot))
                {
                    if (_cachedPerModOriginalSizes.TryGetValue(mod, out var map) && map != null && map.Count > 0)
                    {
                        foreach (var f in files)
                        {
                            try
                            {
                                var rel = Path.GetRelativePath(modRoot, f).Replace('\\', '/');
                                if (map.ContainsKey(rel)) count++;
                            }
                            catch { }
                        }
                    }
                    else if (_cachedPerModSavings.TryGetValue(mod, out var s) && s != null && s.ComparedFiles > 0)
                    {
                        count += Math.Min(s.ComparedFiles, files.Count);
                    }
                }
            }
        }
        foreach (var child in node.Children.Values)
            count += CountConvertedTexturesRecursive(child);
        return count;
    }

    private List<string> CollectFilesRecursive(TableCatNode node, Dictionary<string, List<string>> visibleByMod)
    {
        var files = new List<string>();
        foreach (var mod in node.Mods)
        {
            if (visibleByMod.TryGetValue(mod, out var modFiles) && modFiles != null && modFiles.Count > 0)
            {
                for (int i = 0; i < modFiles.Count; i++)
                    files.Add(modFiles[i]);
            }
        }
        foreach (var child in node.Children.Values)
        {
            var sub = CollectFilesRecursive(child, visibleByMod);
            if (sub != null && sub.Count > 0)
            {
                for (int i = 0; i < sub.Count; i++)
                    files.Add(sub[i]);
            }
        }
        return files;
    }
}