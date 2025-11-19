using Dalamud.Bindings.ImGui;
using Dalamud.Interface.Windowing;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using Dalamud.Interface;
using Dalamud.Interface.Utility.Raii;
using Dalamud.Interface.ManagedFontAtlas;
using Dalamud.Plugin;
using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using ShrinkU.Services;
using System.Numerics;
using System.Reflection;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;

namespace ShrinkU.UI;

public sealed class ReleaseChangelogUI : Window
{
    private readonly IDalamudPluginInterface _pluginInterface;
    private readonly ILogger _logger;
    private readonly ShrinkUConfigService _configService;
    private readonly ChangelogService _changelogService;
    private volatile bool _loading = true;
    private List<ReleaseChangelogViewEntry> _entries = new();
    private string _currentVersion = string.Empty;
    private bool _showAll = false;
    private string _defaultExpandedVersion = string.Empty;
    private readonly IFontHandle _titleFont;

    public ReleaseChangelogUI(IDalamudPluginInterface pluginInterface, ILogger logger, ShrinkUConfigService configService, ChangelogService changelogService)
        : base("ShrinkU Release Notes###ShrinkUReleaseChangelogUI")
    {
        _pluginInterface = pluginInterface;
        _logger = logger;
        _configService = configService;
        _changelogService = changelogService;


        SizeConstraints = new WindowSizeConstraints
        {
            MinimumSize = new Vector2(600, 507),
            MaximumSize = new Vector2(600, 507),
        };
        Flags |= ImGuiWindowFlags.NoResize | ImGuiWindowFlags.NoCollapse;

        try
        {
            var asmVer = typeof(ReleaseChangelogUI).Assembly?.GetName()?.Version?.ToString() ?? string.Empty;
            _currentVersion = asmVer;
        }
        catch { }

        _titleFont = _pluginInterface.UiBuilder.FontAtlas.NewDelegateFontHandle(e =>
        {
            e.OnPreBuild(tk => tk.AddDalamudAssetFont(Dalamud.DalamudAsset.NotoSansJpMedium, new()
            {
                SizePx = 35
            }));
        });

        _ = LoadAsync();
    }

    private async Task LoadAsync()
    {
        try
        {
            var list = await _changelogService.GetChangelogEntriesAsync().ConfigureAwait(false);
            var all = (list ?? new List<ReleaseChangelogViewEntry>())
                .OrderByDescending(e => ParseVersionSafe(e.Version))
                .ToList();
            var cur = ParseVersionSafe(_currentVersion);
            _entries = all.Where(e => ParseVersionSafe(e.Version) <= cur).ToList();
            var exact = _entries.FirstOrDefault(e => ParseVersionSafe(e.Version) == cur)?.Version;
            _defaultExpandedVersion = !string.IsNullOrEmpty(exact) ? exact : _entries.FirstOrDefault()?.Version ?? string.Empty;
        }
        catch { }
        finally
        {
            _loading = false;
        }
    }

    private static Version ParseVersionSafe(string? v)
    {
        if (string.IsNullOrWhiteSpace(v)) return new Version(0,0,0,0);
        try
        {
            return Version.Parse(v);
        }
        catch
        {
            var parts = v!.Split('.', StringSplitOptions.RemoveEmptyEntries);
            int[] nums = parts.Select(p => int.TryParse(p, out var n) ? n : 0).ToArray();
            while (nums.Length < 4)
            {
                nums = nums.Concat(new[] { 0 }).ToArray();
            }
            return new Version(nums[0], nums[1], nums[2], nums[3]);
        }
    }

    public override void Draw()
    {
        // Header
        BigText("What’s New in ShrinkU");
        ImGui.Separator();

        using (var table = ImRaii.Table("ReleaseInfo", 2, ImGuiTableFlags.None))
        {
            if (table)
            {
                ImGui.TableSetupColumn("Label", ImGuiTableColumnFlags.WidthFixed, 120);
                ImGui.TableSetupColumn("Value", ImGuiTableColumnFlags.WidthStretch);

                ImGui.TableNextRow();
                ImGui.TableNextColumn();
                ImGui.Text("Version:");
                ImGui.TableNextColumn();
                ImGui.TextColored(ImGuiColors.HealerGreen, string.IsNullOrEmpty(_currentVersion) ? "Unknown" : _currentVersion);
            }
        }

        ImGui.Spacing();

        // Controls
        ImGui.Spacing();
        if (ImGui.Button(_showAll ? "Show last 5" : "Show all"))
        {
            _showAll = !_showAll;
        }
        ImGui.Spacing();

        using (var child = ImRaii.Child("ChangelogPane", new Vector2(-1, 350), true, ImGuiWindowFlags.NoNav))
        {
            if (child)
            {
                using (ImRaii.PushStyle(ImGuiStyleVar.FrameRounding, 6f))
                using (ImRaii.PushStyle(ImGuiStyleVar.FramePadding, new Vector2(4, 3)))
                using (ImRaii.PushStyle(ImGuiStyleVar.ItemSpacing, new Vector2(8, 4)))
                {
                    if (_loading)
                    {
                        ImGui.TextColored(ShrinkUColors.Accent, "Loading release notes...");
                    }
                    else if (_entries == null || _entries.Count == 0)
                    {
                        ImGui.TextColored(ShrinkUColors.Accent, "No release notes available.");
                    }
                    else
                    {
                        var list = _showAll ? _entries : _entries.Count > 5 ? _entries.GetRange(0, 5) : _entries;
                        foreach (var e in list)
                        {
                            var flags = ImGuiTreeNodeFlags.None;
                            if (!string.IsNullOrEmpty(_defaultExpandedVersion) && e.Version == _defaultExpandedVersion)
                            {
                                ImGui.SetNextItemOpen(true, ImGuiCond.Always);
                                flags |= ImGuiTreeNodeFlags.DefaultOpen;
                            }

                            var headerLabel = $"{e.Version} - {e.Title}###ch_{e.Version}";
                            var opened = ImGui.CollapsingHeader(headerLabel, flags);
                            if (opened)
                            {
                                ImGui.Dummy(new Vector2(0, 2));

                                if (!string.IsNullOrEmpty(e.Description))
                                {
                                    ImGui.PushStyleColor(ImGuiCol.Text, ShrinkUColors.ToImGuiColor(ShrinkUColors.Accent));
                                    ImGui.TextWrapped(e.Description);
                                    ImGui.PopStyleColor();
                                }

                                ImGuiHelpers.ScaledDummy(2f);

                                using (ImRaii.PushIndent(10f))
                                {
                                    foreach (var change in e.Changes)
                                    {
                                        if (change == null)
                                            continue;

                                        var trimmedMain = (change.Text ?? string.Empty).Trim();
                                        if (trimmedMain.StartsWith("- ")) trimmedMain = trimmedMain.Substring(2);
                                        if (trimmedMain.StartsWith("• ")) trimmedMain = trimmedMain.Substring(2);

                                        ImGui.Bullet();
                                        float bulletGap = ImGui.GetStyle().ItemInnerSpacing.X + ImGuiHelpers.GlobalScale * 8f;
                                        ImGui.SameLine(0, bulletGap);
                                        if (change.Sub is { Count: > 0 })
                                        {
                                            var baseColor = ImGuiColors.ParsedBlue;
                                            var textColor = new Vector4(baseColor.X, baseColor.Y, baseColor.Z, 0.90f);
                                            ImGui.PushStyleColor(ImGuiCol.Text, textColor);
                                            ImGui.TextWrapped(trimmedMain);
                                            ImGui.PopStyleColor();
                                        }
                                        else
                                        {
                                            ImGui.TextWrapped(trimmedMain);
                                        }

                                        if (change.Sub is { Count: > 0 })
                                        {
                                            ImGui.Indent(ImGuiHelpers.GlobalScale * 18f);
                                            foreach (var sub in change.Sub)
                                            {
                                                if (string.IsNullOrWhiteSpace(sub))
                                                    continue;

                                                var trimmedSub = sub.Trim();
                                                if (trimmedSub.StartsWith("- ")) trimmedSub = trimmedSub.Substring(2);
                                                if (trimmedSub.StartsWith("• ")) trimmedSub = trimmedSub.Substring(2);

                                                ImGui.Bullet();
                                                ImGui.SameLine(0, bulletGap);
                                                ImGui.TextWrapped(trimmedSub);
                                            }
                                            ImGui.Unindent(ImGuiHelpers.GlobalScale * 18f);
                                            ImGuiHelpers.ScaledDummy(3f);
                                        }
                                    }
                                }
                            }
                            ImGui.Spacing();
                        }
                    }
                }
            }
        }

        ImGui.Spacing();
        ImGui.Separator();

        // Footer actions
        if (ImGui.Button("Okay close!"))
        {
            IsOpen = false;
        }

        // Mark current plugin version as seen when the window is closed by the user
        if (!IsOpen)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(_currentVersion))
                {
                    _configService.Current.LastSeenReleaseChangelogVersion = _currentVersion;
                    _configService.Save();
                    _logger.LogDebug("Stored last-seen ShrinkU changelog version: {ver}", _currentVersion);
                }
            }
            catch { }
        }
    }

    private void BigText(string text)
    {
        using var _ = _titleFont.Push();
        ImGui.TextUnformatted(text);
    }
}
