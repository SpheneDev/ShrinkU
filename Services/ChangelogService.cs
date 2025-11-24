using Microsoft.Extensions.Logging;
using ShrinkU.Configuration;
using System.Net.Http;
using System.Text.Json;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System;

namespace ShrinkU.Services;

public sealed class ChangelogService
{
    private readonly ILogger _logger;
    private readonly HttpClient _httpClient;
    private readonly ShrinkUConfigService _configService;

    public ChangelogService(ILogger logger, HttpClient httpClient, ShrinkUConfigService configService)
    {
        _logger = logger;
        _httpClient = httpClient;
        _configService = configService;
    }

    public async Task<List<ReleaseChangelogViewEntry>> GetChangelogEntriesAsync(CancellationToken ct = default)
    {
        var url = _configService.Current.ReleaseChangelogUrl ?? "https://sphene.online/shrinku/change_log.json";
        if (url.EndsWith("changelog.json", StringComparison.OrdinalIgnoreCase))
        {
            url = "https://sphene.online/shrinku/change_log.json";
            _configService.Current.ReleaseChangelogUrl = url;
            _configService.Save();
        }
        var result = new List<ReleaseChangelogViewEntry>();
        try
        {
            _logger.LogDebug("Fetching ShrinkU changelog entries from {url}", url);
            using var resp = await _httpClient.GetAsync(url, ct).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

            using var jsonDoc = await JsonDocument.ParseAsync(stream, cancellationToken: ct).ConfigureAwait(false);
            var root = jsonDoc.RootElement;
            if (!root.TryGetProperty("changelogs", out var changelogs) || changelogs.ValueKind != JsonValueKind.Array)
            {
                _logger.LogWarning("Changelog JSON did not contain an array 'changelogs'");
                return result;
            }

            foreach (var item in changelogs.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                var entry = new ReleaseChangelogViewEntry();
                entry.Version = item.TryGetProperty("version", out var vProp) && vProp.ValueKind == JsonValueKind.String
                    ? vProp.GetString() ?? string.Empty
                    : string.Empty;
                entry.Title = item.TryGetProperty("title", out var tProp) && tProp.ValueKind == JsonValueKind.String
                    ? (tProp.GetString() ?? string.Empty).Trim()
                    : string.Empty;
                entry.Description = item.TryGetProperty("description", out var dProp) && dProp.ValueKind == JsonValueKind.String
                    ? (dProp.GetString() ?? string.Empty).Trim()
                    : string.Empty;
                entry.IsPrerelease = item.TryGetProperty("isPrerelease", out var prProp) && prProp.ValueKind == JsonValueKind.True;

                if (item.TryGetProperty("changes", out var changesProp) && changesProp.ValueKind == JsonValueKind.Array)
                {
                    foreach (var change in changesProp.EnumerateArray())
                    {
                        var view = new ReleaseChangeView();
                        if (change.ValueKind == JsonValueKind.String)
                        {
                            var line = change.GetString();
                            if (!string.IsNullOrWhiteSpace(line))
                            {
                                view.Text = line!.Trim();
                                entry.Changes.Add(view);
                            }
                        }
                        else if (change.ValueKind == JsonValueKind.Object)
                        {
                            string text = string.Empty;
                            if (change.TryGetProperty("description", out var cdProp) && cdProp.ValueKind == JsonValueKind.String)
                                text = cdProp.GetString() ?? string.Empty;

                            if (!string.IsNullOrWhiteSpace(text))
                            {
                                view.Text = text.Trim();

                                if (change.TryGetProperty("sub", out var subProp) && subProp.ValueKind == JsonValueKind.Array)
                                {
                                    foreach (var sub in subProp.EnumerateArray())
                                    {
                                        if (sub.ValueKind == JsonValueKind.String)
                                        {
                                            var s = sub.GetString();
                                            if (!string.IsNullOrWhiteSpace(s)) view.Sub.Add(s!.Trim());
                                        }
                                        else if (sub.ValueKind == JsonValueKind.Object)
                                        {
                                            if (sub.TryGetProperty("description", out var sdProp) && sdProp.ValueKind == JsonValueKind.String)
                                            {
                                                var s = sdProp.GetString();
                                                if (!string.IsNullOrWhiteSpace(s)) view.Sub.Add(s!.Trim());
                                            }
                                        }
                                    }
                                }

                                entry.Changes.Add(view);
                            }
                        }
                    }
                }

                result.Add(entry);
            }

            // Sort descending by version
            result.Sort((a, b) => ParseVersionSafe(b.Version).CompareTo(ParseVersionSafe(a.Version)));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch or parse ShrinkU changelog entries");
        }

        return result;
    }

    private static Version ParseVersionSafe(string? v)
    {
        if (string.IsNullOrWhiteSpace(v)) return new Version(0,0,0,0);
        try
        {
            var parts = v.Split('-', StringSplitOptions.RemoveEmptyEntries);
            return Version.Parse(parts[0]);
        }
        catch
        {
            return new Version(0,0,0,0);
        }
    }
}

public sealed class ReleaseChangelogViewEntry
{
    public string Version { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public List<ReleaseChangeView> Changes { get; set; } = new();
    public bool IsPrerelease { get; set; }
}

public sealed class ReleaseChangeView
{
    public string Text { get; set; } = string.Empty;
    public List<string> Sub { get; set; } = new();
}
