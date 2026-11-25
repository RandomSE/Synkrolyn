namespace Synkrolyn.Tests.Raft;

public sealed class TlaSpecTests
{
    [Fact]
    public void BecomeLeader_appendsCurrentTerm_andCommitCountsForcedIndex()
    {
        string root = FindRepoRoot();
        string spec = File.ReadAllText(Path.Combine(root, "tla", "Synkrolyn.tla"));
        int start = spec.IndexOf("BecomeLeader(s) ==", StringComparison.Ordinal);
        Assert.True(start >= 0);
        int next = spec.IndexOf("ClientAppend(s) ==", start, StringComparison.Ordinal);
        Assert.True(next > start);
        string body = spec[start..next];
        Assert.Contains("Append(", body, StringComparison.Ordinal);
        Assert.Contains("currentTerm[s]", body, StringComparison.Ordinal);
        Assert.Contains("forcedIndex[s] >= n", spec, StringComparison.Ordinal);
        Assert.Contains("Force(s) ==", spec, StringComparison.Ordinal);
    }

    [Fact]
    public void TlcJob_downloadsTla2tools_withoutAFailingChecksumPin()
    {
        string yaml = File.ReadAllText(Path.Combine(FindRepoRoot(), ".github", "workflows", "ci.yml"));
        Assert.Contains("releases/download/v1.8.0/tla2tools.jar", yaml, StringComparison.Ordinal);
        Assert.DoesNotContain("sha256sum -c", yaml, StringComparison.Ordinal);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "Synkrolyn.slnx")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        return Directory.GetCurrentDirectory();
    }

    [Fact]
    public void PagesDeploy_waitsUntilTheUploadJobHasFinished()
    {
        string yaml = File.ReadAllText(Path.Combine(FindRepoRoot(), ".github", "workflows", "pages.yml"));
        int upload = yaml.IndexOf("actions/upload-pages-artifact@", StringComparison.Ordinal);
        int needs = upload < 0 ? -1 : yaml.IndexOf("needs:", upload, StringComparison.Ordinal);
        int deploy = needs < 0 ? -1 : yaml.IndexOf("actions/deploy-pages@", needs, StringComparison.Ordinal);
        Assert.True(upload >= 0 && needs > upload && deploy > needs);
    }
}
