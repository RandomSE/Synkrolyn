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

    [Fact]
    public void StrykerJob_restoresTheSolution_beforeMutation()
    {
        string yaml = File.ReadAllText(Path.Combine(FindRepoRoot(), ".github", "workflows", "ci.yml"));
        int tools = yaml.IndexOf("dotnet tool restore", StringComparison.Ordinal);
        int restore = yaml.IndexOf("dotnet restore Synkrolyn.slnx", StringComparison.Ordinal);
        int stryker = yaml.IndexOf("dotnet stryker", StringComparison.Ordinal);
        Assert.True(tools >= 0 && restore > tools && stryker > restore);
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
}
