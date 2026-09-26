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
