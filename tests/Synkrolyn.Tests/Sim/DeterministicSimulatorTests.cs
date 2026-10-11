namespace Synkrolyn.Tests.Sim;

/// <summary>Seeded cluster simulation. Phase 6 reads <see cref="SimResult.History"/> and does not live here.</summary>
public sealed class DeterministicSimulatorTests
{
    [Fact]
    public void SameSeed_reproducesTraceHash()
    {
        SimResult first = DeterministicSimulator.Run(7, steps: 40);
        SimResult second = DeterministicSimulator.Run(7, steps: 40);
        Assert.Equal(first.TraceHash, second.TraceHash);
        Assert.NotEqual(first.TraceHash, DeterministicSimulator.Run(8, steps: 40).TraceHash);
    }

    [Fact]
    public void ClientHistory_recordsInvokeAndCompleteWithTime()
    {
        SimResult result = DeterministicSimulator.Run(3, steps: 40);
        Assert.Contains(result.History, static entry => entry.Phase == "invoke");
        Assert.Contains(result.History, static entry => entry.Phase == "complete");
        long previous = -1;
        foreach (ClientHistoryEntry entry in result.History)
        {
            Assert.True(entry.TimeMillis >= previous);
            previous = entry.TimeMillis;
        }
    }

    [Fact]
    public void HealedCluster_electsAndCommitsWithinBound()
    {
        SimResult result = DeterministicSimulator.Run(11, steps: 80);
        Assert.True(result.LeaderElected);
        Assert.True(result.WriteCommitted);
    }

    [Fact]
    public void PushSeeds_holdInvariants()
    {
        int count = ReadCount("SIM_COUNT", 200);
        for (int seed = 1; seed <= count; seed++)
        {
            try
            {
                SimResult result = DeterministicSimulator.Run(seed, steps: 60);
                Assert.True(result.LeaderElected);
                Assert.True(result.WriteCommitted);
            }
            catch (Exception ex)
            {
                throw new Xunit.Sdk.XunitException(DeterministicSimulator.ReproLine(seed) + " " + ex.Message, ex);
            }
        }
    }

    [Fact]
    public void SimSeed_replaysEnvironmentSeed()
    {
        string? raw = Environment.GetEnvironmentVariable("SIM_SEED");
        if (!int.TryParse(raw, out int seed))
        {
            return;
        }

        SimResult result = DeterministicSimulator.Run(seed, steps: 60);
        Assert.True(result.LeaderElected);
        Assert.True(result.WriteCommitted);
    }

    private static int ReadCount(string name, int fallback)
    {
        string? raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out int value) && value > 0 ? value : fallback;
    }
}
