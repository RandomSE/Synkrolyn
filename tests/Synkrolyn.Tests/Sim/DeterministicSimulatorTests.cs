using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Sim;

/// <summary>Seeded cluster simulation. Every seed's history is checked for linearizability.</summary>
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
    public void Run_reportsConfiguredDepthAndFaultWindow()
    {
        SimResult result = DeterministicSimulator.Run(1, steps: 100);
        Assert.Equal(100, result.Steps);
        Assert.Equal(80, result.FaultSteps);
    }

    [Fact]
    public void Crash_dropsOrTearsUnflushedSuffix_andKeepsFlushedPrefix()
    {
        var dropped = new SimDurableLog();
        dropped.Append(new LogEntry(1, 1, [1]));
        dropped.Force();
        dropped.Append(new LogEntry(2, 1, [2]));
        dropped.Append(new LogEntry(3, 1, [3]));
        dropped.LoseUnflushed(0);
        Assert.Equal(1, dropped.LastIndex);
        Assert.Equal(1, dropped.Read(1).Command[0]);

        var torn = new SimDurableLog();
        torn.Append(new LogEntry(1, 1, [1]));
        torn.Force();
        torn.Append(new LogEntry(2, 1, [2]));
        torn.Append(new LogEntry(3, 1, [3]));
        torn.LoseUnflushed(1);
        Assert.Equal(2, torn.LastIndex);
        Assert.Equal(1, torn.Read(1).Command[0]);
        Assert.Equal(2, torn.Read(2).Command[0]);
        Assert.Throws<KeyNotFoundException>(() => torn.Read(3));
    }

    [Fact]
    public void Crash_losesDeferredVote_andKeepsFlushedTerm()
    {
        var state = new SimDurableState();
        state.SetCurrentTerm(2);
        state.DeferFlush();
        state.RecordVote("n1");
        Assert.Equal("n1", state.VotedFor);
        state.LoseUnflushed();
        Assert.Null(state.VotedFor);
        Assert.Equal(2, state.CurrentTerm);
        state.RecordVote("n2");
        state.LoseUnflushed();
        Assert.Equal("n2", state.VotedFor);
        Assert.Equal(2, state.CurrentTerm);
    }

    [Fact]
    public void HealedCluster_electsAndCommitsWithinBound()
    {
        SimResult result = DeterministicSimulator.Run(11, steps: 500);
        Assert.True(result.LeaderElected);
        Assert.True(result.WriteCommitted);
        Assert.True(result.StableLeader);
        Assert.True(result.CommitsAligned);
        Assert.True(result.PendingSettled);
        Assert.True(result.ReplicasEqual);
        Assert.Equal(LinVerdict.Linearizable, LinearizabilityChecker.CheckHistory(result.History));
    }

    [Fact]
    public void PushSeeds_holdInvariants()
    {
        int count = DeterministicSimulator.CiSeeds();
        int steps = DeterministicSimulator.CiSteps();
        for (int seed = 1; seed <= count; seed++)
        {
            try
            {
                SimResult result = DeterministicSimulator.Run(seed, steps: steps);
                Assert.True(result.LeaderElected, "leader");
                Assert.True(result.WriteCommitted, "write");
                Assert.True(result.StableLeader, "stable");
                Assert.True(result.CommitsAligned, "commit");
                Assert.True(result.PendingSettled, "pending");
                Assert.True(result.ReplicasEqual, "replicas");
                LinVerdict verdict = LinearizabilityChecker.CheckHistory(result.History);
                Assert.True(verdict == LinVerdict.Linearizable, verdict + " " + LinearizabilityChecker.Conflict(result.History));
            }
            catch (Exception ex)
            {
                throw new Xunit.Sdk.XunitException(DeterministicSimulator.ReproLine(seed) + " " + ex.Message, ex);
            }
        }
    }

    [Fact]
    public void StaleLocalRead_isViolationOnSomeSeed()
    {
        for (int seed = 1; seed <= 40; seed++)
        {
            SimResult result = DeterministicSimulator.Run(seed, steps: 400, staleReads: true);
            if (LinearizabilityChecker.CheckHistory(result.History) == LinVerdict.Violation)
            {
                return;
            }
        }

        Assert.Fail("stale local reads were linearizable on seeds 1..40");
    }

    [Fact]
    public void ClockJumpAndPause_refusesStaleLease_andIsLinearizable()
    {
        for (int seed = 1; seed <= 5; seed++)
        {
            SimResult result = DeterministicSimulator.RunClockAndPause(seed);
            Assert.True(result.StaleLeaseRefused, "seed " + seed);
            Assert.True(result.LeaderElected, "seed " + seed);
            LinVerdict verdict = LinearizabilityChecker.CheckHistory(result.History);
            Assert.True(
                verdict == LinVerdict.Linearizable,
                verdict + " " + LinearizabilityChecker.Conflict(result.History) + " seed " + seed);
        }
    }

    [Fact]
    public void FullClusterCrash_keepsCommittedPrefix_andIsLinearizable()
    {
        for (int seed = 1; seed <= 5; seed++)
        {
            SimResult result = DeterministicSimulator.RunFullClusterCrash(seed);
            Assert.True(result.CommittedPrefixKept, "seed " + seed);
            Assert.True(result.LeaderElected, "seed " + seed);
            LinVerdict verdict = LinearizabilityChecker.CheckHistory(result.History);
            Assert.True(
                verdict == LinVerdict.Linearizable,
                verdict + " " + LinearizabilityChecker.Conflict(result.History) + " seed " + seed);
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

        SimResult result = DeterministicSimulator.Run(seed, steps: DeterministicSimulator.CiSteps());
        Assert.True(result.LeaderElected, "leader");
        Assert.True(result.WriteCommitted, "write");
        Assert.True(result.StableLeader, "stable");
        Assert.True(result.CommitsAligned, "commit");
        Assert.True(result.PendingSettled, "pending");
        Assert.True(result.ReplicasEqual, "replicas");
        LinVerdict verdict = LinearizabilityChecker.CheckHistory(result.History);
        Assert.True(verdict == LinVerdict.Linearizable, verdict + " " + LinearizabilityChecker.Conflict(result.History));
    }
}
