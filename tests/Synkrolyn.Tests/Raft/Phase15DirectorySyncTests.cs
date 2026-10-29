using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Raft;

/// <summary>A failed directory fsync must not ack the append or publish it as committed.</summary>
public class Phase15DirectorySyncTests
{
    [Fact]
    public void DirectoryFsyncFailure_doesNotAckTheWrite_orReportItCommitted()
    {
        string dir = Directory.CreateTempSubdirectory("synkrolyn-dirsync-").FullName;
        string full = Path.GetFullPath(dir);
        var clock = new FakeClock();
        var transport = new InMemoryTransport(clock);
        AppendEntriesResponse? ack = null;
        transport.Register("n1", envelope =>
        {
            if (envelope.Payload is AppendEntriesResponse response)
            {
                ack = response;
            }
        });

        var log = new FileRaftLog(dir);
        var follower = new RaftNode(
            "n2",
            ["n1"],
            clock,
            transport,
            new InMemoryPersistentState(),
            log,
            TimeSpan.FromMilliseconds(10_000),
            TimeSpan.FromMilliseconds(10_000),
            IStateMachine.NoOp());
        follower.SetElectionJitter(static timeout => timeout);
        transport.Register("n2", follower.Receive);
        follower.Start();
        follower.Drain();

        FilePersistentState.DirectorySyncFault = path =>
            Path.GetFullPath(path) == full;
        try
        {
            follower.Receive(new Envelope(
                "n1",
                "n2",
                new AppendEntries(
                    1,
                    "n1",
                    0,
                    0,
                    [new LogEntry(1, 1, [7])],
                    1,
                    1)));
            Exception? error = Record.Exception(() => follower.Drain());
            if (error is not null)
            {
                Assert.IsType<IOException>(error);
            }

            Assert.True(ack is not { Success: true }, "acked a write whose directory fsync failed");
            Assert.Equal(0, follower.CommitIndex);
            Assert.Equal(0, log.DurableIndex);
        }
        finally
        {
            FilePersistentState.DirectorySyncFault = null;
            log.Dispose();
            Directory.Delete(dir, true);
        }
    }
}
