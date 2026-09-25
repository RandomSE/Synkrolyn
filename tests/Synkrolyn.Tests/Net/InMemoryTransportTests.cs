using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Tests.Net;

public class InMemoryTransportTests
{
    private readonly FakeClock _clock = new();
    private readonly List<Envelope> _a = [];
    private readonly List<Envelope> _b = [];
    private readonly InMemoryTransport _transport;

    public InMemoryTransportTests()
    {
        _transport = new InMemoryTransport(_clock);
        _transport.Register("a", _a.Add);
        _transport.Register("b", _b.Add);
    }

    [Fact]
    public void Send_isFifo_andDropNextSilencesOne()
    {
        _transport.Send("a", "b", "1");
        _transport.Send("a", "b", "2");
        Assert.Equal(["1", "2"], _b.Select(e => e.Payload).ToArray());
        _transport.DropNext("a", "b");
        _transport.Send("a", "b", "lost");
        _transport.Send("a", "b", "kept");
        Assert.Equal("kept", _b[^1].Payload);
        Assert.Equal(3, _b.Count);
    }

    [Fact]
    public void Partition_dropsBothDirections_untilHeal()
    {
        _transport.PartitionBidirectional("a", "b");
        _transport.Send("a", "b", "ab");
        _transport.Send("b", "a", "ba");
        Assert.Empty(_a);
        Assert.Empty(_b);
        _transport.HealBidirectional("a", "b");
        _transport.Send("a", "b", "ok");
        Assert.Equal("ok", Assert.Single(_b).Payload);
    }

    [Fact]
    public void DelayNext_holdsZeroDurationSendUntilClockAdvances_andCanReorder()
    {
        _transport.DelayNext("a", "b", TimeSpan.FromMilliseconds(10));
        _transport.DelayNext("a", "b", TimeSpan.FromMilliseconds(5));
        _transport.Send("a", "b", "first");
        _transport.Send("a", "b", "second");
        Assert.Empty(_b);
        _clock.Advance(5);
        Assert.Equal("second", Assert.Single(_b).Payload);
        _clock.Advance(5);
        Assert.Equal(["second", "first"], _b.Select(e => e.Payload).ToArray());
    }
}
