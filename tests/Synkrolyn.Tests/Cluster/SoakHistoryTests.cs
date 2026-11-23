using Synkrolyn.Tests.Sim;

namespace Synkrolyn.Tests.Cluster;

/// <summary>
/// The Sunday soak failed at slice 54, seed 2096267684, after about 57s.
/// That slice read k2=a-10 and k3=b-11, which no put in the slice had written.
/// </summary>
public sealed class SoakHistoryTests
{
    [Fact]
    public void SliceThatDidNotRewriteAKey_stillReadsTheEarlierValue()
    {
        LinOp[] traffic =
        [
            Op(58674, 58763, "k0", put: true, "b-0", LinEffect.Ok),
            Op(58674, 58786, "k0", put: true, "a-0", LinEffect.Ok),
            Op(58763, 58879, "k1", put: true, "b-1", LinEffect.Ok),
            Op(58879, 59357, "k2", put: true, "b-2", LinEffect.Fail),
            Op(58786, 59357, "k1", put: true, "a-1", LinEffect.Unknown),
            Op(59740, 59741, "k0", put: false, "a-0", LinEffect.Ok),
            Op(59741, 59741, "k1", put: false, "a-1", LinEffect.Ok),
            Op(59741, 59741, "k2", put: false, "a-10", LinEffect.Ok),
            Op(59741, 59742, "k3", put: false, "b-11", LinEffect.Ok),
        ];
        Assert.Equal(LinVerdict.Violation, LinearizabilityChecker.Check(traffic));

        var withPrior = new List<LinOp>();
        SoakHistory.NoteCommitted(withPrior, 58000, "k2", "a-10");
        SoakHistory.NoteCommitted(withPrior, 58000, "k3", "b-11");
        withPrior.AddRange(traffic);
        Assert.Equal(LinVerdict.Linearizable, LinearizabilityChecker.Check(withPrior));
    }

    private static LinOp Op(long invoke, long response, string key, bool put, string value, LinEffect effect) =>
        new(invoke, response, key, put, value, effect);
}
