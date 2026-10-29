namespace Synkrolyn.Tests.Sim;

/// <summary>Small histories for the Wing-Gong checker. These fail until the checker exists.</summary>
public sealed class LinearizabilityCheckerTests
{
    [Fact]
    public void PutThenMatchingRead_isLinearizable()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Ok),
                Op(20, 30, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void UnknownPut_thenMatchingRead_isLinearizable()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Unknown),
                Op(20, 30, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void PendingPut_canSitInsideAConcurrentReadCluster()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Unknown),
                Op(20, 20, "k0", put: false, "", LinEffect.Ok),
                Op(20, 20, "k0", put: false, "v1", LinEffect.Ok),
                Op(20, 20, "k0", put: false, "", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void UnknownPut_canCommitAfterAReadOfTheOldValue()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v0", LinEffect.Ok),
                Op(20, 30, "k0", put: true, "v1", LinEffect.Unknown),
                Op(40, 50, "k0", put: false, "v0", LinEffect.Ok),
                Op(60, 70, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void UnknownPut_overlappingAbsentRead_canCommitBeforeLaterRead()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 0, "k0", put: true, "v1", LinEffect.Unknown),
                Op(0, 0, "k0", put: false, "", LinEffect.Ok),
                Op(0, 0, "k0", put: true, "v1", LinEffect.Unknown),
                Op(5, 5, "k0", put: false, "", LinEffect.Ok),
                Op(5, 5, "k0", put: false, "", LinEffect.Ok),
                Op(10, 10, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void UnknownPut_thenAbsentRead_isLinearizable()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Unknown),
                Op(20, 30, "k0", put: false, "", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void FailedPut_thenAbsentRead_isLinearizable()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Fail),
                Op(20, 30, "k0", put: false, "", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void FailedPut_thenThatValue_isViolation()
    {
        Assert.Equal(
            LinVerdict.Violation,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Fail),
                Op(20, 30, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void StaleRead_afterNewerWrite_isViolation()
    {
        Assert.Equal(
            LinVerdict.Violation,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Ok),
                Op(20, 30, "k0", put: true, "v2", LinEffect.Ok),
                Op(40, 50, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void LostWrite_readAbsentAfterOkPut_isViolation()
    {
        Assert.Equal(
            LinVerdict.Violation,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Ok),
                Op(20, 30, "k0", put: false, "", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void ConcurrentRead_ofTheEarlierValue_isLinearizable()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "v1", LinEffect.Ok),
                Op(20, 40, "k0", put: true, "v2", LinEffect.Ok),
                Op(30, 50, "k0", put: false, "v1", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void Keys_areIndependent()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.Check(
            [
                Op(0, 10, "k0", put: true, "a", LinEffect.Ok),
                Op(0, 10, "k1", put: true, "b", LinEffect.Ok),
                Op(20, 30, "k0", put: false, "a", LinEffect.Ok),
                Op(20, 30, "k1", put: false, "b", LinEffect.Ok),
            ]));
    }

    [Fact]
    public void SequentialPuts_fitASmallBudget()
    {
        var ops = new LinOp[30];
        for (int i = 0; i < ops.Length; i++)
        {
            long time = i * 10L;
            ops[i] = Op(time, time + 1, "k0", put: true, "v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), LinEffect.Ok);
        }

        Assert.Equal(LinVerdict.Linearizable, LinearizabilityChecker.Check(ops, cacheBudget: 1));
    }

    [Fact]
    public void OversizedConcurrentCluster_isUnknown()
    {
        var ops = new LinOp[129];
        for (int i = 0; i < ops.Length; i++)
        {
            ops[i] = Op(0, 1, "k0", put: true, "v", LinEffect.Ok);
        }

        Assert.Equal(LinVerdict.Unknown, LinearizabilityChecker.Check(ops));
    }

    [Fact]
    public void BudgetExhausted_isUnknown()
    {
        var ops = new LinOp[8];
        for (int i = 0; i < ops.Length; i++)
        {
            ops[i] = Op(0, 100, "k0", put: true, "v" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), LinEffect.Ok);
        }

        Assert.Equal(LinVerdict.Unknown, LinearizabilityChecker.Check(ops, cacheBudget: 0));
    }

    [Fact]
    public void History_putThenRead_isLinearizable()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.CheckHistory(
            [
                new ClientHistoryEntry(0, 0, "put", "invoke", "k0", "v1"),
                new ClientHistoryEntry(5, 0, "register", "invoke", "", ""),
                new ClientHistoryEntry(6, 0, "register", "complete", "", "4"),
                new ClientHistoryEntry(10, 0, "put", "complete", "k0", "3"),
                new ClientHistoryEntry(20, 0, "get", "invoke", "k0", ""),
                new ClientHistoryEntry(30, 0, "get", "complete", "k0", "v1"),
            ]));
    }

    [Fact]
    public void History_staleRead_isViolation()
    {
        Assert.Equal(
            LinVerdict.Violation,
            LinearizabilityChecker.CheckHistory(
            [
                new ClientHistoryEntry(0, 0, "put", "invoke", "k0", "v1"),
                new ClientHistoryEntry(10, 0, "put", "complete", "k0", "1"),
                new ClientHistoryEntry(20, 0, "put", "invoke", "k0", "v2"),
                new ClientHistoryEntry(30, 0, "put", "complete", "k0", "2"),
                new ClientHistoryEntry(40, 1, "get", "invoke", "k0", ""),
                new ClientHistoryEntry(50, 1, "get", "complete", "k0", "v1"),
            ]));
    }

    [Fact]
    public void History_lostWrite_isViolation()
    {
        Assert.Equal(
            LinVerdict.Violation,
            LinearizabilityChecker.CheckHistory(
            [
                new ClientHistoryEntry(0, 0, "put", "invoke", "k0", "v1"),
                new ClientHistoryEntry(10, 0, "put", "complete", "k0", "4"),
                new ClientHistoryEntry(20, 0, "get", "invoke", "k0", ""),
                new ClientHistoryEntry(30, 0, "get", "complete", "k0", "absent"),
            ]));
    }

    [Fact]
    public void History_sessionExpired_isNotAWrite()
    {
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.CheckHistory(
            [
                new ClientHistoryEntry(0, 0, "put", "invoke", "k0", "v1"),
                new ClientHistoryEntry(10, 0, "put", "complete", "k0", "SessionExpired"),
                new ClientHistoryEntry(20, 0, "get", "invoke", "k0", ""),
                new ClientHistoryEntry(30, 0, "get", "complete", "k0", "null"),
            ]));
    }

    [Fact]
    public void History_indeterminatePut_branches()
    {
        ClientHistoryEntry[] prefix =
        [
            new(0, 0, "put", "invoke", "k0", "v1"),
            new(10, 0, "put", "complete", "k0", "null"),
        ];
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.CheckHistory(
            [
                ..prefix,
                new ClientHistoryEntry(20, 0, "get", "invoke", "k0", ""),
                new ClientHistoryEntry(30, 0, "get", "complete", "k0", "v1"),
            ]));
        Assert.Equal(
            LinVerdict.Linearizable,
            LinearizabilityChecker.CheckHistory(
            [
                ..prefix,
                new ClientHistoryEntry(20, 0, "get", "invoke", "k0", ""),
                new ClientHistoryEntry(30, 0, "get", "complete", "k0", "absent"),
            ]));
    }

    private static LinOp Op(long invoke, long response, string key, bool put, string value, LinEffect effect) =>
        new(invoke, response, key, put, value, effect);
}
