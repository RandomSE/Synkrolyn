using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using Synkrolyn.Raft;

namespace Synkrolyn.Bench;

/// <summary>
/// Times one linearizable ReadIndex round.
/// commit-2 exposes <c>ReadIndexSatisfied(long)</c>. The commit-1 baseline folds the
/// lease into the parameterless check, so that tree is observed through the ack set
/// BeginReadIndex fills. The wait loop does not allocate.
/// </summary>
internal static class ReadProbe
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly MethodInfo BeginInfo = typeof(RaftNode).GetMethod("BeginReadIndex", Type.EmptyTypes)
        ?? throw new InvalidOperationException("BeginReadIndex is missing");

    private static readonly MethodInfo? TicketInfo = typeof(RaftNode).GetMethod("ReadIndexSatisfied", [typeof(long)]);

    private static readonly Func<RaftNode, long, bool>? TicketSatisfied = TicketInfo?.CreateDelegate<Func<RaftNode, long, bool>>();

    private static readonly Func<RaftNode, long>? CurrentTicket = typeof(RaftNode).GetProperty("CurrentReadTicket")
        ?.GetGetMethod()?.CreateDelegate<Func<RaftNode, long>>();

    private static readonly FieldInfo? EventLock = typeof(RaftNode).GetField("_eventLock", Instance);

    private static readonly FieldInfo? PendingIndex = typeof(RaftNode).GetField("_pendingReadIndex", Instance);

    private static readonly FieldInfo? PendingTerm = typeof(RaftNode).GetField("_pendingReadTerm", Instance);

    private static readonly FieldInfo? Acks = typeof(RaftNode).GetField("_readIndexAcks", Instance);

    private static readonly FieldInfo? AppliedField = typeof(RaftNode).GetField("_lastApplied", Instance);

    private static readonly FieldInfo? RoleField = typeof(RaftNode).GetField("_role", Instance);

    public static string Kind => TicketSatisfied is not null ? "ticket" : "ack-set";

    public static void EnsureAvailable()
    {
        if (TicketSatisfied is not null)
        {
            if (CurrentTicket is null)
            {
                throw new InvalidOperationException("CurrentReadTicket is missing");
            }

            return;
        }

        if (EventLock is null || PendingIndex is null || PendingTerm is null || Acks is null || AppliedField is null || RoleField is null)
        {
            throw new InvalidOperationException("baseline ReadIndex fields are missing");
        }
    }

    public static bool TrySample(RaftNode node, TimeSpan timeout, out double micros, out string detail)
    {
        long started = Stopwatch.GetTimestamp();
        object? begun = BeginInfo.Invoke(node, null);
        if (begun is null)
        {
            micros = 0;
            detail = "BeginReadIndex returned null";
            return false;
        }

        var limit = Stopwatch.StartNew();
        if (TicketSatisfied is not null)
        {
            long ticket = CurrentTicket!(node);
            while (limit.Elapsed < timeout)
            {
                if (TicketSatisfied(node, ticket))
                {
                    micros = TicksToMicros(Stopwatch.GetTimestamp() - started);
                    detail = "";
                    return true;
                }

                if (node.Role != Role.Leader)
                {
                    micros = 0;
                    detail = "leader stepped down during ticket " + ticket.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    return false;
                }

                Thread.SpinWait(24);
            }

            micros = 0;
            detail = "ticket " + ticket.ToString(System.Globalization.CultureInfo.InvariantCulture) + " timed out";
            return false;
        }

        if (begun is not long expectIndex || expectIndex < 1)
        {
            micros = 0;
            detail = "BeginReadIndex returned " + (begun?.ToString() ?? "null");
            return false;
        }

        object gate = EventLock!.GetValue(node) ?? throw new InvalidOperationException("_eventLock is null");
        while (limit.Elapsed < timeout)
        {
            bool confirmed;
            int ackCount;
            lock (gate)
            {
                long pending = (long)PendingIndex!.GetValue(node)!;
                long applied = (long)AppliedField!.GetValue(node)!;
                var role = (Role)RoleField!.GetValue(node)!;
                ackCount = ((HashSet<string>)Acks!.GetValue(node)!).Count;
                long term = (long)PendingTerm!.GetValue(node)!;
                confirmed = role == Role.Leader
                    && term > 0
                    && pending == expectIndex
                    && applied >= expectIndex
                    && ackCount >= 1;
            }

            if (confirmed)
            {
                micros = TicksToMicros(Stopwatch.GetTimestamp() - started);
                detail = "";
                return true;
            }

            Thread.SpinWait(24);
        }

        micros = 0;
        detail = DescribeBaseline(node, expectIndex);
        return false;
    }

    private static string DescribeBaseline(RaftNode node, long expectIndex)
    {
        object gate = EventLock!.GetValue(node)!;
        lock (gate)
        {
            long pending = (long)PendingIndex!.GetValue(node)!;
            long applied = (long)AppliedField!.GetValue(node)!;
            int ackCount = ((HashSet<string>)Acks!.GetValue(node)!).Count;
            var role = (Role)RoleField!.GetValue(node)!;
            return "ack-set timeout expect=" + expectIndex.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " pending=" + pending.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " applied=" + applied.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " acks=" + ackCount.ToString(System.Globalization.CultureInfo.InvariantCulture)
                + " role=" + role;
        }
    }

    private static double TicksToMicros(long ticks) => ticks * 1_000_000.0 / Stopwatch.Frequency;
}
