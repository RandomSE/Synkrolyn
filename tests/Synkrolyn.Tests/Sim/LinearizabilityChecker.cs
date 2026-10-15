namespace Synkrolyn.Tests.Sim;

/// <summary>Whether a call took effect, failed without writing, or might have done either.</summary>
internal enum LinEffect
{
    Ok,
    Fail,
    Unknown,
}

/// <summary>Linearizable, a concrete conflict, or a search that stopped early.</summary>
internal enum LinVerdict
{
    Linearizable,
    Violation,
    Unknown,
}

/// <summary>One KV call. An empty <see cref="Value"/> on a get is an absent read.</summary>
internal readonly record struct LinOp(long Invoke, long Response, string Key, bool Put, string Value, LinEffect Effect);

/// <summary>
/// Wing-Gong search per key. Fail calls and indeterminate gets are no-ops and are dropped.
/// A timed-out put may still commit after its response, so a lone unknown put stays pending
/// until a later read observes it. A cache or cluster cap returns <see cref="LinVerdict.Unknown"/>,
/// which tests treat as a failure.
/// </summary>
internal static class LinearizabilityChecker
{
    internal const int DefaultCacheBudget = 200_000;

    private const int MaxCluster = 128;

    internal static LinVerdict Check(IReadOnlyList<LinOp> ops, int cacheBudget = DefaultCacheBudget)
    {
        if (ops.Count == 0)
        {
            return LinVerdict.Linearizable;
        }

        var byKey = new Dictionary<string, List<LinOp>>(StringComparer.Ordinal);
        foreach (LinOp op in ops)
        {
            if (!Matters(op))
            {
                continue;
            }

            if (!byKey.TryGetValue(op.Key, out List<LinOp>? list))
            {
                list = [];
                byKey.Add(op.Key, list);
            }

            list.Add(op);
        }

        bool unknown = false;
        foreach (List<LinOp> list in byKey.Values)
        {
            LinVerdict verdict = CheckKey(list, cacheBudget);
            if (verdict == LinVerdict.Violation)
            {
                return LinVerdict.Violation;
            }

            if (verdict == LinVerdict.Unknown)
            {
                unknown = true;
            }
        }

        return unknown ? LinVerdict.Unknown : LinVerdict.Linearizable;
    }

    internal static LinVerdict CheckHistory(IReadOnlyList<ClientHistoryEntry> history, int cacheBudget = DefaultCacheBudget) =>
        Check(HistoryOps(history), cacheBudget);

    private static List<LinOp> HistoryOps(IReadOnlyList<ClientHistoryEntry> history)
    {
        var open = new Dictionary<int, List<Open>>();
        var ops = new List<LinOp>(history.Count);
        foreach (ClientHistoryEntry entry in history)
        {
            if (entry.Op is not ("put" or "get"))
            {
                continue;
            }

            if (entry.Phase == "invoke")
            {
                if (!open.TryGetValue(entry.Client, out List<Open>? stack))
                {
                    stack = [];
                    open.Add(entry.Client, stack);
                }

                stack.Add(new Open(entry.TimeMillis, entry.Op, entry.Key, entry.Result));
                continue;
            }

            if (entry.Phase != "complete" || !open.TryGetValue(entry.Client, out List<Open>? pending))
            {
                continue;
            }

            int match = -1;
            for (int i = 0; i < pending.Count; i++)
            {
                if (pending[i].Op == entry.Op && pending[i].Key == entry.Key)
                {
                    match = i;
                    break;
                }
            }

            if (match < 0)
            {
                continue;
            }

            Open invoke = pending[match];
            pending.RemoveAt(match);
            ops.Add(FromHistory(invoke, entry.TimeMillis, entry.Result));
        }

        foreach (List<Open> stack in open.Values)
        {
            foreach (Open invoke in stack)
            {
                ops.Add(new LinOp(invoke.Time, long.MaxValue, invoke.Key, invoke.Op == "put", invoke.Op == "put" ? invoke.Result : "", LinEffect.Unknown));
            }
        }

        return ops;
    }

    internal static string Conflict(IReadOnlyList<ClientHistoryEntry> history)
    {
        List<LinOp> ops = HistoryOps(history);
        var byKey = new Dictionary<string, List<LinOp>>(StringComparer.Ordinal);
        foreach (LinOp op in ops)
        {
            if (!Matters(op))
            {
                continue;
            }

            if (!byKey.TryGetValue(op.Key, out List<LinOp>? list))
            {
                list = [];
                byKey.Add(op.Key, list);
            }

            list.Add(op);
        }

        var text = new System.Text.StringBuilder();
        foreach ((string key, List<LinOp> list) in byKey)
        {
            LinVerdict verdict = CheckKey(list, DefaultCacheBudget);
            if (verdict == LinVerdict.Linearizable)
            {
                continue;
            }

            text.Append(key).Append(' ').Append(verdict).Append(':');
            foreach (LinOp op in list)
            {
                text.Append(' ').Append(op.Put ? "put" : "get").Append(' ').Append(op.Value)
                    .Append(" [").Append(op.Invoke).Append(',').Append(op.Response).Append("] ")
                    .Append(op.Effect);
            }

            return text.ToString();
        }

        return "no per-key conflict";
    }

    private static bool Matters(LinOp op) => op.Put ? op.Effect != LinEffect.Fail : op.Effect == LinEffect.Ok;

    private static LinOp FromHistory(Open invoke, long response, string result)
    {
        if (invoke.Op == "put")
        {
            LinEffect effect = result is "null" or "unsettled"
                ? LinEffect.Unknown
                : result is "no-leader" or "SessionExpired" or "OutOfOrder"
                    ? LinEffect.Fail
                    : IsIndex(result) ? LinEffect.Ok : LinEffect.Unknown;
            return new LinOp(invoke.Time, response, invoke.Key, true, invoke.Result, effect);
        }

        if (result is "unknown" or "no-leader")
        {
            return new LinOp(invoke.Time, response, invoke.Key, false, "", LinEffect.Unknown);
        }

        if (result == "expired")
        {
            return new LinOp(invoke.Time, response, invoke.Key, false, "", LinEffect.Fail);
        }

        string observed = result is "null" or "absent" ? "" : result;
        return new LinOp(invoke.Time, response, invoke.Key, false, observed, LinEffect.Ok);
    }

    private static bool IsIndex(string text)
    {
        if (text.Length == 0)
        {
            return false;
        }

        foreach (char c in text)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        return true;
    }

    private static LinVerdict CheckKey(List<LinOp> ops, int cacheBudget)
    {
        ops.Sort(static (a, b) =>
        {
            int cmp = a.Invoke.CompareTo(b.Invoke);
            return cmp != 0 ? cmp : a.Response.CompareTo(b.Response);
        });

        var ids = new Ids();
        var steps = new Step[ops.Count];
        for (int i = 0; i < ops.Count; i++)
        {
            LinOp op = ops[i];
            steps[i] = new Step(op.Invoke, op.Response, op.Put, op.Effect, ids.Of(op.Value));
        }

        var worlds = new List<World> { new(0, new Dictionary<int, int>()) };
        int index = 0;
        while (index < steps.Length)
        {
            int end = index + 1;
            long maxResponse = steps[index].Response;
            while (end < steps.Length && steps[end].Invoke <= maxResponse)
            {
                if (steps[end].Response > maxResponse)
                {
                    maxResponse = steps[end].Response;
                }

                end++;
            }

            int length = end - index;
            if (length > MaxCluster)
            {
                return LinVerdict.Unknown;
            }

            bool more = end < steps.Length;
            var next = new List<World>();
            bool stopped = false;
            Step step = steps[index];
            if (UnknownPuts(steps, index, length))
            {
                foreach (World world in worlds)
                {
                    Dictionary<int, int> pending = world.Pending;
                    for (int i = index; i < end; i++)
                    {
                        pending = Add(pending, steps[i].Value);
                    }

                    next.Add(new World(world.State, pending));
                }
            }
            else if (length == 1 && step.Put)
            {
                foreach (World world in worlds)
                {
                    next.Add(new World(step.Value, world.Pending));
                }
            }
            else if (length == 1)
            {
                foreach (World world in worlds)
                {
                    if (world.State == step.Value)
                    {
                        next.Add(world);
                    }
                    else if (Take(world.Pending, step.Value, out Dictionary<int, int> pending))
                    {
                        next.Add(new World(step.Value, pending));
                    }
                }
            }
            else
            {
                foreach (World world in worlds)
                {
                    Absorb(world, steps.AsSpan(index, length), cacheBudget, more, next, ref stopped);
                }
            }

            if (next.Count == 0)
            {
                return stopped ? LinVerdict.Unknown : LinVerdict.Violation;
            }

            if (!more)
            {
                return LinVerdict.Linearizable;
            }

            worlds = Distinct(next);
            if (worlds.Count > MaxWorlds)
            {
                return LinVerdict.Unknown;
            }

            index = end;
        }

        return LinVerdict.Linearizable;
    }

    private static bool UnknownPuts(Step[] steps, int index, int length)
    {
        for (int i = index; i < index + length; i++)
        {
            if (!steps[i].Put || steps[i].Effect != LinEffect.Unknown)
            {
                return false;
            }
        }

        return length > 0;
    }

    private const int MaxWorlds = 64;

    private static void Absorb(World world, ReadOnlySpan<Step> cluster, int budget, bool more, List<World> next, ref bool stopped)
    {
        long minInvoke = long.MaxValue;
        long maxResponse = long.MinValue;
        var reads = new HashSet<int>();
        for (int i = 0; i < cluster.Length; i++)
        {
            if (cluster[i].Invoke < minInvoke)
            {
                minInvoke = cluster[i].Invoke;
            }

            if (cluster[i].Response > maxResponse)
            {
                maxResponse = cluster[i].Response;
            }

            if (!cluster[i].Put && cluster[i].Effect == LinEffect.Ok)
            {
                reads.Add(cluster[i].Value);
            }
        }

        Dictionary<int, int> pending = world.Pending;
        var injected = new List<Step>();
        foreach (int value in reads)
        {
            if (!Take(pending, value, out Dictionary<int, int> left))
            {
                continue;
            }

            pending = left;
            injected.Add(new Step(minInvoke, maxResponse, true, LinEffect.Unknown, value));
        }

        var active = new List<Step>(cluster.Length + injected.Count);
        for (int i = 0; i < cluster.Length; i++)
        {
            active.Add(cluster[i]);
        }

        active.AddRange(injected);
        for (int i = active.Count - 1; i >= cluster.Length; i--)
        {
            Step extra = active[i];
            active.RemoveAt(i);
            if (Fits(active, world.State, budget, ref stopped))
            {
                pending = Add(pending, extra.Value);
                continue;
            }

            active.Insert(i, extra);
        }

        if (active.Count > MaxCluster)
        {
            stopped = true;
            return;
        }

        Consider(world.State, pending, active.ToArray(), budget, more, next, ref stopped);
    }

    private static bool Fits(List<Step> ops, int start, int budget, ref bool stopped)
    {
        if (ops.Count == 0)
        {
            return true;
        }

        if (ops.Count > MaxCluster)
        {
            stopped = true;
            return false;
        }

        Result result = ops.Count == 1
            ? Result.One(ops[0], start)
            : Search.Run(ops.ToArray(), start, budget, false);
        if (result.Stopped)
        {
            stopped = true;
        }

        return result.Ok;
    }

    private static void Consider(int start, Dictionary<int, int> pending, ReadOnlySpan<Step> cluster, int budget, bool more, List<World> next, ref bool stopped)
    {
        Result result = Search.Run(cluster, start, budget, more);
        if (!result.Ok)
        {
            if (result.Stopped)
            {
                stopped = true;
            }

            return;
        }

        if (!more)
        {
            next.Add(new World(start, pending));
            return;
        }

        foreach (int endState in result.Ends)
        {
            next.Add(new World(endState, pending));
        }
    }

    private static Dictionary<int, int> Add(Dictionary<int, int> pending, int value)
    {
        var copy = new Dictionary<int, int>(pending);
        copy[value] = copy.GetValueOrDefault(value) + 1;
        return copy;
    }

    private static bool Take(Dictionary<int, int> pending, int value, out Dictionary<int, int> copy)
    {
        if (!pending.TryGetValue(value, out int count) || count <= 0)
        {
            copy = pending;
            return false;
        }

        copy = new Dictionary<int, int>(pending);
        if (count == 1)
        {
            copy.Remove(value);
        }
        else
        {
            copy[value] = count - 1;
        }

        return true;
    }

    private static List<World> Distinct(List<World> worlds)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var kept = new List<World>(worlds.Count);
        foreach (World world in worlds)
        {
            if (seen.Add(world.Key()))
            {
                kept.Add(world);
            }
        }

        return kept;
    }

    private sealed class World(int state, Dictionary<int, int> pending)
    {
        public int State { get; } = state;

        public Dictionary<int, int> Pending { get; } = pending;

        public string Key()
        {
            var text = new System.Text.StringBuilder();
            text.Append(State).Append('|');
            foreach ((int value, int count) in Pending.OrderBy(static pair => pair.Key))
            {
                text.Append(value).Append(':').Append(count).Append(',');
            }

            return text.ToString();
        }
    }

    private readonly struct Open(long time, string op, string key, string result)
    {
        public long Time { get; } = time;

        public string Op { get; } = op;

        public string Key { get; } = key;

        public string Result { get; } = result;
    }

    private readonly struct Step(long invoke, long response, bool put, LinEffect effect, int value)
    {
        public long Invoke { get; } = invoke;

        public long Response { get; } = response;

        public bool Put { get; } = put;

        public LinEffect Effect { get; } = effect;

        public int Value { get; } = value;
    }

    private sealed class Ids
    {
        private readonly Dictionary<string, int> _map = new(StringComparer.Ordinal);

        private int _next = 1;

        public int Of(string value)
        {
            if (value.Length == 0)
            {
                return 0;
            }

            if (_map.TryGetValue(value, out int id))
            {
                return id;
            }

            id = _next++;
            _map.Add(value, id);
            return id;
        }
    }

    private sealed class Result
    {
        private Result(bool ok, bool stopped, int[] ends)
        {
            Ok = ok;
            Stopped = stopped;
            Ends = ends;
        }

        public static Result None { get; } = new(false, false, []);

        public static Result Stop { get; } = new(false, true, []);

        public static Result Exists { get; } = new(true, false, []);

        public static Result Done(int state) => new(true, false, [state]);

        public static Result Many(List<int> ends) => new(true, false, ends.ToArray());

        public bool Ok { get; }

        public bool Stopped { get; }

        public int[] Ends { get; }

        public static Result One(Step step, int state)
        {
            int count = Next(step, state, out int first, out int second);
            if (count == 0)
            {
                return None;
            }

            if (count == 1)
            {
                return new Result(true, false, [first]);
            }

            return first == second
                ? new Result(true, false, [first])
                : new Result(true, false, [first, second]);
        }

        public static int Next(Step step, int state, out int first, out int second)
        {
            first = state;
            second = state;
            if (step.Put)
            {
                if (step.Effect == LinEffect.Fail)
                {
                    return 1;
                }

                if (step.Effect == LinEffect.Ok)
                {
                    first = step.Value;
                    return 1;
                }

                if (step.Value == state)
                {
                    return 1;
                }

                second = step.Value;
                return 2;
            }

            if (step.Effect != LinEffect.Ok)
            {
                return 1;
            }

            return step.Value == state ? 1 : 0;
        }
    }

    private static class Search
    {
        public static Result Run(ReadOnlySpan<Step> cluster, int start, int budget, bool collect)
        {
            var ops = new Step[cluster.Length];
            cluster.CopyTo(ops);
            int count = ops.Length;
            ulong fullLo = count >= 64 ? ulong.MaxValue : (1UL << count) - 1;
            ulong fullHi = count <= 64 ? 0UL : count == 128 ? ulong.MaxValue : (1UL << (count - 64)) - 1;
            var cache = new Dictionary<Key, Result>();
            return Recurse(ops, cache, budget < 0 ? 0 : budget, collect, fullLo, fullHi, 0, 0, start);
        }

        private static Result Recurse(
            Step[] ops,
            Dictionary<Key, Result> cache,
            int budget,
            bool collect,
            ulong fullLo,
            ulong fullHi,
            ulong lo,
            ulong hi,
            int state)
        {
            if (lo == fullLo && hi == fullHi)
            {
                return Result.Done(state);
            }

            var key = new Key(lo, hi, state);
            if (cache.TryGetValue(key, out Result? cached))
            {
                return cached;
            }

            if (cache.Count >= budget)
            {
                return Result.Stop;
            }

            long minResponse = long.MaxValue;
            for (int i = 0; i < ops.Length; i++)
            {
                if (!Taken(lo, hi, i) && ops[i].Response < minResponse)
                {
                    minResponse = ops[i].Response;
                }
            }

            var ends = new List<int>();
            bool any = false;
            bool stopped = false;
            for (int i = 0; i < ops.Length; i++)
            {
                if (Taken(lo, hi, i) || ops[i].Invoke > minResponse)
                {
                    continue;
                }

                int branches = Result.Next(ops[i], state, out int first, out int second);
                for (int branch = 0; branch < branches; branch++)
                {
                    ulong nextLo = lo;
                    ulong nextHi = hi;
                    Take(ref nextLo, ref nextHi, i);
                    Result child = Recurse(ops, cache, budget, collect, fullLo, fullHi, nextLo, nextHi, branch == 0 ? first : second);
                    if (child.Ok)
                    {
                        any = true;
                        if (!collect)
                        {
                            cache[key] = Result.Exists;
                            return Result.Exists;
                        }

                        foreach (int end in child.Ends)
                        {
                            if (!ends.Contains(end))
                            {
                                ends.Add(end);
                            }
                        }
                    }
                    else if (child.Stopped)
                    {
                        stopped = true;
                    }
                }
            }

            Result result = any && !(collect && stopped)
                ? Result.Many(ends)
                : stopped ? Result.Stop : Result.None;
            cache[key] = result;
            return result;
        }

        private static bool Taken(ulong lo, ulong hi, int index) =>
            index < 64 ? (lo & (1UL << index)) != 0 : (hi & (1UL << (index - 64))) != 0;

        private static void Take(ref ulong lo, ref ulong hi, int index)
        {
            if (index < 64)
            {
                lo |= 1UL << index;
                return;
            }

            hi |= 1UL << (index - 64);
        }

        private readonly record struct Key(ulong Lo, ulong Hi, int State);
    }
}
