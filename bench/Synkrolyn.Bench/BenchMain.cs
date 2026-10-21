using System.Diagnostics;
using System.Globalization;
using System.Runtime;
using System.Text;
using Synkrolyn.Kv;
using Synkrolyn.Raft;

namespace Synkrolyn.Bench;

internal static class BenchMain
{
    private const int PipelineDepth = 32;

    public static int Run(string[] args)
    {
        if (args.Contains("-h") || args.Contains("--help"))
        {
            Console.WriteLine("Synkrolyn.Bench");
            Console.WriteLine("  Loopback 3-node write, read, election, and allocation harness.");
            Console.WriteLine("  --label <text>  --out <file>  --quick");
            Console.WriteLine("  --writes N  --pipeline N  --reads N  --read-index N  --elections N");
            return 0;
        }

        BenchConfig config = BenchConfig.Parse(args);
        ReadProbe.EnsureAvailable();
        var report = new StringBuilder();
        void Line(string text)
        {
            Console.WriteLine(text);
            _ = report.AppendLine(text);
        }

        Line("label=" + config.Label);
        Line("framework=" + System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
        Line("os=" + System.Runtime.InteropServices.RuntimeInformation.OSDescription);
        Line("arch=" + System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture);
        Line("processors=" + Environment.ProcessorCount.ToString(CultureInfo.InvariantCulture));
        Line("gc=" + (GCSettings.IsServerGC ? "server" : "workstation"));
        Line("election_timeout_ms=" + LoopbackCluster.ElectionTimeout.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
        Line("heartbeat_ms=" + LoopbackCluster.Heartbeat.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
        Line("read_index_probe=" + ReadProbe.Kind);
        Line("log=in-memory");
        Line("jitter=identity");
        Line("snapshot_threshold=0");

        using (LoopbackCluster warmup = LoopbackCluster.Start())
        {
            Node leader = warmup.WaitForLeader(TimeSpan.FromSeconds(8));
            CommitMany(leader, 40, "warm");
            WaitForLease(leader, TimeSpan.FromSeconds(2));
            KvClient client = new(leader.Raft, leader.Store);
            for (int i = 0; i < 40; i++)
            {
                if (client.LinearizableGet("warm0") is null && leader.Raft.Role != Role.Leader)
                {
                    throw new InvalidOperationException("warmup read lost the leader");
                }
            }

            for (int i = 0; i < 10; i++)
            {
                if (!ReadProbe.TrySample(leader.Raft, TimeSpan.FromSeconds(1), out _, out string detail))
                {
                    throw new InvalidOperationException("warmup ReadIndex failed: " + detail);
                }
            }
        }

        using (LoopbackCluster cluster = LoopbackCluster.Start())
        {
            Node leader = cluster.WaitForLeader(TimeSpan.FromSeconds(8));
            CommitOne(leader, "benchkey", "benchval");
            WaitForLease(leader, TimeSpan.FromSeconds(2));
            string? seen = leader.Store.Get("benchkey");
            if (seen != "benchval")
            {
                throw new InvalidOperationException("seed get returned " + (seen ?? "<null>"));
            }

            List<double> writeUs = TimeWrites(leader, config.Writes);
            WriteStats(Line, "write", writeUs, "us");
            List<double> throughput = TimeThroughput(leader, config.Pipeline, config.ThroughputTrials);
            WriteStats(Line, "write_throughput_ops_s", throughput, "ops");

            KvClient reader = new(leader.Raft, leader.Store);
            List<double> leaseUs = TimeLeaseReads(leader, reader, config.Reads);
            WriteStats(Line, "lease_read", leaseUs, "us");
            List<double> readIndexUs = TimeReadIndex(leader, config.ReadIndex);
            WriteStats(Line, "read_index", readIndexUs, "us");

            Line("alloc_write_bytes=" + Format(MedianAlloc(n => CommitMany(leader, n, "allocw"), config)));
            Line("alloc_lease_read_bytes=" + Format(MedianAlloc(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    _ = reader.LinearizableGet("benchkey");
                }
            }, config)));
            Line("alloc_read_index_bytes=" + Format(MedianAlloc(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    if (!ReadProbe.TrySample(leader.Raft, TimeSpan.FromSeconds(1), out _, out string detail))
                    {
                        throw new InvalidOperationException("alloc ReadIndex failed: " + detail);
                    }
                }
            }, config)));
            cluster.ThrowIfFaulted();
        }

        List<double> elections = TimeElections(config.Elections);
        WriteStats(Line, "election", elections, "ms");

        if (!string.IsNullOrWhiteSpace(config.OutPath))
        {
            File.WriteAllText(config.OutPath, report.ToString());
        }

        return 0;
    }

    private static List<double> TimeWrites(Node leader, int count)
    {
        byte[][] commands = BuildPuts(count, "w");
        var samples = new List<double>(count);
        for (int i = 0; i < count; i++)
        {
            long started = Stopwatch.GetTimestamp();
            WaitApplied(leader, commands[i]);
            samples.Add(TicksToMicros(Stopwatch.GetTimestamp() - started));
        }

        return samples;
    }

    private static List<double> TimeThroughput(Node leader, int count, int trials)
    {
        var samples = new List<double>(trials);
        for (int trial = 0; trial < trials; trial++)
        {
            byte[][] commands = BuildPuts(count, "p" + trial.ToString(CultureInfo.InvariantCulture) + "-");
            long started = Stopwatch.GetTimestamp();
            CommitPipelined(leader, commands);
            double seconds = (Stopwatch.GetTimestamp() - started) / (double)Stopwatch.Frequency;
            samples.Add(count / seconds);
        }

        return samples;
    }

    private static List<double> TimeLeaseReads(Node leader, KvClient reader, int count)
    {
        var samples = new List<double>(count);
        for (int i = 0; i < count; i++)
        {
            if (!leader.Raft.ReadIndexSatisfied())
            {
                WaitForLease(leader, TimeSpan.FromSeconds(2));
            }

            long started = Stopwatch.GetTimestamp();
            string? value = reader.LinearizableGet("benchkey");
            if (value != "benchval")
            {
                throw new InvalidOperationException(
                    "lease read returned " + (value ?? "<null>") + " role=" + leader.Raft.Role);
            }

            samples.Add(TicksToMicros(Stopwatch.GetTimestamp() - started));
        }

        return samples;
    }

    private static List<double> TimeReadIndex(Node leader, int count)
    {
        var samples = new List<double>(count);
        int failures = 0;
        while (samples.Count < count)
        {
            if (leader.Raft.Role != Role.Leader)
            {
                throw new InvalidOperationException("ReadIndex lost the leader");
            }

            if (!ReadProbe.TrySample(leader.Raft, TimeSpan.FromSeconds(1), out double micros, out string detail))
            {
                failures++;
                if (failures > 5)
                {
                    throw new InvalidOperationException("ReadIndex failed: " + detail);
                }

                continue;
            }

            samples.Add(micros);
        }

        return samples;
    }

    private static List<double> TimeElections(int count)
    {
        var samples = new List<double>(count);
        for (int i = 0; i < count + 1; i++)
        {
            using var cluster = LoopbackCluster.Start();
            Node leader = cluster.WaitForLeader(TimeSpan.FromSeconds(8));
            CommitOne(leader, "elect", "1");
            Thread.Sleep(80);
            long term = leader.Raft.CurrentTerm;
            long started = Stopwatch.GetTimestamp();
            cluster.Stop(leader);
            Node next = cluster.WaitForLeader(TimeSpan.FromSeconds(5));
            double ms = (Stopwatch.GetTimestamp() - started) / (double)Stopwatch.Frequency * 1000.0;
            if (next.Raft.CurrentTerm <= term)
            {
                throw new InvalidOperationException(
                    "successor term " + next.Raft.CurrentTerm.ToString(CultureInfo.InvariantCulture)
                    + " did not pass " + term.ToString(CultureInfo.InvariantCulture));
            }

            if (i > 0)
            {
                samples.Add(ms);
            }
        }

        return samples;
    }

    private static double MedianAlloc(Action<int> work, BenchConfig config)
    {
        var trials = new List<double>(config.AllocTrials);
        for (int i = 0; i < config.AllocTrials; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long before = GC.GetTotalAllocatedBytes(precise: true);
            work(config.AllocBatch);
            long after = GC.GetTotalAllocatedBytes(precise: true);
            trials.Add((after - before) / (double)config.AllocBatch);
        }

        trials.Sort();
        return Percentile(trials, 50);
    }

    private static void CommitMany(Node leader, int count, string prefix)
    {
        CommitPipelined(leader, BuildPuts(count, prefix));
    }

    private static void CommitPipelined(Node leader, byte[][] commands)
    {
        var waiters = new RaftNode.ApplyWaiter[PipelineDepth];
        int head = 0;
        int tail = 0;
        int queued = 0;
        int submitted = 0;
        var limit = Stopwatch.StartNew();
        while (submitted < commands.Length || queued > 0)
        {
            if (limit.Elapsed > TimeSpan.FromSeconds(30))
            {
                throw new TimeoutException("pipeline did not finish");
            }

            while (queued < PipelineDepth && submitted < commands.Length)
            {
                waiters[tail] = Propose(leader, commands[submitted]);
                tail = (tail + 1) % PipelineDepth;
                queued++;
                submitted++;
            }

            RaftNode.ApplyWaiter oldest = waiters[head];
            if (oldest.Applied)
            {
                head = (head + 1) % PipelineDepth;
                queued--;
                continue;
            }

            if (oldest.Failed || leader.Raft.Role != Role.Leader)
            {
                throw new InvalidOperationException("pipeline lost the leader at " + submitted.ToString(CultureInfo.InvariantCulture));
            }

            Thread.SpinWait(24);
        }
    }

    private static void CommitOne(Node leader, string key, string value)
    {
        WaitApplied(leader, KvCommandCodec.EncodePut(key, value));
    }

    private static void WaitApplied(Node leader, byte[] command)
    {
        RaftNode.ApplyWaiter waiter = Propose(leader, command);
        var limit = Stopwatch.StartNew();
        while (!waiter.Applied)
        {
            if (waiter.Failed || leader.Raft.Role != Role.Leader)
            {
                throw new InvalidOperationException("commit failed role=" + leader.Raft.Role + " commit=" + leader.Raft.CommitIndex.ToString(CultureInfo.InvariantCulture));
            }

            if (limit.Elapsed > TimeSpan.FromSeconds(2))
            {
                throw new TimeoutException("commit timed out at " + leader.Raft.CommitIndex.ToString(CultureInfo.InvariantCulture));
            }

            Thread.SpinWait(24);
        }
    }

    private static RaftNode.ApplyWaiter Propose(Node leader, byte[] command)
    {
        long? index = leader.Raft.Propose(command);
        if (index is null)
        {
            throw new InvalidOperationException("propose returned null role=" + leader.Raft.Role);
        }

        return leader.Raft.RegisterApplyWaiter(index.Value);
    }

    private static byte[][] BuildPuts(int count, string prefix)
    {
        var commands = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            commands[i] = KvCommandCodec.EncodePut(prefix + i.ToString(CultureInfo.InvariantCulture), "v");
        }

        return commands;
    }

    private static void WaitForLease(Node leader, TimeSpan timeout)
    {
        var wait = Stopwatch.StartNew();
        while (wait.Elapsed < timeout)
        {
            if (leader.Raft.Role == Role.Leader && leader.Raft.ReadIndexSatisfied())
            {
                return;
            }

            if (leader.Raft.Role != Role.Leader)
            {
                throw new InvalidOperationException("lease wait lost the leader");
            }

            Thread.Yield();
        }

        throw new TimeoutException("linearizable lease did not become valid");
    }

    private static void WriteStats(Action<string> line, string name, List<double> samples, string unit)
    {
        samples.Sort();
        if (unit == "ops")
        {
            line(name + "_p50=" + Format(Percentile(samples, 50)));
            line(name + "_iqr=" + Format(Percentile(samples, 75) - Percentile(samples, 25)));
            line(name + "_n=" + samples.Count.ToString(CultureInfo.InvariantCulture));
            return;
        }

        line(name + "_p50_" + unit + "=" + Format(Percentile(samples, 50)));
        line(name + "_p99_" + unit + "=" + Format(Percentile(samples, 99)));
        line(name + "_iqr_" + unit + "=" + Format(Percentile(samples, 75) - Percentile(samples, 25)));
        line(name + "_min_" + unit + "=" + Format(samples[0]));
        line(name + "_max_" + unit + "=" + Format(samples[^1]));
        line(name + "_n=" + samples.Count.ToString(CultureInfo.InvariantCulture));
    }

    private static double Percentile(List<double> sorted, double percentile)
    {
        if (sorted.Count == 1)
        {
            return sorted[0];
        }

        double rank = percentile / 100.0 * (sorted.Count - 1);
        int low = (int)Math.Floor(rank);
        int high = (int)Math.Ceiling(rank);
        if (low == high)
        {
            return sorted[low];
        }

        double weight = rank - low;
        return (sorted[low] * (1.0 - weight)) + (sorted[high] * weight);
    }

    private static double TicksToMicros(long ticks) => ticks * 1_000_000.0 / Stopwatch.Frequency;

    private static string Format(double value) => value.ToString("0.###", CultureInfo.InvariantCulture);
}

internal sealed class BenchConfig
{
    public int Writes { get; private set; } = 500;

    public int Pipeline { get; private set; } = 2000;

    public int Reads { get; private set; } = 500;

    public int ReadIndex { get; private set; } = 200;

    public int Elections { get; private set; } = 15;

    public int AllocTrials { get; private set; } = 5;

    public int AllocBatch { get; private set; } = 80;

    public int ThroughputTrials { get; private set; } = 5;

    public string Label { get; private set; } = "local";

    public string? OutPath { get; private set; }

    public static BenchConfig Parse(string[] args)
    {
        var config = new BenchConfig();
        if (args.Contains("--quick"))
        {
            config.Writes = 30;
            config.Pipeline = 60;
            config.Reads = 30;
            config.ReadIndex = 15;
            config.Elections = 3;
            config.AllocTrials = 1;
            config.AllocBatch = 20;
            config.ThroughputTrials = 2;
            config.Label = "quick";
        }

        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            if (arg == "--label" && i + 1 < args.Length)
            {
                config.Label = args[++i];
            }
            else if (arg == "--out" && i + 1 < args.Length)
            {
                config.OutPath = args[++i];
            }
            else if (arg == "--writes" && i + 1 < args.Length)
            {
                config.Writes = ParseInt(args[++i], arg);
            }
            else if (arg == "--pipeline" && i + 1 < args.Length)
            {
                config.Pipeline = ParseInt(args[++i], arg);
            }
            else if (arg == "--reads" && i + 1 < args.Length)
            {
                config.Reads = ParseInt(args[++i], arg);
            }
            else if (arg == "--read-index" && i + 1 < args.Length)
            {
                config.ReadIndex = ParseInt(args[++i], arg);
            }
            else if (arg == "--elections" && i + 1 < args.Length)
            {
                config.Elections = ParseInt(args[++i], arg);
            }
        }

        return config;
    }

    private static int ParseInt(string text, string name)
    {
        if (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int value) || value < 1)
        {
            throw new ArgumentException(name + " must be a positive integer");
        }

        return value;
    }
}
