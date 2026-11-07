using System.Globalization;
using System.Text;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Sim;
using Xunit.Abstractions;

namespace Synkrolyn.Tests.Cluster;

/// <summary>
/// Real-process soak on the production host. Unset <c>SOAK_SECONDS</c> skips.
/// The KV map stays in memory. File logs and snapshots persist until the temp directory is deleted.
/// This is not an unlimited-duration run. The job timeout caps a dispatch.
/// </summary>
[Trait("Category", "ProcessSoak")]
public sealed class ProcessSoakTests
{
    private const int MaxThreadGrowth = 12;

    private const int MaxFdGrowth = 24;

    private const long MaxRssGrowthKb = 64 * 1024;

    private const long MaxLogBytes = 512 * 1024;

    private const long MaxDirBytes = 2 * 1024 * 1024;

    private static readonly string[] Faults =
    [
        "minority-kill",
        "one-way-from-leader",
        "one-way-to-leader",
        "both-ways",
        "leader-kill",
        "membership",
        "snapshot",
    ];

    private static readonly ManualResetEventSlim Park = new(false);

    private ProcessNode? _extra;

    private bool _promoted;

    private bool _removed;

    private readonly ITestOutputHelper _output;

    public ProcessSoakTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task RealProcessSoak()
    {
        string? raw = Environment.GetEnvironmentVariable("SOAK_SECONDS");
        if (string.IsNullOrEmpty(raw))
        {
            return;
        }

        int seconds = int.Parse(raw, CultureInfo.InvariantCulture);
        Assert.True(seconds > 0, "SOAK_SECONDS must be positive");
        int seed = 1;
        string? seedRaw = Environment.GetEnvironmentVariable("SOAK_SEED");
        if (!string.IsNullOrEmpty(seedRaw)
            && long.TryParse(seedRaw, NumberStyles.None, CultureInfo.InvariantCulture, out long wide))
        {
            seed = (int)(wide % int.MaxValue);
        }

        var log = new SoakLog(seed, seconds);
        using var cluster = ProcessCluster.Start();
        try
        {
            await Run(cluster, log, TimeSpan.FromSeconds(seconds), seed);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(log + cluster.Dump(), ex);
        }
    }

    private async Task Run(ProcessCluster cluster, SoakLog log, TimeSpan duration, int seed)
    {
        ProcessNode leader = cluster.WaitLeader();
        cluster.PutCommitted(leader, "seed", "committed");
        string probe = cluster.Call(cluster.WaitLeader(), "SNAPSHOT");
        if (!probe.StartsWith("OK", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("snapshot command " + probe);
        }

        var baseline = new Dictionary<string, ProcSample>(StringComparer.Ordinal);
        Remember(cluster, baseline);
        var deadline = System.Diagnostics.Stopwatch.StartNew();
        int slice = 0;
        while (deadline.Elapsed < duration || slice < Faults.Length)
        {
            string fault = Faults[(slice + seed) % Faults.Length];
            log.Add("slice " + slice.ToString(CultureInfo.InvariantCulture) + " " + fault + " at " + deadline.Elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture) + "s");
            using var history = new History();
            using var stop = new CancellationTokenSource();
            Task[] writers =
            [
                Traffic(cluster, history, "a", stop.Token),
                Traffic(cluster, history, "b", stop.Token),
            ];
            await Burst(TimeSpan.FromMilliseconds(300), stop.Token);
            await Apply(cluster, fault, log);
            await Burst(TimeSpan.FromMilliseconds(300), stop.Token);
            await stop.CancelAsync();
            await Task.WhenAll(writers);
            cluster.RecordStableReads(history, "k0", "k1", "k2", "k3");
            cluster.AssertLinearizable(history);
            cluster.AssertOneLeaderPerTerm();
            Assert.Equal("committed", cluster.LinearizableGet(cluster.WaitLeader(), "seed"));
            Remember(cluster, baseline);
            slice++;
        }

        await SnapshotAndBound(cluster, log);
        AssertResources(cluster, baseline, log);
        log.Add("completed slices " + slice.ToString(CultureInfo.InvariantCulture));
        _output.WriteLine(log.ToString());
    }

    private async Task Apply(ProcessCluster cluster, string fault, SoakLog log)
    {
        switch (fault)
        {
            case "minority-kill":
                await Restart(cluster, cluster.Follower(), log);
                break;
            case "leader-kill":
                await Restart(cluster, cluster.WaitLeader(), log);
                break;
            case "one-way-from-leader":
                await OneWay(cluster, fromLeader: true, expectRefused: true, log);
                break;
            case "one-way-to-leader":
                await OneWay(cluster, fromLeader: false, expectRefused: false, log);
                break;
            case "both-ways":
                await BothWays(cluster, log);
                break;
            case "membership":
                await Membership(cluster, log);
                break;
            case "snapshot":
                await SnapshotAndBound(cluster, log);
                break;
            default:
                throw new InvalidOperationException(fault);
        }
    }

    private static async Task Restart(ProcessCluster cluster, ProcessNode node, SoakLog log)
    {
        log.Add("restart " + node.Id);
        node.Kill();
        cluster.Restart(node);
        cluster.WaitApplied(node, "seed", "committed");
        await Task.CompletedTask;
    }

    private static async Task OneWay(ProcessCluster cluster, bool fromLeader, bool expectRefused, SoakLog log)
    {
        ProcessNode leader = cluster.WaitLeader();
        ProcessNode follower = cluster.NodeExcept(leader);
        ProcessNode from = fromLeader ? leader : follower;
        ProcessNode to = fromLeader ? follower : leader;
        log.Add("cut " + from.Id + " -> " + to.Id);
        Assert.Equal("OK", cluster.Call(from, "DISCONNECT " + to.Id));
        if (expectRefused)
        {
            cluster.WaitUntil(
                "lease dropped on " + follower.Id,
                () =>
                {
                    NodeStatus status = cluster.Status(follower);
                    return status.Role != "Leader" && !status.FollowerLease;
                },
                TimeSpan.FromSeconds(4));
            Assert.Equal("BGET refused", cluster.Call(follower, "BGET seed"));
        }
        else
        {
            await Burst(TimeSpan.FromMilliseconds(400), CancellationToken.None);
            string line = cluster.Call(follower, "BGET seed");
            if (line.StartsWith("BGET ", StringComparison.Ordinal) && line != "BGET refused" && line != "BGET null")
            {
                Assert.Equal("BGET committed", line);
            }
        }

        Assert.Equal("committed", cluster.LinearizableGet(cluster.WaitLeader(), "seed"));
        Assert.Equal("OK", cluster.Call(from, "RECONNECT " + to.Id));
        cluster.WaitApplied(follower, "seed", "committed");
    }

    private static async Task BothWays(ProcessCluster cluster, SoakLog log)
    {
        ProcessNode leader = cluster.WaitLeader();
        ProcessNode follower = cluster.NodeExcept(leader);
        log.Add("cut both " + leader.Id + " " + follower.Id);
        cluster.CutLink(leader, follower);
        cluster.WaitUntil(
            "lease dropped on " + follower.Id,
            () =>
            {
                NodeStatus status = cluster.Status(follower);
                return status.Role != "Leader" && !status.FollowerLease;
            },
            TimeSpan.FromSeconds(4));
        Assert.Equal("BGET refused", cluster.Call(follower, "BGET seed"));
        Assert.Equal("committed", cluster.LinearizableGet(cluster.WaitLeader(), "seed"));
        cluster.HealLink(leader, follower);
        cluster.WaitApplied(follower, "seed", "committed");
        await Task.CompletedTask;
    }

    private async Task Membership(ProcessCluster cluster, SoakLog log)
    {
        if (_removed)
        {
            await Task.CompletedTask;
            return;
        }

        if (_extra is null)
        {
            log.Add("add n4");
            _extra = cluster.AddNode("n4", TimeSpan.FromSeconds(30), ["n1", "n2", "n3"]);
            await UntilMem(cluster, "ADDLEARNER n4", log);
            cluster.WaitUntil(
                "n4 learner",
                () => cluster.Call(cluster.WaitLeader(), "STATUS").Contains("learners=n4", StringComparison.Ordinal),
                TimeSpan.FromSeconds(6));
            cluster.WaitApplied(_extra, "seed", "committed");
        }

        if (!_promoted)
        {
            log.Add("promote n4");
            await UntilMem(cluster, "PROMOTE n4", log);
            cluster.WaitUntil(
                "n4 voter",
                () => Voters(cluster).Contains("n4", StringComparison.Ordinal),
                TimeSpan.FromSeconds(6));
            cluster.PutCommitted(cluster.WaitLeader(), "joined", "n4");
            cluster.WaitApplied(_extra, "joined", "n4");
            _promoted = true;
        }

        if (!_removed)
        {
            log.Add("remove n4");
            await UntilMem(cluster, "REMOVE n4", log);
            cluster.WaitUntil(
                "n4 removed",
                () => !Voters(cluster).Contains("n4", StringComparison.Ordinal),
                TimeSpan.FromSeconds(6));
            cluster.PutCommitted(cluster.WaitLeader(), "after", "remove");
            _removed = true;
        }
    }

    private static async Task UntilMem(ProcessCluster cluster, string command, SoakLog log)
    {
        var started = System.Diagnostics.Stopwatch.StartNew();
        string last = "";
        while (started.Elapsed < TimeSpan.FromSeconds(6))
        {
            last = cluster.Call(cluster.WaitLeader(), command);
            log.Add(command + " -> " + last);
            if (last.StartsWith("MEM ok ", StringComparison.Ordinal))
            {
                await Task.CompletedTask;
                return;
            }

            Park.Wait(TimeSpan.FromMilliseconds(50));
        }

        throw new TimeoutException(command + " last " + last);
    }

    private static string Voters(ProcessCluster cluster)
    {
        string line = cluster.Call(cluster.WaitLeader(), "STATUS");
        foreach (string part in line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            if (part.StartsWith("voters=", StringComparison.Ordinal))
            {
                return part["voters=".Length..];
            }
        }

        return "";
    }

    private static async Task SnapshotAndBound(ProcessCluster cluster, SoakLog log)
    {
        Assert.Equal("OK", cluster.Call(cluster.WaitLeader(), "SNAPSHOT"));
        cluster.WaitUntil(
            "snapshot file",
            () => cluster.Nodes.Any(node => node.IsRunning && File.Exists(Path.Combine(node.DataDirectory, FileRaftLog.SnapshotFileName))),
            TimeSpan.FromSeconds(6));
        await Burst(TimeSpan.FromMilliseconds(200), CancellationToken.None);
        foreach (ProcessNode node in cluster.Nodes)
        {
            if (!node.IsRunning)
            {
                continue;
            }

            long logBytes = SoakFiles.FileLength(Path.Combine(node.DataDirectory, FileRaftLog.FileName));
            long dirBytes = SoakFiles.DirectoryBytes(node.DataDirectory);
            log.Add(node.Id + " log=" + logBytes.ToString(CultureInfo.InvariantCulture) + " dir=" + dirBytes.ToString(CultureInfo.InvariantCulture));
            Assert.True(logBytes <= MaxLogBytes, node.Id + " log " + logBytes.ToString(CultureInfo.InvariantCulture));
            Assert.True(dirBytes <= MaxDirBytes, node.Id + " dir " + dirBytes.ToString(CultureInfo.InvariantCulture));
        }
    }

    private static void Remember(ProcessCluster cluster, Dictionary<string, ProcSample> baseline)
    {
        foreach (ProcessNode node in cluster.Nodes)
        {
            if (!node.IsRunning || node.Pid <= 0)
            {
                continue;
            }

            if (!baseline.TryGetValue(node.Id, out ProcSample sample) || sample.Pid != node.Pid)
            {
                baseline[node.Id] = ProcSample.Read(node);
            }
        }
    }

    private static void AssertResources(ProcessCluster cluster, Dictionary<string, ProcSample> baseline, SoakLog log)
    {
        foreach (ProcessNode node in cluster.Nodes)
        {
            if (!node.IsRunning || !baseline.TryGetValue(node.Id, out ProcSample start) || start.Pid != node.Pid)
            {
                continue;
            }

            ProcSample now = ProcSample.Read(node);
            log.Add(node.Id + " threads " + start.Threads.ToString(CultureInfo.InvariantCulture) + "->" + now.Threads.ToString(CultureInfo.InvariantCulture)
                + " fds " + start.Fds.ToString(CultureInfo.InvariantCulture) + "->" + now.Fds.ToString(CultureInfo.InvariantCulture)
                + " rssKb " + start.RssKb.ToString(CultureInfo.InvariantCulture) + "->" + now.RssKb.ToString(CultureInfo.InvariantCulture));
            if (start.Threads < 0)
            {
                continue;
            }

            Assert.True(now.Threads <= start.Threads + MaxThreadGrowth, node.Id + " threads");
            Assert.True(now.Fds <= start.Fds + MaxFdGrowth, node.Id + " fds");
            Assert.True(now.RssKb <= start.RssKb + MaxRssGrowthKb, node.Id + " rss");
        }
    }

    private static Task Traffic(ProcessCluster cluster, History history, string writer, CancellationToken stop)
    {
        return Task.Run(() =>
        {
            int i = 0;
            while (!stop.IsCancellationRequested && i < 12)
            {
                string key = "k" + (i % 4).ToString(CultureInfo.InvariantCulture);
                string value = writer + "-" + i.ToString(CultureInfo.InvariantCulture);
                long invoke = cluster.Now();
                LinEffect effect = LinEffect.Unknown;
                try
                {
                    ProcessNode leader = cluster.WaitLeader();
                    string line = cluster.Call(leader, "PUT " + key + " " + value);
                    if (line.StartsWith("PUT ok ", StringComparison.Ordinal))
                    {
                        effect = LinEffect.Ok;
                    }
                    else if (line == "PUT notleader")
                    {
                        effect = LinEffect.Fail;
                    }
                }
                catch (Exception ex) when (ex is TimeoutException or IOException or InvalidOperationException or System.Net.Sockets.SocketException or AggregateException)
                {
                    effect = LinEffect.Unknown;
                }

                history.Add(new LinOp(invoke, cluster.Now(), key, true, value, effect));
                i++;
            }
        }, stop);
    }

    private static async Task Burst(TimeSpan duration, CancellationToken stop)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (clock.Elapsed < duration && !stop.IsCancellationRequested)
        {
            Park.Wait(TimeSpan.FromMilliseconds(20), stop);
            await Task.Yield();
        }
    }
}

internal sealed class SoakLog
{
    private readonly StringBuilder _text = new();

    public SoakLog(int seed, int seconds)
    {
        _text.Append("soak seed=").Append(seed.ToString(CultureInfo.InvariantCulture))
            .Append(" seconds=").Append(seconds.ToString(CultureInfo.InvariantCulture));
    }

    public void Add(string line)
    {
        _text.Append('\n').Append(line);
    }

    public override string ToString() => _text.ToString();
}

internal readonly record struct ProcSample(int Pid, int Threads, int Fds, long RssKb)
{
    public static ProcSample Read(ProcessNode node)
    {
        string proc = "/proc/" + node.Pid.ToString(CultureInfo.InvariantCulture);
        if (!File.Exists(Path.Combine(proc, "status")))
        {
            return new ProcSample(node.Pid, -1, -1, -1);
        }

        string status = File.ReadAllText(Path.Combine(proc, "status"));
        int threads = ParseStatus(status, "Threads:");
        long rss = ParseStatus(status, "VmRSS:");
        int fds = 0;
        string fdDir = Path.Combine(proc, "fd");
        if (Directory.Exists(fdDir))
        {
            fds = Directory.EnumerateFileSystemEntries(fdDir).Count();
        }

        return new ProcSample(node.Pid, threads, fds, rss);
    }

    private static int ParseStatus(string status, string label)
    {
        foreach (string raw in status.Split('\n'))
        {
            if (!raw.StartsWith(label, StringComparison.Ordinal))
            {
                continue;
            }

            string rest = raw[label.Length..].Trim();
            int space = rest.IndexOf(' ');
            string number = space < 0 ? rest : rest[..space];
            return int.Parse(number, CultureInfo.InvariantCulture);
        }

        return -1;
    }
}

internal static class SoakFiles
{
    public static long FileLength(string path) => File.Exists(path) ? new FileInfo(path).Length : 0;

    public static long DirectoryBytes(string path)
    {
        long total = 0;
        if (!Directory.Exists(path))
        {
            return 0;
        }

        foreach (string file in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
        {
            total += new FileInfo(file).Length;
        }

        return total;
    }
}
