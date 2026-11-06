using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Synkrolyn.Raft;
using Synkrolyn.Tests.Sim;

namespace Synkrolyn.Tests.Cluster;

/// <summary>
/// Three operating-system processes, production <see cref="RaftRuntime"/>,
/// <see cref="SocketTransport"/>, and <see cref="FileRaftLog"/>. Disk-full is an
/// IOException from the log force path, not a kernel ENOSPC. SocketTransport has
/// no duplication or reordering injector.
/// </summary>
[Trait("Category", "ProcessCluster")]
public sealed class ProcessClusterTests
{
    private const string SeedKey = "seed";

    private const string SeedValue = "committed";

    [Fact]
    public async Task ConcurrentWrites_surviveMinorityRestart_andStayLinearizable()
    {
        using var cluster = ProcessCluster.Start();
        cluster.WaitLeader();
        long seeded = cluster.Now();
        cluster.PutCommitted(cluster.WaitLeader(), SeedKey, SeedValue);
        using var history = new History();
        history.Add(new LinOp(seeded, cluster.Now(), SeedKey, true, SeedValue, LinEffect.Ok));
        Task[] writers = [cluster.WriteBurst(history, "a", 6), cluster.WriteBurst(history, "b", 6)];
        cluster.WaitUntil("a successful concurrent put", () => history.OkPuts >= 1, TimeSpan.FromSeconds(8));
        ProcessNode minority = cluster.Follower();
        minority.Kill();
        await Task.WhenAll(writers);
        cluster.Restart(minority);
        cluster.WaitApplied(minority, SeedKey, SeedValue);
        cluster.RecordStableReads(history, SeedKey, "k0", "k1");
        cluster.AssertLinearizable(history);
        cluster.AssertOneLeaderPerTerm();
        Assert.True(history.OkPuts >= 3, cluster.Dump());
    }

    [Fact]
    public async Task ConcurrentWrites_surviveLeaderCrash_relect_andStayLinearizable()
    {
        using var cluster = ProcessCluster.Start();
        ProcessNode first = cluster.WaitLeader();
        long seeded = cluster.Now();
        cluster.PutCommitted(first, SeedKey, SeedValue);
        using var history = new History();
        history.Add(new LinOp(seeded, cluster.Now(), SeedKey, true, SeedValue, LinEffect.Ok));
        Task[] writers = [cluster.WriteBurst(history, "a", 6), cluster.WriteBurst(history, "b", 6)];
        cluster.WaitUntil("a successful concurrent put", () => history.OkPuts >= 1, TimeSpan.FromSeconds(8));
        first.Kill();
        ProcessNode elected = cluster.WaitLeader();
        Assert.NotEqual(first.Id, elected.Id);
        await Task.WhenAll(writers);
        cluster.Restart(first);
        cluster.WaitApplied(first, SeedKey, SeedValue);
        cluster.RecordStableReads(history, SeedKey, "k0", "k1");
        cluster.AssertLinearizable(history);
        cluster.AssertOneLeaderPerTerm();
        Assert.Equal(SeedValue, cluster.LinearizableGet(cluster.WaitLeader(), SeedKey));
    }

    [Fact]
    public void BothDirectionsOfOneLink_pastLease_refuseStaleRead_andKeepCommit()
    {
        using var cluster = ProcessCluster.Start();
        ProcessNode leader = cluster.WaitLeader();
        cluster.PutCommitted(leader, SeedKey, SeedValue);
        ProcessNode isolated = cluster.NodeExcept(leader);
        cluster.WaitApplied(isolated, SeedKey, SeedValue);
        cluster.WaitUntil(
            "follower lease",
            () =>
            {
                NodeStatus status = cluster.Status(isolated);
                return status.Role == "Follower" && status.FollowerLease;
            },
            TimeSpan.FromSeconds(4));

        cluster.CutLink(leader, isolated);
        cluster.WaitUntil(
            "lease expired on the isolated follower",
            () =>
            {
                NodeStatus status = cluster.Status(isolated);
                return status.Role != "Leader" && !status.FollowerLease;
            },
            TimeSpan.FromSeconds(6));
        Assert.Equal("BGET refused", cluster.Call(isolated, "BGET " + SeedKey));
        Assert.Equal(SeedValue, cluster.LinearizableGet(cluster.WaitLeader(), SeedKey));
        cluster.PutCommitted(cluster.WaitLeader(), "during", "partition");
        cluster.HealLink(leader, isolated);
        cluster.WaitApplied(isolated, SeedKey, SeedValue);
        cluster.WaitApplied(isolated, "during", "partition");
        cluster.AssertOneLeaderPerTerm();
    }

    [Fact]
    public void StopKillTruncationAndStorageFaults_preserveCommittedEntries()
    {
        using var cluster = ProcessCluster.Start();
        ProcessNode leader = cluster.WaitLeader();
        cluster.PutCommitted(leader, SeedKey, SeedValue);
        ProcessNode follower = cluster.NodeExcept(leader);

        cluster.StopGracefully(follower);
        TruncateLogTail(follower.DataDirectory);
        cluster.Restart(follower);
        cluster.WaitApplied(follower, SeedKey, SeedValue);
        Assert.Equal(SeedValue, cluster.LinearizableGet(cluster.WaitLeader(), SeedKey));

        follower.Kill();
        cluster.Restart(follower);
        cluster.WaitApplied(follower, SeedKey, SeedValue);

        ProcessNode current = cluster.WaitLeader();
        Assert.Equal("OK", cluster.Call(current, "FAULT diskfull"));
        string diskFull = cluster.Call(current, "PUT extra blocked");
        Assert.False(diskFull.StartsWith("PUT ok ", StringComparison.Ordinal), cluster.Dump() + " " + diskFull);
        cluster.WaitUntil("disk-full fail-stop", () => cluster.Status(current).Fatal, TimeSpan.FromSeconds(4));
        Assert.Equal(SeedValue, cluster.LinearizableGet(cluster.WaitLeader(), SeedKey));
        current.Kill();
        cluster.Restart(current);
        cluster.WaitApplied(current, SeedKey, SeedValue);

        ProcessNode ioTarget = cluster.NodeExcept(cluster.WaitLeader());
        Assert.Equal("OK", cluster.Call(ioTarget, "FAULT io"));
        cluster.PutCommitted(cluster.WaitLeader(), "after", "io");
        Assert.Equal(SeedValue, cluster.LinearizableGet(cluster.WaitLeader(), SeedKey));
        Assert.Equal("io", cluster.LinearizableGet(cluster.WaitLeader(), "after"));
        cluster.AssertOneLeaderPerTerm();
    }

    [Fact(Skip = "SocketTransport can drop one link. It has no duplication or reordering injection, and this suite does not add a proxy.")]
    public void ProductionTransport_hasNoDupOrReorderInjection()
    {
    }

    private static void TruncateLogTail(string directory)
    {
        string path = Path.Combine(directory, FileRaftLog.FileName);
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        Assert.True(stream.Length > 1, "raft.log was not durable before truncation");
        stream.SetLength(stream.Length - 1);
    }
}

internal sealed class History : IDisposable
{
    private readonly Lock _gate = new();
    private readonly List<LinOp> _ops = [];
    private int _okPuts;

    public int OkPuts => Volatile.Read(ref _okPuts);

    public void Add(LinOp op)
    {
        if (op.Put && op.Effect == LinEffect.Ok)
        {
            Interlocked.Increment(ref _okPuts);
        }

        lock (_gate)
        {
            _ops.Add(op);
        }
    }

    public List<LinOp> Snapshot()
    {
        lock (_gate)
        {
            return [.. _ops];
        }
    }

    public void Dispose()
    {
    }
}

internal sealed class ProcessCluster : IDisposable
{
    private static readonly TimeSpan[] Elections =
    [
        TimeSpan.FromMilliseconds(180),
        TimeSpan.FromMilliseconds(420),
        TimeSpan.FromMilliseconds(900),
    ];

    private readonly string _root;
    private readonly ProcessNode[] _nodes;
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private readonly ConcurrentDictionary<long, string> _termLeader = new();
    private readonly List<string> _leaderClashes = [];
    private readonly Lock _clashGate = new();
    private bool _disposed;

    private ProcessCluster(string root, ProcessNode[] nodes)
    {
        _root = root;
        _nodes = nodes;
    }

    public static ProcessCluster Start()
    {
        string root = Path.Combine(Path.GetTempPath(), "synkrolyn-proc-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var nodes = new ProcessNode[3];
        try
        {
            for (int i = 0; i < nodes.Length; i++)
            {
                string id = "n" + (i + 1);
                nodes[i] = ProcessNode.Start(root, id, Elections[i]);
            }

            var cluster = new ProcessCluster(root, nodes);
            cluster.ExchangePeers();
            return cluster;
        }
        catch
        {
            foreach (ProcessNode node in nodes)
            {
                node?.Dispose();
            }

            throw;
        }
    }

    public long Now() => _clock.ElapsedMilliseconds;

    public ProcessNode WaitLeader()
    {
        ProcessNode? found = null;
        WaitUntil(
            "exactly one leader",
            () =>
            {
                ProcessNode? leader = null;
                foreach (ProcessNode node in _nodes)
                {
                    if (!node.IsRunning)
                    {
                        continue;
                    }

                    NodeStatus status = Status(node);
                    if (status.Role != "Leader" || status.Fatal)
                    {
                        continue;
                    }

                    if (leader is not null)
                    {
                        return false;
                    }

                    leader = node;
                }

                found = leader;
                return leader is not null;
            },
            TimeSpan.FromSeconds(8));
        return found ?? throw new InvalidOperationException(Dump());
    }

    public ProcessNode Follower()
    {
        ProcessNode leader = WaitLeader();
        return NodeExcept(leader);
    }

    public ProcessNode NodeExcept(ProcessNode excluded)
    {
        foreach (ProcessNode node in _nodes)
        {
            if (node.Id != excluded.Id && node.IsRunning)
            {
                return node;
            }
        }

        throw new InvalidOperationException("no other running node " + Dump());
    }

    public NodeStatus Status(ProcessNode node)
    {
        string line = Call(node, "STATUS");
        NodeStatus status = NodeStatus.Parse(line);
        if (status.Role == "Leader" && !status.Fatal)
        {
            lock (_clashGate)
            {
                if (_termLeader.TryGetValue(status.Term, out string? owner) && owner != node.Id)
                {
                    _leaderClashes.Add(status.Term + " " + owner + " " + node.Id);
                }
                else
                {
                    _termLeader[status.Term] = node.Id;
                }
            }
        }

        return status;
    }

    public string Call(ProcessNode node, string command)
    {
        using var client = new TcpClient();
        if (!client.ConnectAsync(IPAddress.Loopback, node.ControlPort).Wait(TimeSpan.FromSeconds(2)))
        {
            throw new TimeoutException("control connect " + node.Id + " " + Dump());
        }

        client.NoDelay = true;
        client.ReceiveTimeout = 8000;
        client.SendTimeout = 8000;
        using NetworkStream stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.ASCII, bufferSize: 128, leaveOpen: true) { AutoFlush = true, NewLine = "\n" };
        using var reader = new StreamReader(stream, Encoding.ASCII, detectEncodingFromByteOrderMarks: false, bufferSize: 128, leaveOpen: true);
        writer.WriteLine(command);
        try
        {
            return reader.ReadLine() ?? throw new IOException("control closed for " + command + " " + Dump());
        }
        catch (IOException ex)
        {
            throw new TimeoutException(command + " on " + node.Id + " " + Dump(), ex);
        }
    }

    public void PutCommitted(ProcessNode leader, string key, string value)
    {
        string line = Call(leader, "PUT " + key + " " + value);
        if (!line.StartsWith("PUT ok ", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(line + " " + Dump());
        }
    }

    public string LinearizableGet(ProcessNode leader, string key)
    {
        string? seen = null;
        WaitUntil(
            "linearizable get " + key,
            () =>
            {
                if (!leader.IsRunning)
                {
                    return false;
                }

                string line = Call(leader, "LGET " + key);
                if (line == "LGET pending" || line == "LGET notleader")
                {
                    return false;
                }

                if (!line.StartsWith("LGET ", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(line + " " + Dump());
                }

                seen = line["LGET ".Length..];
                return true;
            },
            TimeSpan.FromSeconds(6));
        return seen ?? throw new InvalidOperationException(Dump());
    }

    public void WaitApplied(ProcessNode node, string key, string value)
    {
        WaitUntil(
            node.Id + " applied " + key,
            () => node.IsRunning && Call(node, "GET " + key) == "GET " + value,
            TimeSpan.FromSeconds(8));
    }

    public Task WriteBurst(History history, string writer, int count)
    {
        return Task.Run(() =>
        {
            for (int i = 0; i < count; i++)
            {
                string key = "k" + (i % 2);
                string value = writer + "-" + i;
                long invoke = Now();
                LinEffect effect = LinEffect.Unknown;
                try
                {
                    ProcessNode leader = WaitLeader();
                    string line = Call(leader, "PUT " + key + " " + value);
                    if (line.StartsWith("PUT ok ", StringComparison.Ordinal))
                    {
                        effect = LinEffect.Ok;
                    }
                    else if (line == "PUT notleader")
                    {
                        effect = LinEffect.Fail;
                    }
                }
                catch (Exception ex) when (ex is TimeoutException or IOException or InvalidOperationException or SocketException or AggregateException)
                {
                    effect = LinEffect.Unknown;
                }

                history.Add(new LinOp(invoke, Now(), key, true, value, effect));
            }
        });
    }

    public void RecordStableReads(History history, params string[] keys)
    {
        ProcessNode leader = WaitLeader();
        foreach (string key in keys)
        {
            long invoke = Now();
            string value = LinearizableGet(leader, key);
            history.Add(new LinOp(invoke, Now(), key, false, value == "null" ? "" : value, LinEffect.Ok));
        }
    }

    public void AssertLinearizable(History history)
    {
        List<LinOp> ops = history.Snapshot();
        LinVerdict verdict = LinearizabilityChecker.Check(ops);
        Assert.True(verdict == LinVerdict.Linearizable, verdict + " " + FormatOps(ops) + " " + Dump());
    }

    public void AssertOneLeaderPerTerm()
    {
        foreach (ProcessNode node in _nodes)
        {
            if (node.IsRunning)
            {
                _ = Status(node);
            }
        }

        lock (_clashGate)
        {
            Assert.True(_leaderClashes.Count == 0, string.Join("; ", _leaderClashes) + " " + Dump());
        }
    }

    public void StopGracefully(ProcessNode node)
    {
        try
        {
            _ = Call(node, "STOP");
        }
        catch (Exception ex) when (ex is TimeoutException or IOException or InvalidOperationException)
        {
            // The process can exit as soon as it accepts STOP.
        }

        node.WaitForExit();
    }

    public void Restart(ProcessNode node)
    {
        node.WaitForExit();
        node.StartAgain();
        ExchangePeers();
        foreach (ProcessNode other in _nodes)
        {
            if (other.Id == node.Id || !other.IsRunning)
            {
                continue;
            }

            _ = Call(other, "DISCONNECT " + node.Id);
            _ = Call(other, "RECONNECT " + node.Id);
        }
    }

    public void CutLink(ProcessNode left, ProcessNode right)
    {
        Assert.Equal("OK", Call(left, "DISCONNECT " + right.Id));
        Assert.Equal("OK", Call(right, "DISCONNECT " + left.Id));
    }

    public void HealLink(ProcessNode left, ProcessNode right)
    {
        Assert.Equal("OK", Call(left, "RECONNECT " + right.Id));
        Assert.Equal("OK", Call(right, "RECONNECT " + left.Id));
    }

    public void WaitUntil(string message, Func<bool> condition, TimeSpan budget)
    {
        var started = Stopwatch.StartNew();
        while (!condition())
        {
            if (started.Elapsed > budget)
            {
                throw new TimeoutException(message + " " + Dump());
            }

            Thread.Sleep(2);
        }
    }

    public string Dump()
    {
        var text = new StringBuilder();
        foreach (ProcessNode node in _nodes)
        {
            text.Append(node.Id)
                .Append(" running=").Append(node.IsRunning)
                .Append(" raft=").Append(node.RaftPort)
                .Append(" control=").Append(node.ControlPort)
                .Append(" out=").Append(Tail(node.Output))
                .Append(" err=").Append(Tail(node.Error));
            if (node.IsRunning)
            {
                try
                {
                    text.Append(" status=").Append(Call(node, "STATUS"));
                }
                catch (Exception ex) when (ex is TimeoutException or IOException or InvalidOperationException or SocketException or AggregateException)
                {
                    text.Append(" status-error=").Append(ex.GetType().Name);
                }
            }

            text.Append(" | ");
        }

        return text.ToString();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (ProcessNode node in _nodes)
        {
            node.Dispose();
        }

        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
            // A late handle release should not hide an assertion failure.
        }
    }

    private void ExchangePeers()
    {
        foreach (ProcessNode self in _nodes)
        {
            if (!self.IsRunning)
            {
                continue;
            }

            foreach (ProcessNode peer in _nodes)
            {
                if (peer.Id == self.Id)
                {
                    continue;
                }

                string line = Call(self, "PEER " + peer.Id + " 127.0.0.1 " + peer.RaftPort);
                if (line != "OK")
                {
                    throw new InvalidOperationException(line + " " + Dump());
                }
            }
        }
    }

    private static string Tail(string text)
    {
        const int cap = 400;
        return text.Length <= cap ? text : text[^cap..];
    }

    private static string FormatOps(List<LinOp> ops)
    {
        var text = new StringBuilder();
        foreach (LinOp op in ops)
        {
            text.Append(op.Put ? "put " : "get ")
                .Append(op.Key).Append('=').Append(op.Value)
                .Append(' ').Append(op.Effect)
                .Append(" [").Append(op.Invoke).Append(',').Append(op.Response).Append("] ");
        }

        return text.ToString();
    }
}

internal sealed record NodeStatus(string Role, long Term, string Leader, long Commit, long Applied, bool QuorumLease, bool FollowerLease, bool Fatal)
{
    public static NodeStatus Parse(string line)
    {
        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0 || parts[0] != "STATUS")
        {
            throw new InvalidOperationException("bad status: " + line);
        }

        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 1; i < parts.Length; i++)
        {
            int eq = parts[i].IndexOf('=');
            if (eq <= 0)
            {
                throw new InvalidOperationException("bad status: " + line);
            }

            fields[parts[i][..eq]] = parts[i][(eq + 1)..];
        }

        return new NodeStatus(
            fields["role"],
            long.Parse(fields["term"], CultureInfo.InvariantCulture),
            fields["leader"],
            long.Parse(fields["commit"], CultureInfo.InvariantCulture),
            long.Parse(fields["applied"], CultureInfo.InvariantCulture),
            fields["quorumLease"] == "1",
            fields["followerLease"] == "1",
            fields["fatal"] == "1");
    }
}

internal sealed class ProcessNode : IDisposable
{
    private readonly string _root;
    private readonly TimeSpan _election;
    private Process? _process;
    private readonly StringBuilder _output = new();
    private readonly StringBuilder _error = new();
    private readonly Lock _textGate = new();
    private readonly ManualResetEventSlim _ready = new(false);

    private ProcessNode(string root, string id, TimeSpan election, string dataDirectory)
    {
        _root = root;
        Id = id;
        _election = election;
        DataDirectory = dataDirectory;
    }

    public string Id { get; }

    public string DataDirectory { get; }

    public int RaftPort { get; private set; }

    public int ControlPort { get; private set; }

    public bool IsRunning => _process is { HasExited: false };

    public string Output
    {
        get
        {
            lock (_textGate)
            {
                return _output.ToString();
            }
        }
    }

    public string Error
    {
        get
        {
            lock (_textGate)
            {
                return _error.ToString();
            }
        }
    }

    public static ProcessNode Start(string root, string id, TimeSpan election)
    {
        string data = Path.Combine(root, id);
        Directory.CreateDirectory(data);
        var node = new ProcessNode(root, id, election, data);
        node.Launch();
        return node;
    }

    public void StartAgain()
    {
        if (IsRunning)
        {
            throw new InvalidOperationException(Id + " is still running");
        }

        _ready.Reset();
        RaftPort = 0;
        ControlPort = 0;
        Launch();
    }

    public void Kill()
    {
        if (_process is { HasExited: false } process)
        {
            process.Kill(entireProcessTree: true);
        }

        WaitForExit();
    }

    public void WaitForExit()
    {
        if (_process is null)
        {
            return;
        }

        if (!_process.WaitForExit(8000))
        {
            _process.Kill(entireProcessTree: true);
            _process.WaitForExit(2000);
        }
    }

    public void Dispose()
    {
        try
        {
            Kill();
        }
        catch (InvalidOperationException)
        {
            // Already exited.
        }

        _process?.Dispose();
        _ready.Dispose();
    }

    private void Launch()
    {
        string dll = Path.Combine(AppContext.BaseDirectory, "Synkrolyn.Host.dll");
        if (!File.Exists(dll))
        {
            throw new FileNotFoundException("Synkrolyn.Host.dll is not in the test output", dll);
        }

        var start = new ProcessStartInfo
        {
            FileName = "dotnet",
            WorkingDirectory = AppContext.BaseDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.ArgumentList.Add("exec");
        start.ArgumentList.Add(dll);
        start.ArgumentList.Add("node");
        start.ArgumentList.Add("--id");
        start.ArgumentList.Add(Id);
        start.ArgumentList.Add("--peers");
        start.ArgumentList.Add(Peers());
        start.ArgumentList.Add("--data");
        start.ArgumentList.Add(DataDirectory);
        start.ArgumentList.Add("--election-ms");
        start.ArgumentList.Add(((int)_election.TotalMilliseconds).ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add("--heartbeat-ms");
        start.ArgumentList.Add("40");
        var process = new Process { StartInfo = start, EnableRaisingEvents = true };
        process.OutputDataReceived += (_, args) => OnLine(_output, args.Data, readyLine: true);
        process.ErrorDataReceived += (_, args) => OnLine(_error, args.Data, readyLine: false);
        if (!process.Start())
        {
            throw new InvalidOperationException("failed to start " + Id);
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        _process = process;
        var budget = Stopwatch.StartNew();
        while (!_ready.IsSet)
        {
            if (process.HasExited || budget.Elapsed > TimeSpan.FromSeconds(10))
            {
                throw new InvalidOperationException(
                    Id + " did not become ready exit=" + (process.HasExited ? process.ExitCode.ToString(CultureInfo.InvariantCulture) : "running")
                    + " out=" + Output + " err=" + Error);
            }

            Thread.Sleep(2);
        }
    }

    private string Peers()
    {
        var ids = new List<string>();
        for (int i = 1; i <= 3; i++)
        {
            string id = "n" + i;
            if (id != Id)
            {
                ids.Add(id);
            }
        }

        return string.Join(',', ids);
    }

    private void OnLine(StringBuilder sink, string? line, bool readyLine)
    {
        if (line is null)
        {
            return;
        }

        lock (_textGate)
        {
            sink.AppendLine(line);
        }

        if (!readyLine || !line.StartsWith("READY ", StringComparison.Ordinal))
        {
            return;
        }

        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 4 && parts[1] == Id && int.TryParse(parts[2], out int raft) && int.TryParse(parts[3], out int control))
        {
            RaftPort = raft;
            ControlPort = control;
            _ready.Set();
        }
    }
}
