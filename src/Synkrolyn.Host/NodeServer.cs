using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Synkrolyn.Kv;
using Synkrolyn.Net;
using Synkrolyn.Raft;

/// <summary>One production Raft process: file log, TCP transport, and a loopback control socket.</summary>
internal static class NodeServer
{
    private static readonly TimeSpan CommandBudget = TimeSpan.FromSeconds(3);

    public static int Run(string[] args)
    {
        if (!Options.TryParse(args, out Options options, out string error))
        {
            Console.Error.WriteLine(error);
            return 1;
        }

        var transport = new SocketTransport(options.Id, new RpcWireCodec());
        transport.PeerDeadTimeout = options.Election;
        transport.Bind();
        var log = new FileRaftLog(options.Data);
        FilePersistentState? state = null;
        RaftRuntime? runtime = null;
        TcpListener? control = null;
        try
        {
            state = new FilePersistentState(options.Data);
            var store = new InMemoryKvStore();
            var clock = new SystemRaftClock();
            var node = new RaftNode(
                options.Id,
                options.Peers,
                clock,
                transport,
                state,
                log,
                options.Election,
                options.Heartbeat,
                store);
            node.SetClockDriftBound(RaftNode.DefaultClockDriftBound);
            runtime = RaftRuntime.ForProduction(node, transport, clock);
            control = new TcpListener(IPAddress.Loopback, 0);
            control.Start();
            runtime.Start();
            int controlPort = ((IPEndPoint)control.LocalEndpoint).Port;
            Console.WriteLine(
                "READY " + options.Id + " "
                + transport.LocalPort.ToString(CultureInfo.InvariantCulture) + " "
                + controlPort.ToString(CultureInfo.InvariantCulture));
            Console.Out.Flush();
            Accept(control, node, store, log, transport, runtime);
            return 0;
        }
        finally
        {
            try
            {
                control?.Stop();
            }
            catch (SocketException)
            {
                // Accept is already unblocked.
            }

            if (runtime is not null)
            {
                runtime.Dispose();
            }
            else
            {
                transport.Dispose();
            }

            log.Dispose();
            state?.Dispose();
        }
    }

    private static void Accept(TcpListener control, RaftNode node, InMemoryKvStore store, FileRaftLog log, SocketTransport transport, RaftRuntime runtime)
    {
        var stop = new ManualResetEventSlim(false);
        while (!stop.IsSet)
        {
            TcpClient client;
            try
            {
                client = control.AcceptTcpClient();
            }
            catch (Exception ex) when (ex is SocketException or InvalidOperationException or ObjectDisposedException)
            {
                if (stop.IsSet)
                {
                    break;
                }

                throw;
            }

            var thread = new Thread(() => Serve(client, node, store, log, transport, runtime, stop, control))
            {
                IsBackground = true,
                Name = "synkrolyn-control-" + node.NodeId,
            };
            thread.Start();
        }
    }

    private static void Serve(
        TcpClient client,
        RaftNode node,
        InMemoryKvStore store,
        FileRaftLog log,
        SocketTransport transport,
        RaftRuntime runtime,
        ManualResetEventSlim stop,
        TcpListener control)
    {
        using (client)
        using (NetworkStream stream = client.GetStream())
        using (var reader = new StreamReader(stream, leaveOpen: true))
        using (var writer = new StreamWriter(stream, leaveOpen: true) { AutoFlush = true, NewLine = "\n" })
        {
            var kv = new KvClient(node, store);
            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                writer.WriteLine(Execute(line, kv, node, store, log, transport, runtime));
                if (line == "STOP")
                {
                    stop.Set();
                    control.Stop();
                    return;
                }
            }
        }
    }

    private static string Execute(
        string line,
        KvClient kv,
        RaftNode node,
        InMemoryKvStore store,
        FileRaftLog log,
        SocketTransport transport,
        RaftRuntime runtime)
    {
        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return "ERR empty";
        }

        switch (parts[0])
        {
            case "STATUS":
                return Status(node, runtime);
            case "GET":
                return parts.Length == 2 ? "GET " + Shown(store.Get(parts[1])) : "ERR get";
            case "PUT":
                return parts.Length == 3 ? Put(kv, node, parts[1], parts[2]) : "ERR put";
            case "LGET":
                return parts.Length == 2 ? LinearizableGet(kv, node, store, parts[1]) : "ERR lget";
            case "BGET":
                return parts.Length == 2 ? BoundedGet(kv, node, parts[1]) : "ERR bget";
            case "PEER":
                return Peer(transport, parts);
            case "DISCONNECT":
                return parts.Length == 2 ? Disconnect(transport, parts[1]) : "ERR disconnect";
            case "RECONNECT":
                return parts.Length == 2 ? Reconnect(transport, parts[1]) : "ERR reconnect";
            case "FAULT":
                return Fault(log, parts);
            case "STOP":
                return "OK";
            default:
                return "ERR command";
        }
    }

    private static string Status(RaftNode node, RaftRuntime runtime)
    {
        string leader = string.IsNullOrEmpty(node.LeaderId) ? "-" : node.LeaderId;
        string uncaught = runtime.Uncaught is null ? "-" : runtime.Uncaught.GetType().Name;
        return "STATUS role=" + node.Role
            + " term=" + node.CurrentTerm.ToString(CultureInfo.InvariantCulture)
            + " leader=" + leader
            + " commit=" + node.CommitIndex.ToString(CultureInfo.InvariantCulture)
            + " applied=" + node.LastApplied.ToString(CultureInfo.InvariantCulture)
            + " quorumLease=" + Bit(node.QuorumLeaseValid)
            + " followerLease=" + Bit(node.FollowerReadLeaseValid)
            + " fatal=" + Bit(node.Fatal)
            + " uncaught=" + uncaught
            + " contactAge=" + node.MillisSinceLeaderContact.ToString(CultureInfo.InvariantCulture);
    }

    private static string Put(KvClient kv, RaftNode node, string key, string value)
    {
        if (node.Fatal || node.Role != Role.Leader)
        {
            return "PUT notleader";
        }

        kv.Put(key, value);
        var wait = System.Diagnostics.Stopwatch.StartNew();
        while (wait.Elapsed < CommandBudget)
        {
            if (node.Fatal)
            {
                return "PUT fatal";
            }

            long? index = kv.AwaitCommitted();
            if (index is not null)
            {
                return "PUT ok " + index.Value.ToString(CultureInfo.InvariantCulture);
            }

            if (node.Role != Role.Leader)
            {
                return "PUT unknown";
            }

            Thread.Sleep(2);
        }

        return "PUT timeout";
    }

    private static string LinearizableGet(KvClient kv, RaftNode node, InMemoryKvStore store, string key)
    {
        var wait = System.Diagnostics.Stopwatch.StartNew();
        while (wait.Elapsed < CommandBudget)
        {
            if (node.Fatal || node.Role != Role.Leader)
            {
                return "LGET notleader";
            }

            string? value = kv.LinearizableGet(key);
            if (value is not null)
            {
                return "LGET " + value;
            }

            if (node.ReadIndexSatisfied())
            {
                return "LGET " + Shown(store.Get(key));
            }

            Thread.Sleep(2);
        }

        return "LGET pending";
    }

    private static string BoundedGet(KvClient kv, RaftNode node, string key)
    {
        if (node.Fatal)
        {
            return "BGET refused";
        }

        bool lease = node.Role == Role.Leader
            ? node.QuorumLeaseValid
            : node.Role == Role.Follower && node.FollowerReadLeaseValid;
        if (!lease)
        {
            return "BGET refused";
        }

        return "BGET " + Shown(kv.BoundedStaleGet(key));
    }

    private static string Peer(SocketTransport transport, string[] parts)
    {
        if (parts.Length != 4 || !IPAddress.TryParse(parts[2], out IPAddress? address) || !int.TryParse(parts[3], NumberStyles.None, CultureInfo.InvariantCulture, out int port))
        {
            return "ERR peer";
        }

        try
        {
            transport.SetPeer(parts[1], new IPEndPoint(address, port));
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException)
        {
            return "ERR peer";
        }

        return "OK";
    }

    private static string Disconnect(SocketTransport transport, string peer)
    {
        try
        {
            transport.DisconnectPeer(peer);
        }
        catch (ArgumentException)
        {
            return "ERR disconnect";
        }

        return "OK";
    }

    private static string Reconnect(SocketTransport transport, string peer)
    {
        transport.ReconnectPeer(peer);
        return "OK";
    }

    private static string Fault(FileRaftLog log, string[] parts)
    {
        if (parts.Length != 2)
        {
            return "ERR fault";
        }

        if (parts[1] == "io")
        {
            log.FailNextForce(diskFull: false);
            return "OK";
        }

        if (parts[1] == "diskfull")
        {
            log.FailNextForce(diskFull: true);
            return "OK";
        }

        return "ERR fault";
    }

    private static string Shown(string? value) => value ?? "null";

    private static string Bit(bool value) => value ? "1" : "0";

    private sealed record Options(string Id, string[] Peers, string Data, TimeSpan Election, TimeSpan Heartbeat)
    {
        public static bool TryParse(string[] args, out Options options, out string error)
        {
            options = new Options("", [], "", TimeSpan.Zero, TimeSpan.Zero);
            var map = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int i = 0; i < args.Length; i++)
            {
                string token = args[i];
                if (!token.StartsWith("--", StringComparison.Ordinal) || i + 1 >= args.Length)
                {
                    error = "usage: node --id ID --peers ID,ID --data DIR --election-ms N --heartbeat-ms N";
                    return false;
                }

                map[token[2..]] = args[++i];
            }

            if (!map.TryGetValue("id", out string? id) || string.IsNullOrWhiteSpace(id)
                || !map.TryGetValue("peers", out string? peersText)
                || !map.TryGetValue("data", out string? data) || string.IsNullOrWhiteSpace(data)
                || !map.TryGetValue("election-ms", out string? electionText)
                || !map.TryGetValue("heartbeat-ms", out string? heartbeatText)
                || !int.TryParse(electionText, NumberStyles.None, CultureInfo.InvariantCulture, out int electionMs)
                || !int.TryParse(heartbeatText, NumberStyles.None, CultureInfo.InvariantCulture, out int heartbeatMs)
                || electionMs <= 0
                || heartbeatMs <= 0)
            {
                error = "missing or invalid node arguments";
                return false;
            }

            string[] peers = peersText.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (peers.Length == 0 || peers.Contains(id, StringComparer.Ordinal))
            {
                error = "peers must be other node ids";
                return false;
            }

            options = new Options(id, peers, data, TimeSpan.FromMilliseconds(electionMs), TimeSpan.FromMilliseconds(heartbeatMs));
            error = "";
            return true;
        }
    }
}
