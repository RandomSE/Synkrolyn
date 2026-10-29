using System.Diagnostics;
using System.Globalization;
using System.Text;
using Synkrolyn.Net;
using Synkrolyn.Raft;

namespace Synkrolyn.Bench;

/// <summary>
/// One voter, a real <see cref="FileRaftLog"/> and <see cref="FilePersistentState"/>.
/// Each propose is drained, so the log fsync and the per-append directory fsync run on the write.
/// Not part of <c>dotnet test</c>.
/// </summary>
internal static class FileLogBench
{
    public static int Run(BenchConfig config)
    {
        var report = new StringBuilder();
        void Line(string text)
        {
            Console.WriteLine(text);
            _ = report.AppendLine(text);
        }

        Line("label=" + config.Label);
        Line("log=file");
        Line("writes=" + config.Writes.ToString(CultureInfo.InvariantCulture));
        Line("trials=" + config.FileTrials.ToString(CultureInfo.InvariantCulture));
        var ops = new List<double>(config.FileTrials);
        var latency = new List<double>(config.FileTrials);
        var alloc = new List<double>(config.FileTrials);
        for (int trial = 1; trial <= config.FileTrials; trial++)
        {
            Trial sample = Measure(config.Writes);
            ops.Add(sample.OpsPerSec);
            latency.Add(sample.LatencyP50Us);
            alloc.Add(sample.AllocPerWrite);
            Line("trial=" + trial.ToString(CultureInfo.InvariantCulture)
                + " ops_s=" + sample.OpsPerSec.ToString("F1", CultureInfo.InvariantCulture)
                + " p50_us=" + sample.LatencyP50Us.ToString("F1", CultureInfo.InvariantCulture)
                + " alloc_bytes=" + sample.AllocPerWrite.ToString("F1", CultureInfo.InvariantCulture));
        }

        Line("file_write_ops_s_p50=" + Format(Median(ops)));
        Line("file_write_ops_s_spread=" + Format(Spread(ops)));
        Line("file_write_p50_us_median=" + Format(Median(latency)));
        Line("file_write_p50_us_spread=" + Format(Spread(latency)));
        Line("file_alloc_bytes_median=" + Format(Median(alloc)));
        Line("file_alloc_bytes_spread=" + Format(Spread(alloc)));
        if (!string.IsNullOrWhiteSpace(config.OutPath))
        {
            File.WriteAllText(config.OutPath, report.ToString());
        }

        return 0;
    }

    private static Trial Measure(int writes)
    {
        string root = Directory.CreateTempSubdirectory("synk-file-bench").FullName;
        var clock = new SystemRaftClock();
        var transport = new InMemoryTransport(clock);
        var log = new FileRaftLog(root);
        var state = new FilePersistentState(root);
        var raft = new RaftNode(
            "n1",
            Array.Empty<string>(),
            clock,
            transport,
            state,
            log,
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromMilliseconds(20));
        try
        {
            raft.SetElectionJitter(static timeout => timeout);
            raft.Start();
            var wait = Stopwatch.StartNew();
            while (raft.Role != Role.Leader)
            {
                raft.EnqueueTick();
                _ = raft.Drain();
                if (wait.Elapsed > TimeSpan.FromSeconds(2))
                {
                    throw new TimeoutException("file-log node did not become leader");
                }

                Thread.Sleep(5);
            }

            byte[] warm = Encoding.UTF8.GetBytes("warm");
            for (int i = 0; i < 8; i++)
            {
                if (raft.Propose(warm) is null)
                {
                    throw new InvalidOperationException("warmup propose failed");
                }

                _ = raft.Drain();
            }

            byte[] command = Encoding.UTF8.GetBytes("v");
            var samples = new double[writes];
            long before = GC.GetTotalAllocatedBytes(true);
            for (int i = 0; i < writes; i++)
            {
                long start = Stopwatch.GetTimestamp();
                if (raft.Propose(command) is null)
                {
                    throw new InvalidOperationException("propose failed at " + i.ToString(CultureInfo.InvariantCulture));
                }

                _ = raft.Drain();
                samples[i] = Stopwatch.GetElapsedTime(start).TotalMicroseconds;
            }

            long bytes = GC.GetTotalAllocatedBytes(true) - before;
            Array.Sort(samples);
            double p50 = samples[samples.Length / 2];
            double seconds = 0;
            for (int i = 0; i < samples.Length; i++)
            {
                seconds += samples[i];
            }

            seconds /= 1_000_000d;
            return new Trial(writes / seconds, p50, bytes / (double)writes);
        }
        finally
        {
            log.Dispose();
            state.Dispose();
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
                // The next trial uses a new directory.
            }
        }
    }

    private static double Median(List<double> values)
    {
        values.Sort();
        return values[values.Count / 2];
    }

    private static double Spread(List<double> values)
    {
        values.Sort();
        return values[^1] - values[0];
    }

    private static string Format(double value) => value.ToString("F1", CultureInfo.InvariantCulture);

    private readonly record struct Trial(double OpsPerSec, double LatencyP50Us, double AllocPerWrite);
}
