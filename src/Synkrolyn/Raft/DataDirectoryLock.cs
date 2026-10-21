namespace Synkrolyn.Raft;

/// <summary>
/// Exclusive lock for a whole data directory. One process holds <c>directory.lock</c>.
/// Several components in this process share that hold. A second open of the same component throws.
/// </summary>
internal sealed class DataDirectoryLock : IDisposable
{
    private static readonly Dictionary<string, Hold> Held = [];
    private readonly string _directory;
    private readonly string _component;
    private bool _released;

    private DataDirectoryLock(string directory, string component)
    {
        _directory = directory;
        _component = component;
    }

    /// <summary>
    /// Shares the directory hold when <paramref name="component"/> is new in this process.
    /// A second component name in the same process shares the file. Another process cannot open it.
    /// </summary>
    public static DataDirectoryLock Acquire(string directory, string component)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentException.ThrowIfNullOrWhiteSpace(component);
        string key = Path.GetFullPath(directory);
        lock (Held)
        {
            if (!Held.TryGetValue(key, out Hold? hold))
            {
                Directory.CreateDirectory(key);
                string path = Path.Combine(key, "directory.lock");
                var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                hold = new Hold(stream);
                Held[key] = hold;
            }

            if (!hold.Components.Add(component))
            {
                throw new IOException("data directory is already open: " + directory);
            }

            hold.Refs++;
            return new DataDirectoryLock(key, component);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (Held)
        {
            if (_released)
            {
                return;
            }

            _released = true;
            if (!Held.TryGetValue(_directory, out Hold? hold))
            {
                return;
            }

            hold.Components.Remove(_component);
            hold.Refs--;
            if (hold.Refs > 0)
            {
                return;
            }

            Held.Remove(_directory);
            hold.Stream.Dispose();
        }
    }

    private sealed class Hold(FileStream stream)
    {
        public FileStream Stream { get; } = stream;

        public int Refs { get; set; }

        public HashSet<string> Components { get; } = [];
    }
}
