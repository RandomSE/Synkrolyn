namespace Synkrolyn.Raft;

/// <summary>Exclusive lock for one component of a data directory.</summary>
internal sealed class DataDirectoryLock : IDisposable
{
    private static readonly Dictionary<string, FileStream> Held = [];
    private readonly string _key;
    private bool _released;

    private DataDirectoryLock(string key) => _key = key;

    /// <summary>Fails when this process or another already holds <paramref name="component"/> in <paramref name="directory"/>.</summary>
    public static DataDirectoryLock Acquire(string directory, string component)
    {
        ArgumentNullException.ThrowIfNull(directory);
        string key = Path.GetFullPath(directory) + "\0" + component;
        lock (Held)
        {
            if (Held.ContainsKey(key))
            {
                throw new IOException("data directory is already open: " + directory);
            }

            Directory.CreateDirectory(directory);
            string path = Path.Combine(directory, component + ".lock");
            var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            Held[key] = stream;
            return new DataDirectoryLock(key);
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
            if (Held.Remove(_key, out FileStream? stream))
            {
                stream.Dispose();
            }
        }
    }
}
