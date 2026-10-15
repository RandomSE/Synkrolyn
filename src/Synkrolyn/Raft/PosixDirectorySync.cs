using System.Runtime.InteropServices;

namespace Synkrolyn.Raft;

/// <summary>fsync of a directory. .NET file streams cannot open a directory on Linux.</summary>
internal static class PosixDirectorySync
{
    public static void Sync(string directory)
    {
        int fd = Open(directory, 0);
        if (fd < 0)
        {
            throw new IOException("directory open failed: " + directory + " errno " + Marshal.GetLastPInvokeError());
        }

        try
        {
            if (Fsync(fd) != 0)
            {
                throw new IOException("directory fsync failed: " + directory + " errno " + Marshal.GetLastPInvokeError());
            }
        }
        finally
        {
            _ = Close(fd);
        }
    }

#pragma warning disable SYSLIB1054
#pragma warning disable CA2101 // pathname is marshaled as UTF-8 below
    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int Open([MarshalAs(UnmanagedType.LPUTF8Str)] string pathname, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int Fsync(int fd);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int Close(int fd);
#pragma warning restore CA2101
#pragma warning restore SYSLIB1054
}
