using System;

namespace LevelDB.NET;

public class StartsWithFilter(byte[] key) : IFilter
{
    public int Compare(byte[] key1)
    {
        var minLen = Math.Min(key1.Length, key.Length);
        var cut = key1[..minLen];
        return MemoryExtensions.SequenceCompareTo<byte>(key, cut);
    }
}