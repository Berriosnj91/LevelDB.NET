using System;
using System.Collections.Generic;

namespace LevelDB.NET;

public class BytewiseComparator : IComparer<byte[]>
{
    public int Compare(byte[]? x, byte[]? y)
    {
        return MemoryExtensions.SequenceCompareTo<byte>(x, y);
    }
}