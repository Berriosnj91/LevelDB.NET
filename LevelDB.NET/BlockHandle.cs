using System;

namespace LevelDB.NET;

internal readonly struct BlockHandle(ulong offset, ulong size) : IComparable<BlockHandle>
{
    public const uint MaxEncodedLength = 10 + 10;

    public ulong Offset { get; } = offset;
    public ulong Size { get; } = size;

    public static BlockHandle DecodeFrom(Slice input)
    {
        var offset = Coding.DecodeVarint64(input);
        var size = Coding.DecodeVarint64(input);
        return new BlockHandle(offset, size);
    }

    public override bool Equals(object? obj)
    {
        if (obj is BlockHandle other)
        {
            return Offset == other.Offset && Size == other.Size;
        }

        return false;
    }

    public override int GetHashCode()
    {
        return Offset.GetHashCode() ^ Size.GetHashCode();
    }

    public override string ToString()
    {
        return $"{Offset}, {Size}";
    }

    public int CompareTo(BlockHandle other)
    {
        var s = Offset.CompareTo(other.Offset);
        if (s == 0) s = Size.CompareTo(other.Size);
        return s;
    }
}