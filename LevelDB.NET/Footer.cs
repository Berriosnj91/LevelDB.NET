using System;

namespace LevelDB.NET;

internal class Footer(BlockHandle metaIndexHandle, BlockHandle indexHandle)
{
    public const uint EncodedLength = 2 * BlockHandle.MaxEncodedLength + 8;
    public const ulong TableMagicNumber = 0xdb4775248b80fb57;

    public BlockHandle MetaIndexHandle { get; } = metaIndexHandle;
    public BlockHandle IndexHandle { get; } = indexHandle;

    public static Footer DecodeFrom(Slice slice)
    {
        var startOffset = slice.Offset;
        var startLength = slice.Length;

        var magicLo = Coding.DecodeFixed32(slice.NewSlice(EncodedLength - 8, 4));
        var magicHi = Coding.DecodeFixed32(slice.NewSlice(EncodedLength - 4, 4));
        var magic = ((ulong)magicHi << 32) | magicLo;

        if (magic != TableMagicNumber) throw new Exception("not an sstable (bad magic number)");

        var metaIndexHandle = BlockHandle.DecodeFrom(slice);
        var indexHandle = BlockHandle.DecodeFrom(slice);

        slice.Update(startOffset + EncodedLength, startLength - EncodedLength);
        return new Footer(metaIndexHandle, indexHandle);
    }
}