using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LevelDB.NET;

internal class TableFile(uint level, ulong fileNr, ulong fileSize, Key smallest, Key largest)
    : IDisposable, IEnumerable<KeyValuePair<byte[], byte[]>>
{
    private const int NoCompression = 0x0;
    private const int SnappyCompression = 0x1;
    private LruCache<BlockHandle, Block>? m_blockCache;
    private Block? m_indexBlock;

    private FileStream? m_stream;
    public uint Level { get; } = level;
    public ulong FileNr { get; } = fileNr;
    public ulong FileSize { get; } = fileSize;
    public Key Smallest { get; } = smallest;
    public Key Largest { get; } = largest;

    public void Dispose()
    {
        Close();
    }

    public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
    {
        return GetAllItems().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetAllItems().GetEnumerator();
    }

    public bool ContainsKey(byte[] key, IComparer<byte[]> comparer)
    {
        if (m_indexBlock != null && m_indexBlock.BinarySearch(key, comparer, out var handle))
        {
            var block = GetBlock(handle);
            return block.ContainsKey(key, comparer);
        }

        return false;
    }

    public bool TryGetValue(byte[] key, IComparer<byte[]> comparer, out byte[]? value)
    {
        if (m_indexBlock != null && m_indexBlock.BinarySearch(key, comparer, out var handle))
        {
            var block = GetBlock(handle);
            return block.TryGetValue(key, comparer, out value);
        }

        value = null;
        return false;
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Filter(IFilter filter)
    {
        if (m_indexBlock == null) yield break;
        foreach (var indexItem in m_indexBlock.Filter(filter, Largest.UserKey))
        {
            var block = GetBlock(indexItem.Value);
            foreach (var item in block.Match(filter)) yield return item;
        }
    }

    private Block GetBlock(byte[] key)
    {
        if (m_stream == null) throw new Exception("TableFile not open");
        var handle = BlockHandle.DecodeFrom(new Slice(key));
        if (m_blockCache != null && m_blockCache.TryGetValue(handle, out var block)) return block;

        block = ReadBlock(m_stream, handle);
        m_blockCache?.Add(handle, block);
        return block;
    }

    public void AssureOpen(string path, LruCache<BlockHandle, Block> sharedCache)
    {
        if (m_stream != null) return;
        m_stream = File.OpenRead(Path.Combine(path, $"{FileNr:D6}.ldb"));

        var footerBytes = new byte[Footer.EncodedLength];
        m_stream.Position = m_stream.Length - Footer.EncodedLength;
        m_stream.Read(footerBytes);

        var footer = Footer.DecodeFrom(new Slice(footerBytes));

        m_indexBlock = ReadBlock(m_stream, footer.IndexHandle);
        m_blockCache = sharedCache;
    }

    public void Close()
    {
        m_stream?.Dispose();
        m_stream = null;

        m_indexBlock = null;
        m_blockCache = null;
    }

    private static Block ReadBlock(Stream stream, BlockHandle handle)
    {
        var n = (uint)handle.Size;

        stream.Position = (long)handle.Offset;
        using var reader = new BinaryReader(stream, Encoding.UTF8, true);
        var buf = reader.ReadBytes(unchecked((int)n + 1));
        var encoding = buf[n];
        _ = reader.ReadInt32();

        switch (encoding)
        {
            case NoCompression:
                return new Block(new Slice(buf, 0, n));

            case SnappyCompression:
                buf = SnappyDecompressor.Decompress(buf, 0, unchecked((int)n));
                return new Block(new Slice(buf));
        }

        throw new Exception("bad block type");
    }

    private IEnumerable<KeyValuePair<byte[], byte[]>> GetAllItems()
    {
        if (m_indexBlock == null) yield break;

        foreach (var indexItem in m_indexBlock)
        {
            var block = GetBlock(indexItem.Value);
            foreach (var item in block) yield return item;
        }
    }
}