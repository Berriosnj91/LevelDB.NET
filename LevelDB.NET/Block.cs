using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace LevelDB.NET;

internal class Block : IEnumerable<KeyValuePair<byte[], byte[]>>
{
    private readonly List<byte[]> m_keys = [];
    private readonly List<byte[]> m_values = [];

    public Block(Slice data)
    {
        if (data.Length < sizeof(uint)) throw new Exception("bad block contents");

        // restart points are recorded at the end of the buffer.
        var numRestarts = Coding.DecodeFixed32(data.NewSlice(data.Length - sizeof(uint), sizeof(uint)));
        var restartOffset = data.Length - (1 + numRestarts) * sizeof(uint);

        // we don't really care for these except the first, but we'll parse them.
        var restartPoints = new uint[numRestarts];
        for (uint i = 0; i < numRestarts; ++i)
        {
            var offset = restartOffset + i * sizeof(uint);
            restartPoints[i] = Coding.DecodeFixed32(data.NewSlice(offset, sizeof(uint)));
        }

        // block data is the remaining data.
        var blockData = data.NewSlice(0, restartOffset);

        // parse this block.
        var key = new InternalKey();
        var value = blockData.NewSlice(0, 0);

        var current = restartPoints[0];
        while (current < blockData.Length)
        {
            var p = blockData.NewSlice(current);

            // Decode next entry
            p = DecodeEntry(p, out var shared, out var nonShared, out var valueLength);
            if (p == null || key.Length < shared) throw new Exception("bad entry in block");

            key.Resize(shared);
            key.Append(p.NewSlice(0, nonShared));

            value.Update(p.Offset + nonShared, valueLength);

            // don't add deleted keys to the dictionary.
            var parsedKey = new Key(key.Slice(0, key.Length));
            if (parsedKey.Type == KeyType.kTypeValue)
            {
                m_keys.Add(parsedKey.UserKey);
                m_values.Add(value.ToArray());
            }

            current = value.Offset + value.Length;
        }
    }

    public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
    {
        return GetAllItems().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetAllItems().GetEnumerator();
    }

    public bool BinarySearch(byte[] key, IComparer<byte[]> comparer, [NotNullWhen(true)] out byte[]? value)
    {
        var idx = m_keys.BinarySearch(key, comparer);
        if (idx >= 0)
        {
            value = m_values[idx];
            return true;
        }

        idx = ~idx;
        if (idx < m_keys.Count)
        {
            value = m_values[idx];
            return true;
        }

        value = null;
        return false;
    }

    public bool TryGetValue(byte[] key, IComparer<byte[]> comparer, [NotNullWhen(true)] out byte[]? value)
    {
        var idx = m_keys.BinarySearch(key, comparer);
        if (idx >= 0)
        {
            value = m_values[idx];
            return true;
        }

        value = null;
        return false;
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Filter(IFilter filter, byte[] largest)
    {
        var count = m_keys.Count;
        for (var i = 0; i < count; ++i)
            if (filter.Compare(m_keys[i]) <= 0)
                yield return new KeyValuePair<byte[], byte[]>(m_keys[i], m_values[i]);
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Match(IFilter filter)
    {
        for (var i = 0; i < m_keys.Count; ++i)
            if (filter.Compare(m_keys[i]) == 0)
                yield return new KeyValuePair<byte[], byte[]>(m_keys[i], m_values[i]);
    }

    public bool ContainsKey(byte[] key, IComparer<byte[]> comparer)
    {
        return m_keys.BinarySearch(key, comparer) >= 0;
    }

    private static Slice? DecodeEntry(Slice p, out uint shared, out uint nonShared, out uint valueLength)
    {
        if (p.Length < 3)
        {
            shared = 0;
            nonShared = 0;
            valueLength = 0;
            return null;
        }

        shared = p[0];
        nonShared = p[1];
        valueLength = p[2];
        if ((shared | nonShared | valueLength) < 128)
            // Fast path: all three values are encoded in one byte each
            return p.NewSlice(3);

        shared = Coding.DecodeVarint32(p);
        nonShared = Coding.DecodeVarint32(p);
        valueLength = Coding.DecodeVarint32(p);

        return p.Length < nonShared + valueLength ? null : p;
    }

    private IEnumerable<KeyValuePair<byte[], byte[]>> GetAllItems()
    {
        return m_keys.Select((t, i) => new KeyValuePair<byte[], byte[]>(t, m_values[i]));
    }
}