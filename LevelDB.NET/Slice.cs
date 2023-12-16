using System;
using System.Collections;
using System.Collections.Generic;

namespace LevelDB.NET;

internal class Slice : IEnumerable<byte>
{
    private readonly byte[] m_bytes;

    public Slice(byte[] bytes)
    {
        m_bytes = bytes;
        Length = (uint)bytes.Length;
        Offset = 0;
    }

    public Slice(byte[] bytes, uint offset, uint length)
    {
        m_bytes = bytes;
        Offset = Math.Min((uint)m_bytes.Length, offset);
        Length = Math.Min((uint)m_bytes.Length - Offset, length);
    }

    public uint Length { get; private set; }

    public uint Offset { get; private set; }

    public byte this[uint index] => m_bytes[Offset + index];

    public IEnumerator<byte> GetEnumerator()
    {
        return new SliceEnumerator(m_bytes, Offset, Length);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return new SliceEnumerator(m_bytes, Offset, Length);
    }

    internal byte[] ToArray()
    {
        var result = new byte[Length];
        Array.Copy(m_bytes, Offset, result, 0, Length);
        return result;
    }

    public Slice NewSlice(uint offset, uint length)
    {
        return new Slice(m_bytes, Offset + offset, length);
    }

    public Slice NewSlice(uint offset)
    {
        return new Slice(m_bytes, Offset + offset, Length - offset);
    }

    public void Update(uint offset, uint length)
    {
        Offset = offset;
        Length = length;
    }

    public byte ReadByte()
    {
        var result = m_bytes[Offset];
        Offset++;
        Length--;
        return result;
    }

    public byte[] ReadBytes(uint length)
    {
        var result = new byte[length];
        for (var i = 0; i < length; ++i) result[i] = m_bytes[Offset + i];

        Offset += length;
        Length -= length;
        return result;
    }

    private class SliceEnumerator(IReadOnlyList<byte> bytes, uint offset, uint length) : IEnumerator<byte>
    {
        private int m_index = -1;

        public byte Current => bytes[(int)(offset + m_index)];

        object IEnumerator.Current => bytes[(int)(offset + m_index)];

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            m_index++;
            return m_index >= 0 && m_index < length;
        }

        public void Reset()
        {
            m_index = -1;
        }
    }
}