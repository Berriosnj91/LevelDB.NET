using System;

namespace LevelDB.NET;

internal class InternalKey
{
    private readonly byte[] m_bytes = new byte[128];

    public uint Length { get; private set; }

    public void Clear() => Length = 0;

    public void Resize(uint size)
    {
        Length = size;
    }

    public void Append(Slice slice)
    {
        for (uint i = 0; i < slice.Length; ++i)
        {
            m_bytes[Length] = slice[i];
            Length++;
        }
    }

    public Slice Slice(uint offset, uint length)
    {
        offset = Math.Min(Length, offset);
        length = Math.Min(Length - offset, length);
        return new Slice(m_bytes, offset, length);
    }
}