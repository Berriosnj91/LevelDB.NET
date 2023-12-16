using System.Diagnostics;
using System.Text;

namespace LevelDB.NET;

internal enum KeyType
{
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
}

internal class Key
{
    public Key(byte[] data)
        : this(new Slice(data))
    {
    }

    public Key(Slice data)
    {
        var n = data.Length;
        Debug.Assert(n >= 8);

        var num = Coding.DecodeFixed64(data.NewSlice(n - 8, 8));

        var c = unchecked((byte)(num & 0xff));
        Debug.Assert(c <= (byte)KeyType.kTypeValue);

        Type = (KeyType)c;
        Sequence = num >> 8;
        UserKey = data.NewSlice(0, n - 8).ToArray();
    }

    public byte[] UserKey { get; }
    public ulong Sequence { get; }
    public KeyType Type { get; }

    public override string ToString()
    {
        return Encoding.UTF8.GetString(UserKey);
    }
}