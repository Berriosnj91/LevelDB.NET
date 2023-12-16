using System;
using System.Collections.Generic;
using System.Text;

namespace LevelDB.NET;

internal class VersionSet : IDisposable
{
    private readonly List<TableFile> m_files = [];

    public bool HasComparator { get; private set; }
    public string Comparator { get; private set; } = string.Empty;
    public bool HasLogNumber { get; private set; }
    public ulong LogNumber { get; private set; }
    public bool HasNextFileNumber { get; private set; }
    public ulong NextFileNumber { get; private set; }
    public bool HasLastSequence { get; private set; }
    public ulong LastSequence { get; private set; }
    public bool HasPrevLogNumber { get; private set; }
    public ulong PrevLogNumber { get; private set; }

    public IEnumerable<TableFile> Files => m_files.AsReadOnly();

    public void Dispose()
    {
        foreach (var file in m_files) file.Dispose();
        m_files.Clear();
    }

    public void Add(Record record)
    {
        var actionDict = new[]
        {
            Error,
            ReadComparator,
            ReadLogNumber,
            ReadNextFileNumber,
            ReadLastSequence,
            ReadCompactPointer,
            ReadDeletedFiled,
            ReadNewFile,
            Error,
            ReadPrevLogNumber
        };

        var slice = new Slice(record.Bytes);
        while (slice.Length > 0)
        {
            var type = Coding.DecodeVarint32(slice);
            actionDict[type].Invoke(slice);
        }
    }

    private static void Error(Slice slice)
    {
        throw new Exception("Unknown record type field.");
    }

    private void ReadComparator(Slice slice)
    {
        var bytes = Coding.DecodeLengthPrefixed(slice);
        Comparator = Encoding.ASCII.GetString(bytes);
        HasComparator = true;
    }

    private void ReadLogNumber(Slice slice)
    {
        LogNumber = Coding.DecodeVarint64(slice);
        HasLogNumber = true;
    }

    private void ReadNextFileNumber(Slice slice)
    {
        NextFileNumber = Coding.DecodeVarint64(slice);
        HasNextFileNumber = true;
    }

    private void ReadLastSequence(Slice slice)
    {
        LastSequence = Coding.DecodeVarint64(slice);
        HasLastSequence = true;
    }

    private static void ReadCompactPointer(Slice slice)
    {
        var level = Coding.DecodeVarint32(slice);
        var pointer = Coding.DecodeLengthPrefixed(slice);
    }

    private static void ReadDeletedFiled(Slice slice)
    {
        var level = Coding.DecodeVarint32(slice);
        var fileNr = Coding.DecodeVarint64(slice);
    }

    private void ReadNewFile(Slice slice)
    {
        var level = Coding.DecodeVarint32(slice);
        var fileNr = Coding.DecodeVarint64(slice);
        var fileSize = Coding.DecodeVarint64(slice);
        var smallest = new Key(Coding.DecodeLengthPrefixed(slice));
        var largest = new Key(Coding.DecodeLengthPrefixed(slice));

        m_files.Add(new TableFile(level, fileNr, fileSize, smallest, largest));
    }

    private void ReadPrevLogNumber(Slice slice)
    {
        PrevLogNumber = Coding.DecodeVarint64(slice);
        HasPrevLogNumber = true;
    }

    private enum Tag
    {
        kComparator = 1,
        kLogNumber = 2,
        kNextFileNumber = 3,
        kLastSequence = 4,
        kCompactPointer = 5,
        kDeletedFile = 6,
        kNewFile = 7,

        // 8 was used for large value refs
        kPrevLogNumber = 9
    }
}