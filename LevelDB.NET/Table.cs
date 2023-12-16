using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace LevelDB.NET;

public class Table : IDisposable, IReadOnlyDictionary<byte[], byte[]>
{
    private readonly string m_path;
    private readonly LruCache<BlockHandle, Block> m_sharedCache = new(16);
    private readonly VersionSet m_versionSet;

    private Table(string path, VersionSet versionSet)
    {
        m_versionSet = versionSet;
        m_path = path;

        if (!m_versionSet.HasComparator) return;
        if (m_versionSet.Comparator == "leveldb.BytewiseComparator")
            Comparator = new BytewiseComparator();
        else
            throw new Exception("Not supported yet");
    }

    public IComparer<byte[]> Comparator { get; set; } = new BytewiseComparator();

    public void Dispose()
    {
        m_versionSet.Dispose();
    }

    public IEnumerable<byte[]> Keys
    {
        get { return GetAllItems().Select(s => s.Key); }
    }

    public IEnumerable<byte[]> Values
    {
        get { return GetAllItems().Select(s => s.Value); }
    }

    public int Count => throw new NotSupportedException("Count would have to parse the entire database");

    public byte[] this[byte[] key]
    {
        get
        {
            if (TryGetValue(key, out var result)) return result;
            throw new Exception("Key not found in Table");
        }
    }

#pragma warning disable CS8767 // Nullability of reference types in type of parameter doesn't match implicitly implemented member (possibly because of nullability attributes).
    public bool TryGetValue(byte[] key, [NotNullWhen(true)] out byte[]? value)
#pragma warning restore CS8767 // Nullability of reference types in type of parameter doesn't match implicitly implemented member (possibly because of nullability attributes).
    {
        if (TryFindFile(key, out var file))
        {
            file.AssureOpen(m_path, m_sharedCache);
            return file.TryGetValue(key, Comparator, out value);
        }

        value = null;
        return false;
    }

    public bool ContainsKey(byte[] key)
    {
        if (!TryFindFile(key, out var file)) return false;
        file.AssureOpen(m_path, m_sharedCache);
        return file.ContainsKey(key, Comparator);

    }

    public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
    {
        return GetAllItems().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetAllItems().GetEnumerator();
    }

    public IEnumerable<KeyValuePair<byte[], byte[]>> Filter(IFilter filter)
    {
        foreach (var file in m_versionSet.Files)
            if (filter.Compare(file.Smallest.UserKey) >= 0 && filter.Compare(file.Largest.UserKey) <= 0)
            {
                file.AssureOpen(m_path, m_sharedCache);
                foreach (var item in file.Filter(filter)) yield return item;
            }
    }

    private bool TryFindFile(byte[] key, [NotNullWhen(true)] out TableFile? file)
    {
        // This could probably be a binary seach, but I'm too lazy.
        foreach (var f in m_versionSet.Files)
            if (Comparator.Compare(key, f.Smallest.UserKey) >= 0 &&
                Comparator.Compare(key, f.Largest.UserKey) <= 0)
            {
                file = f;
                return true;
            }

        file = null;
        return false;
    }

    public static Table OpenRead(string directory)
    {
        // get the filename of the manifest.
        var current = File.ReadAllLines(Path.Combine(directory, "CURRENT"));
        if (current.Length < 1) throw new Exception("Invalid CURRENT file.");

        // read the manifest file.
        var versionSet = new VersionSet();

        var block = new byte[32768];
        using (var file = File.OpenRead(Path.Combine(directory, current[0])))
        {
            var recordReader = new RecordReader();
            var numRead = 0;
            do
            {
                numRead = file.Read(block);
                if (numRead <= 0) continue;
                var sequence = new ReadOnlySequence<byte>(block);
                var reader = new SequenceReader<byte>(sequence);
                while (reader.Remaining > 6)
                    if (!recordReader.From(ref reader, record => versionSet.Add(record)))
                        break;
            } while (numRead >= block.Length);
        }

        return new Table(directory, versionSet);
    }

    private IEnumerable<KeyValuePair<byte[], byte[]>> GetAllItems()
    {
        foreach (var f in m_versionSet.Files)
        {
            f.AssureOpen(m_path, m_sharedCache);
            foreach (var item in f) yield return item;
            f.Close();
        }
    }

    private class RecordReader
    {
        private Record m_record = new();

        public bool From(ref SequenceReader<byte> reader, Action<Record> action)
        {
            reader.TryReadLittleEndian(out int _);
            reader.TryReadLittleEndian(out short length);
            reader.TryRead(out var type);
            if (type == 0) return false;

            var bytes = m_record.Reserve(length);
            if (reader.TryCopyTo(bytes)) reader.Advance(length);

            if (type is not (1 or 4)) return true; // FULL or LAST.
            action.Invoke(m_record);
            m_record = new Record();

            return true;
        }
    }
}