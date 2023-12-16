using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace LevelDB.NET;

public class LruCache<TKey, TValue>(int capacity)
    where TKey : notnull
    where TValue : class
{
    private readonly Dictionary<TKey, LinkedListNode<LruCacheItem>> m_cacheMap = new();
    private readonly LinkedList<LruCacheItem> m_lruList = [];

    public int Capacity { get; } = capacity;

    public void Clear()
    {
        lock (m_cacheMap)
        {
            m_cacheMap.Clear();
            m_lruList.Clear();
        }
    }

    public bool TryGetValue(TKey key, [NotNullWhen(true)] out TValue? value)
    {
        lock (m_cacheMap)
        {
            if (m_cacheMap.TryGetValue(key, out var node))
            {
                value = node.Value.Value;
                m_lruList.Remove(node);
                m_lruList.AddLast(node);
                return true;
            }

            value = default;
            return false;
        }
    }

    public void Add(TKey key, TValue value)
    {
        lock (m_cacheMap)
        {
            if (m_cacheMap.ContainsKey(key)) return;
            if (m_cacheMap.Count >= Capacity) RemoveFirst();

            var cacheItem = new LruCacheItem(key, value);
            var node = new LinkedListNode<LruCacheItem>(cacheItem);
            m_lruList.AddLast(node);
            m_cacheMap.Add(key, node);
        }
    }

    private void RemoveFirst()
    {
        var node = m_lruList.First;
        if (node == null) return;
        m_lruList.RemoveFirst();
        m_cacheMap.Remove(node.Value.Key);
    }

    private class LruCacheItem(TKey k, TValue v)
    {
        public TKey Key { get; } = k;
        public TValue Value { get; } = v;
    }
}