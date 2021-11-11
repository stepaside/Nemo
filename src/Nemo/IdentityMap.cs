using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections;
using Nemo.Collections.Extensions;
using Nemo.Extensions;
using Nemo.Fn;

namespace Nemo
{
    public interface IIdentityMap
    {
        bool TryGetValue(string id, out object value);

        void Set(string id, object value);

        bool Remove(string id);
    }

    internal class IdentityMap<T> : IIdentityMap
        where T : class
    {
        private readonly ConcurrentDictionary<string, T> _entities = new ConcurrentDictionary<string, T>();
        private readonly ConcurrentDictionary<string, HashList<string>> _indices = new ConcurrentDictionary<string, HashList<string>>();
        private readonly ConcurrentDictionary<string, HashSet<string>> _indicesReverse = new ConcurrentDictionary<string, HashSet<string>>();
        
        public T Get(string id)
        {
            _entities.TryGetValue(id, out var item);
            return item;
        }

        public void Set(T entity)
        {
            if (entity == null) return;
            _entities[entity.ComputeHash()] = entity;
        }

        public bool Remove(T entity)
        {
            var id = entity.ComputeHash();
            if (!_entities.TryRemove(id, out _)) return false;
            CleanIndices(id);
            return true;
        }

        public IEnumerable<T> GetIndex(string index)
        {
            return !_indices.TryGetValue(index, out var idList) ? null : idList.Select(Get).Where(item => item != null);
        }

        public IEnumerable<T> AddIndex(string index, IEnumerable<T> entities)
        {
            if (entities == null) return null;

            if (entities is IBuffer<T>)
            {
                return entities.Select(e =>
                {
                    var id = e.ComputeHash();
                    _indices.AddOrUpdate(index, k => new HashList<string>(new[] { id }), (k, l) =>
                    {
                        l.Add(id);
                        return l;
                    });

                    if (_entities.TryAdd(id, e))
                    {
                        _indicesReverse.AddOrUpdate(id, k => new HashSet<string>(new[] { index }), (k, set) =>
                        {
                            set.Add(index);
                            return set;
                        });
                    }
                    return e;
                });
            }

            if (_indices.ContainsKey(index)) return entities;

            var list = entities as IList<T> ?? entities.ToList();
            var map = list.Select(e => new { Id = e.ComputeHash(), Entity = e }).GroupBy(e => e.Id).ToDictionary(g => g.Key, g => g.Last().Entity);

            if (!_indices.TryAdd(index, new HashList<string>(map.Keys))) return list;

            foreach (var pair in map.Where(pair => _entities.TryAdd(pair.Key, pair.Value)))
            {
                _indicesReverse.AddOrUpdate(pair.Key, k => new HashSet<string>(new[] { index }), (k, set) =>
                {
                    set.Add(index);
                    return set;
                });
            }
            return list;
        }

        public void CleanIndices(string id)
        {
            if (!_indicesReverse.TryRemove(id, out var indices)) return;
            foreach (var index in indices)
            {
                _indices.TryRemove(index, out _);
            }
        }

        bool IIdentityMap.TryGetValue(string id, out object value)
        {
            var success = _entities.TryGetValue(id, out var item);
            value = item;
            return success;
        }

        void IIdentityMap.Set(string id, object value)
        {
            _entities[id] = (T)value;
        }

        bool IIdentityMap.Remove(string id)
        {
            return _entities.TryRemove(id, out _);
        }
    }
}
