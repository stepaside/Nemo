﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        private readonly ConcurrentDictionary<string, List<string>> _indices = new ConcurrentDictionary<string, List<string>>();
        private readonly ConcurrentDictionary<string, HashSet<string>> _indicesReverse = new ConcurrentDictionary<string, HashSet<string>>();
        
        public T Get(string id)
        {
            T item;
            _entities.TryGetValue(id, out item);
            return item;
        }

        public void Set(T entity)
        {
            if (entity == null) return;
            _entities[entity.ComputeHash()] = entity;
        }

        public bool Remove(T entity)
        {
            T item;
            var id = entity.ComputeHash();
            if (!_entities.TryRemove(id, out item)) return false;
            CleanIndices(id);
            return true;
        }

        public IEnumerable<T> GetIndex(string index)
        {
            List<string> idList;
            return !_indices.TryGetValue(index, out idList) ? null : idList.Select(Get).Where(item => item != null);
        }

        public IEnumerable<T> AddIndex(string index, IEnumerable<T> entities)
        {
            if (entities == null) return null;

            if (entities is Stream<T>)
            {
                return entities.Select(e =>
                {
                    var id = e.ComputeHash();
                    _indices.AddOrUpdate(index, k => new List<string>(new[] { id }), (k, l) =>
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

            if (!_indices.TryAdd(index, map.Keys.ToList())) return list;

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
            HashSet<string> indices;
            if (!_indicesReverse.TryRemove(id, out indices)) return;
            foreach (var index in indices)
            {
                List<string> idList;
                _indices.TryRemove(index, out idList);
            }
        }

        bool IIdentityMap.TryGetValue(string id, out object value)
        {
            T item;
            var success = _entities.TryGetValue(id, out item);
            value = item;
            return success;
        }

        void IIdentityMap.Set(string id, object value)
        {
            _entities[id] = (T)value;
        }

        bool IIdentityMap.Remove(string id)
        {
            T item;
            return _entities.TryRemove(id, out item);
        }
    }
}
