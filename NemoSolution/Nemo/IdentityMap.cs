using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nemo.Extensions;

namespace Nemo
{
    internal class IdentityMap<T> 
        where T : class, IDataEntity
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
            _entities[entity.ComputeHash()] = entity;
        }

        public bool Remove(T entity)
        {
            T item;
            var id = entity.ComputeHash();
            if (_entities.TryRemove(id, out item))
            {
                CleanIndices(id);
                return true;
            }
            return false;
        }

        public IEnumerable<T> GetIndex(string index)
        {
            List<string> idList;
            if (_indices.TryGetValue(index, out idList))
            {
                foreach (var id in idList)
                {
                    var item = this.Get(id);
                    if (item != null)
                    {
                        yield return item;
                    }
                }
            }
        }

        public void AddIndex(string index, IEnumerable<T> entities)
        {
            var map = entities.Select(e => new { Id = e.ComputeHash(), Entity = e }).GroupBy(e => e.Id).ToDictionary(g => g.Key, g => g.Last().Entity);
            if (_indices.TryAdd(index, map.Keys.ToList()))
            {
                foreach (var pair in map)
                {
                    if (_entities.TryAdd(pair.Key, pair.Value))
                    {
                        _indicesReverse.AddOrUpdate(pair.Key, k => new HashSet<string>(new[] { index }), (k, set) => { set.Add(index); return set; });
                    }
                }
            }
        }

        public void CleanIndices(string id)
        {
            HashSet<string> indices;
            if (_indicesReverse.TryRemove(id, out indices))
            {
                foreach (var index in indices)
                {
                    List<string> idList;
                    _indices.TryRemove(index, out idList);
                }
            }
        }
    }
}
