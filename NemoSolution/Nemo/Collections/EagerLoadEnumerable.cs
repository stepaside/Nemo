using Nemo.Collections.Extensions;
using Nemo.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Nemo.Collections
{
    class EagerLoadEnumerable<T> : IEnumerable<T>
    {
        private readonly Dictionary<string, Type> _sqlMap;
        private readonly List<string> _sqlOrder;
        private readonly Func<string, IList<Type>, IEnumerable<T>> _load;

        public EagerLoadEnumerable(IEnumerable<string> sql, IEnumerable<Type> types, Func<string, IList<Type>, IEnumerable<T>> load)
        {
            _sqlOrder = sql.ToList();
            _sqlMap = _sqlOrder.Zip(types, (s, t) => new { Key = s, Value = t }).ToDictionary(t => t.Key, t => t.Value);
            _load = load;
        }

        public IEnumerator<T> GetEnumerator()
        {
            var types = _sqlMap.Arrange(_sqlOrder, t => t.Key).Select(t => t.Value).ToArray();
            return _load(_sqlOrder.ToDelimitedString("; "), types).GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerable<T> Union(IEnumerable<T> other)
        {
            var eagerLoader = other as EagerLoadEnumerable<T>;
            if (eagerLoader != null)
            {
                foreach (var item in eagerLoader._sqlMap.Where(item => !_sqlMap.ContainsKey(item.Key)))
                {
                    _sqlOrder.Add(item.Key);
                    _sqlMap.Add(item.Key, item.Value);
                }
                return this;
            }
            return Enumerable.Union(this, other);
        }
    }
}
