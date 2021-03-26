using System.Linq.Expressions;
using Nemo.Collections.Extensions;
using Nemo.Data;
using Nemo.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;
using Nemo.Configuration;

namespace Nemo.Collections
{
    internal class EagerLoadEnumerable<T> : IEnumerable<T>
        where T : class
    {
        private readonly Dictionary<string, Type> _sqlMap;
        private readonly List<string> _sqlOrder;
        private Func<string, IList<Type>, IEnumerable<T>> _load;

        public EagerLoadEnumerable(IEnumerable<string> sql, IEnumerable<Type> types, Func<string, IList<Type>, IEnumerable<T>> load, Expression<Func<T, bool>> predicate, DialectProvider provider, SelectOption selectOption, string connectionName, DbConnection connection, int page, int pageSize, int skipCount, IConfiguration config)
        {
            _sqlOrder = sql.ToList();
            _sqlMap = _sqlOrder.Zip(types, (s, t) => new { Key = s, Value = t }).ToDictionary(t => t.Key, t => t.Value);
            _load = load;
            Predicate = predicate;
            Provider = provider;
            SelectOption = selectOption;
            ConnectionName = connectionName;
            Connection = connection;
            Page = page;
            PageSize = pageSize;
            SkipCount = skipCount;
            Configuration = config;
        }

        public IEnumerator<T> GetEnumerator()
        {
            var types = _sqlMap.Arrange(_sqlOrder, t => t.Key).Select(t => t.Value).ToArray();
            var result = _load(_sqlOrder.ToDelimitedString("; "), types);

            if (SelectOption == SelectOption.First)
            {
                return new List<T> { result.First() }.GetEnumerator();
            }

            if (SelectOption != SelectOption.FirstOrDefault)
            {
                return result.GetEnumerator();
            }

            var item = result.FirstOrDefault();
            return item != null ? new List<T> { item }.GetEnumerator() : Enumerable.Empty<T>().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        internal Expression<Func<T, bool>> Predicate { get; }

        internal DialectProvider Provider { get; }

        internal SelectOption SelectOption { get; }

        internal string ConnectionName { get; }

        internal DbConnection Connection { get; }

        internal int Page { get; }

        internal int PageSize { get; }

        public int SkipCount { get; }

        public IConfiguration Configuration { get; }

        public IEnumerable<T> Union(IEnumerable<T> other)
        {
            var eagerLoader = other as EagerLoadEnumerable<T>;
            if (eagerLoader != null)
            {
                _load = eagerLoader._load;
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
