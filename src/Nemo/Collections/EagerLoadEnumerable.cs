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
    internal interface IEagerLoadEnumerable
    {
    }

    internal class EagerLoadEnumerable<T> : IEnumerable<T>, IEagerLoadEnumerable
        where T : class
    {
        private readonly Dictionary<string, Type> _sqlMap;
        private readonly List<string> _sqlOrder;
        private Func<string, IList<Type>, IEnumerable<T>> _load;
        private string _operation;
        private Type[] _operationTypes;

        public EagerLoadEnumerable(IEnumerable<string> sql, IEnumerable<Type> types, Func<string, IList<Type>, IEnumerable<T>> load, Expression<Func<T, bool>> predicate, DialectProvider provider, SelectOption selectOption, string connectionName, DbConnection connection, int page, int pageSize, int skipCount, INemoConfiguration config)
        {
            _sqlOrder = sql.ToList();
            _sqlMap = new Dictionary<string, Type>(_sqlOrder.Count);
            using (var type = types.GetEnumerator())
            {
                foreach (var statement in _sqlOrder)
                {
                    if (!type.MoveNext()) break;
                    _sqlMap.Add(statement, type.Current);
                }
            }
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
            var types = _operationTypes ?? (_operationTypes = GetOperationTypes());
            var result = _load(_operation ?? (_operation = GetOperation()), types);

            var multiresult = result as IMultiResult;
            if (multiresult != null)
            {
                result = multiresult.Aggregate<T>(Configuration);
            }

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

        private string GetOperation()
        {
            return _sqlOrder.Count == 1 ? _sqlOrder[0] : _sqlOrder.ToDelimitedString("; ");
        }

        private Type[] GetOperationTypes()
        {
            var types = new Type[_sqlOrder.Count];
            for (var i = 0; i < _sqlOrder.Count; i++)
            {
                types[i] = _sqlMap[_sqlOrder[i]];
            }
            return types;
        }

        internal Expression<Func<T, bool>> Predicate { get; }

        internal DialectProvider Provider { get; }

        internal SelectOption SelectOption { get; }

        internal string ConnectionName { get; }

        internal DbConnection Connection { get; }

        internal int Page { get; }

        internal int PageSize { get; }

        public int SkipCount { get; }

        public INemoConfiguration Configuration { get; }

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
                    _operation = null;
                    _operationTypes = null;
                }
                return this;
            }
            return Enumerable.Union(this, other);
        }
    }
}
