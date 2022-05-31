using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Collections.Extensions;
using Nemo.Configuration;
using Nemo.Data;
using Nemo.Extensions;

namespace Nemo.Collections
{
    internal class EagerLoadEnumerableAsync<T> : IAsyncEnumerable<T>
        where T : class
    {
        private readonly Dictionary<string, Type> _sqlMap;
        private readonly List<string> _sqlOrder;
        private Func<string, IList<Type>, Task<IEnumerable<T>>> _load;
       
        public EagerLoadEnumerableAsync(IEnumerable<string> sql, IEnumerable<Type> types, Func<string, IList<Type>, Task<IEnumerable<T>>> load, Expression<Func<T, bool>> predicate, DialectProvider provider, SelectOption selectOption, string connectionName, DbConnection connection, int page, int pageSize, int skipCount, INemoConfiguration config)
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

        internal async Task<IEnumerator<T>> GetEnumeratorAsync()
        {
            var types = _sqlMap.Arrange(_sqlOrder, t => t.Key).Select(t => t.Value).ToArray();
            var result = await _load(_sqlOrder.ToDelimitedString("; "), types).ConfigureAwait(false);

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
           
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new EagerLoadEnumeratorAsync(GetEnumeratorAsync);
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

        public EagerLoadEnumerableAsync<T> Union(EagerLoadEnumerableAsync<T> other)
        {
            _load = other._load;
            foreach (var item in other._sqlMap.Where(item => !_sqlMap.ContainsKey(item.Key)))
            {
                _sqlOrder.Add(item.Key);
                _sqlMap.Add(item.Key, item.Value);
            }
            return this;
        }

        private class EagerLoadEnumeratorAsync : IAsyncEnumerator<T>
        {
            private readonly Func<Task<IEnumerator<T>>> _loader;
            private IEnumerator<T> _internal;

            public EagerLoadEnumeratorAsync(Func<Task<IEnumerator<T>>> loader)
            {
                _loader = loader;
            }

            public T Current { get; private set; }

            public void Dispose()
            {

            }

            public ValueTask DisposeAsync()
            {
                _internal.Dispose();
                return new ValueTask(Task.CompletedTask);
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                if (_internal == null)
                {
                    _internal = await _loader().ConfigureAwait(false);
                }

                try
                {
                    return _internal.MoveNext();
                }
                finally
                {
                    Current = _internal.Current;
                }
            }
        }
    }
}