using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Collections.Extensions;
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
        private readonly Expression<Func<T, bool>> _predicate;
        private readonly DialectProvider _provider;
        private readonly SelectOption _selectOption;

        public EagerLoadEnumerableAsync(IEnumerable<string> sql, IEnumerable<Type> types, Func<string, IList<Type>, Task<IEnumerable<T>>> load, Expression<Func<T, bool>> predicate, DialectProvider provider, SelectOption selectOption)
        {
            _sqlOrder = sql.ToList();
            _sqlMap = _sqlOrder.Zip(types, (s, t) => new { Key = s, Value = t }).ToDictionary(t => t.Key, t => t.Value);
            _load = load;
            _predicate = predicate;
            _provider = provider;
            _selectOption = selectOption;
        }

        private async Task<IEnumerator<T>> GetEnumeratorAsync()
        {
            var types = _sqlMap.Arrange(_sqlOrder, t => t.Key).Select(t => t.Value).ToArray();
            var result = await _load(_sqlOrder.ToDelimitedString("; "), types);

            if (_selectOption == SelectOption.First)
            {
                return new List<T> { result.First() }.GetEnumerator();
            }

            if (_selectOption != SelectOption.FirstOrDefault)
            {
                return result.GetEnumerator();
            }

            var item = result.FirstOrDefault();
            return item != null ? new List<T> { item }.GetEnumerator() : Enumerable.Empty<T>().GetEnumerator();
        }

        internal Expression<Func<T, bool>> Predicate
        {
            get { return _predicate; }
        }

        internal DialectProvider Provider
        {
            get { return _provider; }
        }

        internal SelectOption SelectOption
        {
            get { return _selectOption; }
        }

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

        public IAsyncEnumerator<T> GetEnumerator()
        {
            return new EagerLoadEnumeratorAsync(GetEnumeratorAsync);
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

            public async Task<bool> MoveNext(CancellationToken cancellationToken)
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