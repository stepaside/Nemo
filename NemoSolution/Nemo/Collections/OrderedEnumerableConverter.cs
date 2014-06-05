using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections.Comparers;

namespace Nemo.Collections
{
    internal class OrderedEnumerableConverter<T> : IOrderedEnumerable<T>
    {
        private readonly IEnumerable<T> _source;
        private readonly ElementComparer<T> _elementComparer;
        private readonly bool _descending;

        public OrderedEnumerableConverter(IEnumerable<T> source, ElementComparer<T> elementComparer)
        {
            _source = source;
            _elementComparer = elementComparer;
            _descending = elementComparer.IsDescending;
        }

        public bool IsDescending
        {
            get { return _descending; }
        }

        #region IOrderedEnumerable<T> Members

        public IOrderedEnumerable<T> CreateOrderedEnumerable<TKey>(Func<T, TKey> keySelector, IComparer<TKey> comparer, bool descending)
        {
            var elementComparer = new ElementComparer<T, TKey>(keySelector, comparer ?? Comparer<TKey>.Default, descending, null);
            return new OrderedEnumerableConverter<T>(_source, _elementComparer.Append(elementComparer));
        }

        #endregion

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            try
            {
                return GetEnumeratorImplementation();
            }
            catch (InvalidOperationException ex)
            {
                return (_descending
                    ? _source.OrderByDescending(x => x, new ComparisonComparer<T>((x, y) => _elementComparer.Compare(x, y)))
                    : _source.OrderBy(x => x, new ComparisonComparer<T>((x, y) => _elementComparer.Compare(x, y)))).GetEnumerator();
            }
        }

        private IEnumerator<T> GetEnumeratorImplementation()
        {
            int? sortingOrder = null;

            if (_source is IOrderedEnumerable<T>)
            {
                foreach (var item in _source)
                {
                    yield return item;
                }
            }
            else
            {
                var previous = default(T);
                var previousExists = false;
                foreach (var current in _source)
                {
                    if (previousExists)
                    {
                        var result = _elementComparer.Compare(previous, current);
                        if (sortingOrder == null)
                        {
                            if (result != 0)
                            {
                                sortingOrder = result;
                                if ((_descending && sortingOrder.Value < 0) || (!_descending && sortingOrder.Value > 0))
                                {
                                    throw new InvalidOperationException("Invalid sorting direction specified. The sequence may not be sorted.");
                                }
                            }
                        }
                        else if ((sortingOrder.Value < 0 && result > 0) || (sortingOrder.Value > 0 && result < 0))
                        {
                            throw new InvalidOperationException("The sequence does not appear to be sorted.");
                        }
                    }
                    previous = current;
                    previousExists = true;
                    yield return current;
                }
            }
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
