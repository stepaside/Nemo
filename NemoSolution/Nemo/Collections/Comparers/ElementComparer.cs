using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Collections.Comparers
{
    // ElementComparer is a helper class for OrderedEnumerable that can compare items in a source array given their indexes.
    // It supports chaining multiple comparers together to allow secondary, tertiary, etc. sorting.
    internal abstract class ElementComparer<TSource>
    {
        // Compares the two items at the specified indexes.
        public abstract int Compare(int left, int right);

        // Compares two items
        public abstract int Compare(TSource left, TSource right);

        // Creates all the keys needed to compare items.
        public abstract void CreateSortKeys(TSource[] source);

        // Creates a new ElementComparer by appending the specified ordering to this ordering, chaining them together.
        public abstract ElementComparer<TSource> Append(ElementComparer<TSource> next);

        public abstract bool IsDescending { get; }
    }

    internal class ElementComparer<TSource, TKey> : ElementComparer<TSource>
    {
        public ElementComparer(Func<TSource, TKey> keySelector, IComparer<TKey> comparer, bool descending, ElementComparer<TSource> next)
        {
            _keySelector = keySelector;
            _comparer = comparer;
            _isDescending = descending;
            _next = next;
        }

        public override void CreateSortKeys(TSource[] source)
        {
            // create the sort key from each item in the source array
            _keys = new TKey[source.Length];
            for (int index = 0; index < source.Length; index++)
                _keys[index] = _keySelector(source[index]);

            // delegate to next if necessary
            if (_next != null)
                _next.CreateSortKeys(source);
        }

        public override int Compare(int left, int right)
        {
            // invoke this level's comparer to get a basic result
            int result = _comparer.Compare(_keys[left], _keys[right]);

            // if elements are different, return their relative order (inverting if descending)
            if (result != 0)
                return _isDescending ? -result : result;

            // if there is a chained ordering, delegate to it
            if (_next != null)
                return _next.Compare(left, right);

            // elements are otherwise equal; to preserve stable sort, sort by original index
            return left - right;
        }

        public override int Compare(TSource left, TSource right)
        {
            // invoke this level's comparer to get a basic result
            int result = _comparer.Compare(_keySelector(left), _keySelector(right));

            // if elements are different, return their relative order (inverting if descending)
            if (result != 0)
                return _isDescending ? -result : result;

            // if there is a chained ordering, delegate to it
            if (_next != null)
                return _next.Compare(left, right);

            return 0;
        }

        public override ElementComparer<TSource> Append(ElementComparer<TSource> next)
        {
            // append the new ordering to the tail of the current chain
            return _next == null ? new ElementComparer<TSource, TKey>(_keySelector, _comparer, _isDescending, next) : _next.Append(next);
        }

        public override bool IsDescending
        {
            get
            {
                return _isDescending;
            }
        }

        readonly Func<TSource, TKey> _keySelector;
        readonly IComparer<TKey> _comparer;
        readonly bool _isDescending;
        readonly ElementComparer<TSource> _next;
        TKey[] _keys;
    }
}
