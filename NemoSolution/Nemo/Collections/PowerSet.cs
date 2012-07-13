using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Extensions;
using Nemo.Fn;

namespace Nemo.Collections
{
    public class PowerSet<T> : ICollection<HashSet<T>>
    {
        private IList<T> _items;
        private HashSet<T> _set;
        private Stream<HashSet<T>> _stream;
        private IEqualityComparer<T> _comparer;

        public PowerSet(IList<T> items) : this(items, EqualityComparer<T>.Default) { }

        public PowerSet(IList<T> items, IEqualityComparer<T> comparer)
        {
            _comparer = comparer;
            _items = items;
            _set = items.ToHashSet(comparer);
            _stream = GeneratePowerSet().AsStream();
        }

        private IEnumerable<HashSet<T>> GeneratePowerSet()
        {
            for (int i = 0; i < this.Count; i++)
            {
                var bits = new BitArray(BitConverter.GetBytes(i)).Cast<bool>().Take(_items.Count).Select((b, k) => b ? k : -1).ToHashSet();
                yield return _items.Where((t, k) => bits.Contains(k)).ToHashSet();
            }
        }

        #region IEnumerable<T> Members

        public IEnumerator<HashSet<T>> GetEnumerator()
        {
            return _stream.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        #endregion

        #region ICollection<HashSet<T>> Members

        void ICollection<HashSet<T>>.Add(HashSet<T> item)
        {
            throw new NotSupportedException();
        }

        void ICollection<HashSet<T>>.Clear()
        {
            throw new NotSupportedException();
        }

        public bool Contains(HashSet<T> item)
        {
            return _set.IsSubsetOf(item);
        }

        public void CopyTo(HashSet<T>[] array, int arrayIndex)
        {
            _stream.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get 
            {
                return 1 << _items.Count;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return true;
            }
        }

        bool ICollection<HashSet<T>>.Remove(HashSet<T> item)
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}
