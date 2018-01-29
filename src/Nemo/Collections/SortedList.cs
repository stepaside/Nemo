using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections.Extensions;

namespace Nemo.Collections
{
    public interface ISortedList
    {
        bool Distinct { get; }
        Type Comparer { get; }
    }

    public class SortedList<T> : IList<T>, IList, ISortedList
    {
        private readonly List<T> _list;
        private readonly IComparer<T> _comparer;
        private readonly bool _distinct;
        private readonly object _syncRoot = new object();

        public SortedList(bool distinct = false) : this(Comparer<T>.Default, distinct) { }

        public SortedList(IEnumerable<T> items, bool distinct = false) : this(items, Comparer<T>.Default, distinct) { }

        public SortedList(IComparer<T> comparer, bool distinct = false)
        {
            _list = new List<T>();
            _comparer = comparer;
            _distinct = distinct;
        }

        public SortedList(IEnumerable<T> items, IComparer<T> comparer, bool distinct = false)
        {
            _comparer = comparer;
            _distinct = distinct;
            if (_distinct)
            {
                _list = new SortedSet<T>(items, _comparer).ToList();
            }
            else if (items.IsSorted(comparer).Item1)
            {
                _list = new List<T>(items);
            }
            else
            {
                _list = items.OrderBy(_ => _, _comparer).ToList();
            }
        }

        public bool Distinct
        {
            get
            {
                return _distinct;
            }
        }

        public Type Comparer
        {
            get
            {
                return _comparer.GetType();
            }
        }

        public event EventHandler<SortedListEventArguments> ItemAdded;
        public event EventHandler<SortedListEventArguments> ItemRemoved;

        public class SortedListEventArguments : EventArgs
        {
            public T Item { get; internal set; }
        }

        #region IList<T> Members

        public int IndexOf(T item)
        {
            return _list.BinarySearch(item, _comparer);
        }

        public void Insert(int index, T item)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(int index)
        {
            _list.RemoveAt(index);
        }

        public T this[int index]
        {
            get
            {
                return _list[index];
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        #endregion

        #region ICollection<T> Members

        private void Add(T item, out int index)
        {
            index = _list.BinarySearch(item, _comparer);
            var notFound = index < 0;
            if (notFound)
            {
                index = ~index;
            }

            if (_distinct && !notFound) return;
            
            _list.Insert(index, item);
            if (ItemAdded != null)
            {
                ItemAdded(this, new SortedListEventArguments { Item = item });
            }
        }

        public void Add(T item)
        {
            int index;
            Add(item, out index);
        }

        public void Clear()
        {
            _list.Clear();
        }

        public bool Contains(T item)
        {
            return _list.BinarySearch(item, _comparer) > -1;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _list.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get
            {
                return _list.Count;
            }
        }

        public bool IsReadOnly
        {
            get 
            {
                return false;
            }
        }

        public bool Remove(T item)
        {
            var index = _list.BinarySearch(item, _comparer);
            
            if (index <= -1) return false;

            _list.RemoveAt(index);
            if (ItemRemoved != null)
            {
                ItemRemoved(this, new SortedListEventArguments { Item = item });
            }
            return true;
        }

        #endregion

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IList Members

        public int Add(object value)
        {
            int index;
            Add((T)value, out index);
            return index;
        }

        public bool Contains(object value)
        {
            return Contains((T)value);
        }

        public int IndexOf(object value)
        {
            return IndexOf((T)value);
        }

        public void Insert(int index, object value)
        {
            Insert(index, (T)value);
        }

        public bool IsFixedSize
        {
            get
            {
                return false;
            }
        }

        public void Remove(object value)
        {
            Remove((T)value);
        }

        object IList.this[int index]
        {
            get
            {
                return this[index];
            }
            set
            {
                this[index] = (T)value;
            }
        }

        #endregion

        #region ICollection Members

        public void CopyTo(Array array, int index)
        {
            CopyTo((T[])array, index);
        }

        public bool IsSynchronized
        {
            get
            {
                return false;
            }
        }

        public object SyncRoot
        {
            get
            {
                return _syncRoot;
            }
        }

        #endregion
    }
}
