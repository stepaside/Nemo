using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Nemo.Collections
{
    public interface ISet 
    {
        Type Comparer { get; }
    }

    public class HashList<T> : IList<T>, IList, ISet<T>, ISet
    {
        private HashSet<T> _set;
        private List<T> _list;
        private IEqualityComparer<T> _comparer;

        public HashList() : this(EqualityComparer<T>.Default) { }

        public HashList(IEnumerable<T> items) : this(items, EqualityComparer<T>.Default) { }

        public HashList(IEqualityComparer<T> comparer)
        {
            _comparer = comparer;
            _set = new HashSet<T>(_comparer);
            _list = new List<T>();
        }

        public HashList(IEnumerable<T> items, IEqualityComparer<T> comparer)
        {
            _comparer = comparer;
            _set = new HashSet<T>(items, _comparer);
            _list = _set.ToList();
        }

        public Type Comparer
        {
            get
            {
                return _comparer.GetType();
            }
        }

        #region IList<T> Members

        public int IndexOf(T item)
        {
            return _list.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            if (!_set.Contains(item))
            {
                _list.Insert(index, item);
                _set.Add(item);
            }
        }

        public void RemoveAt(int index)
        {
            if (index > -1 && index < _list.Count)
            {
                _set.Remove(_list[index]);
            }
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
                if (!_set.Contains(value))
                {
                    if (index > -1 && index < _list.Count)
                    {
                        _set.Remove(_list[index]);
                    }

                    _list[index] = value;
                    _set.Add(value);
                }
            }
        }

        #endregion

        #region ICollection<T> Members

        public void Add(T item)
        {
            if (!_set.Contains(item))
            {
                _list.Add(item);
                _set.Add(item);
            }
        }

        public void Clear()
        {
            _list.Clear();
            _set.Clear();
        }

        public bool Contains(T item)
        {
            return _set.Contains(item);
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
            var index = _list.FindIndex(i => _set.Contains(i));
            if (index > -1 && _set.Remove(item))
            {
                _list.RemoveAt(index);
                return true;
            }
            return false;
        }

        #endregion

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        #endregion

        #region IList Members

        public int Add(object value)
        {
            var oldCount = this.Count;
            this.Add((T)value);
            if (oldCount == this.Count)
            {
                return -1;
            }
            else
            {
                return this.Count - 1;
            }
        }

        public bool Contains(object value)
        {
            return this.Contains((T)value);
        }

        public int IndexOf(object value)
        {
            return this.IndexOf((T)value);
        }

        public void Insert(int index, object value)
        {
            this.Insert(index, (T)value);
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
            this.Remove((T)value);
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

        public void CopyTo(System.Array array, int index)
        {
            this.CopyTo((T[])array, index);
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
                return null;
            }
        }

        #endregion

        #region ISet<T> Members

        bool ISet<T>.Add(T item)
        {
            return this.Add((object)item) > -1;
        }

        void ISet<T>.ExceptWith(IEnumerable<T> other)
        {
            _set.ExceptWith(other);
            _list = _list.Except(other, _set.Comparer).ToList();
        }

        void ISet<T>.IntersectWith(IEnumerable<T> other)
        {
            _set.IntersectWith(other);
            _list = _list.Intersect(other, _set.Comparer).ToList();
        }

        bool ISet<T>.IsProperSubsetOf(IEnumerable<T> other)
        {
            return _set.IsProperSubsetOf(other);
        }

        bool ISet<T>.IsProperSupersetOf(IEnumerable<T> other)
        {
            return _set.IsProperSupersetOf(other);
        }

        bool ISet<T>.IsSubsetOf(IEnumerable<T> other)
        {
            return _set.IsSubsetOf(other);
        }

        bool ISet<T>.IsSupersetOf(IEnumerable<T> other)
        {
            return _set.IsSupersetOf(other);
        }

        bool ISet<T>.Overlaps(IEnumerable<T> other)
        {
            return _set.Overlaps(other);
        }

        bool ISet<T>.SetEquals(IEnumerable<T> other)
        {
            return _set.SetEquals(other);
        }

        void ISet<T>.SymmetricExceptWith(IEnumerable<T> other)
        {
            _set.SymmetricExceptWith(other);
            _list = _list.Except(other, _set.Comparer).Union(other.Except(_list, _set.Comparer)).ToList();
        }

        void ISet<T>.UnionWith(IEnumerable<T> other)
        {
            _set.UnionWith(other);
            _list = _list.Union(other, _set.Comparer).ToList();
        }

        #endregion
    }
}
