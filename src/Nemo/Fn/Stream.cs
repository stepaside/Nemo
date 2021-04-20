using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections.Extensions;
using Nemo.Extensions;

namespace Nemo.Fn
{
    /// <summary>
    /// An infinite stream of values of T
    /// </summary>
    /// <typeparam name="T">Whatever you like :)</typeparam>
    public class Stream<T> : IEnumerable<T>
    {
        private readonly T _head;
        private readonly Func<Stream<T>> _tail;
        private Stream<T> _realized;
        private List<T> _list;

        public Stream(T head)
        {
            _head = head;
        }

        public Stream(T head, Func<Stream<T>> tail)
        {
            _head = head;
            _tail = tail;
        }

        public T Head
        {
            get
            {
                return _head;
            }
        }

        public Stream<T> Tail
        {
            get
            {
                // Once the tail is used 
                // we consider it "realized"
                // so we store it to improve performance
                // instead of needing to regenerate the list again
                if (_realized == null && _tail != null)
                {
                    _realized = _tail();
                }
                return _realized;
            }
        }

        public Stream<T> Append(T value)
        {
            return ((IEnumerable<T>)this).Append(value).AsStream();
        }

        public Stream<T> Prepend(T value)
        {
            return new Stream<T>(value, () => this);
        }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            yield return _head;

            var t = Tail;
            if (t != null)
            {
                foreach (var x in t)
                    yield return x;
            }

        }

        public List<T> ToList()
        {
            return _list ?? (_list = Enumerable.ToList(this));
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region LINQ optimizations

        public int Count()
        {
            return checked((int)LongCount());
        }

        public T ElementAt(int index)
        {
            index.ThrowIfNegative("index");
            var stream = this;
            while (--index >= 0)
            {
                stream = stream.Tail;
                if (stream == null)
                {
                    throw new ArgumentOutOfRangeException("index", "`index` is larger than the collection size.");
                }
            }
            return stream.Head;
        }

        public T First()
        {
            return Head;
        }

        public T FirstOrDefault()
        {
            return Head;
        }

        public long LongCount()
        {
            long count = 1;
            var tail = Tail;
            while (tail != null)
            {
                ++count;
                tail = tail.Tail;
            }
            return count;
        }

        public Stream<T> Reverse()
        {
            var newHead = new Stream<T>(Head);
            var tail = Tail;
            while (tail != null)
            {
                newHead = new Stream<T>(tail.Head, () => newHead);
                tail = tail.Tail;
            }
            return newHead;
        }

        #endregion
    }
}
