using System;
using System.Collections.Generic;
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
        private Func<Stream<T>> _tail;
        private Stream<T> _realized;

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
                // so we store it to imporve performance
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

            var t = this.Tail;
            if (t != null)
            {
                foreach (var x in t)
                    yield return x;
            }

        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return (System.Collections.IEnumerator)GetEnumerator();
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
            Stream<T> stream = this;
            while (--index >= 0)
            {
                stream = stream.Tail;
                if (stream == null)
                    throw new ArgumentOutOfRangeException("index", "`index` is larger than the collection size.");
            }
            return stream.Head;
        }

        public T First()
        {
            return this.Head;
        }

        public T FirstOrDefault()
        {
            return this.Head;
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
            var newHead = new Stream<T>(this.Head);
            var tail = this.Tail;
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
