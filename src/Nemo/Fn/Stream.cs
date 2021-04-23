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
        private readonly Lazy<Stream<T>> _tail;
        private readonly bool _isEmpty;
        private List<T> _list;

        public static readonly Stream<T> Empty = new Stream<T>();

        private Stream() 
        {
            _isEmpty = true;
        }

        public Stream(T head)
        {
            _head = head;
        }

        public Stream(T head, Lazy<Stream<T>> tail)
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

                return _tail.Value;
            }
        }

        public bool IsEmpty()
        {
            return _isEmpty;
        }

        public Stream<T> Append(T value)
        {
            return IsEmpty() ? new Stream<T>(value) : new Stream<T>(Head, new Lazy<Stream<T>>(() => Tail.Append(value)));
        }

        public Stream<T> Prepend(T value)
        {
            return IsEmpty() ? new Stream<T>(value) : new Stream<T>(value, new Lazy<Stream<T>>(() => this));
        }

        public void ForEach(Action<T> action)
        {
            if (IsEmpty()) return;

            action(Head);
            Tail.ForEach(action);
        }

        public static Stream<T> operator + (Stream<T> stream, T value)
        {
            return stream.Append(value);
        }

        public static Stream<T> operator + (T value, Stream<T> stream)
        {
            return stream.Prepend(value);
        }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            if (IsEmpty()) yield break;

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
            return _list ??= Enumerable.ToList(this);
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

            if (IsEmpty())
            {
                throw new ArgumentOutOfRangeException("index", "`index` is larger than the collection size.");
            }

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

        public T ElementAtOrDefault(int index)
        {
            index.ThrowIfNegative("index");

            if (IsEmpty())
            {
                return default;
            }

            var stream = this;
            while (--index >= 0)
            {
                stream = stream.Tail;
                if (stream == null)
                {
                    return default;
                }
            }
            return stream.Head;
        }

        public T First()
        {
            if (IsEmpty())
            {
                throw new InvalidOperationException("Sequence contains no matching elements");
            }
            return Head;
        }

        public T FirstOrDefault()
        {
            return Head;
        }

        public long LongCount()
        {
            if (IsEmpty()) return 0;

            var count = 0L;
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
            if (IsEmpty()) return Empty;

            var newHead = new Stream<T>(Head);
            var tail = Tail;
            while (tail != null)
            {
                newHead = new Stream<T>(tail.Head, new Lazy<Stream<T>>(() => newHead));
                tail = tail.Tail;
            }
            return newHead;
        }

        #endregion
    }
}
