using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Fn.Extensions;

namespace Nemo.Fn
{
    /// <summary>
    /// Several exension method on top of the Stream class.  
    /// These provide many of the most used functions when dealing with
    /// the lazy in a language like Haskell
    /// Function such as Map, Filter, Zip, and Fold
    /// </summary>
    public static class StreamExtensions
    {
        public static Stream<T> AsStream<T>(this IEnumerable<T> source)
        {
            return source.GetEnumerator().AsStream();
        }

        public static Stream<T> AsStream<T>(this IEnumerator<T> iterator)
        {
            if (iterator.MoveNext())
            {
                return new Stream<T>(iterator.Current, iterator.AsStream);
            }
            else
            {
                return null;
            }
        }

        public static Stream<T> ZipWith<U, V, T>(this Stream<U> st1, Stream<V> st2, Func<U, V, T> zipper)
        {
            if (st1 == null || st2 == null)
            {
                return null;
            }
            return new Stream<T>(zipper(st1.Head, st2.Head), () => st1.Tail.ZipWith(st2.Tail, zipper));
        }


        public static Stream<Tuple<U, V>> Zip<U, V>(this Stream<U> st1, Stream<V> st2)
        {
            if (st1 == null || st2 == null)
            {
                return null;
            }
            return new Stream<Tuple<U, V>>(new Tuple<U, V>(st1.Head, st2.Head), () => st1.Tail.Zip(st2.Tail));
        }


        public static T FoldRight<U, T>(this Stream<U> st1, Func<U, T, T> folder, T init)
        {
            if (st1 == null)
            {
                return init;
            }
            if (st1 != null && st1.Tail == null)
            {
                return folder(st1.Head, init);
            }
            return folder(st1.Head, st1.Tail.FoldRight(folder, init));
        }

        public static T FoldLeft<U, T>(this Stream<U> st1, Func<T, U, T> folder, T init)
        {
            return st1 == null ? init : st1.Tail.FoldLeft(folder, folder(init, st1.Head));
        }

        public static Stream<T> Map<U, T>(this Stream<U> st1, Func<U, T> mapper)
        {
            if (st1 == null)
            {
                return null;
            }
            return new Stream<T>(mapper(st1.Head), () => st1.Tail.Map(mapper));
        }

        public static Stream<T> Filter<T>(this Stream<T> st1, Func<T, bool> filter)
        {
            if (st1 == null)
            {
                return null;
            }
            return filter(st1.Head) ? new Stream<T>(st1.Head, () => st1.Tail.Filter(filter)) : st1.Tail.Filter(filter);
        }

        public static Stream<T> Merge<T>(this Stream<T> st1, Stream<T> st2) where T : IComparable<T>
        {
            if (st1 == null)
            {
                return st2;
            }
            if (st2 == null)
            {
                return st1;
            }

            var result = st1.Head.CompareTo(st2.Head);
            if (result < 0)
            {
                return new Stream<T>(st1.Head, () => Merge(st1.Tail, st2));
            }
            if (result > 0)
            {
                return new Stream<T>(st2.Head, () => Merge(st1, st2.Tail));
            }
            return new Stream<T>(st1.Head, () => Merge(st1.Tail, st2.Tail));
        }
    }
}
