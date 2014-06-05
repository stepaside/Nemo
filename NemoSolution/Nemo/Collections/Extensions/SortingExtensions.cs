using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Nemo.Collections;
using Nemo.Collections.Comparers;
using Nemo.Extensions;

namespace Nemo.Collections.Extensions
{
    public enum SortingOrder
    {
        Ascending,
        Descending
    }

    public static class SortingExtensions
    {
        #region IsSorted Methods

        public static Tuple<bool, SortingOrder> IsSorted<T>(this IEnumerable<T> source)
            where T : IComparable
        {
            return source.IsSorted(null);
        }

        public static Tuple<bool, SortingOrder> IsSorted<T>(this IEnumerable<T> source, IComparer<T> comparer)
        {
            source.ThrowIfNull("source");

            var sorted = true;
            int? sortingOrder = null;

            if (source is IOrderedEnumerable<T>)
            {
                if (source is OrderedEnumerable<T>)
                {
                    sortingOrder = ((OrderedEnumerable<T>)source).IsDescending ? 1 : -1;
                }
                else if (source is OrderedEnumerableConverter<T>)
                {
                    sortingOrder = ((OrderedEnumerableConverter<T>)source).IsDescending ? 1 : -1;
                }
                else
                {
                    var field = source.GetType().GetField("descending", BindingFlags.Instance | BindingFlags.NonPublic);
                    if (field != null)
                    {
                        sortingOrder = (bool)field.GetValue(source) ? 1 : -1;
                    }
                }
            }
            else
            {
                if (comparer == null)
                {
                    comparer = Comparer<T>.Default;
                }

                var previous = default(T);
                var previousExists = false;
                foreach (var current in source)
                {
                    if (previousExists)
                    {
                        var result = comparer.Compare(previous, current);
                        if (sortingOrder == null)
                        {
                            if (result != 0)
                            {
                                sortingOrder = result;
                            }
                        }
                        else if ((sortingOrder.Value < 0 && result > 0) || (sortingOrder.Value > 0 && result < 0))
                        {
                            sorted = false;
                            break;
                        }
                    }
                    previous = current;
                    previousExists = true;
                }
            }
            return Tuple.Create(sorted, sortingOrder.HasValue ? (sortingOrder.Value < 0 ? SortingOrder.Ascending : SortingOrder.Descending) : SortingOrder.Ascending);
        }

        #endregion

        #region AsSorted Methods

        public static IOrderedEnumerable<T> AsSorted<T>(this IEnumerable<T> source)
            where T : IComparable
        {
            return source.AsSorted(Comparer<T>.Default);
        }

        public static IOrderedEnumerable<T> AsSorted<T>(this IEnumerable<T> source, IComparer<T> comparer)
        {
            return source.AsSortedImplementation(comparer, false);
        }

        public static IOrderedEnumerable<T> AsSorted<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector)
            where TKey : IComparable
        {
            return source.AsSorted(keySelector, Comparer<TKey>.Default);
        }

        public static IOrderedEnumerable<T> AsSorted<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector, IComparer<TKey> comparer)
        {
            return source.AsSortedImplementation(keySelector, comparer, false);
        }

        public static IOrderedEnumerable<T> AsSortedDescending<T>(this IEnumerable<T> source)
            where T : IComparable
        {
            return source.AsSortedDescending(Comparer<T>.Default);
        }

        public static IOrderedEnumerable<T> AsSortedDescending<T>(this IEnumerable<T> source, IComparer<T> comparer)
        {
            return source.AsSortedImplementation(comparer, true);
        }

        public static IOrderedEnumerable<T> AsSortedDescending<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector)
            where TKey : IComparable
        {
            return source.AsSortedDescending(keySelector, Comparer<TKey>.Default);
        }

        public static IOrderedEnumerable<T> AsSortedDescending<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector, IComparer<TKey> comparer)
        {
            return source.AsSortedImplementation(keySelector, comparer, true);
        }

        private static IOrderedEnumerable<T> AsSortedImplementation<T>(this IEnumerable<T> source, IComparer<T> comparer, bool descending)
        {
            source.ThrowIfNull("source");
            var elementComparer = new ElementComparer<T, T>(t => t, comparer ?? Comparer<T>.Default, descending, null);
            return new OrderedEnumerableConverter<T>(source, elementComparer);
        }

        private static IOrderedEnumerable<T> AsSortedImplementation<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector, IComparer<TKey> comparer, bool descending)
        {
            source.ThrowIfNull("source");
            var elementComparer = new ElementComparer<T, TKey>(keySelector, comparer ?? Comparer<TKey>.Default, descending, null);
            return new OrderedEnumerableConverter<T>(source, elementComparer);
        }

        #endregion

        #region Lazy OrderBy

        /// Implementation was taken from here http://code.logos.com/blog/2010/04/a_truly_lazy_orderby_in_linq.html

        /// <summary>
        /// Sorts the elements of a sequence in ascending order according to a key.
        /// </summary>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="keySelector">A function to extract a key from an element.</param>
        /// <returns>An <see cref="IEnumerable{TSource}"/> whose elements are sorted according to a key.</returns>
        /// <remarks>This method only sorts as much of <paramref name="source"/> as is required to yield the
        /// elements that are requested from the return value.</remarks>
        public static IOrderedEnumerable<TSource> LazyOrderBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            return LazyOrderBy(source, keySelector, null, false);
        }

        /// <summary>
        /// Sorts the elements of a sequence in ascending order according to a key.
        /// </summary>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="keySelector">A function to extract a key from an element.</param>
        /// <param name="comparer">An <see cref="IComparer{T}"/> to compare keys.</param>
        /// <returns>An <see cref="IEnumerable{TSource}"/> whose elements are sorted according to a key.</returns>
        /// <remarks>This method only sorts as much of <paramref name="source"/> as is required to yield the
        /// elements that are requested from the return value.</remarks>
        public static IOrderedEnumerable<TSource> LazyOrderBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return LazyOrderBy(source, keySelector, comparer, false);
        }

        /// <summary>
        /// Sorts the elements of a sequence in descending order according to a key.
        /// </summary>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="keySelector">A function to extract a key from an element.</param>
        /// <returns>An <see cref="IEnumerable{TSource}"/> whose elements are sorted according to a key.</returns>
        /// <remarks>This method only sorts as much of <paramref name="source"/> as is required to yield the
        /// elements that are requested from the return value.</remarks>
        public static IOrderedEnumerable<TSource> LazyOrderByDescending<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            return LazyOrderBy(source, keySelector, null, true);
        }

        /// <summary>
        /// Sorts the elements of a sequence in descending order according to a key.
        /// </summary>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="keySelector">A function to extract a key from an element.</param>
        /// <param name="comparer">An <see cref="IComparer{T}"/> to compare keys.</param>
        /// <returns>An <see cref="IEnumerable{TSource}"/> whose elements are sorted according to a key.</returns>
        /// <remarks>This method only sorts as much of <paramref name="source"/> as is required to yield the
        /// elements that are requested from the return value.</remarks>
        public static IOrderedEnumerable<TSource> LazyOrderByDescending<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return LazyOrderBy(source, keySelector, comparer, true);
        }

        private static IOrderedEnumerable<TSource> LazyOrderBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer, bool descending)
        {
            source.ThrowIfNull("source");
            keySelector.ThrowIfNull("keySelector");

            return new OrderedEnumerable<TSource>(source, new ElementComparer<TSource, TKey>(keySelector, comparer ?? Comparer<TKey>.Default, descending, null));
        }

        #endregion
    }
}
