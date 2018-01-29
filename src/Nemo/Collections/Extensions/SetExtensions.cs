using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections.Extensions
{
    public static class SetExtensions
    {
        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> source, IEqualityComparer<T> comparer)
        {
            source.ThrowIfNull("source");
            return new HashSet<T>(source, comparer ?? EqualityComparer<T>.Default);
        }

        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> source)
        {
            return ToHashSet<T>(source, EqualityComparer<T>.Default);
        }

        public static SortedSet<T> ToSortedSet<T>(this IEnumerable<T> source, IComparer<T> comparer)
        {
            source.ThrowIfNull("source");
            return new SortedSet<T>(source, comparer ?? Comparer<T>.Default);
        }

        public static SortedSet<T> ToSortedSet<T>(this IEnumerable<T> source)
        {
            return ToSortedSet<T>(source, Comparer<T>.Default);
        }
    }
}
