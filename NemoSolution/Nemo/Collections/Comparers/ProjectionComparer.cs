using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections.Comparers
{
    public class ProjectionComparer<TSource, TKey> : IComparer<TSource>
        where TSource : class
    {
        private readonly Func<TSource, TKey> _projection;
        private readonly IComparer<TKey> _comparer;

        public ProjectionComparer(Func<TSource, TKey> projection)
            : this(projection, null)
        { }

        public ProjectionComparer(Func<TSource, TKey> projection, IComparer<TKey> comparer)
        {
            projection.ThrowIfNull("projection");
            _comparer = comparer ?? Comparer<TKey>.Default;
            _projection = projection;
        }

        public int Compare(TSource x, TSource y)
        {
            // Don't want to project from nullity
            if (x ==null && y == null)
            {
                return 0;
            }
            if (x == null)
            {
                return -1;
            }
            return y == null ? 1 : _comparer.Compare(_projection(x), _projection(y));
        }
    }
}
