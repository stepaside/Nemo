using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections.Comparers
{
    public class ProjectionEqualityComparer<TSource, TKey> : IEqualityComparer<TSource>
    {
        private readonly Func<TSource, TKey> _projection;
        private readonly IEqualityComparer<TKey> _comparer;

        public ProjectionEqualityComparer(Func<TSource, TKey> projection)
            : this(projection, null)
        { }

        public ProjectionEqualityComparer(Func<TSource, TKey> projection, IEqualityComparer<TKey> comparer)
        {
            projection.ThrowIfNull("projection");
            _comparer = comparer ?? EqualityComparer<TKey>.Default;
            _projection = projection;
        }

        #region IEqualityComparer<TSource> Members

        public bool Equals(TSource x, TSource y)
        {
            return _comparer.Equals(_projection(x), _projection(y));
        }

        public int GetHashCode(TSource obj)
        {
            return _projection(obj).GetHashCode();
        }

        #endregion
    }
}
