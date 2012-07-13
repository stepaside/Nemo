using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;
using Nemo.Caching;

namespace Nemo
{
    public class ObjectEqualityComparer<T> : IEqualityComparer<T>
        where T : class, IBusinessObject
    {
        public bool Equals(T x, T y)
        {
            return x.ComputeHash() == y.ComputeHash();
        }

        public int GetHashCode(T obj)
        {
            return new CacheKey(obj).GetHashCode();
        }
    }
}
