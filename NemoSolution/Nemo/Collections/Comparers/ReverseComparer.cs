using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections.Comparers
{
    public class ReverseComparer<T> : IComparer<T>
    {
        private readonly IComparer<T> _comparer;

        public ReverseComparer(IComparer<T> comparer)
        {
            comparer.ThrowIfNull("comparer");
            _comparer = comparer;
        }

        public int Compare(T object1, T object2)
        {
            return -_comparer.Compare(object1, object2);
        }
    }
}
