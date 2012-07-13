using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Collections.Comparers
{
    public class ComparerProxy<T> : IComparer<T>
    {
        private readonly IComparer<T>[] _comparers;

        public ComparerProxy(params IComparer<T>[] comparers)
        {
            _comparers = comparers;
        }

        public int Compare(T x, T y)
        {
            int retVal = 0, i = 0;

            while (retVal == 0 && i < _comparers.Length)
                retVal = _comparers[i++].Compare(x, y);

            return retVal;
        }
    }
}
