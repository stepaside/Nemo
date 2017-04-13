using System;
using System.Linq.Expressions;

namespace Nemo
{
    public class Sorting<T>
    {
        public Expression<Func<T, object>> OrderBy { get; set; }

        public bool Reverse { get; set; }
    }
}