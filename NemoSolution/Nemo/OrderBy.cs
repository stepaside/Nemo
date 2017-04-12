using System;
using System.Linq.Expressions;

namespace Nemo
{
    public enum SortingOrder
    {
        Ascending,
        Descending
    }

    public class OrderBy<T>
    {
        public Expression<Func<T, object>> Expression { get; set; }

        public SortingOrder Direction { get; set; }
    }
}