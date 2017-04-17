using System;
using System.Linq.Expressions;

namespace Nemo
{
    public class Sorting<T> : ISorting
    {
        public Expression<Func<T, object>> OrderBy { get; set; }

        void ISorting.SetOrderBy(LambdaExpression expression)
        {
            var orderBy = expression as Expression<Func<T, object>>;
            if (orderBy != null)
            {
                OrderBy = orderBy;
            }
        }

        public bool Reverse { get; set; }
    }

    public interface ISorting
    {
        void SetOrderBy(LambdaExpression expression);

        bool Reverse { get; set; }
    }
}