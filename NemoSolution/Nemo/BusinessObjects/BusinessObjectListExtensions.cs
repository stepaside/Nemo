using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Validation;

namespace Nemo.BusinessObjects
{
    internal static class BusinessObjectListExtensions
    {
        internal static IEnumerable<TResult> GetDataObjects<TSource, TResult>(this IEnumerable<TSource> items)
            where TResult : class, IBusinessObject
            where TSource : BusinessObject<TResult>
        {
            foreach (var item in items)
            {
                if (item.DataObject != null)
                {
                    yield return item.DataObject;
                }
            }
        }

        internal static IEnumerable<Tuple<int, ValidationResult>> Validate<T>(this IEnumerable<T> items)
            where T : class, IBusinessObject
        {
            if (items.Any())
            {
                int index = 0;
                foreach (T item in items)
                {
                    if (item != null)
                    {
                        var errors = item.Validate();
                        if (errors.Count > 0)
                        {
                            yield return Tuple.Create(index, errors);
                        }
                    }
                    index++;
                }
            }
        }
    }
}
