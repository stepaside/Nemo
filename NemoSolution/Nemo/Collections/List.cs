using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Attributes;
using Nemo.Reflection;

namespace Nemo.Collections
{
    public static class List
    {
        private static ConcurrentDictionary<Tuple<Type, bool, bool>, Type> _listTypes = new ConcurrentDictionary<Tuple<Type, bool, bool>, Type>();

        public static IList Create(Type elementType, DistinctAttribute distinctAttribute = null, SortedAttribute sortedAttribute = null)
        {
            var isDisitnct = distinctAttribute != null;
            var isSorted = sortedAttribute != null;

            var listTypeKey = Tuple.Create(elementType, isDisitnct, isSorted);

            var listCreator = Nemo.Reflection.Activator.CreateDelegate(_listTypes.GetOrAdd(listTypeKey,
                                                                        t => t.Item3 ? typeof(SortedList<>).MakeGenericType(t.Item1) 
                                                                            : (t.Item2 ? typeof(HashList<>).MakeGenericType(t.Item1) 
                                                                                : typeof(List<>).MakeGenericType(t.Item1))));
            if (isSorted)
            {
                var comparerTypeCreator = Nemo.Reflection.Activator.CreateDelegate(sortedAttribute.ComparerType);
                return (IList)listCreator(new object[] { comparerTypeCreator, isDisitnct });
            }
            else if (isDisitnct)
            {
                var comparerTypeCreator = Nemo.Reflection.Activator.CreateDelegate(distinctAttribute.EqualityComparerType);
                return (IList)listCreator(new object[] { comparerTypeCreator });
            }
            else
            {
                return (IList)listCreator(new object[] { });
            }
        }

        public static void AddIf<T>(this IList<T> list, T item, Func<T, bool> predicate)
        {
            if (predicate(item))
            {
                list.Add(item);
            }
        }
    }
}
