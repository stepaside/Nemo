using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Collections;
using Nemo.Reflection;

namespace Nemo.Collections
{
    public static class List
    {
        private static ConcurrentDictionary<Type, Type> _listTypes = new ConcurrentDictionary<Type, Type>();

        public static IList Create(Type elementType)
        {
            var listCreator = Nemo.Reflection.Activator.CreateDelegate(_listTypes.GetOrAdd(elementType, t => typeof(List<>).MakeGenericType(t)));
            return (IList)listCreator(new object[] { });
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
