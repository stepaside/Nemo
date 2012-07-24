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
        private static ConcurrentDictionary<Type, Type> _listTypes = new ConcurrentDictionary<Type, Type>();
        private static ConcurrentDictionary<Type, Type> _distinctListTypes = new ConcurrentDictionary<Type, Type>();
        private static ConcurrentDictionary<Type, Type> _distinctSortedListTypes = new ConcurrentDictionary<Type, Type>();
        private static ConcurrentDictionary<Type, Type> _sortedListTypes = new ConcurrentDictionary<Type, Type>();

        public static IList Create(Type elementType, DistinctAttribute distinctAttribute, SortedAttribute sortedAttribute)
        {
            var isDistinct = distinctAttribute != null;
            var isSorted = sortedAttribute != null;

            if (isSorted)
            {
                return CreateSorted(elementType, sortedAttribute.ComparerType, isDistinct);
            }
            else if (isDistinct)
            {
                return CreateDistinct(elementType, distinctAttribute.EqualityComparerType);
            }
            else
            {
                return Create(elementType);
            }
        }

        public static IList Create(Type elementType)
        {
            var listCreator = Nemo.Reflection.Activator.CreateDelegate(_listTypes.GetOrAdd(elementType, t => typeof(List<>).MakeGenericType(t)));
            return (IList)listCreator(new object[] { });
        }

        public static IList CreateDistinct(Type elementType, Type comparerType)
        {
            Type[] types;
            if (comparerType != null)
            {
                types = new[] { comparerType };
            }
            else
            {
                types = Type.EmptyTypes;
            }
            var listCreator = Nemo.Reflection.Activator.CreateDelegate(_distinctListTypes.GetOrAdd(elementType, t => typeof(HashList<>).MakeGenericType(t)), types);

            if (comparerType != null)
            {
                var comparerTypeCreator = Nemo.Reflection.Activator.CreateDelegate(comparerType);
                var comparer = comparerTypeCreator();
                return (IList)listCreator(new object[] { comparer });
            }
            else
            {
                return (IList)listCreator(new object[] { });
            }
        }

        public static IList CreateSorted(Type elementType, Type comparerType, bool distintct)
        {
            Type[] types;
            if (comparerType != null)
            {
                types = new[] { comparerType, typeof(bool) };
            }
            else
            {
                types = new[] { typeof(bool) };
            }

            Nemo.Reflection.Activator.ObjectActivator listCreator;
            if (distintct)
            {
                listCreator = Nemo.Reflection.Activator.CreateDelegate(_distinctSortedListTypes.GetOrAdd(elementType, t => typeof(SortedList<>).MakeGenericType(t)), types);
            }
            else
            {
                listCreator = Nemo.Reflection.Activator.CreateDelegate(_sortedListTypes.GetOrAdd(elementType, t => typeof(SortedList<>).MakeGenericType(t)), types);
            }

            if (comparerType != null)
            {
                var comparerTypeCreator = Nemo.Reflection.Activator.CreateDelegate(comparerType);
                var comparer = comparerTypeCreator();
                return (IList)listCreator(new object[] { comparer, false });
            }
            else
            {
                return (IList)listCreator(new object[] { false });
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
