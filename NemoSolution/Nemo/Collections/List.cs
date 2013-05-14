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
            var listType = _listTypes.GetOrAdd(elementType, t => typeof(List<>).MakeGenericType(t));
            return (IList)Nemo.Reflection.Activator.New(listType);
        }

        public static Array CreateArray(Type elementType, IList list)
        {
            var array = Array.CreateInstance(elementType, list.Count);
            list.CopyTo(array, 0);
            return array;
        }

        public static IList CreateDistinct(Type elementType, Type comparerType)
        {
            var listType = _distinctListTypes.GetOrAdd(elementType, t => typeof(HashList<>).MakeGenericType(t));
            
            if (comparerType != null)
            {
                var comparer = Nemo.Reflection.Activator.New(comparerType);
                return (IList)Nemo.Reflection.Activator.New(listType, comparer);
            }
            else
            {
                return (IList)Nemo.Reflection.Activator.New(listType);
            }
        }

        public static IList CreateSorted(Type elementType, Type comparerType, bool distintct)
        {
            Type listType;
            if (distintct)
            {
                listType = _distinctSortedListTypes.GetOrAdd(elementType, t => typeof(SortedList<>).MakeGenericType(t));
            }
            else
            {
                listType = _sortedListTypes.GetOrAdd(elementType, t => typeof(SortedList<>).MakeGenericType(t));
            }

            if (comparerType != null)
            {
                var comparer = Nemo.Reflection.Activator.New(comparerType);
                return (IList)Nemo.Reflection.Activator.New(listType, comparer, false);
            }
            else
            {
                return (IList)Nemo.Reflection.Activator.New(listType, false);
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
