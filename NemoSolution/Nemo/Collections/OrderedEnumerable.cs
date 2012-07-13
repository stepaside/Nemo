using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nemo.Collections.Comparers;

namespace Nemo.Collections
{
    // OrderedEnumerable<TSource> implements a lazy "OrderBy" for IEnumerable<TSource>. This class is instantiated by the
    // SortingExtensions.LazyOrder* methods.
    // For more information, see http://code.logos.com/blog/2010/04/a_truly_lazy_orderby_in_linq.html
    internal class OrderedEnumerable<TSource> : IOrderedEnumerable<TSource>
    {
        // Creates a new OrderedEnumerable that will sort 'source' according to 'ordering'.
        public OrderedEnumerable(IEnumerable<TSource> source, ElementComparer<TSource> elementComparer)
        {
            m_source = source;
            m_elementComparer = elementComparer;
        }

        public IOrderedEnumerable<TSource> CreateOrderedEnumerable<TKey>(Func<TSource, TKey> keySelector, IComparer<TKey> comparer, bool descending)
        {
            ElementComparer<TSource, TKey> elementComparer = new ElementComparer<TSource, TKey>(keySelector, comparer ?? Comparer<TKey>.Default, descending, null);
            return new OrderedEnumerable<TSource>(m_source, m_elementComparer.Append(elementComparer));
        }

        public IEnumerator<TSource> GetEnumerator()
        {
            // build an array containing the input sequence
            TSource[] array = m_source.ToArray();

            // from the input array, create the sort keys used by each ordering
            m_elementComparer.CreateSortKeys(array);

            // instead of sorting the actual source array, we sort an array of indexes into that sort array; when the
            // sort is finished, this array (when read in order) gives the indexes of the source array in sorted order
            int[] sourceIndexes = new int[array.Length];
            for (int i = 0; i < sourceIndexes.Length; i++)
                sourceIndexes[i] = i;

            // use a random number generator to pick a pivot for partitioning
            Random random = new Random();

            // track the index of the item that is next to be returned
            int index = 0;

            // use a stack to simulate the recursive quicksort algorithm iteratively
            Stack<Tuple<int, int>> stack = new Stack<Tuple<int, int>>();
            stack.Push(Tuple.Create(0, array.Length - 1));
            while (stack.Count > 0)
            {
                // get the range that needs to be sorted
                Tuple<int, int> currentRange = stack.Pop();

                if (currentRange.Item2 - currentRange.Item1 <= 8)
                {
                    // if the range is small enough, terminate the recursion and use insertion sort instead
                    if (currentRange.Item1 != currentRange.Item2)
                        InsertionSort(sourceIndexes, currentRange.Item1, currentRange.Item2);

                    // yield all the items in this sorted sub-array
                    while (index <= currentRange.Item2)
                    {
                        yield return array[sourceIndexes[index]];
                        index++;
                    }
                }
                else
                {
                    // recursive case: pick a pivot in the array and partition the array around it
                    int pivotIndex = Partition(random, sourceIndexes, currentRange.Item1, currentRange.Item2);

                    // "recurse" by pushing the ranges that still need to be processed (in reverse order) on to the stack
                    stack.Push(Tuple.Create(pivotIndex + 1, currentRange.Item2));
                    stack.Push(Tuple.Create(pivotIndex, pivotIndex));
                    stack.Push(Tuple.Create(currentRange.Item1, pivotIndex - 1));
                }
            }
        }

        // Performs a simple insertion sort of the values identified by the indexes in 'sourceIndexes' in the
        // inclusive range [first, last].
        // Algorithm taken from http://en.wikipedia.org/wiki/Insertion_sort.
        private void InsertionSort(int[] sourceIndexes, int first, int last)
        {
            // assume the first item is already sorted, then process all other indexes in the range
            for (int index = first + 1; index <= last; index++)
            {
                // find the place where value can be inserted into the already sorted elements
                int valueIndex = sourceIndexes[index];
                int insertIndex = index - 1;
                bool done;
                do
                {
                    if (m_elementComparer.Compare(sourceIndexes[insertIndex], valueIndex) > 0)
                    {
                        // move the elements to make room
                        sourceIndexes[insertIndex + 1] = sourceIndexes[insertIndex];
                        insertIndex--;
                        done = insertIndex < first;
                    }
                    else
                    {
                        done = true;
                    }
                } while (!done);

                // insert it in the correct location
                sourceIndexes[insertIndex + 1] = valueIndex;
            }
        }

        // Partitions the 'sourceIndexes' array into two halves around a randomly-selected pivot.
        // Returns the index of the pivot in the partitioned array.
        // Algorithm taken from: http://en.wikipedia.org/wiki/Quicksort
        private int Partition(Random random, int[] sourceIndexes, int first, int last)
        {
            // use random choice to pick the pivot
            int randomPivotIndex = random.Next(first, last + 1);

            // move the pivot to the end of the array
            Swap(ref sourceIndexes[randomPivotIndex], ref sourceIndexes[last]);
            int pivotIndex = sourceIndexes[last];

            // process all the items, moving them before/after the pivot
            int storeIndex = first;
            for (int i = first; i < last; i++)
            {
                if (m_elementComparer.Compare(sourceIndexes[i], pivotIndex) <= 0)
                {
                    Swap(ref sourceIndexes[i], ref sourceIndexes[storeIndex]);
                    storeIndex++;
                }
            }

            // move the pivot into the "middle" of the array; return its location
            Swap(ref sourceIndexes[storeIndex], ref sourceIndexes[last]);
            return storeIndex;
        }

        // Swaps two items.
        private static void Swap<T>(ref T first, ref T second)
        {
            T temp = first;
            first = second;
            second = temp;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool IsDescending
        {
            get
            {
                return m_elementComparer.IsDescending;
            }
        }
        
        readonly IEnumerable<TSource> m_source;
        readonly ElementComparer<TSource> m_elementComparer;
    }
}
