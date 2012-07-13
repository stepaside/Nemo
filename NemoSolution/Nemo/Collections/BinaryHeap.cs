using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections
{
    /// <summary>
    /// A binary heap, useful for sorting data and priority queues.
    /// </summary>
    /// <typeparam name="T"><![CDATA[IComparable<T> type of item in the heap]]>.</typeparam>
    public class BinaryHeap<T> : ICollection<T>
    {
        // Constants
        private const int DEFAULT_SIZE = 8;
        // Fields
        private List<T> _data;
        private IComparer<T> _comparer;

        #region Properties
        
        /// <summary>
        /// Gets the number of values in the heap. 
        /// </summary>
        public int Count
        {
            get { return _data.Count; }
        }
        
        /// <summary>
        /// Gets whether or not the binary heap is readonly.
        /// </summary>
        public bool IsReadOnly
        {
            get { return false; }
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new binary heap.
        /// </summary>
        public BinaryHeap()
            : this(DEFAULT_SIZE, null)
        { }

        public BinaryHeap(int capacity)
            : this(capacity, null)
        { }

        public BinaryHeap(IComparer<T> comparer)
            : this(DEFAULT_SIZE, comparer)
        { }
        
        public BinaryHeap(int capacity, IComparer<T> comparer)
        {
            _comparer = comparer ?? Comparer<T>.Default;
            _data = new List<T>(capacity);
        }

        public BinaryHeap(IEnumerable<T> data)
            : this(data, null)
        { }

        public BinaryHeap(IEnumerable<T> data, IComparer<T> comparer)
            : this(DEFAULT_SIZE, comparer)
        {
            _data.AddRange(data);
            BuildHeap();
        }

        public BinaryHeap(IEnumerable<T> data, int count, IComparer<T> comparer)
            : this(count, comparer)
        {
            _data.AddRange(data.Take(count));
            BuildHeap();
        }

        public BinaryHeap(BinaryHeap<T> heap)
            : this(heap._data, heap._comparer)
        { }

        #endregion

        #region Methods

        /// <summary>
        /// Gets the first value in the heap without removing it.
        /// </summary>
        /// <returns>The lowest value of type TValue.</returns>
        public T Peek()
        {
            return _data[0];
        }

        /// <summary>
        /// Removes all items from the heap.
        /// </summary>
        public void Clear()
        {
            _data.Clear();
        }

        /// <summary>
        /// Adds a key and value to the heap.
        /// </summary>
        /// <param name="item">The item to add to the heap.</param>
        public void Add(T item)
        {
            _data.Add(item);
            UpHeap(this.Count - 1);
        }

        /// <summary>
        /// Removes and returns the first item in the heap.
        /// </summary>
        /// <returns>The next value in the heap.</returns>
        public T Remove()
        {
            if (this.Count == 0)
            {
                throw new InvalidOperationException("Cannot remove item, heap is empty.");
            }
            T v = _data[0];
            _data[0] = _data[this.Count - 1];
            _data.RemoveAt(this.Count - 1);
            if (this.Count > 0)
            {
                DownHeap(0);
            }
            return v;
        }

        //helper function that performs up-heap bubbling
        private void UpHeap(int index)
        {
            int p = index;
            T item = _data[p];
            int par = Parent(p);
            while (par > -1 && _comparer.Compare(item, _data[par]) < 0)
            {
                _data[p] = _data[par]; //Swap nodes
                p = par;
                par = Parent(p);
            }
            _data[p] = item;
        }

        //helper function that performs down-heap bubbling
        private void DownHeap(int index)
        {
            int n;
            int p = index;
            T item = _data[p];
            while (true)
            {
                int ch1 = Child1(p);
                if (ch1 >= this.Count)
                    break;
                int ch2 = Child2(p);
                if (ch2 >= this.Count)
                {
                    n = ch1;
                }
                else
                {
                    n = _comparer.Compare(_data[ch1], _data[ch2]) < 0 ? ch1 : ch2;
                }
                if (_comparer.Compare(item, _data[n]) > 0)
                {
                    _data[p] = _data[n]; //Swap nodes
                    p = n;
                }
                else
                {
                    break;
                }
            }
            _data[p] = item;
        }

        private void BuildHeap()
        {
            for (int i = this.Count / 2; i > -1; i--)
            {
                DownHeap(i);
            }
        }

        //helper function that calculates the parent of a node
        private static int Parent(int index)
        {
            return (index - 1) >> 1;
        }

        //helper function that calculates the first child of a node
        private static int Child1(int index)
        {
            return (index << 1) + 1;
        }

        //helper function that calculates the second child of a node
        private static int Child2(int index)
        {
            return (index << 1) + 2;
        }

        /// <summary>
        /// Creates a new instance of an identical binary heap.
        /// </summary>
        /// <returns>A BinaryHeap.</returns>
        public BinaryHeap<T> Copy()
        {
            return new BinaryHeap<T>(_data, this.Count, _comparer);
        }

        /// <summary>
        /// Gets an enumerator for the binary heap.
        /// </summary>
        /// <returns>An IEnumerator of type T.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < this.Count; i++)
            {
                yield return _data[i];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Checks to see if the binary heap contains the specified item.
        /// </summary>
        /// <param name="item">The item to search the binary heap for.</param>
        /// <returns>A boolean, true if binary heap contains item.</returns>
        public bool Contains(T item)
        {
            return _data.Exists(i => _comparer.Compare(i, item) == 0);
        }

        /// <summary>
        /// Copies the binary heap to an array at the specified index.
        /// </summary>
        /// <param name="array">One dimensional array that is the destination of the copied elements.</param>
        /// <param name="arrayIndex">The zero-based index at which copying begins.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            _data.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Removes an item from the binary heap. This utilizes the type T's Comparer and will remove duplicates.
        /// </summary>
        /// <param name="item">The item to be removed.</param>
        /// <returns>Boolean true if the item was removed.</returns>
        public bool Remove(T item)
        {
            return _data.RemoveAll(i => _comparer.Compare(i, item) == 0) > 0;
        }

        #endregion
    }
}
