using System;
using System.Collections.Generic;
using System.Threading;
using System.Collections.Concurrent;
using Nemo.Extensions;

namespace Nemo.Collections
{
    public enum PoolAccessMode { FIFO, LIFO, Circular };
    public enum PoolAcquireMode { Eager, Lazy, LazyExpanding };

    public class Pool<T> : IDisposable
        where T : class
    {
        private bool _isDisposed;
        private Func<Pool<T>, T> _factory;
        private PoolAcquireMode _acquireMode;
        private IItemStore _itemStore;
        private int _size;
        private int _count;
        private Semaphore _sync;
        private readonly bool _blocking;

        public Pool(int size, Func<Pool<T>, T> factory, PoolAcquireMode acquireMode = PoolAcquireMode.LazyExpanding, PoolAccessMode accessMode = PoolAccessMode.FIFO, bool blocking = true)
        {
            size.ThrowIfNonPositive("size");
            factory.ThrowIfNull("factory");

            _size = size;
            _factory = factory;
            _sync = new Semaphore(size, size);
            _blocking = blocking;
            _acquireMode = acquireMode;
            _itemStore = CreateItemStore(accessMode, size);
            if (acquireMode == PoolAcquireMode.Eager)
            {
                PreloadItems();
            }
        }

        public T Acquire()
        {
            if (_blocking)
            {
                _sync.WaitOne();
            }
            switch (_acquireMode)
            {
                case PoolAcquireMode.Eager:
                    return AcquireEager();
                case PoolAcquireMode.Lazy:
                    return AcquireLazy();
                case PoolAcquireMode.LazyExpanding:
                    return AcquireLazyExpanding();
                default:
                    throw new ArgumentException("Unknown PoolLoadingMode encountered.");
            }
        }

        public void Release(T item)
        {
            lock (_itemStore)
            {
                _itemStore.Put(item);
            }
            if (_blocking)
            {
                _sync.Release();
            }
        }

        public int Size
        {
            get
            {
                return _size;
            }
        }

        public int AvailableCount
        {
            get
            {
                lock (_itemStore)
                {
                    return _itemStore.Count;
                }
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }
            _isDisposed = true;
            if (typeof(IDisposable).IsAssignableFrom(typeof(T)))
            {
                lock (_itemStore)
                {
                    while (_itemStore.Count > 0)
                    {
                        IDisposable disposable = (IDisposable)_itemStore.Get();
                        disposable.Dispose();
                    }
                }
            }
            _sync.Close();
        }

        public bool IsDisposed
        {
            get { return _isDisposed; }
        }

        private IItemStore CreateItemStore(PoolAccessMode mode, int capacity)
        {
            switch (mode)
            {
                case PoolAccessMode.FIFO:
                    return new QueueStore(capacity);
                case PoolAccessMode.LIFO:
                    return new StackStore(capacity);
                case PoolAccessMode.Circular:
                    return new CircularStore(capacity);
                default:
                    throw new ArgumentException("Invalid PoolAccessMode specified.");
            }
        }

        private T AcquireEager()
        {
            lock (_itemStore)
            {
                return _itemStore.Get();
            }
        }

        private T AcquireLazy()
        {
            lock (_itemStore)
            {
                if (_itemStore.Count > 0)
                {
                    return _itemStore.Get();
                }
            }
            Interlocked.Increment(ref _count);
            return _factory(this);
        }

        private T AcquireLazyExpanding()
        {
            bool shouldExpand = false;
            if (_count < _size)
            {
                int newCount = Interlocked.Increment(ref _count);
                if (newCount <= _size)
                {
                    shouldExpand = true;
                }
                else
                {
                    // Another thread took the last spot - use the store instead
                    Interlocked.Decrement(ref _count);
                }
            }
            if (shouldExpand)
            {
                return _factory(this);
            }
            else
            {
                lock (_itemStore)
                {
                    return _itemStore.Get();
                }
            }
        }

        private void PreloadItems()
        {
            for (int i = 0; i < _size; i++)
            {
                T item = _factory(this);
                _itemStore.Put(item);
            }
            _count = _size;
        }

        #region Storage Mechanisms

        interface IItemStore
        {
            T Get();
            void Put(T item);
            int Count { get; }
        }

        class QueueStore : Queue<T>, IItemStore
        {
            public QueueStore(int capacity)
                : base(capacity)
            {
            }

            public T Get()
            {
                return Dequeue();
            }

            public void Put(T item)
            {
                Enqueue(item);
            }
        }

        class StackStore : Stack<T>, IItemStore
        {
            public StackStore(int capacity)
                : base(capacity)
            {
            }

            public T Get()
            {
                return Pop();
            }

            public void Put(T item)
            {
                Push(item);
            }
        }

        class CircularStore : IItemStore
        {
            private List<Slot> slots;
            private int freeSlotCount;
            private int position = -1;

            public CircularStore(int capacity)
            {
                slots = new List<Slot>(capacity);
            }

            public T Get()
            {
                if (Count == 0)
                    throw new InvalidOperationException("The buffer is empty.");

                int startPosition = position;
                do
                {
                    Advance();
                    Slot slot = slots[position];
                    if (!slot.IsInUse)
                    {
                        slot.IsInUse = true;
                        --freeSlotCount;
                        return slot.Item;
                    }
                } while (startPosition != position);
                throw new InvalidOperationException("No free slots.");
            }

            public void Put(T item)
            {
                Slot slot = slots.Find(s => object.Equals(s.Item, item));
                if (slot == null)
                {
                    slot = new Slot(item);
                    slots.Add(slot);
                }
                slot.IsInUse = false;
                ++freeSlotCount;
            }

            public int Count
            {
                get { return freeSlotCount; }
            }

            private void Advance()
            {
                position = (position + 1) % slots.Count;
            }

            class Slot
            {
                public Slot(T item)
                {
                    this.Item = item;
                }

                public T Item { get; private set; }
                public bool IsInUse { get; set; }
            }
        }

        #endregion
    }
}
