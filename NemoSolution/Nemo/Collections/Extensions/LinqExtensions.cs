using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Nemo.Collections;
using Nemo.Collections.Comparers;
using Nemo.Extensions;
using Nemo.Fn;
using Nemo.Utilities;

namespace Nemo.Collections.Extensions
{
    /// <summary>
    /// Many methods here were inspired by System.Interactive; 
    /// some method implementations were taken from here http://blogs.bartdesmet.net/blogs/bart/archive/tags/Rx/default.aspx
    /// </summary>
    public static class LinqExtensions
    {
        #region Constructor Methods

        public static IEnumerable<T> Return<T>(this T value)
        {
            yield return value;
        }

        public static IEnumerable<T> AsEnumerable<T>(this IEnumerator<T> enumerator)
        {
            enumerator.ThrowIfNull("enumerator");
            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }

        public static IEnumerable<T> Generate<T>(T initial, Func<T, T> generator)
        {
            return Generate(initial, _ => true, generator);
        }

        public static IEnumerable<T> Generate<T>(T initial, Func<T, bool> condition, Func<T, T> generator)
        {
            condition.ThrowIfNull("condition");
            generator.ThrowIfNull("generator");

            T current = initial;
            while (condition(current))
            {
                yield return current;
                current = generator(current);
            }
        }

        public static IEnumerable<TResult> Generate<T, TResult>(T initial, Func<T, TResult> resultSelector, Func<T, T> generator)
        {
            return Generate(initial, _ => true, resultSelector, generator);
        }

        public static IEnumerable<TResult> Generate<T, TResult>(T initial, Func<T, bool> condition, Func<T, TResult> resultSelector, Func<T, T> generator)
        {
            condition.ThrowIfNull("condition");
            resultSelector.ThrowIfNull("resultSelector");
            generator.ThrowIfNull("generator");

            T current = initial;
            while (condition(current))
            {
                yield return resultSelector(current);
                current = generator(current);
            }
        }

        public static IEnumerable<T> StartWith<T>(this IEnumerable<T> source, params T[] first)
        {
            source.ThrowIfNull("source");
            return first.Concat(source);
        }

        public static IEnumerable<T> StartWith<T>(this IEnumerable<T> source, T first)
        {
            source.ThrowIfNull("source");
            return first.Prepend(source);
        }

        public static IEnumerable<T> Defer<T>(Func<IEnumerable<T>> enumerableFactory)
        {
            enumerableFactory.ThrowIfNull("enumerableFactory");
            return new LazyEnumerable<T>(enumerableFactory);
        }

        private class LazyEnumerable<T> : IEnumerable<T>, IEnumerable
        {
            private Lazy<IEnumerable<T>> _lazyEnumerable;

            public LazyEnumerable(Func<IEnumerable<T>> enumerableFactory)
            {
                _lazyEnumerable = new Lazy<IEnumerable<T>>(enumerableFactory, true);
            }

            public IEnumerator<T> GetEnumerator()
            {
                return _lazyEnumerable.Value.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }
        }
        
        #endregion

        #region Functional Methods

        public static IEnumerable<TResult> Let<TSource, TResult>(this IEnumerable<TSource> source, Func<IEnumerable<TSource>, IEnumerable<TResult>> func)
        {
            func.ThrowIfNull("func");
            return func(source);
        }

        public static TResult Let<TSource, TResult>(this TSource source, Func<TSource, TResult> func)
        {
            func.ThrowIfNull("func");
            return func(source);
        }
        
        #endregion

        #region Imperative Methods

        public static void Consume<T>(this IEnumerable<T> source)
        {
            source.ThrowIfNull("source");
            foreach (var item in source) { }
        }

        public static void Run<T>(this IEnumerable<T> source, Action<T> action)
        {
            source.ThrowIfNull("source");
            action.ThrowIfNull("action");
            foreach (T element in source)
            {
                action(element);
            }
        }

        public static void Run<T>(this IEnumerable<T> source, Action<int, T> action)
        {
            source.ThrowIfNull("source");
            action.ThrowIfNull("action");

            var index = 0;
            foreach (T element in source)
            {
                action(index, element);
                index++;
            }
        }

        public static IEnumerable<T> Iterate<T>(this IEnumerable<T> source)
        {
            source.ThrowIfNull("source");
            foreach (var item in source)
            {
                yield return item;
            }
        }

        public static IEnumerable<T> Do<T>(this IEnumerable<T> source, Action<T> action)
        {
            source.ThrowIfNull("source");
            action.ThrowIfNull("action");

            foreach (var item in source)
            {
                action(item);
                yield return item;
            }
        }

        public static IEnumerable<T> Do<T>(this IEnumerable<T> source, Action<int, T> action)
        {
            source.ThrowIfNull("source");
            action.ThrowIfNull("action");

            var index = 0;
            foreach (var item in source)
            {
                action(index, item);
                index++;
                yield return item;
            }
        }
    
        public static void CopyTo<T>(this IEnumerable<T> source, T[] array, int arrayIndex)
        {
            int index = arrayIndex;
            foreach (var cur in source)
            {
                array[index] = cur;
                ++index;
            }
        }

        public static List<T> ToLazyList<T>(this IEnumerable<T> source)
        {
            if (source is Stream<T>)
            {
                return ((Stream<T>)source).ToList();
            }
            else
            {
                return Enumerable.ToList(source);
            }
        }

        #endregion

        #region Concat Methods

        public static IEnumerable<T> Prepend<T>(this T head, IEnumerable<T> tail)
        {
            tail.ThrowIfNull("tail");
            return head.Return().Concat(tail);
        }

        public static IEnumerable<T> Append<T>(this IEnumerable<T> head, T tail)
        {
            head.ThrowIfNull("head");
            return head.Concat(tail.Return());
        }

        public static IEnumerable<T> Concat<T>(params IEnumerable<T>[] sources)
        {
            return ((IEnumerable<IEnumerable<T>>)sources).Concat();
        }

        public static IEnumerable<T> Concat<T>(this IEnumerable<IEnumerable<T>> sources)
        {
            sources.ThrowIfNull("sources");
            foreach (var source in sources)
            {
                foreach (var item in source)
                {
                    yield return item;
                }
            }
        }

        #endregion

        #region Union Methods

        public static IEnumerable<T> Union<T>(this IEnumerable<T> source, T value)
        {
            return Enumerable.Union(source, value.Return());
        }

        public static IEnumerable<T> Union<T>(this IEnumerable<T> source, T value, IEqualityComparer<T> comparer)
        {
            return Enumerable.Union(source, value.Return(), comparer);
        }

        #endregion

        #region Intersect Methods

        public static IEnumerable<T> Intersect<T>(this IEnumerable<T> source, T value)
        {
            return Enumerable.Intersect<T>(source, value.Return(), EqualityComparer<T>.Default);
        }

        public static IEnumerable<T> Intersect<T>(this IEnumerable<T> source, T value, IEqualityComparer<T> comparer)
        {
            return Enumerable.Intersect<T>(source, value.Return(), comparer);
        }

        #endregion

        #region Except Methods

        public static IEnumerable<T> Except<T>(this IEnumerable<T> source, T value)
        {
            return Enumerable.Except(source, value.Return(), EqualityComparer<T>.Default);
        }

        public static IEnumerable<T> Except<T>(this IEnumerable<T> source, T value, IEqualityComparer<T> comparer)
        {
            return Enumerable.Except(source, value.Return(), comparer);
        }

        #endregion

        #region Zip Methods

        public static IEnumerable<V> Zip<T, U, V>(this IEnumerable<T> first, IEnumerable<U> second, Func<T, U, int, V> selector)
        {
            first.ThrowIfNull("first");
            second.ThrowIfNull("second");
            selector.ThrowIfNull("selector");

            int index = 0;
            using (var e1 = first.GetEnumerator())
            {
                using (var e2 = second.GetEnumerator())
                {
                    while (e1.MoveNext())
                    {
                        if (e2.MoveNext())
                        {
                            yield return selector(e1.Current, e2.Current, index++);
                        }
                        else
                        {
                            yield break;
                        }
                    }
                }
            }
        }

        public static Tuple<IEnumerable<U>, IEnumerable<V>> Unzip<T, U, V>(this IEnumerable<T> source, Func<T, Tuple<U, V>> splitter)
        {
            source.ThrowIfNull("source");
            splitter.ThrowIfNull("splitter");

            var items = source.Select(i => splitter(i));
            return Tuple.Create(items.Select(i => i.Item1), items.Select(i => i.Item2));
        }

        #endregion

        #region Repeat Methods

        public static IEnumerable<T> Repeat<T>(this T value)
        {
            while (true)
            {
                yield return value;
            }
        }

        public static IEnumerable<T> Repeat<T>(this T value, int count)
        {
            return LinqExtensions.Repeat<T>(value).Take(count);
        }

        public static IEnumerable<T> Repeat<T>(this IEnumerable<T> source)
        {
            source.ThrowIfNull("source");
            while (true)
            {
                foreach (var current in source)
                {
                    yield return current;
                }
            }
        }

        public static IEnumerable<T> Repeat<T>(this IEnumerable<T> source, int count)
        {
            count.ThrowIfNonPositive("count");
            return LinqExtensions.Repeat(source).Take(count);
        }

        #endregion
        
        #region Merge Methods

        public static IEnumerable<T> Merge<T>(this IEnumerable<IEnumerable<T>> sources)
        {
            sources.ThrowIfNull("sources");
            return sources.AsParallel().SelectMany(_ => _);
        }

        public static IEnumerable<T> Merge<T>(params IEnumerable<T>[] sources)
        {
            return ((IEnumerable<IEnumerable<T>>)sources).Merge();
        }

        public static IEnumerable<T> Merge<T>(this IEnumerable<IOrderedEnumerable<T>> sources)
            where T : IComparable
        {
            return sources.Merge(Comparer<T>.Default);
        }

        public static IEnumerable<T> Merge<T>(this IEnumerable<IOrderedEnumerable<T>> sources, IComparer<T> comparer)
        {
            sources.ThrowIfNull("sources");
            comparer.ThrowIfNull("comparer");
            var items = sources.Select(s => s.GetEnumerator()).ToArray();

            if (items.Length > 1)
            {
                Comparison<Tuple<int, T>> comparison = (a, b) => comparer.Compare(a.Item2, b.Item2);

                // Create heap
                var heap = new BinaryHeap<Tuple<int, T>>(items.Length, new ComparisonComparer<Tuple<int, T>>(comparison));
                // Populate heap
                for (int i = 0; i < items.Length; i++)
                {
                    if (items[i].MoveNext())
                    {
                        heap.Add(Tuple.Create(i, items[i].Current));
                    }
                }
                // Read from heap
                while (heap.Count > 0)
                {
                    var tuple = heap.Remove();
                    yield return tuple.Item2;
                    if (items[tuple.Item1].MoveNext())
                    {
                        heap.Add(Tuple.Create(tuple.Item1, items[tuple.Item1].Current));
                    }
                    else
                    {
                        var item = items.Select((s, i) => new { Source = s, Index = i }).FirstOrDefault(a => a.Source.MoveNext());
                        if (item != null)
                        {
                            heap.Add(Tuple.Create(item.Index, item.Source.Current));
                        }
                    }
                }
            }
            else if (items.Length == 1)
            {
                while (items[0].MoveNext())
                {
                    yield return items[0].Current;
                }
            }
            yield break;
        }

        public static IEnumerable<T> Merge<T>(this IOrderedEnumerable<T> first, IOrderedEnumerable<T> second)
        {
            return Merge(first, second, Comparer<T>.Default);
        }

        public static IEnumerable<T> Merge<T>(this IOrderedEnumerable<T> first, IOrderedEnumerable<T> second, IComparer<T> comparer)
        {
            second.ThrowIfNull("comparer");

            var comparison = new Comparison<T>((a, b) => comparer.Compare(a, b));
            return Merge(first, second, comparison);
        }

        public static IEnumerable<T> Merge<T>(this IOrderedEnumerable<T> first, IOrderedEnumerable<T> second, Comparison<T> comparison)
        {
            return Merge(first, second, comparison, false);
        }

        private static IEnumerable<T> Merge<T>(this IOrderedEnumerable<T> first, IOrderedEnumerable<T> second, Comparison<T> comparison, bool distinct)
        {
            first.ThrowIfNull("first");
            second.ThrowIfNull("second");
            second.ThrowIfNull("comparison");

            var iter1 = distinct ? first.Distinct().GetEnumerator() : first.GetEnumerator();
            var iter2 = second.GetEnumerator();

            bool list1NotEmpty = iter1.MoveNext();
            bool list2NotEmpty = iter2.MoveNext();

            while (list1NotEmpty || list2NotEmpty)
            {
                var result = !list1NotEmpty ? 1 : !list2NotEmpty ? -1 : comparison(iter1.Current, iter2.Current);
                if (result < 0)
                {
                    yield return iter1.Current;
                    list1NotEmpty = iter1.MoveNext();
                }
                else if (result > 0)
                {
                    yield return iter2.Current;
                    list2NotEmpty = iter2.MoveNext();
                }
                else
                {
                    yield return iter1.Current;
                    if (!distinct)
                    {
                        yield return iter2.Current;
                    }
                    list1NotEmpty = iter1.MoveNext();
                    list2NotEmpty = iter2.MoveNext();
                }
            }
        }

        #endregion

        #region Share Methods

        /// <summary>
        /// Shares an enumerable among multiple consumers guaranteeing that no two consumers can see the same element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static ISharedEnumerable<T> Share<T>(this IEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");
            return new SharedEnumerable<T>(source);
        }

        private sealed class SharedEnumerable<T> : ISharedEnumerable<T>, IDisposable
        {
            private int _enumeratorCount;
            private object _lockObject;
            private IEnumerator<T> _sharedEnumerator;
            private IEnumerable<T> _source;

            public SharedEnumerable(IEnumerable<T> source)
            {
                this._source = source;
                this._lockObject = new object();
            }

            public void Dispose()
            {
                lock (this._lockObject)
                {
                    if (!this.Disposed)
                    {
                        this._sharedEnumerator.Dispose();
                        this.Disposed = true;
                    }
                }
            }

            internal bool Disposed
            {
                get;
                set;
            }

            public IEnumerator<T> GetEnumerator()
            {
                lock (this._lockObject)
                {
                    if (this._sharedEnumerator == null)
                    {
                        this._sharedEnumerator = this._source.GetEnumerator();
                    }
                    this._enumeratorCount++;
                    return new SharedEnumerator((SharedEnumerable<T>)this, this._sharedEnumerator, this._lockObject);
                }
            }

            public NextResult<T> Next()
            {
                var iterator = this.GetEnumerator();
                return new NextResult<T>(iterator.MoveNext(), iterator.Current);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            private class SharedEnumerator : IEnumerator<T>
            {
                private T _current;
                private SharedEnumerable<T> _enumerable;
                private object _lockObject;
                private IEnumerator<T> _sharedEnumerator;

                internal SharedEnumerator(SharedEnumerable<T> enumerable, IEnumerator<T> sharedEnumerator, object lockObject)
                {
                    this._enumerable = enumerable;
                    this._sharedEnumerator = sharedEnumerator;
                    this._lockObject = lockObject;
                }

                public void Dispose()
                {
                    this._enumerable = null;
                    this._sharedEnumerator = null;
                    this._lockObject = null;
                }

                public bool MoveNext()
                {
                    lock (this._lockObject)
                    {
                        if (this._enumerable.Disposed)
                        {
                            return false;
                        }
                        bool flag = this._sharedEnumerator.MoveNext();
                        if (flag)
                        {
                            this._current = this._sharedEnumerator.Current;
                        }
                        if (!flag)
                        {
                            this._sharedEnumerator.Dispose();
                            this._enumerable.Disposed = true;
                        }
                        return flag;
                    }
                }

                public void Reset()
                {
                    throw new NotSupportedException("SharedEnumerators cannot be Reset.");
                }

                public T Current
                {
                    get
                    {
                        return this._current;
                    }
                }

                object IEnumerator.Current
                {
                    get
                    {
                        return this.Current;
                    }
                }
            }
        }

        #endregion

        #region Replicate Methods

        public static IEnumerable<IEnumerable<T>> Replicate<T>(this IEnumerable<T> source)
        {
            source.ThrowIfNull("source");
            while (true)
            {
                yield return source;
            }
        }

        public static IEnumerable<IEnumerable<T>> Replicate<T>(this IEnumerable<T> source, int count)
        {
            count.ThrowIfNonPositive("count");
            return source.Replicate().Take(count);
        }

        #endregion

        #region Partition Methods
        
        public static IEnumerable<IEnumerable<T>> Split<T>(this IEnumerable<T> source, int size)
        {
            source.ThrowIfNull("source");
            size.ThrowIfNonPositive("size");

            T[] items = null;
            var count = 0;
            foreach (var item in source)
            {
                if (items == null)
                {
                    items = new T[size];
                }
                items[count++] = item;
                if (count != size)
                {
                    continue;
                }
                yield return items.Select(x => x);
                items = null;
                count = 0;
            }
            if (items != null && count > 0)
            {
                yield return items.Take(count);
            }
        }
        
        public static Tuple<IList<T>, IList<T>> Partition<T>(this IEnumerable<T> source, Func<T, bool> predicate, bool parallel = false)
        {
            source.ThrowIfNull("source");
            predicate.ThrowIfNull("predicate");

            if (parallel)
            {
                var trueList = new List<T>();
                var falseList = new List<T>();
                var partitionLock = new object(); 

                var items = source is IList<T> ? (IList<T>)source : (IList<T>)source.ToList();
                Parallel.For(0, items.Count,
                    () => Tuple.Create(new List<T>(), new List<T>()),
                    (i, loop, tuple) =>
                    {
                        if (predicate(items[i]))
                        {
                            tuple.Item1.Add(items[i]);
                        }
                        else
                        {
                            tuple.Item2.Add(items[i]);
                        }
                        return tuple;
                    },
                    tuple =>
                    {
                        lock (partitionLock)
                        {
                            trueList.AddRange(tuple.Item1);
                            falseList.AddRange(tuple.Item2);
                        }
                    });

                return Tuple.Create((IList<T>)trueList, (IList<T>)falseList);
            }
            else
            {
                var stream = source.AsStream();
                if (stream != null)
                {
                    return Tuple.Create((IList<T>)stream.Where(s => predicate(s)).ToList(), (IList<T>)stream.Where(s => !predicate(s)).ToList());
                }
                else
                {
                    return Tuple.Create((IList<T>)new T[0], (IList<T>)new T[0]);
                }
            }
        }

        #endregion

        #region Cartesian Product Methods
        
        public static IEnumerable<Tuple<int, T1, T2>> CrossJoin<T1, T2>(this IEnumerable<T1> first, IEnumerable<T2> second)
        {
            first.ThrowIfNull("first");
            second.ThrowIfNull("second");

            int index = 0;
            return first.SelectMany(f => second.Select(s => Tuple.Create(index++, f, s)));
        }

        public static IEnumerable<IEnumerable<T>> CartesianProduct<T>(this IEnumerable<IEnumerable<T>> sources)
        {
            sources.ThrowIfNull("sources");

            IEnumerable<IEnumerable<T>> emptyProduct = new[] { Enumerable.Empty<T>() };
            return sources.Aggregate(emptyProduct, 
                (accumulator, sequence) =>
                    from accseq in accumulator
                    from item in sequence
                    select accseq.Append(item));
        }

        #endregion

        #region PowerSet Method

        public static IEnumerable<IEnumerable<T>> PowerSet<T>(this IList<T> source)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            return Enumerable.Range(0, 1 << source.Count).Select(m => Enumerable.Range(0, source.Count).Where(i => (m & (1 << i)) != 0).Select(i => source[i]));
        }

        #endregion

        #region Flatten Methods

        public static IEnumerable<T> Flatten<T>(this IEnumerable<IEnumerable<T>> source)
        {
            source.ThrowIfNull("source");
            return source.SelectMany(s => s);
        }

        #endregion

        #region Rotate Methods
        
        public static IEnumerable<T> Rotate<T>(this IEnumerable<T> source, int offset)
        {
            source.ThrowIfNull("source");
            return source.Skip(offset).Concat(source.Take(offset));
        }

        #endregion

        #region Step Methods

        public static IEnumerable<T> Step<T>(this IEnumerable<T> source, int step)
        {
            step.ThrowIfNonPositive("step");
            if (step == 1)
            {
                return source;
            }

            return StepImplementation(source, step);
        }

        private static IEnumerable<T> StepImplementation<T>(IEnumerable<T> source, int step)
        {
            int count = 0;
            foreach (T item in source)
            {
                if (count == 0)
                {
                    yield return item;
                }

                count = (count + 1) % step;
            }
        }

        #endregion

        #region Find Index Methods

        public static int FindIndex<T>(this IEnumerable<T> source, Func<T, bool> predicate)
        {
            source.ThrowIfNull("source");
            predicate.ThrowIfNull("predicate");

            return FindIndexImplementation(source, predicate);
        }

        public static int FindLastIndex<T>(this IEnumerable<T> source, Func<T, bool> predicate)
        {
            source.ThrowIfNull("source");
            predicate.ThrowIfNull("predicate");

            if (source is IList<T>)
            {
                var list = (IList<T>)source;
                source = list.Reverse();
            }
            else
            {
                source = source.Reverse();
            }

            return FindIndexImplementation(source, predicate);
        }

        private static IEnumerable<T> Reverse<T>(this IList<T> list)
        {
            for (int i = list.Count - 1; i > -1; i--)
            {
                yield return list[i];
            }
        }

        private static int FindIndexImplementation<T>(this IEnumerable<T> source, Func<T, bool> predicate)
        {
            var indexedItem = source.Select((s, i) => new { Item = s, Index = i }).FirstOrDefault(si => predicate(si.Item));
            if (indexedItem != null)
            {
                return indexedItem.Index;
            }
            return -1;
        }

        #endregion
        
        #region Check Methods

        public static bool IsEmpty<T>(this IEnumerable<T> source)
        {
            return !source.Any();
        }

        public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> source)
        {
            return source ?? Enumerable.Empty<T>();
        }

        public static IEnumerable<T> WhereNot<T>(this IEnumerable<T> source, Predicate<T> predicate)
        {
            source.ThrowIfNull("source");
            predicate.ThrowIfNull("predicate");
            return source.Where(x => !predicate(x));
        }

        public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T> source)
            where T : class
        {
            source.ThrowIfNull("source");
            return source.Where(x => x != null);
        }

        public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T?> source)
            where T : struct
        {
            source.ThrowIfNull("source");
            foreach (T? t in source)
            {
                if (t.HasValue)
                    yield return t.Value;
            }
        }

        #endregion

        #region Shuffle Methods

        public static IEnumerable<T> TakeRandom<T>(this IEnumerable<T> source, int count)
        {
            return source.Shuffle().Take(count);
        }

        public static IEnumerable<T> Shuffle<T>(this IEnumerable<T> source)
        {
            IList<T> items;
            if (source is IList<T>)
            {
                items = (IList<T>)source;
            }
            else
            {
                items = source.ToArray();
            }

            var rnd = new ThreadSafeRandom();

            // i is the number of items remaining to be shuffled.
            for (int i = items.Count; i > 1; i--)
            {
                // Pick a random element to swap with the i-th element.
                int j = rnd.Next(i);  // 0 <= j <= i-1 (0-based array)
                // Swap array elements.
                var tmp = items[j];
                items[j] = items[i - 1];
                items[i - 1] = tmp;
                yield return items[i - 1];
            }

            yield return items[0];
        }

        #endregion

        #region Arrange Methods

        public static IEnumerable<TSource> Arrange<TSource, TKey>(this IEnumerable<TSource> source, IEnumerable<TKey> keys, Func<TSource, TKey> keySelector, IEqualityComparer<TKey> comparer)
        {
            source.ThrowIfNull("source");
            keys.ThrowIfNull("keys");
            keySelector.ThrowIfNull("keySelector");

            if (comparer == null)
            {
                comparer = EqualityComparer<TKey>.Default;
            }

            var map = source.GroupBy(s => keySelector(s), comparer).ToDictionary(g => g.Key, g => g.Select(i => i));
            return ProcessArrangement(keys, map).Flatten();
        }

        public static IEnumerable<TSource> Arrange<TSource, TKey>(this IEnumerable<TSource> source, IEnumerable<TKey> keys, Func<TSource, TKey> keySelector)
            where TKey : IEquatable<TKey>
        {
            return source.Arrange(keys, keySelector, null);
        }

        private static IEnumerable<IEnumerable<TSource>> ProcessArrangement<TSource, TKey>(IEnumerable<TKey> keys, Dictionary<TKey, IEnumerable<TSource>> map)
        {
            foreach (var key in keys)
            {
                IEnumerable<TSource> items;
                if (map.TryGetValue(key, out items))
                {
                    yield return items;
                }
            }
        }

        #endregion

        #region Delay Methods

        public static IEnumerable<TSource> Delay<TSource>(this IEnumerable<TSource> source, int delay)
        {
            return LinqExtensions.Defer(() => { Thread.Sleep(delay); return source; });
        }

        #endregion

        #region Min/Max Methods

        public static TSource MaxElement<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> selector)
            where TKey : IComparable<TKey>
        {
            return MinMaxImplementation(source, selector, true);
        }

        public static TSource MinElement<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> selector)
            where TKey : IComparable<TKey>
        {
            return MinMaxImplementation(source, selector, false);
        }

        private static TSource MinMaxImplementation<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> selector, bool max)
            where TKey : IComparable<TKey>
        {
            source.ThrowIfNull("source");
            selector.ThrowIfNull("selector");
            var firstElement = true;
            var result = default(TSource);
            var minValue = default(TKey);
            foreach (var element in source)
            {
                var candidate = selector(element);
                if (firstElement || (max ? 1 : -1) * candidate.CompareTo(minValue) > 0)
                {
                    firstElement = false;
                    minValue = candidate;
                    result = element;
                }
            }
            return result;
        }

        #endregion
    }
}
