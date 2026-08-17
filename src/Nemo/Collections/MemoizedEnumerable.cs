using System;
using System.Collections;
using System.Collections.Generic;

namespace Nemo.Collections
{
    /// <summary>
    /// Represents a lazily materialized, replayable buffer over an enumerable sequence.
    /// </summary>
    public interface IBuffer<out T> : IEnumerable<T>, IDisposable
    {
    }

    public static class MemoizedEnumerable
    {
        /// <summary>
        /// Creates a buffer over the source sequence that enumerates it at most once,
        /// caching elements so the result can be replayed by multiple enumerators.
        /// </summary>
        public static IBuffer<T> Memoize<T>(this IEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            return new MemoizedEnumerable<T>(source);
        }
    }

    internal sealed class MemoizedEnumerable<T> : IBuffer<T>
    {
        private readonly object _lock = new object();
        private readonly List<T> _buffer = new List<T>();
        private IEnumerator<T> _source;
        private bool _exhausted;
        private bool _disposed;

        public MemoizedEnumerable(IEnumerable<T> source)
        {
            _source = source.GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(MemoizedEnumerable<T>));

            var index = 0;
            while (true)
            {
                T current;
                lock (_lock)
                {
                    if (_disposed) throw new ObjectDisposedException(nameof(MemoizedEnumerable<T>));

                    if (index < _buffer.Count)
                    {
                        current = _buffer[index];
                    }
                    else if (_exhausted)
                    {
                        yield break;
                    }
                    else if (_source.MoveNext())
                    {
                        current = _source.Current;
                        _buffer.Add(current);
                    }
                    else
                    {
                        _exhausted = true;
                        _source.Dispose();
                        _source = null;
                        yield break;
                    }
                }
                index++;
                yield return current;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;
                _source?.Dispose();
                _source = null;
                _buffer.Clear();
            }
        }
    }
}
