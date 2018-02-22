using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Data
{
    public static class DataReaderExtensions
    {
        private class EnumeratorWrapper<T>
        {
            private readonly Func<bool> _moveNext;
            private readonly Func<T> _current;

            public EnumeratorWrapper(Func<bool> moveNext, Func<T> current)
            {
                _moveNext = moveNext;
                _current = current;
            }

            public EnumeratorWrapper<T> GetEnumerator()
            {
                return this;
            }

            public bool MoveNext()
            {
                return _moveNext();
            }

            public T Current
            {
                get { return _current(); }
            }
        }

        private static IEnumerable<T> BuildEnumerable<T>(Func<bool> moveNext, Func<T> current)
        {
            var enumerator = new EnumeratorWrapper<T>(moveNext, current);
            foreach (var item in enumerator)
                yield return item;
        }

        public static IEnumerable<IDataRecord> AsEnumerable(this IDataReader source)
        {
            return BuildEnumerable(source.Read, () => (IDataRecord)source);
        }

        public static IEnumerable<T> AsEnumerable<T>(this IDataReader source)
            where T : class
        {
            return BuildEnumerable(source.Read, () => ObjectFactory.Map<IDataReader, T>(source));
        }
    }
}
