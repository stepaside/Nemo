using Nemo.Configuration;
using Nemo.Fn;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Nemo.Collections
{
    public interface IMultiResult
    {
        Type[] AllTypes { get; }
        IEnumerable<T> Retrieve<T>();
        bool Reset();
    }

    [Serializable]
    public class MultiResult<T1, T2> : IMultiResult, IEnumerable<T1>
        where T1 : class
        where T2 : class
    {
        private readonly IEnumerable<ITypeUnion> _source;
        private IEnumerator<ITypeUnion> _iter;
        private readonly bool _cached;
        private ITypeUnion _last;

        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
        {
            _cached = cached;
            if (cached)
            {
                if (ConfigurationFactory.Get<T1>().DefaultCacheRepresentation == CacheRepresentation.List)
                {
                    _source = source.ToList();
                }
                else
                {
                    _source = source.AsStream();
                }
            }
            else
            {
                _source = source;
            }
            _iter = _source.GetEnumerator();
        }

        IEnumerator<T1> IEnumerable<T1>.GetEnumerator()
        {
            return Retrieve<T1>().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _iter;
        }

        public IEnumerable<T> Retrieve<T>()
        {
            if (typeof(T) == typeof(ObjectFactory.Fake))
            {
                yield break;
            }

            if (_last != null && _last.Is<T>())
            {
                yield return _last.As<T>(); ;
            }

            while (_iter.MoveNext())
            {
                _last = _iter.Current;
                if (_last.Is<T>())
                {
                    yield return _last.As<T>();
                }
                else
                {
                    yield break;
                }
            }
        }

        public virtual Type[] AllTypes
        {
            get { return new[] { typeof(T1), typeof(T2) }; }
        }


        public bool Reset()
        {
            if (_cached)
            {
                _last = null;
                _iter = _source.GetEnumerator();
                return true;
            }
            return false;
        }
    }

    [Serializable]
    public class MultiResult<T1, T2, T3> : MultiResult<T1, T2>, IMultiResult
        where T1 : class
        where T2 : class
        where T3 : class
    {
        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
            : base(source, cached)
        { }

        public override Type[] AllTypes
        {
            get { return new[] { typeof(T1), typeof(T2), typeof(T3) }; }
        }
    }

    [Serializable]
    public class MultiResult<T1, T2, T3, T4> : MultiResult<T1, T2, T3>
        where T1 : class
        where T2 : class
        where T3 : class
        where T4 : class
    {
        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
            : base(source, cached)
        { }

        public override Type[] AllTypes
        {
            get { return new[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4) }; }
        }
    }

    [Serializable]
    public class MultiResult<T1, T2, T3, T4, T5> : MultiResult<T1, T2, T3, T4>
        where T1 : class
        where T2 : class
        where T3 : class
        where T4 : class
        where T5 : class
    {
        public MultiResult(IEnumerable<ITypeUnion> source, bool cached)
            : base(source, cached)
        { }

        public override Type[] AllTypes
        {
            get { return new[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5) }; }
        }
    }

    public static class MultiResult
    {
        private static readonly ConcurrentDictionary<TypeArray, Type> _types = new ConcurrentDictionary<TypeArray, Type>();

        public static IMultiResult Create(IList<Type> types, IEnumerable<ITypeUnion> source, bool cached)
        {
            if (types == null || source == null) return null;

            Type genericType = null;
            switch (types.Count)
            {
                case 2:
                    genericType = typeof(MultiResult<,>);
                    break;
                case 3:
                    genericType = typeof(MultiResult<,,>);
                    break;
                case 4:
                    genericType = typeof(MultiResult<,,,>);
                    break;
                case 5:
                    genericType = typeof(MultiResult<,,,,>);
                    break;
            }

            if (genericType == null) return null;

            var key = new TypeArray(types);
            var type = _types.GetOrAdd(key, t => genericType.MakeGenericType(t.Types is Type[] ? (Type[])t.Types : t.Types.ToArray()));
            var activator = Nemo.Reflection.Activator.CreateDelegate(type, typeof(IEnumerable<ITypeUnion>), typeof(bool));
            var multiResult = (IMultiResult)activator(new object[] { source, cached });
            return multiResult;
        }
    }
}
