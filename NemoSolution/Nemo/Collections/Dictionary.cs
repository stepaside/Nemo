using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Collections
{
    public static class Dictionary
    {
        private static ConcurrentDictionary<Tuple<Type, Type>, Type> _dictionaryTypes = new ConcurrentDictionary<Tuple<Type, Type>, Type>();

        public static IDictionary Create(Type keyType, Type valueType)
        {
            var dictionaryType = _dictionaryTypes.GetOrAdd(Tuple.Create(keyType, valueType), t => typeof(Dictionary<,>).MakeGenericType(t.Item1, t.Item2));
            return (IDictionary)Nemo.Reflection.Activator.New(dictionaryType);
        }
    }
}
