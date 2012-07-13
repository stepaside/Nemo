using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Collections.Extensions
{
    public static class DictionaryExtensions
    {
        public static TValue GetValueOrDefault<TValue>(this IDictionary<string, object> dictionary, string key)
        {
            return dictionary.GetValueOrDefault<string, TValue>(key);
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, object> dictionary, TKey key)
        {
            object value;
            return dictionary.TryGetValue(key, out value) ? (TValue)value : default(TValue);
        }
        
        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue defaultValue)
        {
            TValue value;
            return dictionary.TryGetValue(key, out value) ? value : defaultValue;
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, Func<TValue> defaultValueProvider)
        {
            TValue value;
            return dictionary.TryGetValue(key, out value) ? value : defaultValueProvider();
        }
    }
}
