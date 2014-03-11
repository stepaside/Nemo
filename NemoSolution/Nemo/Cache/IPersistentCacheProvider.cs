using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Cache
{
    public interface IPersistentCacheProvider
    {
        bool Append(string key, string value);
        bool Set(string key, object value, object version);
        object Get(string key, out object version);
        IDictionary<string, object> Get(IEnumerable<string> keys, out IDictionary<string, object> versions);
    }
}
