using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IPersistentCacheProvider
    {
        bool Append(string key, string value);
        bool Save(string key, object value, object version);
        object Retrieve(string key, out object version);
        IDictionary<string, object> Retrieve(IEnumerable<string> keys, out IDictionary<string, object> versions);
    }
}
