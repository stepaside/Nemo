using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Cache
{
    public interface IStaleCacheProvider
    {
        object GetStale(string key);
        IDictionary<string, object> GetStale(IEnumerable<string> keys);
    }
}
