using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IStaleCacheProvider
    {
        object RetrieveStale(string key);
        IDictionary<string, object> RetrieveStale(IEnumerable<string> keys);
    }
}
