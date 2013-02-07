using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IOptimisticConcurrencyProvider
    {
        bool CheckAndSave(string key, object val, ulong cas);
        Tuple<object, ulong> RetrieveWithCas(string key);
        IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys);
    }
}
