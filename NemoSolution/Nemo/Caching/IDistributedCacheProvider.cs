using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IDistributedCacheProvider
    {
        bool CheckAndSave(string key, object val, ulong cas);
        Tuple<object, ulong> RetrieveWithCas(string key);
        IDictionary<string, Tuple<object, ulong>> RetrieveWithCas(IEnumerable<string> keys);
        void AcquireLock(string key);
        bool TryAcquireLock(string key);
        object WaitForItems(string key, int count = -1);
        bool ReleaseLock(string key);
    }
}
