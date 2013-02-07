using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IDistributedCacheProvider
    {
        void AcquireLock(string key);
        bool TryAcquireLock(string key);
        object WaitForItems(string key, int count = -1);
        bool ReleaseLock(string key);
    }
}
