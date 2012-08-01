using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public interface IDistributedCounter
    {
        ulong Increment(string key, ulong delta = 1);
        ulong Decrement(string key, ulong delta = 1);
    }
}
