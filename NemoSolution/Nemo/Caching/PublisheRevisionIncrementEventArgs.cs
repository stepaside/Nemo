using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Caching
{
    public class PublisheRevisionIncrementEventArgs : EventArgs
    {
        public CacheProvider Cache { get; set; }
        public string Key { get; set; }
        public ulong Revision { get; set; }
    }
}
