using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Cache
{
    public class PublishQueryInvalidationEventArgs : EventArgs
    {
        public string Key { get; set; }
        public ulong Version { get; set; }
        public string Variant { get; set; }
    }
}
