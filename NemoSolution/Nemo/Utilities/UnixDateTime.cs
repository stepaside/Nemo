using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Utilities
{
    public static class UnixDateTime
    {
        public static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long GetTicks()
        {
            return (DateTime.UtcNow - UnixDateTime.Epoch).Ticks;
        }
    }
}
