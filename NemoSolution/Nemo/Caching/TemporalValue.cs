using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    [Serializable]
    public class TemporalValue
    {
        public DateTime ExpiresAt { get; set; }
        public object Value { get; set; }

        public bool IsValid()
        {
            return this.ExpiresAt >= DateTimeOffset.Now.DateTime;
        }
    }
}
