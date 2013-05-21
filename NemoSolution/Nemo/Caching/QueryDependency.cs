using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Caching
{
    public class QueryDependency
    {
        public QueryDependency(params string[] properties)
        {
            Properties = properties;
        }

        public string[] Properties { get; private set; }
    }
}
