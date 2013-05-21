using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = true, Inherited = true)]
    public class QueryDependencyAttribute : Attribute
    {
        public QueryDependencyAttribute(params string[] properties) : base() 
        {
            Properties = properties;
        }

        public string[] Properties { get; private set; }
    }
}
