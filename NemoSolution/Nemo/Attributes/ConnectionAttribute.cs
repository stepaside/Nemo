using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = false, Inherited = true)]
    public class ConnectionAttribute : Attribute
    {
        public ConnectionAttribute(string name)
        {
            Name = name;
        }

        public string Name
        {
            get;
            private set;
        }

        public string DatabaseName
        {
            get;
            set;
        }

        public string SchemaName
        {
            get;
            set;
        }
    }
}
