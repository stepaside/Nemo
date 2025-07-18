using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class ParameterAttribute : PropertyAttribute
    {
        public ParameterAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    }

}
