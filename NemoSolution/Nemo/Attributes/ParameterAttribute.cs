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
        private ParameterDirection _direction = ParameterDirection.Input;

        public ParameterAttribute(string name)
        {
            Name = name;
        }

        public string Name
        {
            get;
            private set;
        }

        public ParameterDirection Direction
        {
            get
            {
                return _direction;
            }
            set
            {
                _direction = value;
            }
        }
    }

}
