﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;

namespace Nemo.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public sealed class PrimaryKeyAttribute : PropertyAttribute
    {
        public PrimaryKeyAttribute() { }

        public PrimaryKeyAttribute(int position) 
        { 
            Position = position;
        }

        public int Position { get; set; }
    }
}
