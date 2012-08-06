using System;
using System.Collections;
using System.Collections.Generic;
using Nemo.Fn;
using Nemo.Reflection;

namespace Nemo.Serialization
{
    internal class SerializationReaderState
    {
        internal string Name { get; set; }
        internal IBusinessObject Item { get; set; }
        internal IList List { get; set; }
        internal ITypeUnion Union { get; set; }
        internal object Value { get; set; }
        internal Type ElementType { get; set; }
        internal IDictionary<string, ReflectedProperty> PropertyMap { get; set; }
        internal bool IsSimple { get; set; }
    }
}
