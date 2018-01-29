using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections
{
    internal class TypeArray : IEqualityComparer<IList<Type>>
    {
        private readonly IList<Type> _types;

        public TypeArray(IList<Type> types)
        {
            _types = types;
        }

        public IList<Type> Types
        {
            get
            {
                return _types;
            }
        }

        public bool Equals(IList<Type> x, IList<Type> y)
        {
            return x.SequenceEqual(y, EqualityComparer<Type>.Default);
        }

        public int GetHashCode(IList<Type> obj)
        {
            return obj.Select(t => t.FullName).ToDelimitedString("::").GetHashCode();
        }
    }
}
