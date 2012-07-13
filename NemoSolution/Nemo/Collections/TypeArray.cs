using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Extensions;

namespace Nemo.Collections
{
    internal class TypeArray : IEqualityComparer<IList<Type>>
    {
        public IList<Type> _types;

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
            if (x.Count == y.Count)
            {
                for (int i = 0; i < x.Count; i++)
                {
                    if (x[i] != y[i]) return false;
                }
                return true;
            }
            return false;
        }

        public int GetHashCode(IList<Type> obj)
        {
            return obj.Select(t => t.FullName).ToDelimitedString("::").GetHashCode();
        }
    }
}
