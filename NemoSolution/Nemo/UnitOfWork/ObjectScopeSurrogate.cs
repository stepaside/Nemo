using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Serialization;

namespace Nemo.UnitOfWork
{
    public class ObjectScopeSurrogate<T> : IDataEntity
        where T : class
    {
        public List<T> Items { get; set; }
    }
}
