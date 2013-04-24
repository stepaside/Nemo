using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Serialization;

namespace Nemo.UnitOfWork
{
    internal class ObjectScopeItem
    {
        private IBusinessObject _item;

        public ObjectScopeItem(IBusinessObject items)
        {
            _item = items;
        }

        internal byte[] Serialize()
        {
            return _item.Serialize();
        }

        internal static T Deserialize<T>(byte[] data)
            where T : class, IBusinessObject
        {
            return ObjectSerializer.Deserialize<T>(data);
        }
    }
}
