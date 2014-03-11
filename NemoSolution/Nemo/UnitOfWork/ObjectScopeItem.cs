using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Serialization;

namespace Nemo.UnitOfWork
{
    internal class ObjectScopeItem
    {
        private IDataEntity _item;

        public ObjectScopeItem(IDataEntity items)
        {
            _item = items;
        }

        internal byte[] Serialize()
        {
            return _item.Serialize();
        }

        internal static T Deserialize<T>(byte[] data)
            where T : class, IDataEntity
        {
            return ObjectSerializer.Deserialize<T>(data);
        }
    }
}
