using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Nemo.Utilities;

namespace Nemo.Id
{
    public class CombGuidGenerator : IIdGenerator
    {
        public object Generate()
        {
            return CombGuid.Generate();
        }
    }
}
