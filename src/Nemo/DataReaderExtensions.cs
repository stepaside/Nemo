using System;
using System.Collections.Generic;
using System.Data;

namespace Nemo
{
    internal static class DataReaderExtensions
    {
        internal static ISet<string> GetColumns(this IDataRecord record)
        {
            int count = record.FieldCount;
#if NETSTANDARD2_0 || NET472
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
#else
            var columns = new HashSet<string>(count, StringComparer.OrdinalIgnoreCase);
#endif
            for (var i = 0; i < count; i++)
            {
                columns.Add(record.GetName(i));
            }
            return columns;
        }
    }

}
