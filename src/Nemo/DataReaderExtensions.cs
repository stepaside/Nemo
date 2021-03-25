using System;
using System.Collections.Generic;
using System.Data;

namespace Nemo
{
    internal static class DataReaderExtensions
    {
        internal static ISet<string> GetColumns(this IDataRecord record)
        {
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int count = record.FieldCount;
            for (var i = 0; i < count; i++)
            {
                columns.Add(record.GetName(i));
            }
            return columns;
        }
    }

}
