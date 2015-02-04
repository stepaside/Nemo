using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nemo.Attributes.Converters
{
    public class UtcDateTimeConverter : ITypeConverter<object, DateTime>
    {
        #region ITypeConverter<object, DateTime> Members

        DateTime ITypeConverter<object, DateTime>.ConvertForward(object from)
        {
            return DateTime.SpecifyKind((DateTime)from, DateTimeKind.Utc);
        }

        object ITypeConverter<object, DateTime>.ConvertBackward(DateTime to)
        {
            return to.Kind == DateTimeKind.Utc ? to : to.ToUniversalTime();
        }

        #endregion
    }
}
