using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public enum SeverityType
    {
        Error,
        Warning,
        Infomration,
        Debug
    }

    public interface ISeverityTypeProvider
    {
        SeverityType SeverityType
        {
            get;
            set;
        }
    }
}
