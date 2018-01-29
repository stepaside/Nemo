using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public class RegularExpressionAttribute : System.ComponentModel.DataAnnotations.RegularExpressionAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        public RegularExpressionAttribute(string pattern) : base(pattern) { }

        public SeverityType SeverityType
        {
            get;
            set;
        }

        private string _resourceKey = null;
        public string ResourceKey
        {
            get
            {
                return _resourceKey;
            }
            set
            {
                _resourceKey = value;
                this.SetErrorMessage();
            }
        }
    }
}
