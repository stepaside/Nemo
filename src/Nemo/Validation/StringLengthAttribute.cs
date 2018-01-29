using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public class StringLengthAttribute : System.ComponentModel.DataAnnotations.StringLengthAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        public StringLengthAttribute(int maximumLength) : base(maximumLength) { }

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
