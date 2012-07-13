using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public class RequiredAttribute : System.ComponentModel.DataAnnotations.RequiredAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        public RequiredAttribute() : base() { }

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
