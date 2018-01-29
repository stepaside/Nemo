using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel.DataAnnotations;
using System.Resources;

namespace Nemo.Validation
{
    public abstract class ValidationAttributeBase : ValidationAttribute, ISeverityTypeProvider, IResourceKeyProvider
    {
        private const string DEFAULT_ERROR_MESSAGE = "The field {0} is invalid.";

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

        public string DefaultErrorMessage { get; protected set; }

        protected abstract void InitializeDefaultErrorMessage();

        public override string FormatErrorMessage(string name)
        {
            string defaultError = string.Format(DEFAULT_ERROR_MESSAGE, name);
            string error = null;
            try
            {
                error = base.FormatErrorMessage(name);
            }
            catch { }

            if ((error == null || error == defaultError) && this.DefaultErrorMessage != null)
            {
                error = string.Format(this.DefaultErrorMessage, name);
            }

            return error;
        }
    }
}
