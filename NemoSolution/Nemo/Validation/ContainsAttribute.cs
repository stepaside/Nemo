using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class ContainsAttribute : ValidationAttributeBase
    {
        private const string DEFAULT_ERROR_MESSAGE = "The value of the field {{0}} is not part of the list {{{{{0}}}}}.";

        private HashSet<string> _valueSet = null;

        public ContainsAttribute(string values) : base() 
        {
            this.Values = values;
            this.InitializeDefaultErrorMessage();
        }

        public string Values { get; set; }

        protected override void InitializeDefaultErrorMessage()
        {
            if (string.IsNullOrEmpty(this.DefaultErrorMessage))
            {
                this.DefaultErrorMessage = string.Format(DEFAULT_ERROR_MESSAGE, this.Values);
            }
        }

        public override bool IsValid(object value)
        {
            if (_valueSet == null && !string.IsNullOrEmpty(this.Values))
            {
                _valueSet = new HashSet<string>(this.Values.Split(','));
            }
            else
            {
                _valueSet = new HashSet<string>();
            }

            if (value != null)
            {
                return _valueSet.Contains(value.ToString());
            }
            return false;
        }
    }
}
