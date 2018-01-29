using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;

namespace Nemo.Validation
{
    public sealed class CustomValidatorContext
    {
        public CustomValidatorContext(object instance, string propertyName)
            : this(instance, propertyName, null)
        { }

        public CustomValidatorContext(object instance, string propertyName, ValidationAttribute validationAttribute)
        {
            this.Instance = instance;
            this.PropertyName = propertyName;
            this.ValidationAttribute = validationAttribute;
        }

        public object Instance { get; private set; }
        public string PropertyName { get; private set; }
        public ValidationAttribute ValidationAttribute { get; private set; }
    }
}

