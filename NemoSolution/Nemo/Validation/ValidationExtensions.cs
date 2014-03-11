using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Nemo.Reflection;
using Nemo.Extensions;

namespace Nemo.Validation
{
    public static class ValidationExtensions
    {
        /// <summary>
        /// Validate method implements property validation based on attributes
        /// </summary>
        /// <returns>
        /// Returns a dictionary of properties and error messages
        /// </returns>
        public static ValidationResult Validate<T>(this T dataEntity)
            where T : class, IDataEntity
        {
            // Get properties and build a property map
            var properties = Reflector.GetAllProperties<T>();

            var errors = from prop in properties
                         from attribute in prop.GetCustomAttributes(typeof(ValidationAttribute), false).Cast<ValidationAttribute>()
                         where !IsValid(dataEntity, prop.Name, prop.PropertyType, attribute)
                         select new ValidationError
                         {
                             PropertyName = prop.Name,
                             ErrorMessage = attribute.FormatErrorMessage(string.Join(" ", Regex.Split(prop.Name, "(?=[A-Z])"))),
                             TargetInstance = dataEntity,
                             ValidationType = attribute.GetValidationType(),
                             SeverityType = attribute is ISeverityTypeProvider ? ((ISeverityTypeProvider)attribute).SeverityType : SeverityType.Error
                         };

            return new ValidationResult(errors.GroupBy(e => e.PropertyName).ToDictionary(g => g.Key, g => g.ToList()));
        }

        public static string GetValidationType(this ValidationAttribute attribute)
        {
            string attributeName = attribute.GetType().Name;
            return attributeName.EndsWith("Attribute") ? attributeName.Substring(0, attributeName.Length - 9) : attributeName;
        }

        private static bool IsValid(IDataEntity dataEntity, string propertyName, Type propertyType, ValidationAttribute attribute)
        {
            object value = null;
            if (attribute is CompareAttribute)
            {
                value = new object[] { dataEntity.Property(propertyName), dataEntity.Property(((CompareAttribute)attribute).PropertyName) };
            }
            else if (attribute is CustomAttribute)
            {
                value = new object[] { new CustomValidatorContext(dataEntity, propertyName, attribute), dataEntity.Property(propertyName) };
            }
            else if (attribute is XmlSchemaAttribute && !string.IsNullOrEmpty(((XmlSchemaAttribute)attribute).SchemaProperty))
            {
                value = new object[] { dataEntity.Property(propertyName), dataEntity.Property(((XmlSchemaAttribute)attribute).SchemaProperty) };
            }
            else
            {
                if (!dataEntity.PropertyTryGet(propertyName, out value) || Convert.IsDBNull(value))
                {
                    value = propertyType.GetDefault();
                }
            }
            return attribute.IsValid(value);
        }

        internal static void SetErrorMessage(this ValidationAttribute attribute)
        {
            if (attribute is IResourceKeyProvider)
            {
                var resourceKeyProvider = (IResourceKeyProvider)attribute;
                if (!string.IsNullOrEmpty(resourceKeyProvider.ResourceKey))
                {
                    var errorMessage = string.Empty;
                    if (!string.IsNullOrEmpty(errorMessage) && attribute.ErrorMessage == null)
                    {
                        attribute.ErrorMessage = errorMessage;
                    }
                }
            }
        }
    }
}
