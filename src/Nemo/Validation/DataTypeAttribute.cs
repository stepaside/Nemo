using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Nemo.Validation
{
    public enum DataType
    {
        Boolean,
        Integer,
        Double,
        Currency,
        Date,
        String,
        Alphabetic,
        AlphaNumeric,
        PhoneNumber,
        ZipCode,
        EmailAddress,
        IpAddress,
        Url,
        CreditCard,
        CreditCardLuhn,
        SocialSecurityNumber
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class DataTypeAttribute : ValidationAttributeBase
    {
        private const string DEFAULT_ERROR_MESSAGE = "The field {{0}} expects {0} data type.";

        public DataType DataType { get; internal set; }

        public DataTypeAttribute(DataType dataType)
        {
            this.DataType = dataType;
            this.InitializeDefaultErrorMessage();
            switch (this.DataType)
            {
                case DataType.Boolean:
                    this.UsesCustomValidation = true;
                    break;

                case DataType.Alphabetic:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = AlphabeticMatcher.ToString();
                    break;

                case DataType.AlphaNumeric:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = AlphaNumericMatcher.ToString();
                    break;

                case DataType.PhoneNumber:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = PhoneNumberMatcher.ToString();
                    break;

                case DataType.ZipCode:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = ZipCodeMatcher.ToString();
                    break;

                case DataType.EmailAddress:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = EmailMatcher.ToString();
                    break;

                case DataType.IpAddress:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = IpAddressMatcher.ToString();
                    break;

                case DataType.Url:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = UrlCodeMatcher.ToString();
                    break;

                case DataType.CreditCard:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = CreditCardMatcher.ToString();
                    break;

                case DataType.CreditCardLuhn:
                    this.UsesCustomValidation = true;
                    break;

                case DataType.SocialSecurityNumber:
                    this.UsesRegularExpression = true;
                    this.CurrentRegularExpression = SocialSecurityMatcher.ToString();
                    break;
            }
        }

        protected override void InitializeDefaultErrorMessage()
        {
            if (string.IsNullOrEmpty(this.DefaultErrorMessage))
            {
                this.DefaultErrorMessage = string.Format(DEFAULT_ERROR_MESSAGE, this.DataType);
            }
        }

        #region Regex Declarations

        public readonly static Regex EmailMatcher = new Regex(@"^([0-9a-zA-Z]([-\.\w]*[0-9a-zA-Z])*@([0-9a-zA-Z][-\w]*[0-9a-zA-Z]*\.)+[a-zA-Z]{2,9})$", RegexOptions.Compiled);
        public readonly static Regex CreditCardMatcher = new Regex(@"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})$", RegexOptions.Compiled);
        public readonly static Regex AlphabeticMatcher = new Regex("^[a-zA-Z]+$", RegexOptions.Compiled);
        public readonly static Regex AlphaNumericMatcher = new Regex("^[a-zA-Z0-9]+$", RegexOptions.Compiled);
        public readonly static Regex IpAddressMatcher = new Regex(@"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$", RegexOptions.Compiled);
        public readonly static Regex PhoneNumberMatcher = new Regex(@"^[01]?[- .]?(\([2-9]\d{2}\)|[2-9]\d{2})[- .]?\d{3}[- .]?\d{4}$", RegexOptions.Compiled);
        public readonly static Regex ZipCodeMatcher = new Regex(@"^(\d{5}-\d{4}|\d{5}|\d{9})$|^([a-zA-Z]\d[a-zA-Z] \d[a-zA-Z]\d)$", RegexOptions.Compiled);
        public readonly static Regex UrlCodeMatcher = new Regex(@"^(ht|f)tp(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-\.\?\,\'\/\\\+&amp;%\$#_=]*)?$", RegexOptions.Compiled);
        public readonly static Regex SocialSecurityMatcher = new Regex(@"^\d{3}-\d{2}-\d{4}$", RegexOptions.Compiled);

        #endregion

        public bool UsesRegularExpression { get; protected set; }

        public bool UsesCustomValidation { get; protected set; }

        public string CurrentRegularExpression { get; protected set; }

        public override bool IsValid(object value)
        {
            string input = Convert.ToString(value);
            bool success = true;
            if (string.IsNullOrEmpty(input)) return success;
            
            switch (this.DataType)
            {
                case DataType.Boolean:
                    bool result1;
                    success = bool.TryParse(input, out result1);
                    break;

                case DataType.Integer:
                    int result2;
                    success = int.TryParse(input, out result2);
                    break;

                case DataType.Double:
                    double result3;
                    success = double.TryParse(input, out result3);
                    break;

                case DataType.Currency:
                    decimal result4;
                    success = decimal.TryParse(input, System.Globalization.NumberStyles.Currency, null, out result4);
                    break;

                case DataType.Date:
                    DateTime result5;
                    success = DateTime.TryParse(input, out result5);
                    break;

                case DataType.String:
                    success = value is string;
                    break;

                case DataType.Alphabetic:
                    success = AlphabeticMatcher.IsMatch(input);
                    break;

                case DataType.AlphaNumeric:
                    success = AlphaNumericMatcher.IsMatch(input);
                    break;

                case DataType.PhoneNumber:
                    success = PhoneNumberMatcher.IsMatch(input);
                    break;

                case DataType.ZipCode:
                    success = ZipCodeMatcher.IsMatch(input);
                    break;

                case DataType.EmailAddress:
                    success = EmailMatcher.IsMatch(input);
                    break;

                case DataType.IpAddress:
                    success = IpAddressMatcher.IsMatch(input);
                    break;

                case DataType.Url:
                    success = UrlCodeMatcher.IsMatch(input);
                    break;

                case DataType.CreditCard:
                    success = CreditCardMatcher.IsMatch(input);
                    break;

                case DataType.CreditCardLuhn:
                    success = ValidationFunctions.IsValidCreditCard(input);
                    break;

                case DataType.SocialSecurityNumber:
                    success = SocialSecurityMatcher.IsMatch(input);
                    break;
            }

            return success;
        }
    }
}
