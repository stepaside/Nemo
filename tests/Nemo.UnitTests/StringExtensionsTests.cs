using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Extensions;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Nemo.UnitTests
{
    [TestClass]
    public class StringExtensionsTests
    {
        [TestMethod]
        public void ToDelimitedString_EmptyCollection_ReturnsEmptyString()
        {
            var emptyList = new List<string>();
            
            var result = emptyList.ToDelimitedString(",");
            
            Assert.AreEqual(string.Empty, result);
        }

        [TestMethod]
        public void ToDelimitedString_SingleItem_ReturnsSingleItem()
        {
            var singleItem = new List<string> { "test" };
            
            var result = singleItem.ToDelimitedString(",");
            
            Assert.AreEqual("test", result);
        }

        [TestMethod]
        public void ToDelimitedString_MultipleItems_ReturnsDelimitedString()
        {
            var items = new List<string> { "a", "b", "c" };
            
            var result = items.ToDelimitedString(",");
            
            Assert.AreEqual("a,b,c", result);
        }

        [TestMethod]
        public void ToDelimitedString_CustomDelimiter_UsesCustomDelimiter()
        {
            var items = new List<string> { "apple", "banana", "cherry" };
            
            var result = items.ToDelimitedString(" | ");
            
            Assert.AreEqual("apple | banana | cherry", result);
        }

        [TestMethod]
        public void ToDelimitedString_NullDelimiter_UsesCultureDefault()
        {
            var items = new List<string> { "a", "b" };
            var expectedDelimiter = CultureInfo.CurrentCulture.TextInfo.ListSeparator;
            
            var result = items.ToDelimitedString(null);
            
            Assert.AreEqual($"a{expectedDelimiter}b", result);
        }

        [TestMethod]
        public void ToDelimitedString_IntegerCollection_ConvertsToString()
        {
            var items = new List<int> { 1, 2, 3, 4, 5 };
            
            var result = items.ToDelimitedString(",");
            
            Assert.AreEqual("1,2,3,4,5", result);
        }

        [TestMethod]
        public void ToDelimitedString_CollectionWithNulls_HandlesNulls()
        {
            var items = new List<string> { "a", null, "c" };
            
            var result = items.ToDelimitedString(",");
            
            Assert.AreEqual("a,,c", result);
        }

        [TestMethod]
        public void ToDelimitedString_LargeCollection_PerformsWell()
        {
            var items = Enumerable.Range(1, 10000).Select(i => i.ToString()).ToList();
            
            var result = items.ToDelimitedString(",");
            
            Assert.IsTrue(result.StartsWith("1,2,3"));
            Assert.IsTrue(result.EndsWith("9998,9999,10000"));
            Assert.AreEqual(10000, result.Split(',').Length);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ToDelimitedString_NullSource_ThrowsArgumentNullException()
        {
            List<string> nullList = null;
            
            nullList.ToDelimitedString(",");
        }

        [TestMethod]
        public void ToNumericPhoneNumber_EmptyString_ReturnsEmptyString()
        {
            var input = string.Empty;
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual(string.Empty, result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_NumericOnly_ReturnsUnchanged()
        {
            var input = "1234567890";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("1234567890", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_LettersToNumbers_ConvertsCorrectly()
        {
            var input = "ABC";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("222", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_DEF_ConvertsTo333()
        {
            var input = "DEF";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("333", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_GHI_ConvertsTo444()
        {
            var input = "GHI";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("444", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_JKL_ConvertsTo555()
        {
            var input = "JKL";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("555", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_MNO_ConvertsTo666()
        {
            var input = "MNO";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("666", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_PQRS_ConvertsTo7777()
        {
            var input = "PQRS";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("7777", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_TUV_ConvertsTo888()
        {
            var input = "TUV";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("888", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_WXYZ_ConvertsTo9999()
        {
            var input = "WXYZ";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("9999", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_MixedCaseLetters_ConvertsCorrectly()
        {
            var input = "AbC";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("222", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_MixedInput_ConvertsLettersKeepsOthers()
        {
            var input = "1-800-FLOWERS";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("1-800-3569377", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_SpecialCharacters_RemainsUnchanged()
        {
            var input = "123-456-7890";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("123-456-7890", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_ParenthesesAndSpaces_RemainsUnchanged()
        {
            var input = "(555) 123-HELP";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("(555) 123-4357", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_OnlySpecialCharacters_RemainsUnchanged()
        {
            var input = "()-. ";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("()-. ", result);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_LongString_PerformsWell()
        {
            var input = new string('A', 10000);
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual(new string('2', 10000), result);
            Assert.AreEqual(10000, result.Length);
        }

        [TestMethod]
        public void ToNumericPhoneNumber_RealWorldExample_ConvertsCorrectly()
        {
            var input = "1-800-CALL-NOW";
            
            var result = input.ToNumericPhoneNumber();
            
            Assert.AreEqual("1-800-2255-669", result);
        }
    }
}
