using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo;
using System;
using System.Linq.Expressions;

namespace Nemo.UnitTests
{
    [TestClass]
    public class SortingTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
            public decimal Amount { get; set; }
        }

        [TestMethod]
        public void Sorting_DefaultConstructor_SetsDefaultValues()
        {
            var sorting = new Sorting<TestEntity>();

            Assert.IsNull(sorting.OrderBy);
            Assert.IsFalse(sorting.Reverse);
        }

        [TestMethod]
        public void Sorting_OrderBy_CanBeSet()
        {
            var sorting = new Sorting<TestEntity>();
            Expression<Func<TestEntity, object>> orderBy = x => x.Name;

            sorting.OrderBy = orderBy;

            Assert.AreEqual(orderBy, sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_Reverse_CanBeSet()
        {
            var sorting = new Sorting<TestEntity>();

            sorting.Reverse = true;

            Assert.IsTrue(sorting.Reverse);
        }

        [TestMethod]
        public void Sorting_SetOrderBy_WithValidExpression_SetsOrderBy()
        {
            var sorting = new Sorting<TestEntity>();
            Expression<Func<TestEntity, object>> orderBy = x => x.Id;
            ISorting iSorting = sorting;

            iSorting.SetOrderBy(orderBy);

            Assert.AreEqual(orderBy, sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_SetOrderBy_WithInvalidExpression_DoesNotSetOrderBy()
        {
            var sorting = new Sorting<TestEntity>();
            Expression<Func<TestEntity, string>> invalidExpression = x => x.Name;
            ISorting iSorting = sorting;

            iSorting.SetOrderBy(invalidExpression);

            Assert.IsNull(sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_SetOrderBy_WithNullExpression_DoesNotSetOrderBy()
        {
            var sorting = new Sorting<TestEntity>();
            ISorting iSorting = sorting;

            iSorting.SetOrderBy(null);

            Assert.IsNull(sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_ISorting_ReverseProperty_WorksCorrectly()
        {
            var sorting = new Sorting<TestEntity>();
            ISorting iSorting = sorting;

            iSorting.Reverse = true;

            Assert.IsTrue(sorting.Reverse);
            Assert.IsTrue(iSorting.Reverse);
        }

        [TestMethod]
        public void Sorting_WithDifferentPropertyTypes_WorksCorrectly()
        {
            var intSorting = new Sorting<TestEntity> { OrderBy = x => x.Id };
            var stringSorting = new Sorting<TestEntity> { OrderBy = x => x.Name };
            var dateSorting = new Sorting<TestEntity> { OrderBy = x => x.CreatedDate };
            var decimalSorting = new Sorting<TestEntity> { OrderBy = x => x.Amount };

            Assert.IsNotNull(intSorting.OrderBy);
            Assert.IsNotNull(stringSorting.OrderBy);
            Assert.IsNotNull(dateSorting.OrderBy);
            Assert.IsNotNull(decimalSorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_OrderByExpression_CanAccessPropertyInfo()
        {
            var sorting = new Sorting<TestEntity> { OrderBy = x => x.Name };

            var memberExpression = sorting.OrderBy.Body as MemberExpression;
            if (memberExpression == null && sorting.OrderBy.Body is UnaryExpression unary)
            {
                memberExpression = unary.Operand as MemberExpression;
            }

            Assert.IsNotNull(memberExpression);
            Assert.AreEqual("Name", memberExpression.Member.Name);
        }

        [TestMethod]
        public void Sorting_MultipleInstances_AreIndependent()
        {
            var sorting1 = new Sorting<TestEntity> { OrderBy = x => x.Id, Reverse = true };
            var sorting2 = new Sorting<TestEntity> { OrderBy = x => x.Name, Reverse = false };

            Assert.AreNotEqual(sorting1.OrderBy, sorting2.OrderBy);
            Assert.AreNotEqual(sorting1.Reverse, sorting2.Reverse);
        }

        [TestMethod]
        public void Sorting_ImplementsISorting_Correctly()
        {
            var sorting = new Sorting<TestEntity>();

            Assert.IsTrue(sorting is ISorting);
            Assert.IsInstanceOfType(sorting, typeof(ISorting));
        }

        [TestMethod]
        public void ISorting_SetOrderBy_WithWrongGenericType_DoesNotThrow()
        {
            var sorting = new Sorting<TestEntity>();
            ISorting iSorting = sorting;

            Expression<Func<string, object>> wrongTypeExpression = x => x.Length;

            iSorting.SetOrderBy(wrongTypeExpression);

            Assert.IsNull(sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_OrderByComplexExpression_WorksCorrectly()
        {
            var sorting = new Sorting<TestEntity>();

            sorting.OrderBy = x => x.Name.Length;

            Assert.IsNotNull(sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_OrderByNestedProperty_WorksCorrectly()
        {
            var sorting = new Sorting<TestEntity>();

            sorting.OrderBy = x => x.CreatedDate.Year;

            Assert.IsNotNull(sorting.OrderBy);
        }

        [TestMethod]
        public void Sorting_SetOrderBy_PreservesOriginalExpression()
        {
            var sorting = new Sorting<TestEntity>();
            Expression<Func<TestEntity, object>> originalExpression = x => x.Amount;
            ISorting iSorting = sorting;

            iSorting.SetOrderBy(originalExpression);

            Assert.AreSame(originalExpression, sorting.OrderBy);
        }
    }
}
