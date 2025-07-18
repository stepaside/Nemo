using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Data;
using Nemo.Attributes;
using System;
using System.Linq.Expressions;

namespace Nemo.UnitTests
{
    [TestClass]
    public class PredicateVisitorTests
    {
        public class TestEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
            public bool IsActive { get; set; }
            public decimal Amount { get; set; }
            public int? ParentId { get; set; }
        }

        [TestMethod]
        public void Visit_EqualExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id == 1;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Id] = 1"));
        }

        [TestMethod]
        public void Visit_NotEqualExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id != 1;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Id] <> 1"));
        }

        [TestMethod]
        public void Visit_GreaterThanExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount > 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] > 100"));
        }

        [TestMethod]
        public void Visit_LessThanExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount < 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] < 100"));
        }

        [TestMethod]
        public void Visit_GreaterThanOrEqualExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount >= 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] >= 100"));
        }

        [TestMethod]
        public void Visit_LessThanOrEqualExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount <= 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] <= 100"));
        }

        [TestMethod]
        public void Visit_AndExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id == 1 && x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Id] = 1"));
            Assert.IsTrue(result.Contains("AND"));
            Assert.IsTrue(result.Contains("t.[IsActive] = (1=1)"));
        }

        [TestMethod]
        public void Visit_OrExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id == 1 || x.Id == 2;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Id] = 1"));
            Assert.IsTrue(result.Contains("OR"));
            Assert.IsTrue(result.Contains("t.[Id] = 2"));
        }

        [TestMethod]
        public void Visit_NotExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => !(x.IsActive == true);
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("NOT"));
            Assert.IsTrue(result.Contains("t.[IsActive] = (1=1)"));
        }

        [TestMethod]
        public void Visit_StringStartsWith_GeneratesLikeSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name.StartsWith("Test");
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("upper(t.[Name]) like 'TEST%'"));
        }

        [TestMethod]
        public void Visit_StringEndsWith_GeneratesLikeSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name.EndsWith("Test");
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("upper(t.[Name]) like '%TEST'"));
        }

        [TestMethod]
        public void Visit_StringContains_GeneratesLikeSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name.Contains("Test");
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("upper(t.[Name]) like '%TEST%'"));
        }

        [TestMethod]
        public void Visit_StringToUpper_GeneratesUpperSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name.ToUpper() == "TEST";
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("upper(t.[Name]) = 'TEST'"));
        }

        [TestMethod]
        public void Visit_StringToLower_GeneratesLowerSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name.ToLower() == "test";
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("lower(t.[Name]) = 'test'"));
        }

        [TestMethod]
        public void Visit_NullComparison_GeneratesIsNullSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name == null;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Name] is null"));
        }

        [TestMethod]
        public void Visit_NotNullComparison_GeneratesIsNotNullSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name != null;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Name] is not null"));
        }

        [TestMethod]
        public void Visit_BooleanTrue_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[IsActive]='True'"));
        }

        [TestMethod]
        public void Visit_BooleanFalse_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => !x.IsActive;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("NOT"));
            Assert.IsTrue(result.Contains("t.[IsActive]='True'"));
        }

        [TestMethod]
        public void Visit_ConstantTrue_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("(1=1)"));
        }

        [TestMethod]
        public void Visit_ConstantFalse_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => false;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("(1=0)"));
        }

        [TestMethod]
        public void Visit_ArithmeticAdd_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount + 10 > 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] + 10"));
            Assert.IsTrue(result.Contains("> 100"));
        }

        [TestMethod]
        public void Visit_ArithmeticSubtract_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount - 10 > 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] - 10"));
            Assert.IsTrue(result.Contains("> 100"));
        }

        [TestMethod]
        public void Visit_ArithmeticMultiply_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount * 2 > 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] * 2"));
            Assert.IsTrue(result.Contains("> 100"));
        }

        [TestMethod]
        public void Visit_ArithmeticDivide_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount / 2 > 100;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] / 2"));
            Assert.IsTrue(result.Contains("> 100"));
        }

        [TestMethod]
        public void Visit_ArithmeticModulo_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id % 2 == 0;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("MOD(t.[Id],2)"));
            Assert.IsTrue(result.Contains("= 0"));
        }

        [TestMethod]
        public void Visit_DifferentDialects_GenerateDifferentEscaping()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Name == "Test";

            var sqlServerResult = PredicateVisitor.Visit<TestEntity>(predicate, SqlServerDialectProvider.Instance, "t");
            var postgresResult = PredicateVisitor.Visit<TestEntity>(predicate, PostgresDialectProvider.Instance, "t");
            var mysqlResult = PredicateVisitor.Visit<TestEntity>(predicate, MySqlDialectProvider.Instance, "t");

            Assert.IsTrue(sqlServerResult.Contains("t.[Name]"));
            Assert.IsTrue(postgresResult.Contains("t.\"Name\""));
            Assert.IsTrue(mysqlResult.Contains("t.`Name`"));
        }

        [TestMethod]
        public void Visit_NoAlias_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id == 1;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, null);

            Assert.IsTrue(result.Contains("[Id] = 1"));
            Assert.IsFalse(result.Contains("t."));
        }

        [TestMethod]
        public void Visit_ComplexNestedExpression_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => (x.Id == 1 || x.Id == 2) && (x.IsActive == true && x.Amount > 100);
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Id] = 1"));
            Assert.IsTrue(result.Contains("OR"));
            Assert.IsTrue(result.Contains("t.[Id] = 2"));
            Assert.IsTrue(result.Contains("AND"));
            Assert.IsTrue(result.Contains("t.[IsActive] = (1=1)"));
            Assert.IsTrue(result.Contains("t.[Amount] > 100"));
        }

        [TestMethod]
        public void Visit_ComplexNestedExpression_GeneratesCorrectSql_WithBoolean()
        {
            Expression<Func<TestEntity, bool>> predicate = x => (x.Id == 1 || x.Id == 2) && (x.IsActive && x.Amount > 100);
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Id] = 1"));
            Assert.IsTrue(result.Contains("OR"));
            Assert.IsTrue(result.Contains("t.[Id] = 2"));
            Assert.IsTrue(result.Contains("AND"));
            Assert.IsTrue(result.Contains("t.[IsActive]='True'"));
            Assert.IsTrue(result.Contains("t.[Amount] > 100"));
        }

        [TestMethod]
        public void Visit_NullExpression_ReturnsEmptyString()
        {
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(null, dialect, "t");

            Assert.AreEqual(string.Empty, result);
        }

        [TestMethod]
        public void Visit_DateTimeComparison_GeneratesCorrectSql()
        {
            var testDate = new DateTime(2023, 1, 1);
            Expression<Func<TestEntity, bool>> predicate = x => x.CreatedDate > testDate;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[CreatedDate] >"));
            Assert.IsTrue(result.Contains("2023"));
        }

        [TestMethod]
        public void Visit_DecimalComparison_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Amount == 123.45m;
            var dialect = SqlServerDialectProvider.Instance;

            var result = PredicateVisitor.Visit<TestEntity>(predicate, dialect, "t");

            Assert.IsTrue(result.Contains("t.[Amount] = 123.45"));
        }
    }
}
