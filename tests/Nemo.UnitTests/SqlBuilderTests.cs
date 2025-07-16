using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Data;
using Nemo.Attributes;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;

namespace Nemo.UnitTests
{
    [TestClass]
    public class SqlBuilderTests
    {
        public class TestEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
            public bool IsActive { get; set; }
            public decimal Amount { get; set; }
        }

        public class TestEntityWithSchema
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public class TestJoinEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public int TestEntityId { get; set; }
            public string Description { get; set; }
        }

        [TestMethod]
        public void GetSelectStatement_SimpleEntity_GeneratesCorrectSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.Id == 1;
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectStatement(predicate, 0, 0, 0, false, dialect);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
            Assert.IsTrue(result.Contains("[Id]"));
        }

        [TestMethod]
        public void GetSelectStatement_WithPagination_IncludesPagingLogic()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectStatement(predicate, 1, 10, 0, false, dialect);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
        }

        [TestMethod]
        public void GetSelectStatement_WithOrderBy_IncludesOrderClause()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;
            var orderBy = new Sorting<TestEntity> { OrderBy = x => x.Name, Reverse = false };

            var result = SqlBuilder.GetSelectStatement(predicate, 0, 0, 0, false, dialect, orderBy);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsTrue(result.Contains("ORDER BY"));
        }

        [TestMethod]
        public void GetSelectStatement_NullPredicate_GeneratesSelectWithoutWhere()
        {
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectStatement<TestEntity>(null, 0, 0, 0, false, dialect);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsFalse(result.Contains("WHERE"));
        }

        [TestMethod]
        public void GetSelectStatement_FirstFlag_LimitsResults()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectStatement(predicate, 0, 0, 0, true, dialect);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
        }

        [TestMethod]
        public void GetSelectCountStatement_WithPredicate_GeneratesCountSql()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectCountStatement(predicate, dialect);

            Assert.IsTrue(result.Contains("SELECT COUNT(*)"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
        }

        [TestMethod]
        public void GetSelectCountStatement_NullPredicate_GeneratesCountWithoutWhere()
        {
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectCountStatement<TestEntity>(null, dialect);

            Assert.IsTrue(result.Contains("SELECT COUNT(*)"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsFalse(result.Contains("WHERE"));
        }

        [TestMethod]
        public void GetSelectAggregationStatement_WithProjection_GeneratesAggregateSql()
        {
            Expression<Func<TestEntity, decimal>> projection = x => x.Amount;
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectAggregationStatement("SUM", projection, predicate, dialect);

            Assert.IsTrue(result.Contains("SELECT SUM("));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
            Assert.IsTrue(result.Contains("[Amount]"));
        }

        [TestMethod]
        public void GetInsertStatement_WithAutoGenerated_ExcludesAutoGeneratedColumns()
        {
            var parameters = new[]
            {
                new Param { Name = "Id", Source = "Id", IsAutoGenerated = true, IsPrimaryKey = true },
                new Param { Name = "Name", Source = "Name", Value = "Test" },
                new Param { Name = "CreatedDate", Source = "CreatedDate", Value = DateTime.Now }
            };
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetInsertStatement(typeof(TestEntity), parameters, dialect);

            Assert.IsTrue(result.Contains("INSERT INTO"));
            Assert.IsFalse(result.Contains("[Id]"));
            Assert.IsTrue(result.Contains("[Name]"));
            Assert.IsTrue(result.Contains("[CreatedDate]"));
            Assert.IsTrue(result.Contains("SCOPE_IDENTITY()"));
        }

        [TestMethod]
        public void GetInsertStatement_NoAutoGenerated_IncludesAllColumns()
        {
            var parameters = new[]
            {
                new Param { Name = "Id", Source = "Id", Value = 1, IsPrimaryKey = true },
                new Param { Name = "Name", Source = "Name", Value = "Test" },
                new Param { Name = "CreatedDate", Source = "CreatedDate", Value = DateTime.Now }
            };
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetInsertStatement(typeof(TestEntity), parameters, dialect);

            Assert.IsTrue(result.Contains("INSERT INTO"));
            Assert.IsTrue(result.Contains("[Id]"));
            Assert.IsTrue(result.Contains("[Name]"));
            Assert.IsTrue(result.Contains("[CreatedDate]"));
            Assert.IsFalse(result.Contains("SCOPE_IDENTITY()"));
        }

        [TestMethod]
        public void GetUpdateStatement_WithPrimaryKey_GeneratesCorrectSql()
        {
            var parameters = new List<Param>
            {
                new Param { Name = "Name", Source = "Name", Value = "Updated Name" },
                new Param { Name = "CreatedDate", Source = "CreatedDate", Value = DateTime.Now }
            };
            var primaryKey = new List<Param>
            {
                new Param { Name = "Id", Source = "Id", Value = 1, IsPrimaryKey = true }
            };
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetUpdateStatement(typeof(TestEntity), parameters, primaryKey, dialect);

            Assert.IsTrue(result.Contains("UPDATE"));
            Assert.IsTrue(result.Contains("SET"));
            Assert.IsTrue(result.Contains("WHERE"));
            Assert.IsTrue(result.Contains("[Name] = @Name"));
            Assert.IsTrue(result.Contains("[Id] = @Id"));
        }

        [TestMethod]
        public void GetDeleteStatement_WithPrimaryKey_GeneratesCorrectSql()
        {
            var primaryKey = new List<Param>
            {
                new Param { Name = "Id", Source = "Id", Value = 1, IsPrimaryKey = true }
            };
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetDeleteStatement(typeof(TestEntity), primaryKey, dialect);

            Assert.IsTrue(result.Contains("DELETE FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
            Assert.IsTrue(result.Contains("[Id] = @Id"));
        }

        [TestMethod]
        public void GetDeleteStatement_WithSoftDelete_GeneratesUpdateSql()
        {
            var primaryKey = new List<Param>
            {
                new Param { Name = "Id", Source = "Id", Value = 1, IsPrimaryKey = true }
            };
            var dialect = SqlServerDialectProvider.Instance;
            var softDeleteColumn = "IsDeleted";

            var result = SqlBuilder.GetDeleteStatement(typeof(TestEntity), primaryKey, dialect, softDeleteColumn);

            Assert.IsTrue(result.Contains("UPDATE"));
            Assert.IsTrue(result.Contains("SET"));
            Assert.IsTrue(result.Contains("IsDeleted"));
            Assert.IsTrue(result.Contains("WHERE"));
        }

        [TestMethod]
        public void GetDeleteStatement_CompositePrimaryKey_HandlesMultipleKeys()
        {
            var primaryKey = new List<Param>
            {
                new Param { Name = "Id", Source = "Id", Value = 1, IsPrimaryKey = true },
                new Param { Name = "SecondaryId", Source = "SecondaryId", Value = 2, IsPrimaryKey = true }
            };
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetDeleteStatement(typeof(TestEntity), primaryKey, dialect);

            Assert.IsTrue(result.Contains("DELETE FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
            Assert.IsTrue(result.Contains("[Id] = @Id"));
            Assert.IsTrue(result.Contains("[SecondaryId] = @SecondaryId"));
            Assert.IsTrue(result.Contains(" AND "));
        }

        [TestMethod]
        public void GetTableNameForSql_SimpleType_ReturnsTableName()
        {
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetTableNameForSql(typeof(TestEntity), dialect);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.Contains("TestEntity"));
        }

        [TestMethod]
        public void SqlBuilder_DifferentDialects_GeneratesDifferentSyntax()
        {
            var parameters = new[]
            {
                new Param { Name = "Name", Source = "Name", Value = "Test" }
            };

            var sqlServerResult = SqlBuilder.GetInsertStatement(typeof(TestEntity), parameters, SqlServerDialectProvider.Instance);
            var postgresResult = SqlBuilder.GetInsertStatement(typeof(TestEntity), parameters, PostgresDialectProvider.Instance);
            var mysqlResult = SqlBuilder.GetInsertStatement(typeof(TestEntity), parameters, MySqlDialectProvider.Instance);

            Assert.IsTrue(sqlServerResult.Contains("[Name]"));
            Assert.IsTrue(postgresResult.Contains("\"Name\""));
            Assert.IsTrue(mysqlResult.Contains("`Name`"));
        }

        [TestMethod]
        public void GetSelectStatement_ComplexPredicate_HandlesMultipleConditions()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true && x.Amount > 100 && x.Name.Contains("Test");
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectStatement(predicate, 0, 0, 0, false, dialect);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
            Assert.IsTrue(result.Contains("WHERE"));
            Assert.IsTrue(result.Contains("AND"));
        }

        [TestMethod]
        public void GetSelectStatement_WithSkipCount_HandlesPagination()
        {
            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive == true;
            var dialect = SqlServerDialectProvider.Instance;

            var result = SqlBuilder.GetSelectStatement(predicate, 0, 10, 5, false, dialect);

            Assert.IsTrue(result.Contains("SELECT"));
            Assert.IsTrue(result.Contains("FROM"));
        }

        [TestMethod]
        public void GetInsertStatement_OrderedParameters_UsesQuestionMarks()
        {
            var parameters = new[]
            {
                new Param { Name = "Name", Source = "Name", Value = "Test" }
            };
            var dialect = new TestOrderedParameterDialect();

            var result = SqlBuilder.GetInsertStatement(typeof(TestEntity), parameters, dialect);

            Assert.IsTrue(result.Contains("?"));
            Assert.IsFalse(result.Contains("@"));
        }

        private class TestOrderedParameterDialect : SqlServerDialectProvider
        {
            public TestOrderedParameterDialect()
            {
                UseOrderedParameters = true;
            }
        }
    }
}
