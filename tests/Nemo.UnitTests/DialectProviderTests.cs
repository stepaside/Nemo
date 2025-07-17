using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Data;
using System;
using System.Collections.Generic;
using System.Data;

namespace Nemo.UnitTests
{
    [TestClass]
    public class DialectProviderTests
    {
        [TestMethod]
        public void SqlServerDialectProvider_Properties_SetCorrectly()
        {
            var provider = SqlServerDialectProvider.Instance;

            Assert.AreEqual("SCOPE_IDENTITY()", provider.AutoIncrementComputation);
            Assert.AreEqual("@", provider.ParameterPrefix);
            Assert.AreEqual("@", provider.VariablePrefix);
            Assert.AreEqual("[", provider.IdentifierEscapeStartCharacter);
            Assert.AreEqual("]", provider.IdentifierEscapeEndCharacter);
            Assert.AreEqual("+", provider.StringConcatenationOperator);
            Assert.AreEqual("SUBSTRING", provider.SubstringFunction);
            Assert.IsFalse(provider.UseOrderedParameters);
            Assert.IsTrue(provider.SupportsTemporaryTables);
            Assert.AreEqual(2100, provider.MaximumNumberOfParameters);
        }

        [TestMethod]
        public void PostgresDialectProvider_Properties_SetCorrectly()
        {
            var provider = PostgresDialectProvider.Instance;

            Assert.AreEqual("@", provider.ParameterPrefix);
            Assert.AreEqual("", provider.VariablePrefix);
            Assert.AreEqual("\"", provider.IdentifierEscapeStartCharacter);
            Assert.AreEqual("\"", provider.IdentifierEscapeEndCharacter);
            Assert.AreEqual("||", provider.StringConcatenationOperator);
            Assert.AreEqual("substring", provider.SubstringFunction);
            Assert.IsFalse(provider.UseOrderedParameters);
            Assert.IsTrue(provider.SupportsTemporaryTables);
            Assert.IsTrue(provider.SupportsArrays);
            Assert.IsTrue(provider.SupportsPositionalParameters);
            Assert.AreEqual(short.MaxValue, provider.MaximumNumberOfParameters);
        }

        [TestMethod]
        public void MySqlDialectProvider_Properties_SetCorrectly()
        {
            var provider = MySqlDialectProvider.Instance;

            Assert.AreEqual("LAST_INSERT_ID()", provider.AutoIncrementComputation);
            Assert.AreEqual("@", provider.ParameterPrefix);
            Assert.AreEqual("@", provider.VariablePrefix);
            Assert.AreEqual("`", provider.IdentifierEscapeStartCharacter);
            Assert.AreEqual("`", provider.IdentifierEscapeEndCharacter);
            Assert.AreEqual("CONCAT", provider.StringConcatenationFunction);
            Assert.AreEqual("SUBSTRING", provider.SubstringFunction);
            Assert.IsFalse(provider.UseOrderedParameters);
            Assert.IsTrue(provider.SupportsTemporaryTables);
            Assert.AreEqual(int.MaxValue, provider.MaximumNumberOfParameters);
        }

        [TestMethod]
        public void SqliteDialectProvider_Properties_SetCorrectly()
        {
            var provider = SqliteDialectProvider.Instance;

            Assert.AreEqual("last_insert_rowid()", provider.AutoIncrementComputation);
            Assert.AreEqual("@", provider.ParameterPrefix);
            Assert.AreEqual("", provider.VariablePrefix);
            Assert.AreEqual("\"", provider.IdentifierEscapeStartCharacter);
            Assert.AreEqual("\"", provider.IdentifierEscapeEndCharacter);
            Assert.AreEqual("||", provider.StringConcatenationOperator);
            Assert.AreEqual("SUBSTR", provider.SubstringFunction);
            Assert.IsFalse(provider.UseOrderedParameters);
            Assert.IsTrue(provider.SupportsTemporaryTables);
            Assert.AreEqual(999, provider.MaximumNumberOfParameters);
        }

        [TestMethod]
        public void OracleDialectProvider_Properties_SetCorrectly()
        {
            var provider = OracleDialectProvider.Instance;

            Assert.AreEqual(":", provider.ParameterPrefix);
            Assert.AreEqual("", provider.VariablePrefix);
            Assert.AreEqual("\"", provider.IdentifierEscapeStartCharacter);
            Assert.AreEqual("\"", provider.IdentifierEscapeEndCharacter);
            Assert.AreEqual("||", provider.StringConcatenationOperator);
            Assert.AreEqual("SUBSTR", provider.SubstringFunction);
            Assert.IsFalse(provider.UseOrderedParameters);
            Assert.IsTrue(provider.SupportsTemporaryTables);
            Assert.AreEqual(short.MaxValue, provider.MaximumNumberOfParameters);
        }

        [TestMethod]
        public void SqlServerDialectProvider_ComputeAutoIncrement_ReturnsCorrectSql()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.ComputeAutoIncrement("Id", () => "TestTable");

            Assert.AreEqual("SET @Id = SCOPE_IDENTITY();", result);
        }

        [TestMethod]
        public void PostgresDialectProvider_ComputeAutoIncrement_ReturnsCorrectSql()
        {
            var provider = PostgresDialectProvider.Instance;

            var result = provider.ComputeAutoIncrement("Id", () => "TestTable");

            Assert.IsTrue(result.Contains("Id := currval("));
            Assert.IsTrue(result.Contains("TestTable"));
            Assert.IsTrue(result.Contains("id_sequence"));
        }

        [TestMethod]
        public void MySqlDialectProvider_ComputeAutoIncrement_ReturnsCorrectSql()
        {
            var provider = MySqlDialectProvider.Instance;

            var result = provider.ComputeAutoIncrement("Id", () => "TestTable");

            Assert.AreEqual("SET @Id = LAST_INSERT_ID();", result);
        }

        [TestMethod]
        public void SqliteDialectProvider_ComputeAutoIncrement_ReturnsCorrectSql()
        {
            var provider = SqliteDialectProvider.Instance;

            var result = provider.ComputeAutoIncrement("Id", () => "TestTable");

            Assert.IsTrue(result.Contains("INSERT INTO __VARS"));
            Assert.IsTrue(result.Contains("Id"));
            Assert.IsTrue(result.Contains("last_insert_rowid()"));
        }

        [TestMethod]
        public void OracleDialectProvider_ComputeAutoIncrement_ReturnsCorrectSql()
        {
            var provider = OracleDialectProvider.Instance;

            var result = provider.ComputeAutoIncrement("Id", () => "TestTable");

            Assert.IsTrue(result.Contains("Id := "));
            Assert.IsTrue(result.Contains("TestTable"));
            Assert.IsTrue(result.Contains("id_sequence"));
            Assert.IsTrue(result.Contains("CURRVAL"));
        }

        [TestMethod]
        public void SqlServerDialectProvider_CreateTemporaryTable_ReturnsCorrectSql()
        {
            var provider = SqlServerDialectProvider.Instance;
            var columns = new Dictionary<string, DbType>
            {
                { "Id", DbType.Int32 },
                { "Name", DbType.String }
            };

            var result = provider.CreateTemporaryTable("TestTable", columns);

            Assert.IsTrue(result.Contains("CREATE TABLE"));
            Assert.IsTrue(result.Contains("#TestTable"));
            Assert.IsTrue(result.Contains("[Id] INT"));
            Assert.IsTrue(result.Contains("[Name] NVARCHAR(4000)"));
        }

        [TestMethod]
        public void PostgresDialectProvider_CreateTemporaryTable_ReturnsCorrectSql()
        {
            var provider = PostgresDialectProvider.Instance;
            var columns = new Dictionary<string, DbType>
            {
                { "Id", DbType.Int32 },
                { "Name", DbType.String }
            };

            var result = provider.CreateTemporaryTable("TestTable", columns);

            Assert.IsTrue(result.Contains("CREATE TEMPORARY TABLE"));
            Assert.IsTrue(result.Contains("TestTable"));
            Assert.IsTrue(result.Contains("\"Id\" integer"));
            Assert.IsTrue(result.Contains("\"Name\" text"));
        }

        [TestMethod]
        public void MySqlDialectProvider_CreateTemporaryTable_ReturnsCorrectSql()
        {
            var provider = MySqlDialectProvider.Instance;
            var columns = new Dictionary<string, DbType>
            {
                { "Id", DbType.Int32 },
                { "Name", DbType.String }
            };

            var result = provider.CreateTemporaryTable("TestTable", columns);

            Assert.IsTrue(result.Contains("CREATE TEMPORARY TABLE"));
            Assert.IsTrue(result.Contains("TestTable"));
            Assert.IsTrue(result.Contains("`Id` INT"));
            Assert.IsTrue(result.Contains("`Name` VARCHAR(65535)"));
        }

        [TestMethod]
        public void SqliteDialectProvider_CreateTemporaryTable_ReturnsCorrectSql()
        {
            var provider = SqliteDialectProvider.Instance;
            var columns = new Dictionary<string, DbType>
            {
                { "Id", DbType.Int32 },
                { "Name", DbType.String }
            };

            var result = provider.CreateTemporaryTable("TestTable", columns);

            Assert.IsTrue(result.Contains("CREATE TEMP TABLE IF NOT EXISTS"));
            Assert.IsTrue(result.Contains("TestTable"));
            Assert.IsTrue(result.Contains("\"Id\" INTEGER"));
            Assert.IsTrue(result.Contains("\"Name\" TEXT"));
        }

        [TestMethod]
        public void SqlServerDialectProvider_DeclareVariable_ReturnsCorrectSql()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.DeclareVariable("TestVar", DbType.String);

            Assert.AreEqual("DECLARE @TestVar NVARCHAR(4000);", result);
        }

        [TestMethod]
        public void PostgresDialectProvider_DeclareVariable_ReturnsCorrectSql()
        {
            var provider = PostgresDialectProvider.Instance;

            var result = provider.DeclareVariable("TestVar", DbType.String);

            Assert.AreEqual("DECLARE TestVar text;", result);
        }

        [TestMethod]
        public void SqlServerDialectProvider_AssignVariable_WithString_ReturnsCorrectSql()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.AssignVariable("TestVar", "TestValue");

            Assert.AreEqual("SET @TestVar = 'TestValue';", result);
        }

        [TestMethod]
        public void SqlServerDialectProvider_AssignVariable_WithNumber_ReturnsCorrectSql()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.AssignVariable("TestVar", 123);

            Assert.AreEqual("SET @TestVar = 123;", result);
        }

        [TestMethod]
        public void SqlServerDialectProvider_AssignVariable_WithNull_ReturnsCorrectSql()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.AssignVariable("TestVar", null);

            Assert.AreEqual("SET @TestVar = NULL;", result);
        }

        [TestMethod]
        public void PostgresDialectProvider_AssignVariable_ReturnsCorrectSql()
        {
            var provider = PostgresDialectProvider.Instance;

            var result = provider.AssignVariable("TestVar", "TestValue");

            Assert.AreEqual("TestVar := 'TestValue';", result);
        }

        [TestMethod]
        public void SqliteDialectProvider_AssignVariable_ReturnsCorrectSql()
        {
            var provider = SqliteDialectProvider.Instance;

            var result = provider.AssignVariable("TestVar", "TestValue");

            Assert.IsTrue(result.Contains("INSERT INTO __VARS"));
            Assert.IsTrue(result.Contains("TestVar"));
            Assert.IsTrue(result.Contains("TestValue"));
        }

        [TestMethod]
        public void SqlServerDialectProvider_GetTemporaryTableName_AddsPrefix()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.GetTemporaryTableName("TestTable");

            Assert.IsTrue(result.StartsWith("#"));
            Assert.IsTrue(result.Contains("TestTable"));
        }

        [TestMethod]
        public void PostgresDialectProvider_GetTemporaryTableName_AddsPrefix()
        {
            var provider = PostgresDialectProvider.Instance;

            var result = provider.GetTemporaryTableName("TestTable");

            Assert.IsTrue(result.StartsWith("temp_"));
            Assert.IsTrue(result.Contains("TestTable"));
        }

        [TestMethod]
        public void MySqlDialectProvider_GetTemporaryTableName_AddsPrefix()
        {
            var provider = MySqlDialectProvider.Instance;

            var result = provider.GetTemporaryTableName("TestTable");

            Assert.IsTrue(result.StartsWith("TEMP_"));
            Assert.IsTrue(result.Contains("TestTable"));
        }

        [TestMethod]
        public void SqliteDialectProvider_GetTemporaryTableName_AddsPrefix()
        {
            var provider = SqliteDialectProvider.Instance;

            var result = provider.GetTemporaryTableName("TestTable");

            Assert.IsTrue(result.StartsWith("TEMP_"));
            Assert.IsTrue(result.Contains("TestTable"));
        }

        [TestMethod]
        public void SqliteDialectProvider_EvaluateVariable_ReturnsCorrectSql()
        {
            var provider = SqliteDialectProvider.Instance;

            var result = provider.EvaluateVariable("TestVar");

            Assert.IsTrue(result.Contains("SELECT value FROM __VARS"));
            Assert.IsTrue(result.Contains("TestVar"));
        }

        [TestMethod]
        public void SqlServerDialectProvider_EvaluateVariable_ReturnsVariableName()
        {
            var provider = SqlServerDialectProvider.Instance;

            var result = provider.EvaluateVariable("TestVar");

            Assert.AreEqual("TestVar", result);
        }

        [TestMethod]
        public void DialectProvider_DataTypeDefinitions_AreCorrect()
        {
            var sqlServer = SqlServerDialectProvider.Instance;
            var postgres = PostgresDialectProvider.Instance;
            var mysql = MySqlDialectProvider.Instance;
            var sqlite = SqliteDialectProvider.Instance;
            var oracle = OracleDialectProvider.Instance;

            Assert.AreEqual("BIGINT", sqlServer.BigIntDefinition);
            Assert.AreEqual("bigint", postgres.BigIntDefinition);
            Assert.AreEqual("BIGINT", mysql.BigIntDefinition);
            Assert.AreEqual("INTEGER", sqlite.BigIntDefinition);
            Assert.AreEqual("NUMBER(38)", oracle.BigIntDefinition);

            Assert.AreEqual("UNIQUEIDENTIFIER", sqlServer.GuidDefinition);
            Assert.AreEqual("varchar(36)", postgres.GuidDefinition);
            Assert.AreEqual("VARCHAR(36)", mysql.GuidDefinition);
            Assert.AreEqual("TEXT", sqlite.GuidDefinition);
            Assert.AreEqual("RAW(16)", oracle.GuidDefinition);
        }

        [TestMethod]
        public void SqlServerLatestDialectProvider_SplitString_ReturnsCorrectSql()
        {
            var provider = SqlServerLatestDialectProvider.Instance;

            var result = provider.SplitString("TestVar", "VARCHAR", ",");

            Assert.IsTrue(result.Contains("STRING_SPLIT"));
            Assert.IsTrue(result.Contains("@TestVar"));
            Assert.IsTrue(result.Contains("','"));
        }

        [TestMethod]
        public void SqlServerLatestDialectProvider_SplitString_WithTypeConversion_ReturnsCorrectSql()
        {
            var provider = SqlServerLatestDialectProvider.Instance;

            var result = provider.SplitString("TestVar", "INT", ",");

            Assert.IsTrue(result.Contains("CAST([value] AS INT)"));
            Assert.IsTrue(result.Contains("STRING_SPLIT"));
        }

        [TestMethod]
        public void SqlServerLatestDialectProvider_SplitString_WithStringType_NoConversion()
        {
            var provider = SqlServerLatestDialectProvider.Instance;

            var result = provider.SplitString("TestVar", "VARCHAR", ",");

            Assert.IsFalse(result.Contains("CAST"));
            Assert.IsTrue(result.Contains("[value]"));
        }
    }
}
