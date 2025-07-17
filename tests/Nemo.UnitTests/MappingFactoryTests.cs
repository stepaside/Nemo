using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Configuration.Mapping;
using Nemo.Attributes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace Nemo.UnitTests
{
    [TestClass]
    public class MappingFactoryTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            
            [MapColumn("entity_name")]
            public string Name { get; set; }
            
            [MapProperty("source_description")]
            public string Description { get; set; }
        }

        public class TestEntityMap : EntityMap<TestEntity>
        {
            public TestEntityMap()
            {
                Property(e => e.Id).Column("mapped_id");
                Property(e => e.Name).Column("mapped_name").SourceProperty("mapped_source_name");
            }
        }

        [TestMethod]
        public void GetPropertyOrColumnName_WithoutMapping_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("Id");
            
            var result = MappingFactory.GetPropertyOrColumnName(property, false, null, true);
            
            Assert.AreEqual("Id", result);
        }

        [TestMethod]
        public void GetPropertyOrColumnName_WithMapColumnAttribute_ReturnsColumnName()
        {
            var property = typeof(TestEntity).GetProperty("Name");
            
            var result = MappingFactory.GetPropertyOrColumnName(property, false, null, true);
            
            Assert.AreEqual("entity_name", result);
        }

        [TestMethod]
        public void GetPropertyOrColumnName_WithMapPropertyAttribute_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("Description");
            
            var result = MappingFactory.GetPropertyOrColumnName(property, false, null, false);
            
            Assert.AreEqual("source_description", result);
        }

        [TestMethod]
        public void GetPropertyOrColumnName_WithIgnoreMappings_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("Name");
            
            var result = MappingFactory.GetPropertyOrColumnName(property, true, null, true);
            
            Assert.AreEqual("Name", result);
        }

        [TestMethod]
        public void GetItem_FromDictionary_ReturnsValue()
        {
            var source = new Dictionary<string, object>
            {
                { "key1", "value1" },
                { "key2", 123 },
                { "key3", null }
            };
            
            var result1 = MappingFactory.GetItem(source, "key1", "default");
            var result2 = MappingFactory.GetItem(source, "key2", 0);
            var result3 = MappingFactory.GetItem(source, "key3", "default");
            var result4 = MappingFactory.GetItem(source, "missing", "default");
            
            Assert.AreEqual("value1", result1);
            Assert.AreEqual(123, result2);
            Assert.AreEqual("default", result3);
            Assert.AreEqual("default", result4);
        }

        [TestMethod]
        public void GetItem_FromNullDictionary_ReturnsDefault()
        {
            Dictionary<string, object> source = null;
            
            var result = MappingFactory.GetItem(source, "key", "default");
            
            Assert.AreEqual("default", result);
        }

        [TestMethod]
        public void IsIndexer_Dictionary_ReturnsTrue()
        {
            var source = new Dictionary<string, object>();
            
            var result = MappingFactory.IsIndexer(source);
            
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void IsIndexer_RegularObject_ReturnsFalse()
        {
            var source = new TestEntity();
            
            var result = MappingFactory.IsIndexer(source);
            
            Assert.IsFalse(result);
        }

        [TestMethod]
        public void GetIndexerType_Dictionary_ReturnsDictionaryType()
        {
            var source = new Dictionary<string, object>();
            
            var result = MappingFactory.GetIndexerType(source);
            
            Assert.AreEqual(typeof(IDictionary<string, object>), result);
        }

        [TestMethod]
        public void GetIndexerType_RegularObject_ReturnsDataRowType()
        {
            var source = new TestEntity();
            
            var result = MappingFactory.GetIndexerType(source);
            
            Assert.AreEqual(typeof(DataRow), result);
        }

        [TestMethod]
        public void GetTypeConverter_WithoutEntityMap_ReturnsAttributeConverter()
        {
            var property = typeof(TestEntity).GetProperty("Name");
            
            var result = MappingFactory.GetTypeConverter(typeof(string), property, null);
            
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void GetTypeConverter_WithNullProperty_HandlesGracefully()
        {
            var result = MappingFactory.GetTypeConverter(typeof(string), null, null);
            
            Assert.IsNotNull(result);
        }

        private class MockDataRecord : IDataRecord
        {
            private readonly Dictionary<string, object> _data;

            public MockDataRecord(Dictionary<string, object> data)
            {
                _data = data ?? new Dictionary<string, object>();
            }

            public object this[int i] => throw new NotImplementedException();
            public object this[string name] => _data.ContainsKey(name) ? _data[name] : null;
            public int FieldCount => _data.Count;

            public bool GetBoolean(int i) => throw new NotImplementedException();
            public byte GetByte(int i) => throw new NotImplementedException();
            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
            public char GetChar(int i) => throw new NotImplementedException();
            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
            public IDataReader GetData(int i) => throw new NotImplementedException();
            public string GetDataTypeName(int i) => throw new NotImplementedException();
            public DateTime GetDateTime(int i) => throw new NotImplementedException();
            public decimal GetDecimal(int i) => throw new NotImplementedException();
            public double GetDouble(int i) => throw new NotImplementedException();
            public Type GetFieldType(int i) => throw new NotImplementedException();
            public float GetFloat(int i) => throw new NotImplementedException();
            public Guid GetGuid(int i) => throw new NotImplementedException();
            public short GetInt16(int i) => throw new NotImplementedException();
            public int GetInt32(int i) => throw new NotImplementedException();
            public long GetInt64(int i) => throw new NotImplementedException();
            public string GetName(int i) => throw new NotImplementedException();
            public int GetOrdinal(string name) => throw new NotImplementedException();
            public string GetString(int i) => throw new NotImplementedException();
            public object GetValue(int i) => throw new NotImplementedException();
            public int GetValues(object[] values) => throw new NotImplementedException();
            public bool IsDBNull(int i) => throw new NotImplementedException();
        }

        [TestMethod]
        public void GetItem_FromDataRecord_ReturnsValue()
        {
            var data = new Dictionary<string, object>
            {
                { "field1", "value1" },
                { "field2", 456 }
            };
            var source = new MockDataRecord(data);
            
            var result1 = MappingFactory.GetItem(source, "field1", "default");
            var result2 = MappingFactory.GetItem(source, "field2", 0);
            var result3 = MappingFactory.GetItem(source, "missing", "default");
            
            Assert.AreEqual("value1", result1);
            Assert.AreEqual(456, result2);
            Assert.AreEqual("default", result3);
        }

        [TestMethod]
        public void GetItem_FromNullDataRecord_ReturnsDefault()
        {
            IDataRecord source = null;
            
            var result = MappingFactory.GetItem(source, "field", "default");
            
            Assert.AreEqual("default", result);
        }
    }
}
