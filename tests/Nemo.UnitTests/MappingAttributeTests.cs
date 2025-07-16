using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using System.Reflection;

namespace Nemo.UnitTests
{
    [TestClass]
    public class MappingAttributeTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            
            [MapColumn("entity_name")]
            public string Name { get; set; }
            
            [MapProperty("source_description")]
            public string Description { get; set; }
            
            public string UnmappedProperty { get; set; }
        }

        [TestMethod]
        public void MapColumnAttribute_GetMappedColumnName_WithAttribute_ReturnsSourceName()
        {
            var property = typeof(TestEntity).GetProperty("Name");
            
            var result = MapColumnAttribute.GetMappedColumnName(property);
            
            Assert.AreEqual("entity_name", result);
        }

        [TestMethod]
        public void MapColumnAttribute_GetMappedColumnName_WithoutAttribute_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("Id");
            
            var result = MapColumnAttribute.GetMappedColumnName(property);
            
            Assert.AreEqual("Id", result);
        }

        [TestMethod]
        public void MapColumnAttribute_GetMappedColumnName_UnmappedProperty_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("UnmappedProperty");
            
            var result = MapColumnAttribute.GetMappedColumnName(property);
            
            Assert.AreEqual("UnmappedProperty", result);
        }

        [TestMethod]
        public void MapPropertyAttribute_GetMappedPropertyName_WithAttribute_ReturnsSourceName()
        {
            var property = typeof(TestEntity).GetProperty("Description");
            
            var result = MapPropertyAttribute.GetMappedPropertyName(property);
            
            Assert.AreEqual("source_description", result);
        }

        [TestMethod]
        public void MapPropertyAttribute_GetMappedPropertyName_WithoutAttribute_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("Id");
            
            var result = MapPropertyAttribute.GetMappedPropertyName(property);
            
            Assert.AreEqual("Id", result);
        }

        [TestMethod]
        public void MapPropertyAttribute_GetMappedPropertyName_UnmappedProperty_ReturnsPropertyName()
        {
            var property = typeof(TestEntity).GetProperty("UnmappedProperty");
            
            var result = MapPropertyAttribute.GetMappedPropertyName(property);
            
            Assert.AreEqual("UnmappedProperty", result);
        }

        [TestMethod]
        public void MapColumnAttribute_Constructor_SetsSourceName()
        {
            var attribute = new MapColumnAttribute("test_column");
            
            Assert.AreEqual("test_column", attribute.SourceName);
        }

        [TestMethod]
        public void MapPropertyAttribute_Constructor_SetsSourceName()
        {
            var attribute = new MapPropertyAttribute("test_property");
            
            Assert.AreEqual("test_property", attribute.SourceName);
        }

        [TestMethod]
        public void MapColumnAttribute_NullProperty_ReturnsNull()
        {
            var result = MapColumnAttribute.GetMappedColumnName(null);
            
            Assert.IsNull(result);
        }

        [TestMethod]
        public void MapPropertyAttribute_NullProperty_ReturnsNull()
        {
            var result = MapPropertyAttribute.GetMappedPropertyName(null);
            
            Assert.IsNull(result);
        }
    }
}
