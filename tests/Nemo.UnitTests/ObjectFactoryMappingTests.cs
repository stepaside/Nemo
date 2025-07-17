using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo;
using Nemo.Attributes;
using System;
using System.Collections.Generic;
using System.Data;

namespace Nemo.UnitTests
{
    [TestClass]
    public class ObjectFactoryMappingTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
            public bool IsActive { get; set; }
            public decimal Amount { get; set; }
        }

        public class TestEntityWithAttributes
        {
            [MapProperty("entity_id")]
            public int Id { get; set; }
            
            [MapProperty("entity_name")]
            public string Name { get; set; }
            
            [MapColumn("created_timestamp")]
            public DateTime CreatedDate { get; set; }
        }

        public class SourceEntity
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
            public bool IsActive { get; set; }
        }

        [TestMethod]
        public void Map_FromDictionary_MapsPropertiesCorrectly()
        {
            var source = new Dictionary<string, object>
            {
                { "Id", 123 },
                { "Name", "Test Entity" },
                { "CreatedDate", new DateTime(2023, 1, 1) },
                { "IsActive", true },
                { "Amount", 99.99m }
            };

            var result = ObjectFactory.Map<TestEntity>(source);

            Assert.AreEqual(123, result.Id);
            Assert.AreEqual("Test Entity", result.Name);
            Assert.AreEqual(new DateTime(2023, 1, 1), result.CreatedDate);
            Assert.AreEqual(true, result.IsActive);
            Assert.AreEqual(99.99m, result.Amount);
        }

        [TestMethod]
        public void Map_FromDictionaryWithNullValues_HandlesNullsCorrectly()
        {
            var source = new Dictionary<string, object>
            {
                { "Id", 123 },
                { "Name", null },
                { "CreatedDate", new DateTime(2023, 1, 1) },
                { "IsActive", false }
            };

            var result = ObjectFactory.Map<TestEntity>(source);

            Assert.AreEqual(123, result.Id);
            Assert.IsNull(result.Name);
            Assert.AreEqual(new DateTime(2023, 1, 1), result.CreatedDate);
            Assert.AreEqual(false, result.IsActive);
        }

        [TestMethod]
        public void Map_FromDictionaryWithMissingProperties_UsesDefaultValues()
        {
            var source = new Dictionary<string, object>
            {
                { "Id", 456 },
                { "Name", "Partial Entity" }
            };

            var result = ObjectFactory.Map<TestEntity>(source);

            Assert.AreEqual(456, result.Id);
            Assert.AreEqual("Partial Entity", result.Name);
            Assert.AreEqual(default(DateTime), result.CreatedDate);
            Assert.AreEqual(default(bool), result.IsActive);
            Assert.AreEqual(default(decimal), result.Amount);
        }

        [TestMethod]
        public void Map_FromEmptyDictionary_CreatesObjectWithDefaults()
        {
            var source = new Dictionary<string, object>();

            var result = ObjectFactory.Map<TestEntity>(source);

            Assert.IsNotNull(result);
            Assert.AreEqual(default(int), result.Id);
            Assert.IsNull(result.Name);
            Assert.AreEqual(default(DateTime), result.CreatedDate);
            Assert.AreEqual(default(bool), result.IsActive);
            Assert.AreEqual(default(decimal), result.Amount);
        }

        [TestMethod]
        public void Map_GenericObjectToObject_MapsMatchingProperties()
        {
            var source = new SourceEntity
            {
                Id = 789,
                Name = "Source Entity",
                CreatedDate = new DateTime(2023, 6, 15),
                IsActive = true
            };

            var result = ObjectFactory.Map<SourceEntity, TestEntity>(source);

            Assert.AreEqual(789, result.Id);
            Assert.AreEqual("Source Entity", result.Name);
            Assert.AreEqual(new DateTime(2023, 6, 15), result.CreatedDate);
            Assert.AreEqual(true, result.IsActive);
            Assert.AreEqual(default(decimal), result.Amount);
        }

        [TestMethod]
        public void Map_GenericObjectMapping_CreatesNewInstance()
        {
            var source = new SourceEntity
            {
                Id = 999,
                Name = "Test Source"
            };

            var result = ObjectFactory.Map<TestEntity>(source);

            Assert.IsNotNull(result);
            Assert.AreNotSame(source, result);
            Assert.AreEqual(999, result.Id);
            Assert.AreEqual("Test Source", result.Name);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Map_NullDictionaryToTarget_ThrowsArgumentNullException()
        {
            var target = new TestEntity();
            ObjectFactory.Map((IDictionary<string, object>)null, target);
        }

        [TestMethod]
        public void Map_NullSource_ReturnsNull()
        {
            var result = ObjectFactory.Map<TestEntity>((object)null);
            Assert.IsNull(result);
        }

        [TestMethod]
        public void Map_DictionaryToExistingTarget_UpdatesTargetProperties()
        {
            var source = new Dictionary<string, object>
            {
                { "Id", 555 },
                { "Name", "Updated Name" }
            };

            var target = new TestEntity
            {
                Id = 111,
                Name = "Original Name",
                IsActive = true,
                Amount = 50.0m
            };

            ObjectFactory.Map(source, target);

            Assert.AreEqual(555, target.Id);
            Assert.AreEqual("Updated Name", target.Name);
            Assert.AreEqual(true, target.IsActive);
            Assert.AreEqual(50.0m, target.Amount);
        }

        [TestMethod]
        public void Map_WithTypeCoercion_ConvertsCompatibleTypes()
        {
            var source = new Dictionary<string, object>
            {
                { "Id", "123" },
                { "Amount", "99.99" },
                { "IsActive", "true" }
            };

            var result = ObjectFactory.Map<TestEntity>(source);

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void Map_LargeObjectGraph_PerformsEfficiently()
        {
            var sources = new List<Dictionary<string, object>>();
            for (int i = 0; i < 1000; i++)
            {
                sources.Add(new Dictionary<string, object>
                {
                    { "Id", i },
                    { "Name", $"Entity {i}" },
                    { "CreatedDate", DateTime.Now.AddDays(-i) },
                    { "IsActive", i % 2 == 0 },
                    { "Amount", i * 10.5m }
                });
            }

            var results = new List<TestEntity>();
            foreach (var source in sources)
            {
                results.Add(ObjectFactory.Map<TestEntity>(source));
            }

            Assert.AreEqual(1000, results.Count);
            Assert.AreEqual(0, results[0].Id);
            Assert.AreEqual(999, results[999].Id);
        }
    }
}
