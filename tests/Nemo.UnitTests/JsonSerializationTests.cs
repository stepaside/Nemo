using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.Serialization;

namespace Nemo.UnitTests
{
    [TestClass]
    public class JsonSerializationTests
    {
        public enum Status
        {
            None = 0,
            Active = 1
        }

        public class Child
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Label { get; set; }
        }

        public class Entity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public decimal Price { get; set; }
            public double Ratio { get; set; }
            public char Grade { get; set; }
            public DateTime Created { get; set; }
            public string Name { get; set; }
            public string Missing { get; set; }
            public Status State { get; set; }
            public List<Child> Children { get; set; }
            public Dictionary<string, int> Map { get; set; }

            [DoNotSerialize]
            public string Secret { get; set; }

            public string ReadOnly => "computed";
        }

        public abstract class Person
        {
            public string Name { get; set; }
            public DateTime DateOfBirth { get; set; }
        }

        public class Employee : Person
        {
            public DateTime HireDate { get; set; }
        }

        public class Manager : Employee
        {
            public List<Employee> Employees { get; set; } = new List<Employee>();
        }

        public class Company
        {
            public string Name { get; set; }
            public List<Person> Contacts { get; set; } = new List<Person>();
        }

        public class Node
        {
            public string Name { get; set; }
            public Node Parent { get; set; }
            public Node Child { get; set; }
        }

        public class Trackable : ITrackableDataEntity
        {
            public int Id { get; set; }
            public ObjectState ObjectState { get; set; }
        }

        public interface IKid
        {
            [PrimaryKey]
            int Id { get; set; }
            string Label { get; set; }
        }

        public interface IParent
        {
            [PrimaryKey]
            int Id { get; set; }
            string Name { get; set; }
            IList<IKid> Kids { get; set; }
        }

        private static Entity CreateEntity()
        {
            return new Entity
            {
                Id = 1,
                Price = 1234.56m,
                Ratio = 0.5,
                Grade = 'A',
                Created = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc),
                Name = "quote \" backslash \\ newline \n tab \t",
                State = Status.Active,
                Secret = "secret",
                Children = new List<Child> { new Child { Id = 2, Label = "child" } },
                Map = new Dictionary<string, int> { { "key", 3 } }
            };
        }

        [TestMethod]
        public void ToJson_FromJson_RoundTripsEscapedStrings()
        {
            var entity = CreateEntity();

            var result = entity.ToJson().FromJson<Entity>();

            Assert.AreEqual(entity.Name, result.Name);
        }

        [TestMethod]
        public void ToJson_FromJson_RoundTripsValues()
        {
            var entity = CreateEntity();

            var result = entity.ToJson().FromJson<Entity>();

            Assert.AreEqual(entity.Id, result.Id);
            Assert.AreEqual(entity.Price, result.Price);
            Assert.AreEqual(entity.Ratio, result.Ratio);
            Assert.AreEqual(entity.Grade, result.Grade);
            Assert.AreEqual(entity.Created, result.Created);
            Assert.AreEqual(entity.State, result.State);
            Assert.AreEqual(1, result.Children.Count);
            Assert.AreEqual("child", result.Children[0].Label);
            Assert.AreEqual(3, result.Map["key"]);
        }

        [TestMethod]
        public void ToJson_QuotesCharValues()
        {
            var json = new Entity { Grade = 'A' }.ToJson();

            StringAssert.Contains(json, "\"Grade\":\"A\"");
        }

        [TestMethod]
        public void ToJson_UsesInvariantCultureForNumbers()
        {
            var culture = Thread.CurrentThread.CurrentCulture;
            try
            {
                Thread.CurrentThread.CurrentCulture = new CultureInfo("de-DE");

                var json = new Entity { Price = 1234.56m, Ratio = 0.5 }.ToJson();

                StringAssert.Contains(json, "\"Price\":1234.56");
                StringAssert.Contains(json, "\"Ratio\":0.5");
                Assert.AreEqual(1234.56m, json.FromJson<Entity>().Price);
            }
            finally
            {
                Thread.CurrentThread.CurrentCulture = culture;
            }
        }

        [TestMethod]
        public void ToJson_SkipsNullReadOnlyAndNonSerializableProperties()
        {
            var json = CreateEntity().ToJson();

            Assert.IsFalse(json.Contains("Missing"), "null properties should be omitted");
            Assert.IsFalse(json.Contains("Secret"), "DoNotSerialize properties should be omitted");
            Assert.IsFalse(json.Contains("ReadOnly"), "read-only properties should be omitted");
        }

        [TestMethod]
        public void ToJson_IgnoresReferenceCycles()
        {
            var parent = new Node { Name = "parent" };
            parent.Child = new Node { Name = "child", Parent = parent };

            var json = parent.ToJson();

            StringAssert.Contains(json, "\"Name\":\"child\"");
        }

        [TestMethod]
        public void ToJson_FromJson_RoundTripsCollections()
        {
            var entities = new List<Entity> { CreateEntity(), CreateEntity() };

            var result = entities.ToJson().FromJson<List<Entity>>();

            Assert.AreEqual(2, result.Count);
            Assert.AreEqual(entities[0].Name, result[0].Name);
        }

        [TestMethod]
        public void ToJson_WritesToTextWriter()
        {
            var entity = CreateEntity();

            using (var writer = new StringWriter())
            {
                entity.ToJson(writer);
                Assert.AreEqual(entity.ToJson(), writer.ToString());
            }
        }

        [TestMethod]
        public void PolymorphicCollection_RoundTripsRuntimeTypes()
        {
            var manager = new Manager { Name = "Manager 1", HireDate = new DateTime(1990, 12, 20) };
            manager.Employees.Add(new Employee { Name = "Employee 1.1", HireDate = new DateTime(1991, 1, 5) });
            var company = new Company { Name = "Test Company" };
            company.Contacts.Add(manager);
            company.Contacts.Add(new Employee { Name = "Employee 2", HireDate = new DateTime(1992, 2, 6) });

            var json = company.ToJson();
            var result = json.FromJson<Company>();

            Assert.IsTrue(json.Contains("\"$type\""));
            Assert.AreEqual(2, result.Contacts.Count);
            var resultManager = (Manager)result.Contacts[0];
            Assert.AreEqual("Manager 1", resultManager.Name);
            Assert.AreEqual(new DateTime(1990, 12, 20), resultManager.HireDate);
            Assert.AreEqual(1, resultManager.Employees.Count);
            Assert.AreEqual("Employee 1.1", resultManager.Employees[0].Name);
            Assert.IsInstanceOfType(result.Contacts[1], typeof(Employee));
            Assert.AreEqual("Employee 2", result.Contacts[1].Name);
        }

        [TestMethod]
        public void PolymorphicValue_WithoutDiscriminator_Throws()
        {
            Assert.Throws<System.Text.Json.JsonException>(() => "{\"Contacts\":[{\"Name\":\"x\"}]}".FromJson<Company>());
        }

        [TestMethod]
        public void PolymorphicValue_WithUnrelatedType_Throws()
        {
            var json = "{\"Contacts\":[{\"$type\":\"" + typeof(Child).FullName + "," + typeof(Child).Assembly.GetName().Name + "\"}]}";

            Assert.Throws<System.Text.Json.JsonException>(() => json.FromJson<Company>());
        }

        [TestMethod]
        public void FromJson_MaterializesInterfaceAsAdapter()
        {
            var parent = ObjectFactory.Create<IParent>();
            parent.Id = 7;
            parent.Name = "parent";
            var kid = ObjectFactory.Create<IKid>();
            kid.Id = 8;
            kid.Label = "kid";
            parent.Kids = new List<IKid> { kid };

            var result = parent.ToJson().FromJson<IParent>();

            Assert.IsInstanceOfType(result, typeof(IParent));
            Assert.AreEqual(7, result.Id);
            Assert.AreEqual("parent", result.Name);
            Assert.AreEqual(1, result.Kids.Count);
            Assert.AreEqual("kid", result.Kids[0].Label);
        }

        [TestMethod]
        public void FromJson_SetsObjectStateOnTrackableEntities()
        {
            var result = "{\"Id\":5}".FromJson<Trackable>();

            Assert.AreEqual(ObjectState.Clean, result.ObjectState);
        }

        [TestMethod]
        public void FromJson_ReturnsNullForNullJson()
        {
            Assert.IsNull(((string)null).FromJson<Entity>());
        }
    }
}
