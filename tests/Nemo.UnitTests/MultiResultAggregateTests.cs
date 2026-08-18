using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.Collections;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Nemo.UnitTests
{
    [TestClass]
    public class MultiResultAggregateTests
    {
        public class Company
        {
            [PrimaryKey]
            public string Id { get; set; }
            public string Name { get; set; }
            public List<Employee> Employees { get; set; }
            public Address Address { get; set; }
        }

        public class Employee
        {
            [PrimaryKey]
            public int Id { get; set; }
            [References(typeof(Company))]
            public string CompanyId { get; set; }
            public string Name { get; set; }
            public Company Company { get; set; }
        }

        public class Address
        {
            [PrimaryKey]
            public int Id { get; set; }
            [References(typeof(Company))]
            public string CompanyId { get; set; }
            public string City { get; set; }
            public Company Company { get; set; }
        }

        public class SortedCompany
        {
            [PrimaryKey]
            public string Id { get; set; }
            [Distinct, Sorted]
            public IList<Tag> Tags { get; set; }
        }

        public class Tag : IComparable<Tag>
        {
            [PrimaryKey]
            public string Name { get; set; }
            [References(typeof(SortedCompany))]
            public string CompanyId { get; set; }

            public int CompareTo(Tag other)
            {
                return string.Compare(Name, other?.Name, StringComparison.Ordinal);
            }

            public override bool Equals(object obj)
            {
                return obj is Tag other && string.Equals(Name, other.Name, StringComparison.Ordinal);
            }

            public override int GetHashCode()
            {
                return Name != null ? StringComparer.Ordinal.GetHashCode(Name) : 0;
            }
        }

        public class Region
        {
            [PrimaryKey(Position = 0)]
            public string Country { get; set; }
            [PrimaryKey(Position = 1)]
            public string Code { get; set; }
            public List<Store> Stores { get; set; }
        }

        public class Store
        {
            [PrimaryKey]
            public int Id { get; set; }
            [References(typeof(Region), Position = 0)]
            public string Country { get; set; }
            [References(typeof(Region), Position = 1)]
            public string RegionCode { get; set; }
        }

        private static IMultiResult CreateMultiResult(params IEnumerable<object>[] sets)
        {
            var types = sets.Select(s => s.First().GetType()).ToArray();

            var items = new List<MultiResultItem>();
            for (var i = 0; i < sets.Length; i++)
            {
                foreach (var item in sets[i])
                {
                    items.Add(new MultiResultItem { Item = item, ItemType = types[i], ItemTypeIndex = i });
                }
            }

            return MultiResult.Create(types, items, false, null);
        }

        [TestMethod]
        public void Aggregate_PopulatesCollectionAndForeignKeyBackReference()
        {
            var company = new Company { Id = "A", Name = "Acme" };
            var employees = new[]
            {
                new Employee { Id = 1, CompanyId = "A", Name = "First" },
                new Employee { Id = 2, CompanyId = "A", Name = "Second" },
                new Employee { Id = 3, CompanyId = "B", Name = "Other" }
            };

            var result = CreateMultiResult(new[] { company }, employees).Aggregate<Company>().ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(2, result[0].Employees.Count);
            CollectionAssert.AreEqual(new[] { "First", "Second" }, result[0].Employees.Select(e => e.Name).ToArray());
            Assert.AreSame(company, result[0].Employees[0].Company);
            Assert.AreSame(company, result[0].Employees[1].Company);
            Assert.IsNull(employees[2].Company);
        }

        [TestMethod]
        public void Aggregate_PopulatesSingleRelationAndBackReference()
        {
            var company = new Company { Id = "A" };
            var addresses = new[]
            {
                new Address { Id = 10, CompanyId = "A", City = "Springfield" },
                new Address { Id = 11, CompanyId = "Z", City = "Elsewhere" }
            };

            var result = CreateMultiResult(new[] { company }, addresses).Aggregate<Company>().ToList();

            Assert.AreEqual("Springfield", result[0].Address.City);
            Assert.AreSame(company, result[0].Address.Company);
        }

        [TestMethod]
        public void Aggregate_MatchesCompositeRelationKeys()
        {
            var regions = new[]
            {
                new Region { Country = "US", Code = "W" },
                new Region { Country = "CA", Code = "W" }
            };
            var stores = new[]
            {
                new Store { Id = 1, Country = "US", RegionCode = "W" },
                new Store { Id = 2, Country = "CA", RegionCode = "W" },
                new Store { Id = 3, Country = "US", RegionCode = "E" }
            };

            var result = CreateMultiResult(regions, stores).Aggregate<Region>().ToList();

            Assert.AreEqual(2, result.Count);
            CollectionAssert.AreEqual(new[] { 1 }, result[0].Stores.Select(s => s.Id).ToArray());
            CollectionAssert.AreEqual(new[] { 2 }, result[1].Stores.Select(s => s.Id).ToArray());
        }

        [TestMethod]
        public void Aggregate_AppliesDistinctAndSortedCollectionSemantics()
        {
            var company = new SortedCompany { Id = "A" };
            var tags = new[]
            {
                new Tag { Name = "b", CompanyId = "A" },
                new Tag { Name = "a", CompanyId = "A" },
                new Tag { Name = "b", CompanyId = "A" }
            };

            var result = CreateMultiResult(new[] { company }, tags).Aggregate<SortedCompany>().ToList();

            //  A sorted collection keeps duplicates regardless of the distinct attribute, which List.Create ignores when sorting
            CollectionAssert.AreEqual(new[] { "a", "b", "b" }, result[0].Tags.Select(t => t.Name).ToArray());
        }

        [TestMethod]
        public void Aggregate_IgnoresUnrelatedResultSets()
        {
            var company = new Company { Id = "A" };
            var tags = new[] { new Tag { Name = "a", CompanyId = "A" } };

            var result = CreateMultiResult(new[] { company }, tags).Aggregate<Company>().ToList();

            Assert.AreEqual(1, result.Count);
            Assert.IsNull(result[0].Employees);
        }

        [TestMethod]
        public void Aggregate_ToleratesNullRelationKeys()
        {
            var company = new Company { Id = "A" };
            var employees = new[]
            {
                new Employee { Id = 1, CompanyId = null, Name = "Unassigned" },
                new Employee { Id = 2, CompanyId = "A", Name = "Assigned" }
            };

            var result = CreateMultiResult(new[] { company }, employees).Aggregate<Company>().ToList();

            Assert.AreEqual(1, result[0].Employees.Count);
            Assert.AreEqual("Assigned", result[0].Employees[0].Name);
        }

        [TestMethod]
        public void TypeArray_UsesValueEquality()
        {
            var first = new TypeArray(new[] { typeof(Company), typeof(Employee) });
            var second = new TypeArray(new[] { typeof(Company), typeof(Employee) });
            var reordered = new TypeArray(new[] { typeof(Employee), typeof(Company) });
            var shorter = new TypeArray(new[] { typeof(Company) });

            Assert.AreEqual(first, second);
            Assert.AreEqual(first.GetHashCode(), second.GetHashCode());
            Assert.AreNotEqual(first, reordered);
            Assert.AreNotEqual(first, shorter);

            var map = new Dictionary<TypeArray, int> { [first] = 1 };
            Assert.IsTrue(map.ContainsKey(second));
        }
    }
}
