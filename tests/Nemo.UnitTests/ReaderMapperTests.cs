using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.Reflection;
using System;
using System.Data;

namespace Nemo.UnitTests
{
    [TestClass]
    public class ReaderMapperTests
    {
        public class Person
        {
            [MapColumn("person_id")]
            public int Id { get; set; }
            public string Name { get; set; }
            public int? ManagerId { get; set; }
            public DateTime? HiredOn { get; set; }
            public double Salary { get; set; }
            public bool Active { get; set; }
        }

        private static IDataReader CreateReader(params object[] values)
        {
            var table = new DataTable();
            table.Columns.Add("person_id", typeof(int));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("ManagerId", typeof(int));
            table.Columns.Add("HiredOn", typeof(DateTime));
            table.Columns.Add("Salary", typeof(double));
            table.Columns.Add("Active", typeof(bool));
            table.Rows.Add(values);

            var reader = table.CreateDataReader();
            reader.Read();
            return reader;
        }

        [TestMethod]
        public void ReaderMapper_MapsAllColumnsByOrdinal()
        {
            using var reader = CreateReader(7, "Jane", 3, new DateTime(2020, 5, 1), 1234.5d, true);
            var mapper = Mapper.CreateReaderDelegate(reader, typeof(Person), true);

            var person = new Person();
            mapper(reader, person);

            Assert.AreEqual(7, person.Id);
            Assert.AreEqual("Jane", person.Name);
            Assert.AreEqual(3, person.ManagerId);
            Assert.AreEqual(new DateTime(2020, 5, 1), person.HiredOn);
            Assert.AreEqual(1234.5d, person.Salary);
            Assert.IsTrue(person.Active);
        }

        [TestMethod]
        public void ReaderMapper_MapsNullsToDefaults()
        {
            using var reader = CreateReader(7, DBNull.Value, DBNull.Value, DBNull.Value, 0d, false);
            var mapper = Mapper.CreateReaderDelegate(reader, typeof(Person), true);

            var person = new Person { Name = "Jane", ManagerId = 3, HiredOn = DateTime.Today };
            mapper(reader, person);

            Assert.IsNull(person.Name);
            Assert.IsNull(person.ManagerId);
            Assert.IsNull(person.HiredOn);
        }

        [TestMethod]
        public void ReaderMapper_IgnoresColumnsNotPresentInResultSet()
        {
            var table = new DataTable();
            table.Columns.Add("person_id", typeof(int));
            table.Rows.Add(11);

            using var reader = table.CreateDataReader();
            reader.Read();

            var mapper = Mapper.CreateReaderDelegate(reader, typeof(Person), true);

            var person = new Person { Name = "unchanged" };
            mapper(reader, person);

            Assert.AreEqual(11, person.Id);
            Assert.AreEqual("unchanged", person.Name);
        }

        [TestMethod]
        public void ReaderMapper_CoercesMismatchedFieldTypes()
        {
            var table = new DataTable();
            table.Columns.Add("person_id", typeof(long));
            table.Columns.Add("Salary", typeof(decimal));
            table.Rows.Add(42L, 10.5m);

            using var reader = table.CreateDataReader();
            reader.Read();

            var mapper = Mapper.CreateReaderDelegate(reader, typeof(Person), true);

            var person = new Person();
            mapper(reader, person);

            Assert.AreEqual(42, person.Id);
            Assert.AreEqual(10.5d, person.Salary);
        }

        [TestMethod]
        public void ReaderMapper_IsCachedPerResultSetShape()
        {
            using var first = CreateReader(1, "a", 1, DateTime.Today, 1d, true);
            using var second = CreateReader(2, "b", 2, DateTime.Today, 2d, false);

            var mapper1 = Mapper.CreateReaderDelegate(first, typeof(Person), true);
            var mapper2 = Mapper.CreateReaderDelegate(second, typeof(Person), true);

            Assert.AreSame(mapper1, mapper2);

            var table = new DataTable();
            table.Columns.Add("Name", typeof(string));
            table.Rows.Add("c");
            using var third = table.CreateDataReader();
            third.Read();

            Assert.AreNotSame(mapper1, Mapper.CreateReaderDelegate(third, typeof(Person), true));
        }
    }
}
