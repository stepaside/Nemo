using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.Extensions;
using Nemo.Serialization;

namespace Nemo.UnitTests
{
    [TestClass]
    public class BinarySerializationTests
    {
        public class Child
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Label { get; set; }
            public Parent Parent { get; set; }
        }

        public class Parent
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
            public List<Child> Children { get; set; }
            public Dictionary<string, int> Map { get; set; }
        }

        public class NullableEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public int? Count { get; set; }
            public DateTime? When { get; set; }
            public Guid? Key { get; set; }
            public decimal? Amount { get; set; }
            public string Name { get; set; }
        }

        public class TemporalEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public DateTime Created { get; set; }
            public DateTimeOffset Stamp { get; set; }
        }

        public class NoDefaultConstructor
        {
            public NoDefaultConstructor(int id)
            {
                Id = id;
            }

            [PrimaryKey]
            public int Id { get; set; }
        }

        private static readonly SerializationMode[] AllModes =
        {
            SerializationMode.Compact,
            SerializationMode.IncludePropertyNames,
            SerializationMode.SerializeAll,
            SerializationMode.SerializeAll | SerializationMode.IncludePropertyNames
        };

        [TestMethod]
        public void NullableProperties_RoundTripInEveryMode()
        {
            var when = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
            var key = Guid.NewGuid();

            foreach (var mode in AllModes)
            {
                var entity = new NullableEntity { Id = 7, Count = 42, When = when, Key = key, Amount = 12.34m, Name = "n" };

                var result = entity.Serialize(mode).Deserialize<NullableEntity>();

                Assert.AreEqual(42, result.Count, mode.ToString());
                Assert.AreEqual(when, result.When, mode.ToString());
                Assert.AreEqual(key, result.Key, mode.ToString());
                Assert.AreEqual(12.34m, result.Amount, mode.ToString());
                Assert.AreEqual("n", result.Name, mode.ToString());
            }
        }

        [TestMethod]
        public void NullNullableProperties_RoundTripAsNull()
        {
            foreach (var mode in AllModes)
            {
                var result = new NullableEntity { Id = 7 }.Serialize(mode).Deserialize<NullableEntity>();

                Assert.IsNull(result.Count, mode.ToString());
                Assert.IsNull(result.When, mode.ToString());
                Assert.IsNull(result.Key, mode.ToString());
                Assert.IsNull(result.Amount, mode.ToString());
            }
        }

        [TestMethod]
        public void Clone_PreservesNullableProperties()
        {
            var entity = new NullableEntity { Id = 7, Count = 42, Name = "n" };

            var clone = entity.Clone();

            Assert.AreEqual(42, clone.Count);
            Assert.AreEqual("n", clone.Name);
        }

        [TestMethod]
        public void DateTimeOffset_RoundTripsNonZeroOffset()
        {
            var stamp = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(-5));

            var result = new TemporalEntity { Id = 1, Stamp = stamp }.Serialize(SerializationMode.SerializeAll).Deserialize<TemporalEntity>();

            Assert.AreEqual(stamp, result.Stamp);
            Assert.AreEqual(stamp.Offset, result.Stamp.Offset);
        }

        [TestMethod]
        public void DateTime_PreservesKind()
        {
            foreach (var kind in new[] { DateTimeKind.Unspecified, DateTimeKind.Local, DateTimeKind.Utc })
            {
                var created = new DateTime(2024, 1, 2, 3, 4, 5, kind);

                var result = new TemporalEntity { Id = 1, Created = created }.Serialize(SerializationMode.SerializeAll).Deserialize<TemporalEntity>();

                Assert.AreEqual(kind, result.Created.Kind);
                Assert.AreEqual(created, result.Created);
            }
        }

        [TestMethod]
        public void DateTime_PreservesMinAndMaxValue()
        {
            foreach (var created in new[] { DateTime.MinValue, DateTime.MaxValue })
            {
                var result = new TemporalEntity { Id = 1, Created = created }.Serialize(SerializationMode.SerializeAll).Deserialize<TemporalEntity>();

                Assert.AreEqual(created, result.Created);
            }
        }

        [TestMethod]
        public void LegacyPayloadWithoutFormatFlag_StillDeserializes()
        {
            var data = new Parent { Id = 1, Name = "p", Map = new Dictionary<string, int> { { "a", 1 } } }.Serialize(SerializationMode.SerializeAll);
            data[0] &= 0x7f; // clear the format flag, as payloads written by earlier versions do

            var result = data.Deserialize<Parent>();

            Assert.AreEqual(1, result.Id);
            Assert.AreEqual("p", result.Name);
            Assert.AreEqual(1, result.Map["a"]);
        }

        [TestMethod]
        public void LegacyPayloadWithoutDateTimeKind_ReadsAsUtc()
        {
            byte[] legacy;
            using (var writer = SerializationWriter.CreateWriter(SerializationMode.Manual))
            {
                writer.Write(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc));
                var current = writer.GetBytes();
                // Earlier versions wrote no kind byte (it is the last byte here) and no format flag.
                legacy = new byte[current.Length - 1];
                Array.Copy(current, legacy, legacy.Length);
                legacy[0] &= 0x7f;
            }

            using (var reader = SerializationReader.CreateReader(legacy))
            {
                var result = reader.ReadDateTime();

                Assert.AreEqual(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), result);
                Assert.AreEqual(DateTimeKind.Utc, result.Kind);
            }
        }

        [TestMethod]
        public void CombinedMode_IncludesAllPropertiesAndPropertyNames()
        {
            var parent = new Parent { Id = 1, Name = "p", Children = new List<Child> { new Child { Id = 2, Label = "c" } } };

            var withNames = parent.Serialize(SerializationMode.SerializeAll | SerializationMode.IncludePropertyNames);
            var withoutNames = parent.Serialize(SerializationMode.SerializeAll);

            // Property names have to be part of the payload when the flag is combined with SerializeAll.
            Assert.IsTrue(withNames.Length > withoutNames.Length);

            var result = withNames.Deserialize<Parent>();
            Assert.AreEqual("p", result.Name);
            Assert.AreEqual(1, result.Children.Count);
            Assert.AreEqual("c", result.Children[0].Label);
        }

        [TestMethod]
        public void SamePropertySet_ReusesGeneratedDeserializerAcrossPayloads()
        {
            var first = new Parent { Id = 1, Name = "one" }.Serialize(SerializationMode.IncludePropertyNames).Deserialize<Parent>();
            var second = new Parent { Id = 2, Name = "two" }.Serialize(SerializationMode.IncludePropertyNames).Deserialize<Parent>();

            Assert.AreEqual("one", first.Name);
            Assert.AreEqual("two", second.Name);
            Assert.AreEqual(2, second.Id);
        }

        [TestMethod]
        public void WriteObjectWithType_RoundTripsThroughRuntimeType()
        {
            var buffer = SerializationWriter.WriteObjectWithType(new Parent { Id = 3, Name = "p" });

            var result = SerializationReader.ReadObjectWithType(buffer) as Parent;

            Assert.IsNotNull(result);
            Assert.AreEqual(3, result.Id);
            Assert.AreEqual("p", result.Name);
        }

        [TestMethod]
        public void MissingParameterlessConstructor_ThrowsDescriptiveException()
        {
            var data = new NoDefaultConstructor(1).Serialize(SerializationMode.SerializeAll);

            var exception = Assert.Throws<NotSupportedException>(() => data.Deserialize<NoDefaultConstructor>());

            StringAssert.Contains(exception.Message, typeof(NoDefaultConstructor).FullName);
        }

        [TestMethod]
        public void NullDictionary_RoundTripsAsNull()
        {
            var result = new Parent { Id = 1, Name = "p" }.Serialize(SerializationMode.SerializeAll).Deserialize<Parent>();

            Assert.IsNull(result.Map);
            Assert.IsNull(result.Children);
        }

        [TestMethod]
        public void EmptyDictionary_RoundTripsAsEmpty()
        {
            var result = new Parent { Id = 1, Map = new Dictionary<string, int>() }.Serialize(SerializationMode.SerializeAll).Deserialize<Parent>();

            Assert.IsNotNull(result.Map);
            Assert.AreEqual(0, result.Map.Count);
        }

        [TestMethod]
        public void EqualButDistinctObjects_AreNotTreatedAsCycles()
        {
            var parent = new Parent
            {
                Id = 1,
                Name = "p",
                Children = new List<Child> { new Child { Id = 2, Label = "same" }, new Child { Id = 2, Label = "same" } }
            };

            var result = parent.Serialize(SerializationMode.SerializeAll).Deserialize<Parent>();

            Assert.AreEqual(2, result.Children.Count);
            Assert.AreEqual("same", result.Children[0].Label);
            Assert.AreEqual("same", result.Children[1].Label);
        }

        [TestMethod]
        public void Cycles_TerminateByDroppingBackReference()
        {
            var parent = new Parent { Id = 1, Name = "p" };
            var child = new Child { Id = 2, Label = "c", Parent = parent };
            parent.Children = new List<Child> { child };

            var result = parent.Serialize(SerializationMode.SerializeAll).Deserialize<Parent>();

            Assert.AreEqual(1, result.Children.Count);
            Assert.IsNull(result.Children[0].Parent);
        }
    }
}
