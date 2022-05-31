using Nemo;
using Nemo.Attributes;
using Nemo.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemoTestCore.Features
{
    internal class Mapping
    {
        public class PersonLegacy
        {
            public int person_id { get; set; }
            public string name { get; set; }
            public DateTime date_of_birth { get; set; }
        }

        public interface IPerson : IDataEntity
        {
            [MapProperty("person_id")]
            int Id { get; set; }
            [MapProperty("name")]
            string Name { get; set; }
            [MapProperty("date_of_birth")]
            DateTime DateOfBirth { get; set; }
        }

        public class Person
        {
            [MapProperty("person_id")]
            public int Id { get; set; }
            [MapProperty("name")]
            public string Name { get; set; }
            [MapProperty("date_of_birth")]
            public DateTime DateOfBirth { get; set; }
        }

        public interface IPersonReadOnly : IDataEntity
        {
            [MapProperty("person_id")]
            int Id { get; }
            [MapProperty("name")]
            string Name { get; }
            [MapProperty("date_of_birth")]
            DateTime DateOfBirth { get; }
        }

        public static void BindToLegacyInstance()
        {
            var person_legacy = new PersonLegacy
            {
                person_id = 12345,
                name = "John Doe",
                date_of_birth = new DateTime(1980, 1, 10)
            };
            var bound_legacy = ObjectFactory.Bind<IPerson>(person_legacy);
        }

        public static void BindToAnonymousObject()
        {
            var person_anonymous = new
            {
                person_id = 12345,
                name = "John Doe",
                date_of_birth = new DateTime(1980, 1, 10)
            };
            var bound_anonymous = ObjectFactory.Bind<IPersonReadOnly>(person_anonymous);
        }

        public static void ConvertAnonymousObjectToDictionary()
        {
            var person_anonymous = new
            {
                person_id = 12345,
                name = "John Doe",
                date_of_birth = new DateTime(1980, 1, 10)
            };
            var dictionary = person_anonymous.ToDictionary();
        }

        public static void MapToLegacyInstance()
        {
            var person_legacy = new PersonLegacy
            {
                person_id = 12345,
                name = "John Doe",
                date_of_birth = new DateTime(1980, 1, 10)
            };

            var mapped_legacy_interface = ObjectFactory.Map<PersonLegacy, IPerson>(person_legacy);
            
            var mapped_legacy = ObjectFactory.Map<PersonLegacy, Person>(person_legacy);
        }

        public static void MapToAnonymousInstance()
        {
            var person_anonymous = new
            {
                person_id = 12345,
                name = "John Doe",
                date_of_birth = new DateTime(1980, 1, 10)
            };

            var mapped_anonymous_interface = ObjectFactory.Map<IPerson>(person_anonymous);

            var mapped_anonymous = ObjectFactory.Map<Person>(person_anonymous);

            var mapped_bound_anonymous_interface = ObjectFactory.Map<IPerson>(ObjectFactory.Bind<IPersonReadOnly>(person_anonymous));

            var mapped_bound_anonymous = ObjectFactory.Map<Person>(ObjectFactory.Bind<IPersonReadOnly>(person_anonymous));
        }

        public static void ForceReadOnly()
        {
            var person_anonymous = new
            {
                person_id = 12345,
                name = "John Doe",
                date_of_birth = new DateTime(1980, 1, 10)
            };

            var mapped_anonymous_interface = ObjectFactory.Map<IPerson>(person_anonymous);
            
            var read_only_person = mapped_anonymous_interface.AsReadOnly();
            
            var is_read_only = read_only_person.IsReadOnly();
        }
    }
}
