using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Attributes;
using Nemo.UnitOfWork;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Nemo.UnitTests
{
    [TestClass]
    public class UnitOfWorkTests
    {
        public class Parent
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
            public List<Child> Children { get; set; }
            public List<Keyless> Keyless { get; set; }
        }

        public class Child
        {
            [PrimaryKey]
            public int Id { get; set; }
            [References(typeof(Parent))]
            public int ParentId { get; set; }
            public string Name { get; set; }
        }

        public class Keyless
        {
            public string Name { get; set; }
        }

        private sealed class OpenConnection : DbConnection
        {
            public override string ConnectionString { get; set; } = string.Empty;
            public override string Database => string.Empty;
            public override string DataSource => string.Empty;
            public override string ServerVersion => string.Empty;
            public override ConnectionState State => ConnectionState.Open;
            public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();
            public override void Close() { }
            public override void Open() { }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
            protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
        }

        [TestInitialize]
        public void ClearScopes()
        {
            ObjectScope.ClearScopes();
        }

        [TestMethod]
        public void Dispose_WithExternalOpenConnection_DoesNotThrow()
        {
            using (var connection = new OpenConnection())
            {
                var entity = new Parent { Id = 1, Name = "a" };

                using (ObjectScope.New(entity, connection: connection))
                {
                    entity.Name = "b";
                }

                Assert.AreEqual(0, ObjectScope.ScopeCount);
                Assert.IsNull(ObjectScope.Current);
            }
        }

        [TestMethod]
        public void Dispose_DoesNotMaskExceptionThrownInScopeBody()
        {
            using (var connection = new OpenConnection())
            {
                var entity = new Parent { Id = 1, Name = "a" };

                var exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
                {
                    using (ObjectScope.New(entity, connection: connection))
                    {
                        throw new InvalidOperationException("body");
                    }
                });

                Assert.AreEqual("body", exception.Message);
                Assert.AreEqual(0, ObjectScope.ScopeCount);
            }
        }

        [TestMethod]
        public void Dispose_OutOfOrder_RemovesOnlyTheDisposedScope()
        {
            using (var connection = new OpenConnection())
            {
                var outerEntity = new Parent { Id = 1 };
                var innerEntity = new Parent { Id = 2 };

                var outer = ObjectScope.New(outerEntity, connection: connection);
                var inner = ObjectScope.New(innerEntity, connection: connection);

                outer.Dispose();

                Assert.AreEqual(1, ObjectScope.ScopeCount);
                Assert.AreSame(inner, ObjectScope.Current);

                inner.Dispose();

                Assert.AreEqual(0, ObjectScope.ScopeCount);
            }
        }

        [TestMethod]
        public void Dispose_MiddleScope_PreservesOrderOfRemainingScopes()
        {
            using (var connection = new OpenConnection())
            {
                var outer = ObjectScope.New(new Parent { Id = 1 }, connection: connection);
                var middle = ObjectScope.New(new Parent { Id = 2 }, connection: connection);
                var inner = ObjectScope.New(new Parent { Id = 3 }, connection: connection);

                middle.Dispose();

                CollectionAssert.AreEqual(new[] { inner, outer }, ObjectScope.ScopeArray);

                inner.Dispose();
                Assert.AreSame(outer, ObjectScope.Current);

                outer.Dispose();
                Assert.AreEqual(0, ObjectScope.ScopeCount);
            }
        }

        [TestMethod]
        public void Dispose_CalledTwice_RemovesScopeOnce()
        {
            using (var connection = new OpenConnection())
            {
                var outer = ObjectScope.New(new Parent { Id = 1 }, connection: connection);
                var inner = ObjectScope.New(new Parent { Id = 2 }, connection: connection);

                inner.Dispose();
                inner.Dispose();

                Assert.AreEqual(1, ObjectScope.ScopeCount);
                Assert.AreSame(outer, ObjectScope.Current);

                outer.Dispose();
            }
        }

        [TestMethod]
        public void CompareObjects_TwoNewChildrenWithUnassignedKeys_ProducesTwoInsertNodes()
        {
            using (var connection = new OpenConnection())
            {
                var entity = new Parent
                {
                    Id = 1,
                    Name = "a",
                    Children = new List<Child> { new Child { Id = 1, ParentId = 1, Name = "one" } }
                };

                using (ObjectScope.New(entity, connection: connection))
                {
                    entity.Children.Add(new Child { ParentId = 1, Name = "new one" });
                    entity.Children.Add(new Child { ParentId = 1, Name = "new two" });

                    var changes = ObjectScopeExtensions.CompareObjects(entity, entity.Old());

                    var childNodes = ChildNodes(changes, nameof(Parent.Children));
                    Assert.AreEqual(2, childNodes.Count(n => n.ObjectState == ObjectState.New));
                    Assert.AreEqual(0, childNodes.Count(n => n.ObjectState == ObjectState.Deleted));
                }
            }
        }

        [TestMethod]
        public void CompareObjects_ChildCollectionWithoutPrimaryKey_DoesNotThrow()
        {
            using (var connection = new OpenConnection())
            {
                var entity = new Parent
                {
                    Id = 1,
                    Keyless = new List<Keyless> { new Keyless { Name = "one" }, new Keyless { Name = "two" } }
                };

                using (ObjectScope.New(entity, connection: connection))
                {
                    entity.Keyless.Add(new Keyless { Name = "three" });

                    var changes = ObjectScopeExtensions.CompareObjects(entity, entity.Old());

                    var childNodes = ChildNodes(changes, nameof(Parent.Keyless));
                    Assert.AreEqual(1, childNodes.Count(n => n.ObjectState == ObjectState.New));
                }
            }
        }

        [TestMethod]
        public void CompareObjects_ChildCollectionChanges_AreClassifiedByPrimaryKey()
        {
            using (var connection = new OpenConnection())
            {
                var entity = new Parent
                {
                    Id = 1,
                    Children = new List<Child>
                    {
                        new Child { Id = 1, ParentId = 1, Name = "one" },
                        new Child { Id = 2, ParentId = 1, Name = "two" }
                    }
                };

                using (ObjectScope.New(entity, connection: connection))
                {
                    entity.Children[0].Name = "one modified";
                    entity.Children.RemoveAt(1);
                    entity.Children.Add(new Child { Id = 3, ParentId = 1, Name = "three" });

                    var changes = ObjectScopeExtensions.CompareObjects(entity, entity.Old());

                    var childNodes = ChildNodes(changes, nameof(Parent.Children));
                    Assert.AreEqual(1, childNodes.Count(n => n.ObjectState == ObjectState.Dirty));
                    Assert.AreEqual(1, childNodes.Count(n => n.ObjectState == ObjectState.New));
                    Assert.AreEqual(1, childNodes.Count(n => n.ObjectState == ObjectState.Deleted));
                }
            }
        }

        [TestMethod]
        public void CompareObjects_NullChildEntries_AreIgnored()
        {
            using (var connection = new OpenConnection())
            {
                var entity = new Parent
                {
                    Id = 1,
                    Children = new List<Child> { new Child { Id = 1, ParentId = 1, Name = "one" } }
                };

                using (ObjectScope.New(entity, connection: connection))
                {
                    entity.Children.Add(null);

                    var changes = ObjectScopeExtensions.CompareObjects(entity, entity.Old());

                    var childNodes = ChildNodes(changes, nameof(Parent.Children));
                    Assert.AreEqual(0, childNodes.Count(n => n.ObjectState == ObjectState.New));
                    Assert.AreEqual(0, childNodes.Count(n => n.ObjectState == ObjectState.Deleted));
                }
            }
        }

        private static List<ChangeNode> ChildNodes(ChangeNode root, string propertyName)
        {
            var listNode = root.Nodes.FirstOrDefault(n => n.PropertyName == propertyName);
            return listNode == null ? new List<ChangeNode>() : listNode.Nodes.ToList();
        }
    }
}
