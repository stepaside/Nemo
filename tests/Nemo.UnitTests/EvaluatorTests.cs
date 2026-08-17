using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nemo.Linq.Expressions;
using System;
using System.Linq.Expressions;

namespace Nemo.UnitTests
{
    [TestClass]
    public class EvaluatorTests
    {
        private class Holder
        {
            public int Field;
            public string Property { get; set; }
            public Holder Nested { get; set; }
            public static int StaticProperty => 42;
        }

        [TestMethod]
        public void TryEvaluateFast_Constant_ReturnsValue()
        {
            Assert.IsTrue(Evaluator.TryEvaluateFast(Expression.Constant(5), out var value));
            Assert.AreEqual(5, value);
        }

        [TestMethod]
        public void TryEvaluateFast_CapturedField_ReturnsValue()
        {
            var captured = 17;
            Expression<Func<int>> lambda = () => captured;

            Assert.IsTrue(Evaluator.TryEvaluateFast(lambda.Body, out var value));
            Assert.AreEqual(17, value);
        }

        [TestMethod]
        public void TryEvaluateFast_NestedMemberChain_ReturnsValue()
        {
            var holder = new Holder { Nested = new Holder { Property = "abc", Field = 3 } };
            Expression<Func<string>> lambda = () => holder.Nested.Property;

            Assert.IsTrue(Evaluator.TryEvaluateFast(lambda.Body, out var value));
            Assert.AreEqual("abc", value);
        }

        [TestMethod]
        public void TryEvaluateFast_StaticProperty_ReturnsValue()
        {
            Expression<Func<int>> lambda = () => Holder.StaticProperty;

            Assert.IsTrue(Evaluator.TryEvaluateFast(lambda.Body, out var value));
            Assert.AreEqual(42, value);
        }

        [TestMethod]
        public void TryEvaluateFast_InstancePropertyOnNullInstance_ReturnsFalse()
        {
            Holder holder = null;
            Expression<Func<string>> lambda = () => holder.Property;
            Assert.IsFalse(Evaluator.TryEvaluateFast(lambda.Body, out _));
        }

        [TestMethod]
        public void TryEvaluateFast_MethodCall_ReturnsFalse()
        {
            Expression<Func<int>> lambda = () => Math.Abs(-1);

            Assert.IsFalse(Evaluator.TryEvaluateFast(lambda.Body, out _));
        }

        [TestMethod]
        public void TryEvaluateFast_BoxingConvert_ReturnsValue()
        {
            var captured = 9;
            Expression<Func<object>> lambda = () => captured;

            Assert.IsTrue(Evaluator.TryEvaluateFast(lambda.Body, out var value));
            Assert.AreEqual(9, value);
        }

        [TestMethod]
        public void TryEvaluateFast_NullableValue_ReturnsUnderlyingValue()
        {
            DateTime? cutoff = new DateTime(2020, 1, 2);
            Expression<Func<DateTime>> lambda = () => cutoff.Value;

            Assert.IsTrue(Evaluator.TryEvaluateFast(lambda.Body, out var value));
            Assert.AreEqual(new DateTime(2020, 1, 2), value);
        }

        [TestMethod]
        public void TryEvaluateFast_NullableHasValue_ReturnsFlag()
        {
            int? present = 5;
            int? missing = null;
            Expression<Func<bool>> presentLambda = () => present.HasValue;
            Expression<Func<bool>> missingLambda = () => missing.HasValue;

            Assert.IsTrue(Evaluator.TryEvaluateFast(presentLambda.Body, out var presentValue));
            Assert.AreEqual(true, presentValue);
            Assert.IsTrue(Evaluator.TryEvaluateFast(missingLambda.Body, out var missingValue));
            Assert.AreEqual(false, missingValue);
        }

        [TestMethod]
        public void TryEvaluateFast_NullableValueOnEmpty_ReturnsFalse()
        {
            int? missing = null;
            Expression<Func<int>> lambda = () => missing.Value;

            Assert.IsFalse(Evaluator.TryEvaluateFast(lambda.Body, out _));
        }

        [TestMethod]
        public void PartialEval_CapturedNullableValue_BecomesConstant()
        {
            int? threshold = 10;
            Expression<Func<int, bool>> lambda = x => x > threshold.Value;

            var evaluated = (Expression<Func<int, bool>>)Evaluator.PartialEval(lambda);
            var binary = (BinaryExpression)evaluated.Body;

            Assert.AreEqual(ExpressionType.Constant, binary.Right.NodeType);
            Assert.AreEqual(10, ((ConstantExpression)binary.Right).Value);
        }

        [TestMethod]
        public void PartialEval_CapturedValues_BecomeConstants()
        {
            var threshold = 10;
            Expression<Func<int, bool>> lambda = x => x > threshold;

            var evaluated = (Expression<Func<int, bool>>)Evaluator.PartialEval(lambda);
            var binary = (BinaryExpression)evaluated.Body;

            Assert.AreEqual(ExpressionType.Constant, binary.Right.NodeType);
            Assert.AreEqual(10, ((ConstantExpression)binary.Right).Value);
        }

        [TestMethod]
        public void PartialEval_MethodCallOverCapturedValues_BecomesConstant()
        {
            var text = "abc";
            Expression<Func<string, bool>> lambda = x => x == text.ToUpper();

            var evaluated = (Expression<Func<string, bool>>)Evaluator.PartialEval(lambda);
            var binary = (BinaryExpression)evaluated.Body;

            Assert.AreEqual(ExpressionType.Constant, binary.Right.NodeType);
            Assert.AreEqual("ABC", ((ConstantExpression)binary.Right).Value);
        }
    }
}
