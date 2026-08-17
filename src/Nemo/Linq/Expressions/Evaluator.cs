using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Nemo.Linq.Expressions
{
    /// <summary>
    /// Enables the partial evaluation of queries.
    /// </summary>
    /// <remarks>
    /// From http://msdn.microsoft.com/en-us/library/bb546158.aspx
    /// Copyright notice http://msdn.microsoft.com/en-gb/cc300389.aspx#O
    /// </remarks>
    internal static class Evaluator
    {
        /// <summary>
        /// Performs evaluation & replacement of independent sub-trees
        /// </summary>
        /// <param name="expression">The root of the expression tree.</param>
        /// <param name="fnCanBeEvaluated">A function that decides whether a given expression node can be part of the local function.</param>
        /// <returns>A new tree with sub-trees evaluated and replaced.</returns>
        public static Expression PartialEval(Expression expression, Func<Expression, bool> fnCanBeEvaluated)
        {
            return new SubtreeEvaluator(new Nominator(fnCanBeEvaluated).Nominate(expression)).Eval(expression);
        }

        /// <summary>
        /// Performs evaluation & replacement of independent sub-trees
        /// </summary>
        /// <param name="expression">The root of the expression tree.</param>
        /// <returns>A new tree with sub-trees evaluated and replaced.</returns>
        public static Expression PartialEval(Expression expression)
        {
            return PartialEval(expression, CanBeEvaluatedLocally);
        }

        private static bool CanBeEvaluatedLocally(Expression expression)
        {
            return expression.NodeType != ExpressionType.Parameter;
        }

        /// <summary>
        /// Evaluates & replaces sub-trees when first candidate is reached (top-down)
        /// </summary>
        private class SubtreeEvaluator : ExpressionVisitor
        {
            private readonly HashSet<Expression> _candidates;

            internal SubtreeEvaluator(HashSet<Expression> candidates)
            {
                _candidates = candidates;
            }

            internal Expression Eval(Expression exp)
            {
                return Visit(exp);
            }

            public override Expression Visit(Expression exp)
            {
                if (exp == null)
                {
                    return null;
                }
                if (_candidates.Contains(exp))
                {
                    return Evaluate(exp);
                }
                return base.Visit(exp);
            }

            private static Expression Evaluate(Expression e)
            {
                if (e.NodeType == ExpressionType.Constant)
                {
                    return e;
                }
                if (TryEvaluateFast(e, out var value))
                {
                    return Expression.Constant(value, e.Type);
                }
                var lambda = Expression.Lambda<Func<object>>(Expression.Convert(e, typeof(object)));
                return Expression.Constant(lambda.Compile()(), e.Type);
            }
        }

        /// <summary>
        /// Evaluates constant and captured-member sub-trees without compiling a lambda.
        /// </summary>
        internal static bool TryEvaluateFast(Expression expression, out object value)
        {
            switch (expression)
            {
                case ConstantExpression constant:
                    value = constant.Value;
                    return true;

                case MemberExpression member:
                    object instance = null;
                    if (member.Expression != null && !TryEvaluateFast(member.Expression, out instance))
                    {
                        value = null;
                        return false;
                    }
                    switch (member.Member)
                    {
                        case FieldInfo field:
                            value = field.GetValue(instance);
                            return true;
                        case PropertyInfo property when property.GetIndexParameters().Length == 0 && (property.GetMethod?.IsStatic == true || instance != null):
                            value = property.GetValue(instance);
                            return true;
                    }
                    value = null;
                    return false;

                case UnaryExpression unary when unary.NodeType == ExpressionType.Convert && unary.Method == null && unary.Type.IsAssignableFrom(unary.Operand.Type):
                    return TryEvaluateFast(unary.Operand, out value);

                default:
                    value = null;
                    return false;
            }
        }

        /// <summary>
        /// Performs bottom-up analysis to determine which nodes can possibly
        /// be part of an evaluated sub-tree.
        /// </summary>
        class Nominator : ExpressionVisitor
        {
            readonly Func<Expression, bool> _fnCanBeEvaluated;
            HashSet<Expression> _candidates;
            bool _cannotBeEvaluated;

            internal Nominator(Func<Expression, bool> fnCanBeEvaluated)
            {
                _fnCanBeEvaluated = fnCanBeEvaluated;
            }

            internal HashSet<Expression> Nominate(Expression expression)
            {
                _candidates = new HashSet<Expression>();
                Visit(expression);
                return _candidates;
            }

            public override Expression Visit(Expression expression)
            {
                if (expression == null) return null;
                var saveCannotBeEvaluated = _cannotBeEvaluated;
                _cannotBeEvaluated = false;
                base.Visit(expression);
                if (!_cannotBeEvaluated)
                {
                    if (_fnCanBeEvaluated(expression))
                    {
                        _candidates.Add(expression);
                    }
                    else
                    {
                        _cannotBeEvaluated = true;
                    }
                }
                _cannotBeEvaluated |= saveCannotBeEvaluated;
                return expression;
            }
        }
    }
}
