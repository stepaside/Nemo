﻿/**
 * This is based on ExpressionVisitor implementation from ServiceStack.OrmLite
 * https://github.com/ServiceStack/ServiceStack.OrmLite
 * **/

using Nemo.Extensions;
using Nemo.Reflection;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Nemo.Data
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class PredicateVisitor
    {
        public static string Visit<T>(Expression exp, DialectProvider dialect, string alias)
        {
            if (exp == null) return string.Empty;
            switch (exp.NodeType)
            {
                case ExpressionType.Lambda:
                    return VisitLambda<T>(exp as LambdaExpression, dialect, alias);
                case ExpressionType.MemberAccess:
                    return VisitMemberAccess<T>(exp as MemberExpression, dialect, alias);
                case ExpressionType.Constant:
                    return VisitConstant(exp as ConstantExpression, dialect, alias);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                    return "(" + VisitBinary<T>(exp as BinaryExpression, dialect, alias) + ")";
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                    return VisitUnary<T>(exp as UnaryExpression, dialect, alias);
                case ExpressionType.Parameter:
                    return VisitParameter(exp as ParameterExpression, dialect, alias);
                case ExpressionType.Call:
                    return VisitMethodCall<T>(exp as MethodCallExpression, dialect, alias);
                case ExpressionType.New:
                    return VisitNew<T>(exp as NewExpression, dialect, alias);
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    return VisitNewArray<T>(exp as NewArrayExpression, dialect, alias);
                default:
                    return exp.ToString();
            }
        }

        private static string VisitLambda<T>(LambdaExpression lambda, DialectProvider dialect, string alias)
        {
            if (lambda.Body.NodeType == ExpressionType.MemberAccess)
            {
                var m = lambda.Body as MemberExpression;

                if (m.Expression != null)
                {
                    var r = VisitMemberAccess<T>(m, dialect, alias);
                    return $"{r}={GetQuotedTrueValue()}";
                }
            }
            return Visit<T>(lambda.Body, dialect, alias);
        }

        private static string VisitBinary<T>(BinaryExpression b, DialectProvider dialect, string alias)
        {
            string left, right;
            var operand = BindOperant(b.NodeType);
            if (operand == "AND" || operand == "OR")
            {
                var m = b.Left as MemberExpression;
                if (m != null && m.Expression != null)
                {
                    string r = VisitMemberAccess<T>(m, dialect, alias);
                    left = $"{r}={GetQuotedTrueValue()}";
                }
                else
                {
                    left = Visit<T>(b.Left, dialect, alias);
                }
                m = b.Right as MemberExpression;
                if (m != null && m.Expression != null)
                {
                    string r = VisitMemberAccess<T>(m, dialect, alias);
                    right = $"{r}={GetQuotedTrueValue()}";
                }
                else
                {
                    right = Visit<T>(b.Right, dialect, alias);
                }
            }
            else
            {
                left = Visit<T>(b.Left, dialect, alias);
                right = Visit<T>(b.Right, dialect, alias);
            }

            if (operand == "=" && right == "null") operand = "is";
            else if (operand == "<>" && right == "null") operand = "is not";
            else if (operand == "=" || operand == "<>")
            {
                if (IsTrueExpression(right)) right = GetQuotedTrueValue();
                else if (IsFalseExpression(right)) right = GetQuotedFalseValue();

                if (IsTrueExpression(left)) left = GetQuotedTrueValue();
                else if (IsFalseExpression(left)) left = GetQuotedFalseValue();

            }

            switch (operand)
            {
                case "MOD":
                case "COALESCE":
                    return $"{operand}({left},{right})";
                default:
                    return left + " " + operand + " " + right;
            }
        }

        private static string VisitMemberAccess<T>(MemberExpression m, DialectProvider dialect, string alias)
        {
            Type elementType;
            if (m.Expression != null && m.Expression.Type == typeof(T))
            {
                var parentPropertyMap = Reflector.GetPropertyMap(typeof(T));
                ReflectedProperty property;
                parentPropertyMap.TryGetValue((PropertyInfo)m.Member, out property);
                var columnName = property != null ? property.MappedColumnName : m.Member.Name;
                return (alias != null ? alias + "." : "") + dialect.IdentifierEscapeStartCharacter + columnName + dialect.IdentifierEscapeEndCharacter;
            }

            if (m.Expression != null && Reflector.IsDataEntityList(m.Expression.Type, out elementType) && m.Member.Name == "Count")
            {
                var parentTable = ObjectFactory.GetTableName(typeof(T));
                var childTable = ObjectFactory.GetTableName(elementType);

                var parentPropertyMap = Reflector.GetPropertyMap(typeof(T));
                var whereClause =
                    parentPropertyMap.Where(p => p.Value.IsPrimaryKey)
                        .Select(p => $"{alias ?? (dialect.IdentifierEscapeStartCharacter + parentTable + dialect.IdentifierEscapeEndCharacter)}.{dialect.IdentifierEscapeStartCharacter}{p.Value.MappedColumnName}{dialect.IdentifierEscapeEndCharacter} = {dialect.IdentifierEscapeStartCharacter}{childTable}{dialect.IdentifierEscapeEndCharacter}.{dialect.IdentifierEscapeStartCharacter}{p.Value.MappedColumnName}{dialect.IdentifierEscapeEndCharacter}")
                        .ToDelimitedString(" AND ");

                return $"(SELECT COUNT(*) FROM {dialect.IdentifierEscapeStartCharacter}{childTable}{dialect.IdentifierEscapeEndCharacter} WHERE {whereClause})";
            }

            var member = Expression.Convert(m, typeof(object));
            var lambda = Expression.Lambda<Func<object>>(member);
            var getter = lambda.Compile();
            var o = getter();
            return GetQuotedValue(o, o.GetType());
        }

        private static string VisitNew<T>(NewExpression nex, DialectProvider dialect, string alias)
        {
            // TODO : check !
            var member = Expression.Convert(nex, typeof(object));
            var lambda = Expression.Lambda<Func<object>>(member);
            try
            {
                var getter = lambda.Compile();
                var o = getter();
                return GetQuotedValue(o, o.GetType());
            }
            catch (InvalidOperationException)
            { 
                var exprs = VisitExpressionList<T>(nex.Arguments, dialect, alias);
                var r = new StringBuilder();
                foreach (var e in exprs)
                {
                    r.AppendFormat("{0}{1}", r.Length > 0 ? "," : "", e);
                }
                return r.ToString();
            }
        }

        private static string VisitParameter(ParameterExpression p, DialectProvider dialect, string alias)
        {
            return p.Name;
        }

        private static string VisitConstant(ConstantExpression c, DialectProvider dialect, string alias)
        {
            if (c.Value == null)
            {
                return "null";
            }
            if (c.Value is bool value)
            {
                return value ? "(1=1)" : "(1=0)";
            }
            return GetQuotedValue(c.Value, c.Value.GetType());
        }

        private static string VisitUnary<T>(UnaryExpression u, DialectProvider dialect, string alias)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    var o = Visit<T>(u.Operand, dialect, alias);
                    if (u.Operand.NodeType == ExpressionType.MemberAccess && u.Operand.Type == typeof(bool))
                    {
                        return $"NOT ({o}={GetQuotedTrueValue()})";
                    }
                    else
                    {
                        return $"NOT ({o})";
                    }
                default:
                    return Visit<T>(u.Operand, dialect, alias);
            }
        }

        private static string VisitMethodCall<T>(MethodCallExpression m, DialectProvider dialect, string alias)
        {
            var args = VisitExpressionList<T>(m.Arguments, dialect, alias);

            object r;
            if (m.Object != null)
            {
                r = Visit<T>(m.Object, dialect, alias);
            }
            else
            {
                r = args[0];
                args.RemoveAt(0);
            }

            switch (m.Method.Name)
            {
                case "ToUpper":
                    return $"upper({r})";
                case "ToLower":
                    return $"lower({r})";
                case "StartsWith":
                    return $"upper({r}) like '{RemoveQuote(args[0].ToString()).ToUpper()}%' ";
                case "EndsWith":
                    return $"upper({r}) like '%{RemoveQuote(args[0].ToString()).ToUpper()}'";
                case "Contains":
                    return $"upper({r}) like '%{RemoveQuote(args[0].ToString()).ToUpper()}%'";
                case "Substring":
                    var startIndex = int.Parse(args[0].ToString()) + 1;
                    if (args.Count == 2)
                    {
                        var length = int.Parse(args[1].ToString());
                        return dialect.SubstringFunction + $"({r}, {startIndex}, {length})";
                    }
                    return dialect.SubstringFunction + $"({r}, {startIndex})";
                case "Round":
                case "Floor":
                case "Ceiling":
                case "Coalesce":
                case "Abs":
                case "Sum":
                    return $"{m.Method.Name}({r}{(args.Count == 1 ? string.Format(",{0}", args[0]) : "")})";
                case "Concat":
                    if (dialect.StringConcatenationFunction != null)
                    {
                        var s = new StringBuilder();
                        s.Append(dialect.StringConcatenationFunction);
                        s.Append("(");
                        for (var i = 0; i < args.Count; i++)
                        {
                            s.AppendFormat("{0}", args[i]);
                            if (i != args.Count - 1)
                            {
                                s.AppendFormat(",");
                            }
                        }
                        s.Append(")");
                        return $"{r}{s}";

                    }
                    else
                    {
                        var s = new StringBuilder();
                        foreach (var e in args)
                        {
                            s.AppendFormat(" {0} {1}", dialect.StringConcatenationOperator, e);
                        }
                        return $"{r}{s}";
                    }
                case "In":

                    var member = Expression.Convert(m.Arguments[1], typeof(object));
                    var lambda = Expression.Lambda<Func<object>>(member);
                    var getter = lambda.Compile();

                    var inArgs = getter() as object[];

                    var sIn = new StringBuilder();
                    foreach (var e in inArgs)
                    {
                        if (e.GetType().ToString() != "System.Collections.Generic.List`1[System.Object]")
                        {
                            sIn.AppendFormat("{0}{1}", sIn.Length > 0 ? "," : "", GetQuotedValue(e, e.GetType()));
                        }
                        else
                        {
                            var listArgs = e as IList<object>;
                            foreach (var el in listArgs)
                            {
                                sIn.AppendFormat("{0}{1}", sIn.Length > 0 ? "," : "", GetQuotedValue(el, el.GetType()));
                            }
                        }
                    }

                    return $"{r} {m.Method.Name} ({sIn})";
                case "Desc":
                    return $"{r} DESC";
                case "As":
                    return $"{r} As [{RemoveQuote(args[0].ToString())}]";
                case "ToString":
                    return r.ToString();
                default:
                    var s2 = new StringBuilder();
                    foreach (var e in args)
                    {
                        s2.AppendFormat(",{0}", GetQuotedValue(e, e.GetType()));
                    }
                    return $"{m.Method.Name}({r}{s2})";
            }
        }

        private static List<Object> VisitExpressionList<T>(ReadOnlyCollection<Expression> original, DialectProvider dialect, string alias)
        {
            var list = new List<Object>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                if (original[i].NodeType == ExpressionType.NewArrayInit || original[i].NodeType == ExpressionType.NewArrayBounds)
                {
                    list.AddRange(VisitNewArrayFromExpressionList<T>(original[i] as NewArrayExpression, dialect, alias));
                }
                else
                {
                    list.Add(Visit<T>(original[i], dialect, alias));
                }
            }
            return list;
        }

        private static string VisitNewArray<T>(NewArrayExpression na, DialectProvider dialect, string alias)
        {
            var exprs = VisitExpressionList<T>(na.Expressions, dialect, alias);
            var r = new StringBuilder();
            foreach (var e in exprs)
            {
                r.Append(r.Length > 0 ? "," + e : e);
            }
            return r.ToString();
        }

        private static IEnumerable<object> VisitNewArrayFromExpressionList<T>(NewArrayExpression na, DialectProvider dialect, string alias)
        {
            var exprs = VisitExpressionList<T>(na.Expressions, dialect, alias);
            return exprs;
        }

        private static string BindOperant(ExpressionType e)
        {
            switch (e)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.OrElse:
                    return "OR";
                case ExpressionType.Add:
                    return "+";
                case ExpressionType.Subtract:
                    return "-";
                case ExpressionType.Multiply:
                    return "*";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Modulo:
                    return "MOD";
                case ExpressionType.Coalesce:
                    return "COALESCE";
                default:
                    return e.ToString();
            }
        }

        private static string RemoveQuote(string exp)
        {
            if (exp.StartsWith("'"))
            {
                exp = exp.Remove(0, 1);
            }
            if (exp.EndsWith("'"))
            {
                exp = exp.Remove(exp.Length - 1, 1);
            }
            return exp;
        }

        private static string GetTrueExpression()
        {
            var o = GetQuotedTrueValue();
            return $"({o}={o})";
        }

        private static string GetFalseExpression()
        {
            return $"({GetQuotedTrueValue()}={GetQuotedFalseValue()})";
        }

        private static bool IsTrueExpression(string exp)
        {
            return (exp == GetTrueExpression());
        }

        private static bool IsFalseExpression(string exp)
        {
            return (exp == GetFalseExpression());
        }

        private static string GetQuotedTrueValue()
        {
            return GetQuotedValue(true, typeof(bool));
        }

        private static string GetQuotedFalseValue()
        {
            return GetQuotedValue(false, typeof(bool));
        }

        private static string GetQuotedValue(object value, Type fieldType)
        {
            if (value == null) return "NULL";

            if (!fieldType.UnderlyingSystemType.IsValueType && fieldType != typeof(string))
            {
                return "'" + EscapeParam(value.SafeCast<string>()) + "'";
            }

            if (fieldType == typeof(float))
            {
                return ((float)value).ToString(CultureInfo.InvariantCulture);
            }

            if (fieldType == typeof(double))
            {
                return ((double)value).ToString(CultureInfo.InvariantCulture);
            }

            if (fieldType == typeof(decimal))
            {
                return ((decimal)value).ToString(CultureInfo.InvariantCulture);
            }

            if (fieldType == typeof(bool))
            {
                return $"'{value}'";
            }

            return !Reflector.IsNumeric(fieldType) ? "'" + EscapeParam(value) + "'" : value.ToString();
        }

        private static string EscapeParam(object paramValue)
        {
            return paramValue.ToString().Replace("'", "''");
        }
    }
}
