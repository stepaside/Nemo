using System;

namespace Nemo.Attributes.Converters
{
	/// <summary>Composite converter, allows to compound two converters together.</summary>
	/// <typeparam name="C1"></typeparam>
	/// <typeparam name="C2"></typeparam>
	/// <typeparam name="F"></typeparam>
	/// <typeparam name="I"></typeparam>
	/// <typeparam name="T"></typeparam>
	public class CompositeConverter<C1, C2, F, I, T> : ITypeConverter<F, T>
		where C1 : ITypeConverter<F, I>, new()
		where C2 : ITypeConverter<I, T>, new()
	{
		private readonly ITypeConverter<F, I> converter1 = new C1();
		private readonly ITypeConverter<I, T> converter2 = new C2();

		#region ITypeConverter<F,T> Members
		T ITypeConverter<F, T>.ConvertForward(F from)
		{
			I intermediate = converter1.ConvertForward(from);

			return converter2.ConvertForward(intermediate);
		}

		F ITypeConverter<F, T>.ConvertBackward(T to)
		{
			I intermediate = converter2.ConvertBackward(to);

			return converter1.ConvertBackward(intermediate);
		}
		#endregion
	}
}