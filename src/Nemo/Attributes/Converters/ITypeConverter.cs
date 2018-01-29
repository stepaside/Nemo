using System;

namespace Nemo.Attributes.Converters
{
	/// <summary>Represents an type converter, able to convert a type to another and back.</summary>
	/// <typeparam name="F"></typeparam>
	/// <typeparam name="T"></typeparam>
	public interface ITypeConverter<F, T>
	{
		/// <summary>
		/// Converts an instance of type <typeparamref name="F"/> to an instance of type
		/// <typeparamref name="T"/>.
		/// </summary>
		/// <param name="from"></param>
		/// <returns></returns>
		T ConvertForward(F from);

		/// <summary>
		/// Converts an instance of type <typeparamref name="T"/> back to an instance of type
		/// <typeparamref name="F"/>.
		/// </summary>
		/// <param name="to"></param>
		/// <returns></returns>
		F ConvertBackward(T to);
	}
}