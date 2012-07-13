using System;

namespace Nemo.Attributes.Converters
{
	/// <summary>Exception thrown when the emitter isn't able to generate a type.</summary>
	public class TypeConverterException : ApplicationException
	{
		/// <summary>Constructor of an emitter exception.</summary>
		/// <param name="message"></param>
		public TypeConverterException(string message) : base(message)
		{
		}
	}
}