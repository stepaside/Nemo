using System;
using System.Reflection;

namespace Nemo.Attributes
{
    public abstract class MapAttribute : Attribute
	{
		private readonly string _sourceName;

		public MapAttribute(string sourceName)
		{
			this._sourceName = sourceName;
		}

		public string SourceName
		{
			get { return _sourceName; }
		}
	}
}