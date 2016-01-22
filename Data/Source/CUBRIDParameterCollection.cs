/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// This class implements a collection of parameters tied to a <see cref="CUBRIDCommand"/>.
	/// </summary>
	public sealed class CUBRIDParameterCollection : DbParameterCollection
	{
		/*
		http://msdn.microsoft.com/en-us/library/yy6y35y8%28v=vs.80%29.aspx
		The syntax for parameter placeholders depends on the data source. 
		The .NET Framework data providers handle naming and specifying parameters and parameter placeholders differently. 
		This syntax is tailored to a specific data source, as described in the following table.

		SqlClient
			Uses named parameters in the format @parametername.

		OleDb
			Uses positional parameter markers indicated by a question mark (?).

		Odbc
			Uses positional parameter markers indicated by a question mark (?).

		OracleClient
			Uses named parameters in the format :parmname (or parmname).

		TODO - Confirm below:
		CUBRIDClient
			Uses named parameters in the format ?parmname
		  or
			Uses positional parameter markers indicated by a question mark (?). These are not supported by thsi class - CUBRIDParameterCollection.
		*/

		private List<CUBRIDParameter> paramList = new List<CUBRIDParameter>();
        private Encoding parametersEncoding = Encoding.Default;

        /// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameterCollection"/> class.
		/// </summary>
		public CUBRIDParameterCollection()
		{
			Clear();
		}

		/// <summary>
		/// Removes all <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> values from the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameterCollection"/>.
		/// </summary>
		public override void Clear()
		{
			this.paramList.Clear();
		}

        /// <summary>
        /// Sets the collection's ebcoding.
        /// </summary>
        public void SetParametersEncoding(Encoding encoding)
        {
            this.parametersEncoding = encoding;
        }

        /// <summary>
        /// Gets the collection.
        /// </summary>
        /// <returns>
        /// The collection's encoding.
        ///   </returns>
        public Encoding GetParametersEncoding()
        {
            return this.parametersEncoding;
        }

		/// <summary>
		/// Returns the number of items in the collection.
		/// </summary>
		/// <returns>
		/// The number of items in the collection.
		///   </returns>
		public override int Count
		{
			get { return paramList.Count; }
		}

		/// <summary>
		/// Specifies whether the collection is a fixed size.
		/// </summary>
		/// <returns>true if the collection is a fixed size; otherwise false.
		///   </returns>
		public override bool IsFixedSize
		{
			//get { return (this.paramList as IList).IsFixedSize; }
			get { return false; }
		}

		/// <summary>
		/// Specifies whether the collection is read-only.
		/// </summary>
		/// <returns>true if the collection is read-only; otherwise false.
		///   </returns>
		public override bool IsReadOnly
		{
			//get { return (this.paramList as IList).IsReadOnly; }
			get { return false; }
		}

		/// <summary>
		/// Specifies whether the collection is synchronized.
		/// </summary>
		/// <returns>true if the collection is synchronized; otherwise false.
		///   </returns>
		public override bool IsSynchronized
		{
			get { return (this.paramList as IList).IsSynchronized; }
		}

		/// <summary>
		/// Specifies the <see cref="T:System.Object"/> to be used to synchronize access to the collection.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.Object"/> to be used to synchronize access to the <see cref="T:System.Data.Common.DbParameterCollection"/>.
		///   </returns>
		public override object SyncRoot
		{
			get { return (this.paramList as IList).SyncRoot; }
		}

		/// <summary>
		/// Validates the index value.
		/// </summary>
		/// <param name="index">The index value.</param>
		private void ValidateIndex(int index)
		{
			if (index < 0 || index >= this.Count)
				throw new IndexOutOfRangeException();
		}

		/// <summary>
		/// Gets and sets the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> with the specified name.
		/// </summary>
		/// <returns>
		/// The <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> with the specified name.
		///   </returns>
		///   
		/// <exception cref="T:System.IndexOutOfRangeException">
		/// The specified index does not exist.
		///   </exception>
		public new CUBRIDParameter this[int index]
		{
			get { return (CUBRIDParameter)GetParameter(index); }
			set { SetParameter(index, value); }
		}

		/// <summary>
		/// Gets and sets the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> with the specified name.
		/// </summary>
		/// <returns>
		/// The <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> with the specified name.
		///   </returns>
		///   
		/// <exception cref="T:System.IndexOutOfRangeException">
		/// The specified index does not exist.
		///   </exception>
		public new CUBRIDParameter this[string name]
		{
			get { return (CUBRIDParameter)GetParameter(name); }
			set { SetParameter(name, value); }
		}

		/// <summary>
		/// Returns the <see cref="T:System.Data.Common.DbParameter"/> object at the specified index in the collection.
		/// </summary>
		/// <param name="index">The index of the <see cref="T:System.Data.Common.DbParameter"/> in the collection.</param>
		/// <returns>
		/// The <see cref="T:System.Data.Common.DbParameter"/> object at the specified index in the collection.
		/// </returns>
		protected override DbParameter GetParameter(int index)
		{
			ValidateIndex(index);

			return (DbParameter)paramList[index];
		}

		/// <summary>
		/// Returns <see cref="T:System.Data.Common.DbParameter"/> the object with the specified name.
		/// </summary>
		/// <param name="parameterName">The name of the <see cref="T:System.Data.Common.DbParameter"/> in the collection.</param>
		/// <returns>
		/// The <see cref="T:System.Data.Common.DbParameter"/> the object with the specified name.
		/// </returns>
		protected override DbParameter GetParameter(string parameterName)
		{
			int index = IndexOf(parameterName);
			if (index < 0)
			{
				//the parameters are stored without "?"...?
				if (parameterName.StartsWith("?"))
				{
					index = IndexOf(parameterName.Substring(1));
					if (index >= 0)
						return (DbParameter)paramList[index];
				}
				else
				{
					//User has forgot to prefix parameter name with '?'
					throw new ArgumentException(Utils.GetStr(MsgId.ParameterNotFoundMissingPrefix));
				}

				throw new ArgumentException(parameterName + ": " + Utils.GetStr(MsgId.ParameterNotFound));
			}

			return (DbParameter)paramList[index];
		}

		/// <summary>
		/// Adds a new "empty" parameter to the collection.
		/// </summary>
		public void Add()
		{
			paramList.Add(new CUBRIDParameter());
		}

		/// <summary>
		/// Adds a <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> item with the specified value to the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameterCollection"/>.
		/// </summary>
		/// <param name="value">The <see cref="P:CUBRID.Data.CUBRIDClient.CUBRIDParameter.Value"/> of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> to add to the collection.</param>
		/// <returns>
		/// The index of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object in the collection.
		/// </returns>
		public override int Add(object value)
		{
			CUBRIDParameter parameter = value as CUBRIDParameter;

			if (parameter == null)
				throw new ArgumentException(Utils.GetStr(MsgId.OnlyCUBRIDParameterObjectsAreValid));

			if (parameter.ParameterName == null || parameter.ParameterName == String.Empty)
				throw new ArgumentException(Utils.GetStr(MsgId.ParametersMustBeNamed));

			if (!parameter.ParameterName.StartsWith("?"))
				throw new ArgumentException(Utils.GetStr(MsgId.ParameterNameMustStartWith));

			if (paramList.Contains(parameter))
				throw new ArgumentException(Utils.GetStr(MsgId.ParameterAlreadyAdded));

            parameter.SetParameterEncoding(this.GetParametersEncoding());

			paramList.Add(parameter);

			return IndexOf(parameter);
		}

		/// <summary>
		/// Adds a parameter with the specified parameter name.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="dataType">CUBRID Data Type.</param>
		/// <returns></returns>
		public CUBRIDParameter Add(string parameterName, CUBRIDDataType dataType)
		{
			CUBRIDParameter parameter = new CUBRIDParameter(parameterName, dataType);
			int pos = Add(parameter);

			return paramList[pos];
		}

		/// <summary>
		/// Adds a parameter with the specified parameter name.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="dataType">CUBRID Data Type.</param>
		/// <param name="size">The size.</param>
		/// <returns></returns>
		public CUBRIDParameter Add(string parameterName, CUBRIDDataType dataType, int size)
		{
			CUBRIDParameter parameter = new CUBRIDParameter(parameterName, dataType, size);
			int pos = Add(parameter);

			return paramList[pos];
		}

		/// <summary>
		/// Adds an array of parameters with the specified values to the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameterCollection"/>.
		/// </summary>
		/// <param name="values">An array of values of type <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> to add to the collection.</param>
		public override void AddRange(Array values)
		{
			foreach (DbParameter p in values)
				Add(p);
		}

		/// <summary>
		/// Indicates whether a <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> with the specified <see cref="P:CUBRID.Data.CUBRIDClient.CUBRIDParameter.Value"/> is contained in the collection.
		/// </summary>
		/// <param name="value">The <see cref="P:CUBRID.Data.CUBRIDClient.CUBRIDParameter.Value"/> of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> to look for in the collection.</param>
		/// <returns>
		/// true if the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> is in the collection; otherwise false.
		/// </returns>
		public override bool Contains(object value)
		{
			CUBRIDParameter parameter = value as CUBRIDParameter;

			if (null == parameter)
				throw new ArgumentException(Utils.GetStr(MsgId.ArgumentMustBeCUBRIDParameter));

			return paramList.Contains(parameter);
		}

		/// <summary>
		/// Gets a value indicating whether there is a parameter in the collection has the specified name.
		/// </summary>
		/// <param name="parameterName">The name of the parameter.</param>
		/// <returns>
		/// true if the collection contains the parameter; otherwise, false.
		/// </returns>
		public override bool Contains(string parameterName)
		{
			return IndexOf(parameterName) >= 0;
		}

		/// <summary>
		/// Copies an array of parameters to the collection starting at the specified index.
		/// </summary>
		/// <param name="array">The array of parameters to copy to the collection.</param>
		/// <param name="index">The index in the collection to copy the parameters.</param>
		public override void CopyTo(Array array, int index)
		{
			paramList.ToArray().CopyTo(array, index);
		}

		/// <summary>
		/// Exposes the <see cref="M:System.Collections.IEnumerable.GetEnumerator"/> method, which supports a simple iteration over a collection by a .NET Framework data provider.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Collections.IEnumerator"/> that can be used to iterate through the collection.
		/// </returns>
		public override IEnumerator GetEnumerator()
		{
			return paramList.GetEnumerator();
		}

		/// <summary>
		/// Returns the index of the specified <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object.
		/// </summary>
		/// <param name="value">The <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object in the collection.</param>
		/// <returns>
		/// The index of the specified <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object.
		/// </returns>
		public override int IndexOf(object value)
		{
			CUBRIDParameter parameter = value as CUBRIDParameter;

			if (parameter == null)
				throw new ArgumentException(Utils.GetStr(MsgId.ArgumentMustBeCUBRIDParameter));

			return this.paramList.IndexOf(parameter);
		}

		/// <summary>
		/// Returns the index of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object with the specified name.
		/// </summary>
		/// <param name="parameterName">The name of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object in the collection.</param>
		/// <returns>
		/// The index of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object with the specified name.
		/// </returns>
		public override int IndexOf(string parameterName)
		{
			int ret = -1;

			int index = 0;
			foreach (CUBRIDParameter p in this.paramList)
			{
				if (p.ParameterName.Equals(parameterName, StringComparison.InvariantCultureIgnoreCase))
				{
					return index;
				}
				index++;
			}

			return ret;
		}

		/// <summary>
		/// Inserts the specified <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object with the specified name into the collection at the specified index.
		/// </summary>
		/// <param name="index">The index at which to insert the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object.</param>
		/// <param name="value">The <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object to insert into the collection.</param>
		public override void Insert(int index, object value)
		{
			ValidateIndex(index);
			CUBRIDParameter parameter = value as CUBRIDParameter;

			if (parameter == null)
				throw new ArgumentException(Utils.GetStr(MsgId.OnlyCUBRIDParameterObjectsAreValid));

			if (parameter.ParameterName == null || parameter.ParameterName == String.Empty)
				throw new ArgumentException(Utils.GetStr(MsgId.ParametersMustBeNamed));

			if (!parameter.ParameterName.StartsWith("?"))
				throw new ArgumentException(Utils.GetStr(MsgId.ParameterNameMustStartWith));

			if (paramList.Contains(parameter))
				throw new ArgumentException(Utils.GetStr(MsgId.ParameterAlreadyAdded));

			paramList.Insert(index, new CUBRIDParameter(value));
		}

		/// <summary>
		/// Removes the specified <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object from the collection.
		/// </summary>
		/// <param name="value">The <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object to remove.</param>
		public override void Remove(object value)
		{
			CUBRIDParameter parameter = (value as CUBRIDParameter);
			int index = IndexOf(parameter);

			if (index >= 0)
				paramList.Remove(parameter);
		}

		/// <summary>
		/// Removes the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object at the specified from the collection.
		/// </summary>
		/// <param name="index">The index where the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object is located.</param>
		public override void RemoveAt(int index)
		{
			ValidateIndex(index);
			paramList.RemoveAt(index);
		}

		/// <summary>
		/// Removes the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object with the specified name from the collection.
		/// </summary>
		/// <param name="parameterName">The name of the <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> object to remove.</param>
		public override void RemoveAt(string parameterName)
		{
			CUBRIDParameter parameter = (CUBRIDParameter)GetParameter(parameterName);

			Remove(parameter);
		}

		/// <summary>
		/// Sets the <see cref="T:System.Data.Common.DbParameter"/> object at the specified index to a new value.
		/// </summary>
		/// <param name="index">The index where the <see cref="T:System.Data.Common.DbParameter"/> object is located.</param>
		/// <param name="value">The new <see cref="T:System.Data.Common.DbParameter"/> value.</param>
		protected override void SetParameter(int index, DbParameter value)
		{
			ValidateIndex(index);

			CUBRIDParameter parameter = (value as CUBRIDParameter);

			if (!parameter.ParameterName.StartsWith("?"))
				throw new ArgumentException(Utils.GetStr(MsgId.ParameterNameMustStartWith));

			paramList[index] = parameter;
		}

		/// <summary>
		/// Sets the <see cref="T:System.Data.Common.DbParameter"/> object with the specified name to a new value.
		/// </summary>
		/// <param name="parameterName">The name of the <see cref="T:System.Data.Common.DbParameter"/> object in the collection.</param>
		/// <param name="value">The new <see cref="T:System.Data.Common.DbParameter"/> value.</param>
		protected override void SetParameter(string parameterName, DbParameter value)
		{
			int index = IndexOf(parameterName);

			if (index < 0)
				throw new ArgumentException(parameterName + ": " + Utils.GetStr(MsgId.ParameterNotFound));
			
			SetParameter(index, value);
		}

	}
}
