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
using System.Collections.Generic;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	internal class ResultTuple
	{
		private int index;
		private CUBRIDOid oid;
		private int valueCount;
		private object[] valueArray;
		private Dictionary<string, object> valueDictionary;

		internal ResultTuple(int count)
		{
			this.valueCount = count;
			this.valueArray = new object[count];
			this.valueDictionary = new Dictionary<string, object>();
		}

		internal int Index
		{
			get { return this.index; }
			set { this.index = value; }
		}

		internal CUBRIDOid Oid
		{
			get { return this.oid; }
			set { this.oid = value; }
		}

		internal object this[int idx]
		{
			get { return this.valueArray[idx]; }
			set { this.valueArray[idx] = value; }
		}

		internal object this[string name]
		{
			get { return this.valueDictionary[name]; }
			set
			{
				if (this.valueDictionary.ContainsKey(name))
				{
					this.valueDictionary[name] = value;
				}
				else
				{
					this.valueDictionary.Add(name, value);
				}
			}
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();

			for (int i = 0; i < valueCount; i++)
			{
				if (this.valueArray[i] != null)
				{
					builder.Append("Tuple: index = " + i);
					builder.Append(", value = " + valueArray[i]);
					builder.Append(", data type = " + valueArray[i].GetType() + "\n");
				}
				else
				{
					builder.Append("Tuple: index = " + i + ", value = null, data type = null\n");
				}
			}

			return builder.ToString();
		}
	}

}
