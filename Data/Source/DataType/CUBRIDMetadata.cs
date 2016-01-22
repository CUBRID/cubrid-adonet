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
using System.Globalization;

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// Implementation of the CUBRIDMetaData class.
	/// </summary>
	public class CUBRIDMetaData
	{
		/// <summary>
		/// Determines whether is numeric type the specified type name.
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns>
		///   <c>true</c> if is numeric type the specified type name; otherwise, <c>false</c>.
		/// </returns>
		public static bool IsNumericType(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "int":
				case "integer":
				case "numeric":
				case "decimal":
				case "real":
				case "double":
				case "float":
				case "serial":
				case "smallint": 
					return true;
			}

			return false;
		}

		/// <summary>
		/// Determines whether is text/string/char type the specified type name.
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns>
		///   <c>true</c> if is text/string/char type the specified type name; otherwise, <c>false</c>.
		/// </returns>
		public static bool IsTextType(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "varchar":
				case "char":
				case "string":
				case "nchar":
				case "nvarchar":
					return true;
			}

			return false;
		}

		/// <summary>
		/// Determines whether is bit type the specified type name
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns>
		///   <c>true</c> if is bit type the specified type name; otherwise, <c>false</c>.
		/// </returns>
		public static bool IsBitType(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "bit":
				case "bit varying":
				case "varbit":
					return true;
			}

			return false;
		}

		/// <summary>
		/// Determines whether is collection type the specified type name
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns>
		///   <c>true</c> if is collection type the specified type name; otherwise, <c>false</c>.
		/// </returns>
		public static bool IsCollectionType(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "set":
				case "multiset":
				case "list":
				case "sequence":
					return true;
			}

			return false;
		}

		/// <summary>
		/// Determines whether is date time type the specified type name
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns>
		///   <c>true</c> if is date time type the specified type name; otherwise, <c>false</c>.
		/// </returns>
		public static bool IsDateTimeType(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "date":
				case "time":
				case "datetime":
				case "timestamp":
					return true;
			}

			return false;
		}

		/// <summary>
		/// Determines whether is LOB type the specified type name
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns>
		///   <c>true</c> if is LOB type the specified type name; otherwise, <c>false</c>.
		/// </returns>
		public static bool IsLOBType(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "blob":
				case "clob":
					return true;
			}

			return false;
		}

		/// <summary>
		/// Determine if data type supports the Scale property.
		/// </summary>
		/// <param name="typeName">Name of the type.</param>
		/// <returns></returns>
		public static bool SupportsScale(string typeName)
		{
			switch (typeName.ToLower(CultureInfo.InvariantCulture))
			{
				case "numeric":
				case "decimal":
				case "float":
				case "real": 
					return true;
			}

			return false;
		}

		/// <summary>
		/// Returns a CUBRID data type from a CUBRID data type name.
		/// </summary>
		/// <param name="typeName">Data type name.</param>
		/// <returns></returns>
		public static CUBRIDDataType NameToType(string typeName)
		{
			switch (typeName.ToUpper(CultureInfo.InvariantCulture))
			{
				case "STRING": 
					return CUBRIDDataType.CCI_U_TYPE_STRING;
				case "CHAR":
					return CUBRIDDataType.CCI_U_TYPE_CHAR;
				case "VARCHAR": 
					return CUBRIDDataType.CCI_U_TYPE_VARNCHAR;
				case "DATE": 
					return CUBRIDDataType.CCI_U_TYPE_DATE;
				case "DATETIME": 
					return CUBRIDDataType.CCI_U_TYPE_DATETIME;
				case "TIME":
					return CUBRIDDataType.CCI_U_TYPE_TIME;
				case "TIMESTAMP":
					return CUBRIDDataType.CCI_U_TYPE_TIMESTAMP;
				case "NUMERIC":
					return CUBRIDDataType.CCI_U_TYPE_NUMERIC;
				case "DECIMAL":
					return CUBRIDDataType.CCI_U_TYPE_NUMERIC;
				case "SET": 
					return CUBRIDDataType.CCI_U_TYPE_SET;
				case "MULTISET":
					return CUBRIDDataType.CCI_U_TYPE_MULTISET;
				case "SEQUENCE":
					return CUBRIDDataType.CCI_U_TYPE_SEQUENCE;
				case "SHORT": 
					return CUBRIDDataType.CCI_U_TYPE_SHORT;
				case "BIT": 
					return CUBRIDDataType.CCI_U_TYPE_BIT;
				case "VARBIT":
					return CUBRIDDataType.CCI_U_TYPE_VARBIT;
				case "INT":
				case "INTEGER":
					return CUBRIDDataType.CCI_U_TYPE_INT;
				case "BIGINT":
					return CUBRIDDataType.CCI_U_TYPE_BIGINT;
				case "FLOAT": 
					return CUBRIDDataType.CCI_U_TYPE_FLOAT;
				case "DOUBLE": 
					return CUBRIDDataType.CCI_U_TYPE_DOUBLE;
				case "BLOB":
					return CUBRIDDataType.CCI_U_TYPE_BLOB;
				case "CLOB":
					return CUBRIDDataType.CCI_U_TYPE_CLOB;
				case "MONETARY":
					return CUBRIDDataType.CCI_U_TYPE_MONETARY;
				case "NCHAR":
					return CUBRIDDataType.CCI_U_TYPE_NCHAR;
				case "VARNCHAR":
					return CUBRIDDataType.CCI_U_TYPE_VARNCHAR;
				case "OBJECT":
					return CUBRIDDataType.CCI_U_TYPE_OBJECT;
			}

			return CUBRIDDataType.CCI_U_TYPE_UNKNOWN;
		}

	}
}
