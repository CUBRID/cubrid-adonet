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
using System.Data;
using System.Data.Common;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// CUBRID Implementation of the <see cref="T:System.Data.Common.DbParameter"/> class.
	/// </summary>
	public sealed class CUBRIDParameter : DbParameter, IDataParameter, IDbDataParameter, ICloneable
	{
		private string paramName;
		private CUBRIDDataType paramCUBRIDDataType;
        private CUBRIDDataType innerCUBRIDDataType;
		private DbType paramDbType;
		private object paramValue;
		private ParameterDirection paramDirection = ParameterDirection.Input;
		private byte paramPrecision;
		private byte paramScale;
		private int paramSize;
		string paramSourceColumn;
		bool paramSourceColumnNullMapping = true;
		DataRowVersion paramSourceVersion = DataRowVersion.Current;
		bool paramIsNullable;
        private Encoding parameterEncoding = Encoding.Default;

		private bool inferDataTypesFromValue = true;

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		public CUBRIDParameter()
		{
			this.inferDataTypesFromValue = true; //enable recalculate data types from parameter value
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="value">The value.</param>
		public CUBRIDParameter(Object value)
		{
			this.Value = value;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="cubridDataType">The CUBRID data type.</param>
		public CUBRIDParameter(CUBRIDDataType cubridDataType)
		{
			this.CUBRIDDataType = cubridDataType;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="value">The value.</param>
		public CUBRIDParameter(String parameterName, Object value)
			: this()
		{
			this.paramName = parameterName;
			this.Value = value;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="dbType">Data Type.</param>
		public CUBRIDParameter(string parameterName, DbType dbType)
			: this()
		{
			this.paramName = parameterName;
			this.DbType = dbType;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="cubridDataType">CUBRID Data Type.</param>
		public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType)
		{
			this.paramName = parameterName;
			this.CUBRIDDataType = cubridDataType;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="type">The data type.</param>
		/// <param name="dir">The direction.</param>
		/// <param name="col">The column name.</param>
		/// <param name="ver">The DataRow version.</param>
		/// <param name="val">The value.</param>
		public CUBRIDParameter(String parameterName, DbType type, ParameterDirection dir, String col, DataRowVersion ver, Object val)
			: this()
		{
			this.paramName = parameterName;
			this.paramDirection = dir;
			this.paramSourceColumn = col;
			this.paramSourceVersion = ver;
			this.paramValue = val; //avoid calculate data types
			this.DbType = type;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="cubridDataType">CUBRID data type.</param>
		/// <param name="size">The size.</param>
		public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, int size)
			: this(parameterName, cubridDataType)
		{
			this.paramSize = size;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="cubridDataType">CUBRID data type.</param>
		/// <param name="size">The size.</param>
		/// <param name="sourceColumn">The source column.</param>
		public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, int size, string sourceColumn)
			: this(parameterName, cubridDataType, size)
		{
			this.paramDirection = ParameterDirection.Input;
			this.paramSourceColumn = sourceColumn;
			this.paramSourceVersion = DataRowVersion.Current;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="cubridDataType">CUBRID data type.</param>
		/// <param name="dir">The direction.</param>
		/// <param name="col">The column.</param>
		/// <param name="ver">The DataRow version.</param>
		/// <param name="val">The value.</param>
		public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, ParameterDirection dir, string col,
														DataRowVersion ver, object val)
		{
			this.paramName = parameterName;
			this.CUBRIDDataType = cubridDataType;
			this.paramDirection = dir;
			this.paramSourceColumn = col;
			this.paramSourceVersion = ver;
			this.paramValue = val; //avoid calculate data types
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDParameter"/> class.
		/// </summary>
		/// <param name="parameterName">Name of the parameter.</param>
		/// <param name="cubridDataType">CUBRID data type.</param>
		/// <param name="size">The size.</param>
		/// <param name="direction">The direction.</param>
		/// <param name="isNullable">If set to <c>true</c> is nullable.</param>
		/// <param name="precision">The precision.</param>
		/// <param name="scale">The scale.</param>
		/// <param name="sourceColumn">The source column.</param>
		/// <param name="sourceVersion">The source version.</param>
		/// <param name="value">The parameter value.</param>
		public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, int size, ParameterDirection direction,
														bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion,
														Object value
		)
		{
			this.paramName = parameterName;
			this.CUBRIDDataType = cubridDataType;
			this.paramSize = size;
			this.paramDirection = direction;
			this.paramIsNullable = isNullable;
			this.Precision = precision;
			this.paramScale = scale;
			this.paramSourceColumn = sourceColumn;
			this.paramSourceVersion = sourceVersion;
			this.paramValue = value; //avoid calculate data types
		}

		/// <summary>
		/// Gets or sets the <see cref="T:System.Data.DbType"/> of the parameter.
		/// </summary>
		/// <returns>
		/// One of the <see cref="T:System.Data.DbType"/> values. The default is <see cref="F:System.Data.DbType.String"/>.
		///   </returns>
		///   
		/// <exception cref="T:System.ArgumentException">
		/// The property is not set to a valid <see cref="T:System.Data.DbType"/>.
		///   </exception>
		public override DbType DbType
		{
			get { return this.paramDbType; }
			set
			{
				this.paramDbType = value;
				SetCUBRIDDataTypeFromDbType();
				this.inferDataTypesFromValue = false; //no need to recalculate data types from parameter value
			}
		}

		/// <summary>
		/// Gets or sets the CUBRID data type.
		/// </summary>
		/// <value>
		/// The CUBRID data type.
		/// </value>
		public CUBRIDDataType CUBRIDDataType
		{
			get { return this.paramCUBRIDDataType; }
			set
			{
				this.paramCUBRIDDataType = value;
				SetDbTypeFromCUBRIDDataType();
				this.inferDataTypesFromValue = false; //no need to recalculate data types from parameter value
			}
		}

        public CUBRIDDataType InnerCUBRIDDataType
        {
            get { return innerCUBRIDDataType; }
            set { this.innerCUBRIDDataType = value; }
        }
/*
        public void SetInnerCUBRIDDataType(DbType value)
        {
        }
*/
		/// <summary>
		/// Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
		/// </summary>
		/// <returns>
		/// One of the <see cref="T:System.Data.ParameterDirection"/> values. The default is Input.
		///   </returns>
		///   
		/// <exception cref="T:System.ArgumentException">
		/// The property is not set to one of the valid <see cref="T:System.Data.ParameterDirection"/> values.
		///   </exception>
		public override ParameterDirection Direction
		{
			get { return this.paramDirection; }
			set { this.paramDirection = value; }
		}

		/// <summary>
		/// Gets or sets a value that indicates whether the parameter accepts null values.
		/// </summary>
		/// <returns>True if null values are accepted; otherwise false. The default is false.
		///   </returns>
		public override bool IsNullable
		{
			get { return this.paramIsNullable; }
			set { this.paramIsNullable = value; }
		}

		/// <summary>
		/// Gets or sets the name of the <see cref="T:System.Data.Common.DbParameter"/>.
		/// </summary>
		/// <returns>
		/// The name of the <see cref="T:System.Data.Common.DbParameter"/>. The default is an empty string ("").
		///   </returns>
		public override string ParameterName
		{
			get { return this.paramName; }
			set { this.paramName = value; }
		}

		/// <summary>
		/// Gets or sets the maximum size, in bytes, of the data within the column.
		/// </summary>
		/// <returns>
		/// The maximum size, in bytes, of the data within the column. The default value is inferred from the parameter value.
		///   </returns>
		public override int Size
		{
			get { return this.paramSize; }
			set { this.paramSize = value; }
		}

		/// <summary>
		/// Gets or sets the name of the source column mapped to the <see cref="T:System.Data.DataSet"/> and used for loading or returning the <see cref="P:System.Data.Common.DbParameter.Value"/>.
		/// </summary>
		/// <returns>
		/// The name of the source column mapped to the <see cref="T:System.Data.DataSet"/>. The default is an empty string.
		///   </returns>
		public override string SourceColumn
		{
			get { return this.paramSourceColumn; }
			set { this.paramSourceColumn = value; }
		}

		/// <summary>
		/// Sets or gets a value which indicates whether the source column is nullable. This allows <see cref="T:System.Data.Common.DbCommandBuilder"/> to correctly generate Update statements for nullable columns.
		/// </summary>
		/// <returns>True if the source column is nullable; false if it is not.
		///   </returns>
		public override bool SourceColumnNullMapping
		{
			get { return this.paramSourceColumnNullMapping; }
			set { this.paramSourceColumnNullMapping = value; }
		}

		/// <summary>
		/// Gets or sets the <see cref="T:System.Data.DataRowVersion"/> to use when you load <see cref="P:System.Data.Common.DbParameter.Value"/>.
		/// </summary>
		/// <returns>
		/// One of the <see cref="T:System.Data.DataRowVersion"/> values. The default is Current.
		///   </returns>
		///   
		/// <exception cref="T:System.ArgumentException">
		/// The property is not set to one of the <see cref="T:System.Data.DataRowVersion"/> values.
		///   </exception>
		public override DataRowVersion SourceVersion
		{
			get { return this.paramSourceVersion; }
			set { this.paramSourceVersion = value; }
		}

		/// <summary>
		/// Gets or sets the value of the parameter.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Object"/> that is the value of the parameter. The default value is null.
		///   </returns>
		public override object Value
		{
			get { return this.paramValue; }
			set
			{
				this.paramValue = value;

				//Try to determine size
				byte[] valueAsByte = value as byte[];
				string valueAsString = value as string;

				if (valueAsByte != null)
					this.paramSize = valueAsByte.Length;
				else if (valueAsString != null)
					this.paramSize = valueAsString.Length;

				//Try to determine data types
				SetDataTypesFromValue();
			}
		}

		/// <summary>
		/// Indicates the precision of numeric parameters.
		/// </summary>
		/// <returns>
		/// The maximum number of digits used to represent the Value property of a data provider Parameter object. The default value is 0, which indicates that a data provider sets the precision for Value.
		///   </returns>
		public byte Precision
		{
			get { return this.paramPrecision; }
			set { this.paramPrecision = value; }
		}

		/// <summary>
		/// Indicates the scale of numeric parameters.
		/// </summary>
		/// <returns>
		/// The number of decimal places to which <see cref="T:System.Data.OleDb.OleDbParameter.Value"/> is resolved. The default is 0.
		///   </returns>
		public byte Scale
		{
			get { return this.paramScale; }
			set { this.paramScale = value; }
		}

		/// <summary>
		/// Resets the DbType property to its original settings.
		/// </summary>
		public override void ResetDbType()
		{
			this.inferDataTypesFromValue = true;
		}


		private void SetCUBRIDDataTypeFromDbType()
		{
			switch (this.paramDbType)
			{
				case DbType.Int16:
				case DbType.UInt16:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case DbType.Int32:
				case DbType.UInt32:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
					break;
				case DbType.Int64:
				case DbType.UInt64:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
					break;
				case DbType.String:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
					break;
				case DbType.Single:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_FLOAT;
					break;
				case DbType.Double:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DOUBLE;
					break;
				case DbType.Decimal:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NUMERIC; //TODO Verify mapping
					break;
				case DbType.Date:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DATE;
					break;
				case DbType.Time:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_TIME;
					break;
				case DbType.DateTime:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_TIMESTAMP;
					break;
				case DbType.Boolean:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case DbType.Currency:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_MONETARY;
					break;
				case DbType.StringFixedLength:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CHAR; //TODO Verify mapping
					break;
				case DbType.Binary:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
					break;
				case DbType.Byte:
				case DbType.SByte:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case DbType.Object:
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
					break;
				default:
					throw new Exception(Utils.GetStr(MsgId.DbTypeCantBeMappedToCUBRIDDataType));
			}
		}

		private void SetDbTypeFromCUBRIDDataType()
		{
			switch (this.paramCUBRIDDataType)
			{
				case CUBRIDDataType.CCI_U_TYPE_NULL:
					this.paramDbType = DbType.Object; //No other matching
					break;
				case CUBRIDDataType.CCI_U_TYPE_SHORT:
					this.paramDbType = DbType.Int16;
					break;
				case CUBRIDDataType.CCI_U_TYPE_INT:
					this.paramDbType = DbType.Int32;
					break;
				case CUBRIDDataType.CCI_U_TYPE_BIGINT:
					this.paramDbType = DbType.Int64;
					break;
				case CUBRIDDataType.CCI_U_TYPE_STRING:
				case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
					this.paramDbType = DbType.String;
					break;
				case CUBRIDDataType.CCI_U_TYPE_FLOAT:
					this.paramDbType = DbType.Single;
					break;
				case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
				case CUBRIDDataType.CCI_U_TYPE_MONETARY:
					this.paramDbType = DbType.Double;
					break;
				case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
					this.paramDbType = DbType.Decimal;
					break;
				case CUBRIDDataType.CCI_U_TYPE_DATE:
					this.paramDbType = DbType.Date;
					break;
				case CUBRIDDataType.CCI_U_TYPE_TIME:
					this.paramDbType = DbType.Time;
					break;
				case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
				case CUBRIDDataType.CCI_U_TYPE_DATETIME:
					this.paramDbType = DbType.DateTime;
					break;
				case CUBRIDDataType.CCI_U_TYPE_CHAR:
				case CUBRIDDataType.CCI_U_TYPE_NCHAR:
					this.paramDbType = DbType.StringFixedLength;
					break;
				case CUBRIDDataType.CCI_U_TYPE_BLOB:
				case CUBRIDDataType.CCI_U_TYPE_CLOB:
				case CUBRIDDataType.CCI_U_TYPE_VARBIT:
					this.paramDbType = DbType.Binary;
					break;
				case CUBRIDDataType.CCI_U_TYPE_BIT:
					this.paramDbType = DbType.Byte;
					break;
				case CUBRIDDataType.CCI_U_TYPE_OBJECT:
					this.paramDbType = DbType.Object;
					break;
				case CUBRIDDataType.CCI_U_TYPE_SET: //java.sql.Types.OTHER
				case CUBRIDDataType.CCI_U_TYPE_MULTISET: //java.sql.Types.OTHER
				case CUBRIDDataType.CCI_U_TYPE_SEQUENCE: //java.sql.Types.OTHER
					this.paramDbType = DbType.Object;
					break;
				case CUBRIDDataType.CCI_U_TYPE_RESULTSET:
					this.paramDbType = DbType.Object;
					break;
				default:
					throw new Exception(Utils.GetStr(MsgId.CUBRIDDataTypeCantBeMappedToDbType));
			}
		}

		/// <summary>
		/// Sets the data types from value.
		/// </summary>
		private void SetDataTypesFromValue()
		{
			if (!this.inferDataTypesFromValue)
				return;

			if (this.paramValue == null || this.paramValue == DBNull.Value)
			{
				this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NULL;
				//this.paramDbType = DbType.Object;

				return;
			}

			if (this.paramValue is TimeSpan)
			{
				this.DbType = DbType.UInt64;
				this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;

				return;
			}

			TypeCode typeCode = System.Type.GetTypeCode(this.paramValue.GetType());
			switch (typeCode)
			{
				case TypeCode.Boolean:
					this.paramDbType = DbType.Boolean;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case TypeCode.SByte:
					this.paramDbType = DbType.SByte;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case TypeCode.Byte:
					this.paramDbType = DbType.Byte;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case TypeCode.Int16:
					this.paramDbType = DbType.Int16;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case TypeCode.UInt16:
					this.paramDbType = DbType.UInt16;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
					break;
				case TypeCode.Int32:
					this.paramDbType = DbType.Int32;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
					break;
				case TypeCode.UInt32:
					this.paramDbType = DbType.UInt32;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
					break;
				case TypeCode.Int64:
					this.paramDbType = DbType.Int64;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
					break;
				case TypeCode.UInt64:
					this.paramDbType = DbType.UInt64;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
					break;
				case TypeCode.DateTime:
					this.paramDbType = DbType.DateTime;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DATETIME;
					break;
				case TypeCode.String:
					this.paramDbType = DbType.String;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
					break;
				case TypeCode.Single:
					this.paramDbType = DbType.Single;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_FLOAT;
					break;
				case TypeCode.Double:
					this.paramDbType = DbType.Double;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DOUBLE;
					break;
				case TypeCode.Decimal:
					this.paramDbType = DbType.Decimal;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NUMERIC;
					break;
				case TypeCode.Object:
					this.paramDbType = DbType.Object;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
					break;
				case TypeCode.DBNull:
					//this.paramDbType = DbType.Object;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NULL;
					break;
				default:
					this.paramDbType = DbType.Object;
					this.paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
					break;
			}
		}

		/// <summary>
		/// Writes the data to the stream.
		/// </summary>
		/// <param name="stream">The stream.</param>
		internal void Write(CUBRIDStream stream)
		{
			if (this.paramValue == null)
			{
				stream.WriteByteArg((byte)CUBRIDDataType.CCI_U_TYPE_NULL);
				stream.WriteNullArg();
			}
			else
			{
				stream.WriteByteArg((byte)paramCUBRIDDataType);

				switch (paramCUBRIDDataType)
				{
					case CUBRIDDataType.CCI_U_TYPE_NULL:
						stream.WriteNullArg();
						break;

					case CUBRIDDataType.CCI_U_TYPE_CHAR:
					case CUBRIDDataType.CCI_U_TYPE_NCHAR:
					case CUBRIDDataType.CCI_U_TYPE_STRING:
					case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
						stream.WriteStringArg(paramValue.ToString(), this.parameterEncoding);
						break;

					case CUBRIDDataType.CCI_U_TYPE_SHORT:
						stream.WriteShortArg((short)Convert.ToInt16(paramValue));
						break;

					case CUBRIDDataType.CCI_U_TYPE_INT:
						stream.WriteIntArg((int)paramValue);
						break;

					case CUBRIDDataType.CCI_U_TYPE_BIGINT:
						stream.WriteLongArg((long)Convert.ToInt64(paramValue));
						break;

					case CUBRIDDataType.CCI_U_TYPE_FLOAT:
						stream.WriteFloatArg((float)Convert.ToDouble(paramValue));
						break;

					case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
					case CUBRIDDataType.CCI_U_TYPE_MONETARY:
						stream.WriteDoubleArg((double)Convert.ToDouble(paramValue));
						break;

					case CUBRIDDataType.CCI_U_TYPE_DATE:
						stream.WriteDateArg((DateTime)paramValue);
						break;

					case CUBRIDDataType.CCI_U_TYPE_TIME:
						stream.WriteTimeArg((DateTime)paramValue);
						break;

					case CUBRIDDataType.CCI_U_TYPE_DATETIME:
						stream.WriteDateTimeArg((DateTime)paramValue);
						break;

					case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
						stream.WriteTimeStampArg((DateTime)paramValue);
						break;

                    case CUBRIDDataType.CCI_U_TYPE_SET:
                    case CUBRIDDataType.CCI_U_TYPE_MULTISET:
                    case CUBRIDDataType.CCI_U_TYPE_SEQUENCE:
                        {
                            object[] oArray = null;
                            Array array = paramValue as Array;
                            if (array != null)
                            {
                                if ((array.GetValue(0) as Array) == null)
                                {
                                    oArray = new object[array.Length];
                                    array.CopyTo(oArray, 0);
                                }
                                else
                                {
                                    throw new NotImplementedException();
                                }
                            }
                            else
                            {
                                oArray = new object[1];
                                oArray[0] = paramValue;
                            }

                            if (oArray != null)
                            {
                                stream.WriteCollection(oArray, this.innerCUBRIDDataType);
                            }
                        }
                        // stream.WriteCollection((object[])paramValue, this.paramCUBRIDDataType);
                        break;

					case CUBRIDDataType.CCI_U_TYPE_BIT:
					case CUBRIDDataType.CCI_U_TYPE_VARBIT:
						byte[] valueAsByte = paramValue as byte[];
						if (valueAsByte != null && valueAsByte.Length > 1)
						{
							stream.WriteBytesArg((byte[])paramValue);
						}
						else
						{
							stream.WriteByteArg((byte)Convert.ToByte(paramValue));
						}
						break;

					case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
						//INFO NUMERIC(=DECIMAL=DEC) is sent as null-terminated string value
                        stream.WriteStringArg(Convert.ToDecimal(paramValue).ToString(System.Globalization.CultureInfo.InvariantCulture), Encoding.Default); // Added convertion and culture info.
						break;

					case CUBRIDDataType.CCI_U_TYPE_BLOB:
						stream.WriteBlob((CUBRIDBlob)paramValue);
						break;

					case CUBRIDDataType.CCI_U_TYPE_CLOB:
						stream.WriteClob((CUBRIDClob)paramValue);
						break;

					default:
						throw new Exception(Utils.GetStr(MsgId.DontKnowHowToWriteParameter));
				}
			}
		}

        /// <summary>
        /// Sets the parameter's encoding property.
        /// </summary>
        public void SetParameterEncoding(Encoding encoding)
        {
            this.parameterEncoding = encoding;
        }

        /// <summary>
        /// Returns the parameter's encoding property.
        /// </summary>
        /// <returns>
        /// The parameter's encoding property.
        /// </returns>
        public Encoding GetParameterEncoding()
        {
            return this.parameterEncoding;
        }

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this parameter instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this parameter instance.
		/// </returns>
		public override string ToString()
		{
			return this.paramName;
		}

		#region ICloneable

		/// <summary>
		/// Clones this instance.
		/// </summary>
		/// <returns></returns>
		public CUBRIDParameter Clone()
		{
			CUBRIDParameter clone = new CUBRIDParameter(this.paramName, this.paramCUBRIDDataType, this.paramSize,
																									this.paramDirection, this.paramIsNullable, this.paramPrecision,
																									this.paramScale, this.paramSourceColumn, this.paramSourceVersion,
																									this.paramValue);
			return clone;
		}

		object ICloneable.Clone()
		{
			return this.Clone();
		}

		#endregion

	}
}
