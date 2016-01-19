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
using System.Globalization;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID Implementation of the <see cref="T:System.Data.Common.DbParameter" /> class.
  /// </summary>
  public sealed class CUBRIDParameter : DbParameter, IDbDataParameter, ICloneable
  {
    private bool inferDataTypesFromValue = true;
    private CUBRIDDataType innerCUBRIDDataType;
    private CUBRIDDataType paramCUBRIDDataType;
    private DbType paramDbType;
    private ParameterDirection paramDirection = ParameterDirection.Input;
    private bool paramIsNullable;
    private string paramName;
    private byte paramPrecision;
    private byte paramScale;
    private int paramSize;
    private string paramSourceColumn;
    private bool paramSourceColumnNullMapping = true;
    private DataRowVersion paramSourceVersion = DataRowVersion.Current;
    private object paramValue;
    private Encoding parameterEncoding = null;
    private int pos = 0;
    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    public CUBRIDParameter()
    {
      inferDataTypesFromValue = true; //enable recalculate data types from parameter value
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="value"> The value. </param>
    public CUBRIDParameter(Object value)
    {
      Value = value;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="cubridDataType"> The CUBRID data type. </param>
    public CUBRIDParameter(CUBRIDDataType cubridDataType)
    {
      CUBRIDDataType = cubridDataType;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="value"> The value. </param>
    public CUBRIDParameter(String parameterName, Object value)
      : this()
    {
      paramName = parameterName;
      Value = value;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="dbType"> Data Type. </param>
    public CUBRIDParameter(string parameterName, DbType dbType)
      : this()
    {
      paramName = parameterName;
      DbType = dbType;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="cubridDataType"> CUBRID Data Type. </param>
    public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType)
    {
      paramName = parameterName;
      CUBRIDDataType = cubridDataType;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="type"> The data type. </param>
    /// <param name="dir"> The direction. </param>
    /// <param name="col"> The column name. </param>
    /// <param name="ver"> The DataRow version. </param>
    /// <param name="val"> The value. </param>
    public CUBRIDParameter(String parameterName, DbType type, ParameterDirection dir, String col, DataRowVersion ver,
                           Object val)
      : this()
    {
      paramName = parameterName;
      paramDirection = dir;
      paramSourceColumn = col;
      paramSourceVersion = ver;
      paramValue = val; //avoid calculate data types
      DbType = type;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="cubridDataType"> CUBRID data type. </param>
    /// <param name="size"> The size. </param>
    public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, int size)
      : this(parameterName, cubridDataType)
    {
      paramSize = size;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="cubridDataType"> CUBRID data type. </param>
    /// <param name="size"> The size. </param>
    /// <param name="sourceColumn"> The source column. </param>
    public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, int size, string sourceColumn)
      : this(parameterName, cubridDataType, size)
    {
      paramDirection = ParameterDirection.Input;
      paramSourceColumn = sourceColumn;
      paramSourceVersion = DataRowVersion.Current;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="cubridDataType"> CUBRID data type. </param>
    /// <param name="dir"> The direction. </param>
    /// <param name="col"> The column. </param>
    /// <param name="ver"> The DataRow version. </param>
    /// <param name="val"> The value. </param>
    public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, ParameterDirection dir, string col,
                           DataRowVersion ver, object val)
    {
      paramName = parameterName;
      CUBRIDDataType = cubridDataType;
      paramDirection = dir;
      paramSourceColumn = col;
      paramSourceVersion = ver;
      paramValue = val; //avoid calculate data types
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDParameter" /> class.
    /// </summary>
    /// <param name="parameterName"> Name of the parameter. </param>
    /// <param name="cubridDataType"> CUBRID data type. </param>
    /// <param name="size"> The size. </param>
    /// <param name="direction"> The direction. </param>
    /// <param name="isNullable"> If set to <c>true</c> is nullable. </param>
    /// <param name="precision"> The precision. </param>
    /// <param name="scale"> The scale. </param>
    /// <param name="sourceColumn"> The source column. </param>
    /// <param name="sourceVersion"> The source version. </param>
    /// <param name="value"> The parameter value. </param>
    public CUBRIDParameter(string parameterName, CUBRIDDataType cubridDataType, int size, ParameterDirection direction,
                           bool isNullable, byte precision, byte scale, string sourceColumn,
                           DataRowVersion sourceVersion,
                           Object value
      )
    {
      paramName = parameterName;
      CUBRIDDataType = cubridDataType;
      paramSize = size;
      paramDirection = direction;
      paramIsNullable = isNullable;
      Precision = precision;
      paramScale = scale;
      paramSourceColumn = sourceColumn;
      paramSourceVersion = sourceVersion;
      paramValue = value; //avoid calculate data types
    }

    /// <summary>
    ///   Gets or sets the CUBRID data type.
    /// </summary>
    /// <value> The CUBRID data type. </value>
    public CUBRIDDataType CUBRIDDataType
    {
      get { return paramCUBRIDDataType; }
      set
      {
        paramCUBRIDDataType = value;
        SetDbTypeFromCUBRIDDataType();
        inferDataTypesFromValue = false; //no need to recalculate data types from parameter value
      }
    }

    /// <summary>
    ///   Gets or sets the inner CUBRID data type.
    /// </summary>
    /// <value> The inner CUBRID data type. </value>
    public CUBRIDDataType InnerCUBRIDDataType
    {
      get { return innerCUBRIDDataType; }
      set { innerCUBRIDDataType = value; }
    }

    /// <summary>
    ///   Sets or gets a value which indicates whether the source column is nullable. This allows <see
    ///    cref="T:System.Data.Common.DbCommandBuilder" /> to correctly generate Update statements for nullable columns.
    /// </summary>
    /// <returns> True if the source column is nullable; false if it is not. </returns>
    public override bool SourceColumnNullMapping
    {
      get { return paramSourceColumnNullMapping; }
      set { paramSourceColumnNullMapping = value; }
    }

    #region IDataParameter Members

    /// <summary>
    ///   Gets or sets the <see cref="T:System.Data.DbType" /> of the parameter.
    /// </summary>
    /// <returns> One of the <see cref="T:System.Data.DbType" /> values. The default is <see cref="F:System.Data.DbType.String" /> . </returns>
    /// <exception cref="T:System.ArgumentException">The property is not set to a valid
    ///   <see cref="T:System.Data.DbType" />
    ///   .</exception>
    public override DbType DbType
    {
      get { return paramDbType; }
      set
      {
        paramDbType = value;
        SetCUBRIDDataTypeFromDbType();
        inferDataTypesFromValue = false; //no need to recalculate data types from parameter value
      }
    }

    /// <summary>
    ///   Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
    /// </summary>
    /// <returns> One of the <see cref="T:System.Data.ParameterDirection" /> values. The default is Input. </returns>
    /// <exception cref="T:System.ArgumentException">The property is not set to one of the valid
    ///   <see cref="T:System.Data.ParameterDirection" />
    ///   values.</exception>
    public override ParameterDirection Direction
    {
      get { return paramDirection; }
      set { paramDirection = value; }
    }

    /// <summary>
    ///   Gets or sets the pos of parameter
    /// </summary>
    /// <returns> The pos of parameter. </returns>
    public  int Pos
    {
        get { return pos; }
        set { pos = value; }
    }

    /// <summary>
    ///   Gets or sets a value that indicates whether the parameter accepts null values.
    /// </summary>
    /// <returns> True if null values are accepted; otherwise false. The default is false. </returns>
    public override bool IsNullable
    {
      get { return paramIsNullable; }
      set { paramIsNullable = value; }
    }

    /// <summary>
    ///   Gets or sets the name of the <see cref="T:System.Data.Common.DbParameter" />.
    /// </summary>
    /// <returns> The name of the <see cref="T:System.Data.Common.DbParameter" /> . The default is an empty string (""). </returns>
    public override string ParameterName
    {
      get { return paramName; }
      set { paramName = value; }
    }

    /// <summary>
    ///   Gets or sets the name of the source column mapped to the <see cref="T:System.Data.DataSet" /> and used for loading or returning the <see
    ///    cref="P:System.Data.Common.DbParameter.Value" />.
    /// </summary>
    /// <returns> The name of the source column mapped to the <see cref="T:System.Data.DataSet" /> . The default is an empty string. </returns>
    public override string SourceColumn
    {
      get { return paramSourceColumn; }
      set { paramSourceColumn = value; }
    }

    /// <summary>
    ///   Gets or sets the <see cref="T:System.Data.DataRowVersion" /> to use when you load <see
    ///    cref="P:System.Data.Common.DbParameter.Value" />.
    /// </summary>
    /// <returns> One of the <see cref="T:System.Data.DataRowVersion" /> values. The default is Current. </returns>
    /// <exception cref="T:System.ArgumentException">The property is not set to one of the
    ///   <see cref="T:System.Data.DataRowVersion" />
    ///   values.</exception>
    public override DataRowVersion SourceVersion
    {
      get { return paramSourceVersion; }
      set { paramSourceVersion = value; }
    }

    /// <summary>
    ///   Gets or sets the value of the parameter.
    /// </summary>
    /// <returns> An <see cref="T:System.Object" /> that is the value of the parameter. The default value is null. </returns>
    public override object Value
    {
      get { return paramValue; }
      set
      {
        paramValue = value;

        //Try to determine size
        byte[] valueAsByte = value as byte[];
        string valueAsString = value as string;

        if (valueAsByte != null)
          paramSize = valueAsByte.Length;
        else if (valueAsString != null)
          paramSize = valueAsString.Length;

        //Try to determine data types
        SetDataTypesFromValue();
      }
    }

    #endregion

    #region IDbDataParameter Members

    /// <summary>
    ///   Gets or sets the maximum size, in bytes, of the data within the column.
    /// </summary>
    /// <returns> The maximum size, in bytes, of the data within the column. The default value is inferred from the parameter value. </returns>
    public override int Size
    {
      get { return paramSize; }
      set { paramSize = value; }
    }

    /// <summary>
    ///   Indicates the precision of numeric parameters.
    /// </summary>
    /// <returns> The maximum number of digits used to represent the Value property of a data provider Parameter object. The default value is 0, which indicates that a data provider sets the precision for Value. </returns>
    public byte Precision
    {
      get { return paramPrecision; }
      set { paramPrecision = value; }
    }

    /// <summary>
    ///   Indicates the scale of numeric parameters.
    /// </summary>
    /// <returns> The number of decimal places. The default is 0. </returns>
    public byte Scale
    {
      get { return paramScale; }
      set { paramScale = value; }
    }

    #endregion

    /// <summary>
    ///   Resets the DbType property to its original settings.
    /// </summary>
    public override void ResetDbType()
    {
      inferDataTypesFromValue = true;
    }


    private void SetCUBRIDDataTypeFromDbType()
    {
      switch (paramDbType)
      {
        case DbType.Int16:
        case DbType.UInt16:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
          break;
        case DbType.Int32:
        case DbType.UInt32:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
          break;
        case DbType.Int64:
        case DbType.UInt64:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
          break;
        case DbType.String:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
          break;
        case DbType.Single:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_FLOAT;
          break;
        case DbType.Double:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DOUBLE;
          break;
        case DbType.Decimal:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NUMERIC; //TODO Verify mapping
          break;
        case DbType.Date:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DATE;
          break;
        case DbType.Time:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_TIME;
          break;
        case DbType.DateTime:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_TIMESTAMP;
          break;
        case DbType.Boolean:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIT;
          break;
        case DbType.Currency:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_MONETARY;
          break;
        case DbType.StringFixedLength:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CHAR; //TODO Verify mapping
          break;
        case DbType.Binary:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
          break;
        case DbType.AnsiString:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
          break;
        case DbType.Byte:
        case DbType.SByte:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_VARBIT;
          break;
        case DbType.Object:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
          break;
        case DbType.VarNumeric:
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
          break;
        default:
          throw new Exception(Utils.GetStr(MsgId.DbTypeCantBeMappedToCUBRIDDataType));
      }
    }

    private void SetDbTypeFromCUBRIDDataType()
    {
      switch (paramCUBRIDDataType)
      {
        case CUBRIDDataType.CCI_U_TYPE_NULL:
          paramDbType = DbType.Object; //No other matching
          break;
        case CUBRIDDataType.CCI_U_TYPE_SHORT:
          paramDbType = DbType.Int16;
          break;
        case CUBRIDDataType.CCI_U_TYPE_INT:
          paramDbType = DbType.Int32;
          break;
        case CUBRIDDataType.CCI_U_TYPE_BIGINT:
          paramDbType = DbType.Int64;
          break;
        case CUBRIDDataType.CCI_U_TYPE_STRING:
        case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
        case CUBRIDDataType.CCI_U_TYPE_ENUM:
          paramDbType = DbType.String;
          break;
        case CUBRIDDataType.CCI_U_TYPE_FLOAT:
          paramDbType = DbType.Single;
          break;
        case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
        case CUBRIDDataType.CCI_U_TYPE_MONETARY:
          paramDbType = DbType.Double;
          break;
        case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
          paramDbType = DbType.Decimal;
          break;
        case CUBRIDDataType.CCI_U_TYPE_DATE:
          paramDbType = DbType.Date;
          break;
        case CUBRIDDataType.CCI_U_TYPE_TIME:
          paramDbType = DbType.Time;
          break;
        case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
        case CUBRIDDataType.CCI_U_TYPE_DATETIME:
          paramDbType = DbType.DateTime;
          break;
        case CUBRIDDataType.CCI_U_TYPE_CHAR:
        case CUBRIDDataType.CCI_U_TYPE_NCHAR:
          paramDbType = DbType.StringFixedLength;
          break;
        case CUBRIDDataType.CCI_U_TYPE_BLOB:
        case CUBRIDDataType.CCI_U_TYPE_CLOB:
        case CUBRIDDataType.CCI_U_TYPE_VARBIT:
          paramDbType = DbType.Binary;
          break;
        case CUBRIDDataType.CCI_U_TYPE_BIT:
          paramDbType = DbType.Byte;
          break;
        case CUBRIDDataType.CCI_U_TYPE_OBJECT:
          paramDbType = DbType.Object;
          break;
        case CUBRIDDataType.CCI_U_TYPE_SET: //java.sql.Types.OTHER
        case CUBRIDDataType.CCI_U_TYPE_MULTISET: //java.sql.Types.OTHER
        case CUBRIDDataType.CCI_U_TYPE_SEQUENCE: //java.sql.Types.OTHER
          paramDbType = DbType.Object;
          break;
        case CUBRIDDataType.CCI_U_TYPE_RESULTSET:
          paramDbType = DbType.Object;
          break;
        default:
          throw new Exception(Utils.GetStr(MsgId.CUBRIDDataTypeCantBeMappedToDbType));
      }
    }

    /// <summary>
    ///   Sets the data types from value.
    /// </summary>
    private void SetDataTypesFromValue()
    {
      if (!inferDataTypesFromValue)
        return;

      if (paramValue == null || paramValue == DBNull.Value)
      {
        paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NULL;
        //this.paramDbType = DbType.Object;

        return;
      }

      if (paramValue is TimeSpan)
      {
        DbType = DbType.UInt64;
        paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;

        return;
      }

      TypeCode typeCode = Type.GetTypeCode(paramValue.GetType());
      switch (typeCode)
      {
        case TypeCode.Boolean:
          paramDbType = DbType.Boolean;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
          break;
        case TypeCode.SByte:
          paramDbType = DbType.SByte;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
          break;
        case TypeCode.Byte:
          paramDbType = DbType.Byte;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
          break;
        case TypeCode.Int16:
          paramDbType = DbType.Int16;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
          break;
        case TypeCode.UInt16:
          paramDbType = DbType.UInt16;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
          break;
        case TypeCode.Int32:
          paramDbType = DbType.Int32;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
          break;
        case TypeCode.UInt32:
          paramDbType = DbType.UInt32;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
          break;
        case TypeCode.Int64:
          paramDbType = DbType.Int64;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
          break;
        case TypeCode.UInt64:
          paramDbType = DbType.UInt64;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
          break;
        case TypeCode.DateTime:
          paramDbType = DbType.DateTime;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DATETIME;
          break;
        case TypeCode.String:
          paramDbType = DbType.String;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
          break;
        case TypeCode.Single:
          paramDbType = DbType.Single;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_FLOAT;
          break;
        case TypeCode.Double:
          paramDbType = DbType.Double;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DOUBLE;
          break;
        case TypeCode.Decimal:
          paramDbType = DbType.Decimal;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NUMERIC;
          break;
        case TypeCode.Object:
          paramDbType = DbType.Object;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
          break;
        case TypeCode.DBNull:
          //this.paramDbType = DbType.Object;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NULL;
          break;
        default:
          paramDbType = DbType.Object;
          paramCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
          break;
      }
    }
    /// <summary>
    ///   Sets the parameter's encoding property.
    /// </summary>
    public void SetParameterEncoding(Encoding encoding)
    {
      parameterEncoding = encoding;
    }

    /// <summary>
    ///   Returns the parameter's encoding property.
    /// </summary>
    /// <returns> The parameter's encoding property. </returns>
    public Encoding GetParameterEncoding()
    {
      return parameterEncoding;
    }

    /// <summary>
    ///   Returns a <see cref="System.String" /> that represents this parameter instance.
    /// </summary>
    /// <returns> A <see cref="System.String" /> that represents this parameter instance. </returns>
    public override string ToString()
    {
      return paramName;
    }

    #region ICloneable

    object ICloneable.Clone()
    {
      return Clone();
    }

    /// <summary>
    ///   Clones this instance.
    /// </summary>
    /// <returns> </returns>
    public CUBRIDParameter Clone()
    {
      CUBRIDParameter clone = new CUBRIDParameter(paramName, paramCUBRIDDataType, paramSize,
                                                  paramDirection, paramIsNullable, paramPrecision,
                                                  paramScale, paramSourceColumn, paramSourceVersion,
                                                  paramValue);
      return clone;
    }

    #endregion
  }
}