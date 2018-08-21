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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID implementation of the <see cref="T:System.Data.Common.DbDataReader" /> class.
  /// </summary>
  public sealed class CUBRIDDataReader : DbDataReader, IDataRecord
  {
    private  ColumnMetaData[] columnMetaData;
    private  int resultCount; //Resultset rows count; Equivalent to SELECT COUNT(*)...
    private  ResultTuple resultTuple;
    private CommandBehavior CommandBehavior; //TODO Add support
    private CUBRIDConnection conn;
    private int currentRow;
    internal int handle;
    private bool hasRows; //TODO Add support
    private bool isClosed;
    private const int recordsAffected = 1; //TODO Add support
    private CUBRIDCommand stmt;
    private int tupleCount; //Actually retrieved number of records in the pack; always <= resultCount
    private const int visibleFieldCount = 1; //TODO Add support
    private Dictionary<CUBRIDDataType, string> ColumnTypeName;

    /// <summary>
    ///  commandBehavior <see cref="T:System.Data.Common.DbDataReader" /> contains one or more rows.
    /// </summary>
    public CommandBehavior commandBehavior
    {
        get { return CommandBehavior; }
        set { CommandBehavior = value; }
    }
    private void InitColumnTypeName()
    {
        ColumnTypeName =
          new Dictionary<CUBRIDDataType, string>();

        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_CHAR, "CHAR");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_NCHAR, "NCHAR");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_STRING, "STRING");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_VARNCHAR, "VARCHAR");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_SHORT, "SHORT");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_INT, "INT");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_FLOAT, "FLOAT");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_DOUBLE, "DOUBLE");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_MONETARY, "MONETARY");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_NUMERIC, "NUMERIC");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_DATETIME, "DATETIME");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_DATE, "DATE");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_TIME, "TIME");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_TIMESTAMP, "TIMESTAMP");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_OBJECT, "OBJECT");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_BIT, "BIT");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_VARBIT, "VARBIT");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_SET, "SET");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_MULTISET, "MULTISET");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_SEQUENCE, "SEQUENCE");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_BLOB, "BLOB");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_CLOB, "CLOB");
        ColumnTypeName.Add(CUBRIDDataType.CCI_U_TYPE_ENUM, "ENUM");
    }
    /*
    internal CUBRIDDataReader(CUBRIDCommand stmt, int handle, int count, ColumnMetaData[] columnInfos)
    {
      this.stmt = stmt;
      conn = (CUBRIDConnection)stmt.Connection;
      this.handle = handle;
      resultCount = count;
      columnMetaData = columnInfos;
      currentRow = 0;
      resultTuple = new ResultTuple(columnInfos.Length);
      commandBehavior = CommandBehavior.Default;
      InitColumnTypeName();
    }
    */
    internal CUBRIDDataReader(CUBRIDCommand stmt, int handle, int count, ColumnMetaData[] columnInfos, int tupleCount)
    {
      this.stmt = stmt;
      conn = (CUBRIDConnection)stmt.Connection;
      this.handle = handle;
      resultCount = count;
      columnMetaData = columnInfos;
      currentRow = 0;
      this.tupleCount = tupleCount;
      resultTuple = new ResultTuple(columnInfos.Length);
      commandBehavior = CommandBehavior.Default;
      InitColumnTypeName();
    }

    /// <summary>
    ///   Gets a value that indicates whether this <see cref="T:System.Data.Common.DbDataReader" /> contains one or more rows.
    /// </summary>
    /// <returns> true if the <see cref="T:System.Data.Common.DbDataReader" /> contains one or more rows; otherwise false. </returns>
    public override bool HasRows
    {
      get
      {
        hasRows = (tupleCount > 0);
        return hasRows;
      }
    }

    /// <summary>
    ///   Gets the number of fields in the <see cref="T:System.Data.Common.DbDataReader" /> that are not hidden.
    /// </summary>
    /// <returns> The number of fields that are not hidden. </returns>
    public override int VisibleFieldCount
    {
      get { return visibleFieldCount; }
    }

    internal CUBRIDCommand Command
    {
      get { return stmt; }
    }

    #region IDataReader Members

    /// <summary>
    ///   This method is not supported; always returns 0.
    /// </summary>
    public override int Depth
    {
      get { return 0; }
    }

    /// <summary>
    ///   Gets the number of columns in the current row.
    /// </summary>
    /// <returns> The number of columns in the current row. </returns>
    public override int FieldCount
    {
      get { return GetColumnCount(); }
    }

    /// <summary>
    ///   Gets a value indicating whether the <see cref="T:System.Data.Common.DbDataReader" /> is closed.
    /// </summary>
    /// <returns> true if the <see cref="T:System.Data.Common.DbDataReader" /> is closed; otherwise false. </returns>
    public override bool IsClosed
    {
      get { return isClosed; }
    }

    /// <summary>
    ///   Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
    /// </summary>
    /// <returns> The number of rows changed, inserted, or deleted. -1 for SELECT statements; 0 if no rows were affected or the statement failed. </returns>
    public override int RecordsAffected
    {
      get { return recordsAffected; }
    }

    /// <summary>
    ///   Gets the value of the specified column as an instance of <see cref="T:System.Object" />.
    /// </summary>
    /// <returns> The value of the specified column. </returns>
    public override object this[int ordinal]
    {
      get { return GetObject(ordinal); }
    }

    /// <summary>
    ///   Gets the value of the specified column as an instance of <see cref="T:System.Object" />.
    /// </summary>
    /// <returns> The value of the specified column. </returns>
    public override object this[string name]
    {
      get { return GetObject(name); }
    }

    /// <summary>
    ///   Closes the <see cref="CUBRIDDataReader" /> object.
    /// </summary>
    public override void Close()
    {
      if (isClosed)
        return;

      bool shouldCloseConnection = (commandBehavior & CommandBehavior.CloseConnection) != 0;
      commandBehavior = CommandBehavior.Default;

      //Clear all remaining resultsets
      try
      {
        while (NextResult())
        {
        }
      }
      catch { }

      try
      {
          if (shouldCloseConnection)
          {
              conn.Close();
          }
          stmt.Close();
      }
      catch
      {
          //Do not propagate exceptions
      }
      finally
      {
          stmt = null;
          conn = null;
          isClosed = true;
      }
    }

    /// <summary>
    ///   Gets the value of the specified column as a Boolean.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override bool GetBoolean(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
      return Convert.ToBoolean(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a byte.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override byte GetByte(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        //byte[] b = (byte[])GetObject(ordinal);

        return Convert.ToByte(GetObject(ordinal));
    }

    /// <summary>
    ///   Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset" />, into the buffer, starting at the location indicated by <paramref
    ///    name="bufferOffset" />.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <param name="dataOffset"> The index within the row from which to begin the read operation. </param>
    /// <param name="buffer"> The buffer into which to copy the data. </param>
    /// <param name="bufferOffset"> The index with the buffer to which the data will be copied. </param>
    /// <param name="length"> The maximum number of characters to read. </param>
    /// <returns> The actual number of bytes read. </returns>
    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
      // [APIS-217] Check the index is correct.
      if (ordinal >= FieldCount || ordinal < 0)
        throw new IndexOutOfRangeException();

      if (currentRow > resultCount) //Are we at the end of the data?
      {
          throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
      }

      object val = GetValue(ordinal);
      string type = GetColumnTypeName(ordinal);

      if (type != "BLOB" && type != "CLOB" && type != "BIT" && type != "VARBIT")
        throw new CUBRIDException(Utils.GetStr(MsgId.GetBytesCanBeCalledOnlyOnBinaryColumns));

      // [APIS-217] Check the offset value is correct.
      if (dataOffset < 0)
        throw new IndexOutOfRangeException(Utils.GetStr(MsgId.DataOffsetMustBeValidPositionInField));

      // [APIS-217] If buffer is a null pointer, return 0.
      if (buffer == null)
        return 0;

      if (bufferOffset >= buffer.Length || bufferOffset < 0)
        throw new IndexOutOfRangeException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));

      if (buffer.Length < (bufferOffset + length))
        throw new ArgumentException(Utils.GetStr(MsgId.BufferNotLargeEnoughToHoldRequestedData));

      //[APIS-217] Does not determine the val is a NULL pointer.
      if (val == null)
        return 0;

      byte[] bytes;
      //[APIS-217] CUBRIDDataReader.GetBytes, threw an exception.

      if (type == "BIT" || type == "VARBIT")
      {
          bytes = (byte[])GetObject(ordinal);
          Debug.Assert(bytes != null, "bit != null");
          if (dataOffset < bytes.Length && dataOffset + length <= bytes.Length)
          { 
            for (long i = 0; i < length; i++) buffer[i + bufferOffset] = bytes[i + dataOffset];
          }
          else
          {
            throw new IndexOutOfRangeException(Utils.GetStr(MsgId.DataOffsetMustBeValidPositionInField));
          }

          return length;
      }

      if (type == "BLOB")
      {
        CUBRIDBlob blob = val as CUBRIDBlob;
        // [APIS-217] Check the offset value is correct.
        Debug.Assert(blob != null, "blob != null");
        if (dataOffset < blob.BlobLength && dataOffset + length <= blob.BlobLength)
        {
          bytes = blob.GetBytes(dataOffset + 1, length);
        }
        else
        {
          throw new IndexOutOfRangeException(Utils.GetStr(MsgId.DataOffsetMustBeValidPositionInField));
        }
      }
      else // if it is a CLOB type.
      {
        CUBRIDClob clob = val as CUBRIDClob;
        Debug.Assert(clob != null, "clob != null");
        if (dataOffset < clob.ClobLength && dataOffset + length <= clob.ClobLength)
        {
          bytes = conn.GetEncoding().GetBytes(clob.GetString(dataOffset + 1, length));
        }
        else
        {
          throw new IndexOutOfRangeException(Utils.GetStr(MsgId.DataOffsetMustBeValidPositionInField));
        }
      }

      dataOffset = 0;

      Buffer.BlockCopy(bytes, (int)dataOffset, buffer, bufferOffset, length);

      return length;
    }

    /// <summary>
    ///   Gets the value of the specified column as a single character.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override char GetChar(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToChar(GetObject(ordinal));
    }

    /// <summary>
    ///   Reads a stream of characters from the specified column, starting at location indicated by <paramref name="ordinal" />, into the buffer, starting at the location indicated by <paramref
    ///    name="dataOffset" />.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <param name="dataOffset"> The index within the row from which to begin the read operation. </param>
    /// <param name="buffer"> The buffer into which to copy the data. </param>
    /// <param name="bufferOffset"> The index with the buffer to which the data will be copied. </param>
    /// <param name="length"> The maximum number of characters to read. </param>
    /// <returns> The actual number of characters read. </returns>
    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
      if (ordinal >= FieldCount)
        throw new IndexOutOfRangeException();

      string valAsString = GetString(ordinal);

      if (bufferOffset >= buffer.Length || bufferOffset < 0)
        throw new IndexOutOfRangeException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));

      if (buffer.Length < (bufferOffset + length))
        throw new ArgumentException(Utils.GetStr(MsgId.BufferNotLargeEnoughToHoldRequestedData));

      if (dataOffset < 0 || dataOffset >= valAsString.Length)
        throw new IndexOutOfRangeException(Utils.GetStr(MsgId.DataOffsetMustBeValidPositionInField));

      if (valAsString.Length < length)
        length = valAsString.Length;

      valAsString.CopyTo((int)dataOffset, buffer, bufferOffset, length);

      return length;
    }

    /// <summary>
    ///   Gets name of the data type of the specified column.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> A string representing the name of the data type. </returns>
    public override string GetDataTypeName(int ordinal)
    {
      return GetColumnTypeName(ordinal);
    }

    /// <summary>
    ///   Gets the data type of the specified column.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The data type of the specified column. </returns>
    public override Type GetFieldType(int ordinal)
    {
      return GetColumnType(ordinal);
    }

    /// <summary>
    ///   Gets the value of the specified column as a <see cref="T:System.DateTime" /> object.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override DateTime GetDateTime(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToDateTime(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a <see cref="T:System.Decimal" /> object.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override decimal GetDecimal(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToDecimal(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a double-precision floating point number.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override double GetDouble(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToDouble(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a single-precision floating point number.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override float GetFloat(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToSingle(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a globally-unique identifier (GUID).
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override Guid GetGuid(int ordinal)
    {
      object v = GetObject(ordinal);

      if (v is Guid)
      {
        return (Guid)v;
      }

      if (v is string)
      {
        return new Guid(v as string);
      }

      var bytes = v as byte[];
      if (bytes != null)
      {
        if (bytes.Length == 16)
        {
          return new Guid(bytes);
        }
      }

      throw new ArgumentException();
    }

    /// <summary>
    ///   Gets the value of the specified column as a 16-bit signed integer.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override short GetInt16(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToInt16(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a 32-bit signed integer.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override int GetInt32(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToInt32(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the value of the specified column as a 64-bit signed integer.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override long GetInt64(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        GetObject(ordinal);

        return Convert.ToInt64(GetObject(ordinal));
    }

    /// <summary>
    ///   Gets the name of the column, given the zero-based column ordinal.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The name of the specified column. </returns>
    public override string GetName(int ordinal)
    {
      if (isClosed)
        throw new CUBRIDException(Utils.GetStr(MsgId.ResultsetIsClosed));

      if (ordinal < 0 || ordinal >= FieldCount)
        throw new IndexOutOfRangeException();

      return GetColumnName(ordinal);
    }

    /// <summary>
    ///   Gets the column ordinal given the name of the column.
    /// </summary>
    /// <param name="name"> The name of the column. </param>
    /// <returns> The zero-based column ordinal. </returns>
    /// <exception cref="T:System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
    public override int GetOrdinal(string name)
    {
      if (isClosed)
        throw new CUBRIDException(Utils.GetStr(MsgId.ResultsetIsClosed));

      for (int i = 0; i < columnMetaData.Length; i++)
      {
        if (columnMetaData[i].Name.Equals(name, StringComparison.InvariantCultureIgnoreCase))
          return i;
      }

      throw new ArgumentException();
    }

    /// <summary>
    ///   Gets the value of the specified column as an instance of <see cref="T:System.String" />.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override string GetString(int ordinal)
    {
        if (currentRow > resultCount) //Are we at the end of the data?
        {
            throw new InvalidOperationException(Utils.GetStr(MsgId.BufferIndexMustBeValidIndexInBuffer));
        }
        return Convert.ToString(GetObject(ordinal));
    }

    /// <summary>
    ///   Returns a <see cref="T:System.Data.DataTable" /> that describes the column metadata of the <see
    ///    cref="T:System.Data.Common.DbDataReader" />.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.DataTable" /> that describes the column metadata. </returns>
    public override DataTable GetSchemaTable()
    {
      if (FieldCount == 0)
        return null;

      DataTable dataTableSchema = new DataTable("SchemaTable");

      //http://msdn.microsoft.com/en-us/library/system.data.datatablereader.getschematable.aspx
      /*
      ColumnName	The name of the column; this might not be unique. If the column name cannot be determined, a null value is returned. This name always reflects the most recent naming of the column in the current view or command text.
      ColumnOrdinal	The ordinal of the column. This is zero for the bookmark column of the row, if any. Other columns are numbered starting with 1. This column cannot contain a null value.
      ColumnSize	The maximum possible length of a value in the column. For columns that use a fixed-length data type, this is the size of the data type.
      NumericPrecision	If DbType is a numeric data type, this is the maximum precision of the column. The precision depends on the definition of the column. If DbType is not a numeric data type, this is a null value.
      NumericScale	If DbType is Decimal, the number of digits to the right of the decimal point. Otherwise, this is a null value.
      DataType	Maps to the .Net Framework type of the column.
      ProviderType	The indicator of the column's data type.
      IsLong	true if the column contains a BLOB that contains very long data.
      AllowDBNull	true if the consumer can set the column to a null value. Otherwise, false. A column may contain null values, even if it cannot be set to a null value.
      IsReadOnly	true if the column can not be modified; otherwise, false.
      IsRowVersion	Set if the column contains a persistent row identifier that cannot be written to, and has no meaningful value except to identity the row.
      IsUnique	true: No two rows in the base table-the table returned in BaseTableName-can have the same value in this column. IsUnique is guaranteed to be true if the column constitutes a key by itself or if there is a constraint of type UNIQUE that applies only to this column. false: The column can contain duplicate values in the base table.The default for this column is false.
      IsKey	true: The column is one of a set of columns in the rowset that, taken together, uniquely identify the row. The set of columns with IsKey set to true must uniquely identify a row in the rowset. There is no requirement that this set of columns is a minimal set of columns. This set of columns may be generated from a base table primary key, a unique constraint or a unique index. false: The column is not required to uniquely identify the row.
      IsAutoIncrement	true if the column assigns values to new rows in fixed increments; otherwise, false. The default value for this column is false.
      BaseSchemaName	The name of the schema in the database that contains the column. NULL if the base catalog name cannot be determined. The default for this column is a null value.
      BaseCatalogName	The name of the catalog in the data store that contains the column. NULL if the base catalog name cannot be determined. The default value for this column is a null value.
      BaseTableName	The name of the table or view in the data store that contains the column. A null value if the base table name cannot be determined. The default value of this column is a null value.
      BaseColumnName	The name of the column in the data store. This might be different than the column name returned in the ColumnName column if an alias was used. A null value if the base column name cannot be determined or if the rowset column is derived, but not identical to, a column in the data store. The default value for this column is a null value.
      IsAliased	true if the column name is an alias; otherwise, false.
      IsExpression	true if the column is an expression; otherwise, false.
      */

      dataTableSchema.Columns.Add("ColumnName", typeof(string));
      dataTableSchema.Columns.Add("ColumnOrdinal", typeof(int));
      dataTableSchema.Columns.Add("ColumnSize", typeof(int));
      dataTableSchema.Columns.Add("NumericPrecision", typeof(int));
      dataTableSchema.Columns.Add("NumericScale", typeof(int));
      dataTableSchema.Columns.Add("IsUnique", typeof(bool));
      dataTableSchema.Columns.Add("IsReverseUnique", typeof(bool));
      dataTableSchema.Columns.Add("IsKey", typeof(bool));
      DataColumn dc = dataTableSchema.Columns["IsKey"];
      dc.AllowDBNull = false;
      dataTableSchema.Columns.Add("IsForeignKey", typeof(bool));
      dataTableSchema.Columns.Add("IsPrimaryKey", typeof(bool));
      dataTableSchema.Columns.Add("IsShared", typeof(bool));
      dataTableSchema.Columns.Add("BaseCatalogName", typeof(string));
      dataTableSchema.Columns.Add("BaseColumnName", typeof(string));
      dataTableSchema.Columns.Add("BaseSchemaName", typeof(string));
      dataTableSchema.Columns.Add("BaseTableName", typeof(string));
      dataTableSchema.Columns.Add("DataType", typeof(Type));
      dataTableSchema.Columns.Add("ProviderType", typeof(CUBRIDDataType));
      dataTableSchema.Columns.Add("AllowDBNull", typeof(bool));
      dataTableSchema.Columns.Add("DefaultValue", typeof(string));
      dataTableSchema.Columns.Add("IsAliased", typeof(bool));
      dataTableSchema.Columns.Add("IsExpression", typeof(bool));
      dataTableSchema.Columns.Add("IsIdentity", typeof(bool));
      dataTableSchema.Columns.Add("IsAutoIncrement", typeof(bool));
      dataTableSchema.Columns.Add("IsRowVersion", typeof(bool));
      dataTableSchema.Columns.Add("IsHidden", typeof(bool));
      dataTableSchema.Columns.Add("IsLong", typeof(bool));
      dataTableSchema.Columns.Add("IsReadOnly", typeof(bool));

      for (int i = 0; i < FieldCount; i++)
      {
        ColumnMetaData columnMetadata = columnMetaData[i];

        DataRow row = dataTableSchema.NewRow();

        row["ColumnName"] = columnMetadata.Name;
        row["ColumnOrdinal"] = i + 1;
        row["ColumnSize"] = columnMetadata.Precision;
        int precision = columnMetadata.Precision;
        if (precision != -1 && Int16.MaxValue > precision)
          row["NumericPrecision"] = Convert.ToInt16(precision);
        int scale = columnMetadata.Scale;
        if (scale != -1 && Int16.MaxValue > scale)
          row["NumericScale"] = Convert.ToInt16(scale);

        row["DataType"] = GetFieldType(i);
        row["ProviderType"] = columnMetadata.Type;

        row["AllowDBNull"] = columnMetadata.IsNullable;

        row["DefaultValue"] = columnMetadata.DefaultValue;

        row["IsUnique"] = columnMetadata.IsUniqueKey;
        row["IsReverseUnique"] = columnMetadata.IsReverseUnique;
        row["IsKey"] = columnMetadata.IsPrimaryKey;
        row["IsAutoIncrement"] = columnMetadata.IsAutoIncrement;
        row["IsForeignKey"] = columnMetadata.IsForeignKey;
        row["IsPrimaryKey"] = columnMetadata.IsPrimaryKey;
        row["IsShared"] = columnMetadata.IsShared;

        row["BaseCatalogName"] = null;
        row["BaseSchemaName"] = null;

        row["BaseColumnName"] = columnMetadata.Name;
        row["BaseTableName"] = columnMetadata.Table;

        row["IsAliased"] = false;
        row["IsExpression"] = false;
        row["IsIdentity"] = false;
        row["IsRowVersion"] = false;
        row["IsHidden"] = false;
        row["IsLong"] = false;
        //TODO True if the data type of the column is String and its MaxLength property is -1. Otherwise, false.
        row["IsReadOnly"] = false;

        dataTableSchema.Rows.Add(row);
      }

      return dataTableSchema;
    }

    /// <summary>
    ///   Gets the value of the specified column as an instance of <see cref="T:System.Object" />.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public override object GetValue(int ordinal)
    {
      return GetObject(ordinal);
    }

    /// <summary>
    ///   Gets all attribute columns in the collection for the current row.
    /// </summary>
    /// <param name="values"> An array of <see cref="T:System.Object" /> into which to copy the attribute columns. </param>
    /// <returns> The number of instances of <see cref="T:System.Object" /> in the array. </returns>
    public override int GetValues(object[] values)
    {
      int columnsCount = Math.Min(values.Length, FieldCount);

      for (int i = 0; i < columnsCount; i++)
      {
        values[i] = GetValue(i);
      }

      return columnsCount;
    }

    IDataReader IDataRecord.GetData(int i)
    {
      return GetData(i);
    }

    /// <summary>
    ///   Gets a value that indicates whether the column contains nonexistent or missing values.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> true if the specified column is equivalent to <see cref="T:System.DBNull" /> ; otherwise false. </returns>
    public override bool IsDBNull(int ordinal)
    {
      return DBNull.Value == GetValue(ordinal);
      //return IsNull(ordinal);
    }

    /// <summary>
    ///   Advances the reader to the next result when reading the results of a batch of statements.
    /// </summary>
    /// <returns> true if there are more result sets; otherwise false. </returns>
    public override bool NextResult()
    {
      if (isClosed)
      {
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidAttemptToReadDataWhenReaderNotOpen));
      }

      T_CCI_ERROR err = new T_CCI_ERROR();
      bool bRet = false;
      resultCount = CciInterface.cci_next_result(handle, ref err);
      if (resultCount >= 0)
      {
          columnMetaData = CciInterface.cci_get_result_info(conn, handle);
          currentRow = 0;
          resultTuple = new ResultTuple(columnMetaData.Length);
          commandBehavior = CommandBehavior.Default;
          bRet = true;
      }

      return bRet;
    }

    internal T_CCI_ERROR_CODE ReadResultTuple()
	{
	  resultTuple.Index = currentRow-1;
	  resultTuple.Oid = null;
	  T_CCI_ERROR err = new T_CCI_ERROR();

	  int res = CciInterface.cci_cursor(handle, 1, CCICursorPosition.CCI_CURSOR_CURRENT, ref err);
	  if (res == (int)T_CCI_ERROR_CODE.CCI_ER_NO_MORE_DATA)
	  {
          return T_CCI_ERROR_CODE.CCI_ER_NO_MORE_DATA;
	  }
	  if (res < 0)
	  {
	    throw new CUBRIDException(err.err_msg);
	  }

	  if ((res = CciInterface.cci_fetch(handle, ref err)) < 0)
	  {
	    throw new CUBRIDException(err.err_msg);
	  }

	  for (int i = 1; i <= columnMetaData.Length; i++)
	  {
	    res = CciInterface.cci_get_data(resultTuple, handle, i, (int)columnMetaData[i-1].Type,conn);
        if (columnMetaData[i - 1].Name == null)
        {
            columnMetaData[i - 1].Name = i.ToString();
        }
	    resultTuple[columnMetaData[i - 1].Name] = resultTuple[i - 1];
	  }
      return T_CCI_ERROR_CODE.CCI_ER_NO_ERROR;
	}
    /// <summary>
    ///   Advances the reader to the next record in a result set.
    /// </summary>
    /// <returns> true if there are more rows; otherwise false. </returns>
    public override bool Read()
    {
      if (isClosed)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidAttemptToReadDataWhenReaderNotOpen));

      currentRow++;

      if (commandBehavior == CommandBehavior.SingleRow && currentRow>1 )
      {
          return false;
      }
      if (currentRow > resultCount) //Are we at the end of the data?
      {
        return false;
      }

      //Save current tuple count (Remember: tuple count is the number of all tuples fetched so far from the server)
      T_CCI_ERROR_CODE err = ReadResultTuple();
      if (T_CCI_ERROR_CODE.CCI_ER_NO_ERROR != err)
      {
        return false;
      }
      return true;
    }

    #endregion
    /// <summary>
    ///   Releases the managed resources used by the <see cref="T:System.Data.Common.DbDataReader" /> and optionally releases the unmanaged resources.
    /// </summary>
    /// <param name="disposing"> true to release managed and unmanaged resources; false to release only unmanaged resources. </param>
    protected override void Dispose(bool disposing)
    {
      Close();
    }

    /// <summary>
    ///   Gets the date in format yyyy-MM-dd.
    /// </summary>
    /// <param name="ordinal"> The column ordinal. </param>
    /// <returns> </returns>
    public string GetDate(int ordinal)
    {
      DateTime dt = Convert.ToDateTime(GetObject(ordinal));
      return dt.ToString("yyyy-MM-dd");
    }

    /// <summary>
    ///   Gets the date.
    /// </summary>
    /// <param name="ordinal"> The column ordinal. </param>
    /// <param name="dateFormat"> The date format. </param>
    /// <returns> </returns>
    public string GetDate(int ordinal, string dateFormat)
    {
      DateTime dt = Convert.ToDateTime(GetObject(ordinal));
      return dt.ToString(dateFormat, CultureInfo.InvariantCulture);
    }

    /// <summary>
    ///   Gets the time in format HH:mm:ss.
    /// </summary>
    /// <param name="ordinal"> The column ordinal. </param>
    /// <returns> </returns>
    public string GetTime(int ordinal)
    {
      DateTime dt = Convert.ToDateTime(GetObject(ordinal));
      return dt.ToString("HH:mm:ss");
    }

    /// <summary>
    ///   Gets the time.
    /// </summary>
    /// <param name="ordinal"> The column ordinal. </param>
    /// <param name="timeFormat"> The time format. </param>
    /// <returns> </returns>
    public string GetTime(int ordinal, string timeFormat)
    {
      DateTime dt = Convert.ToDateTime(GetObject(ordinal));
      return dt.ToString(timeFormat);
    }

    /// <summary>
    ///   Gets the timestamp in format yyyy-MM-dd HH:mm:ss.SSS.
    /// </summary>
    /// <param name="ordinal"> The column ordinal. </param>
    /// <returns> </returns>
    public string GetTimestamp(int ordinal)
    {
      DateTime dt = Convert.ToDateTime(GetObject(ordinal));
      return dt.ToString("yyyy-MM-dd HH:mm:ss.fff");
    }

    /// <summary>
    ///   Gets the timestamp.
    /// </summary>
    /// <param name="ordinal"> The column ordinal. </param>
    /// <param name="timestampFormat"> The timestamp format. </param>
    /// <returns> </returns>
    public string GetTimestamp(int ordinal, string timestampFormat)
    {
      DateTime dt = Convert.ToDateTime(GetObject(ordinal));
      return dt.ToString(timestampFormat);
    }

    /// <summary>
    ///   Gets the value of the specified column as a 16-bit signed integer.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public short GetShort(int ordinal)
    {
      return GetInt16(ordinal);
    }

    /// <summary>
    ///   Gets the value of the specified column as a 32-bit signed integer.
    /// </summary>
    /// <param name="ordinal"> The zero-based column ordinal. </param>
    /// <returns> The value of the specified column. </returns>
    public int GetInt(int ordinal)
    {
      return GetInt32(ordinal);
    }

    /// <summary>
    ///   Gets the object value for the column.
    /// </summary>
    /// <param name="name"> The column name. </param>
    /// <returns> </returns>
    public object GetObject(string name)
    {
      return resultTuple[name];
    }

    /// <summary>
    ///   Gets the CUBRID OID.
    /// </summary>
    /// <returns> </returns>
    public CUBRIDOid GetOid()
    {
      return resultTuple.Oid;
    }

    /// <summary>
    ///   Gets the column value as array.
    /// </summary>
    /// <param name="index"> The index of the column. </param>
    /// <returns> </returns>
    public object[] GetArray(int index)
    {
      return (object[])GetObject(index);
    }

    /// <summary>
    ///   Gets the value of the specified column as an instance of <see cref="T:System.String" />.
    /// </summary>
    /// <param name="name"> The name of the column. </param>
    /// <returns> </returns>
    public string GetString(string name)
    {
      return Convert.ToString(GetObject(name));
    }

    /// <summary>
    ///   Gets the data reader.
    /// </summary>
    /// <param name="ordinal"> The ordinal. </param>
    /// <returns> </returns>
    public DbDataReader GetDataReader(int ordinal)
    {
      return (CUBRIDDataReader)GetObject(ordinal);
    }

    /// <summary>
    ///   Returns an <see cref="T:System.Collections.IEnumerator" /> that can be used to iterate through the rows in the data reader.
    /// </summary>
    /// <returns> An <see cref="T:System.Collections.IEnumerator" /> that can be used to iterate through the rows in the data reader. </returns>
    public override IEnumerator GetEnumerator()
    {
      return new DbEnumerator(this, (commandBehavior & CommandBehavior.CloseConnection) != 0);
    }

    /// <summary>
    ///   Gets the object value for the column.
    /// </summary>
    /// <param name="index"> The column index. </param>
    /// <returns> </returns>
    public object GetObject(int index)
    {
      if (index < 0 || index >= FieldCount)
        throw new IndexOutOfRangeException();

      return resultTuple[index];
    }

    /// <summary>
    ///   Gets the name of the column.
    /// </summary>
    /// <param name="index"> The column index. </param>
    /// <returns> The column name </returns>
    public string GetColumnName(int index)
    {
      if (index < 0 || index >= FieldCount)
        throw new IndexOutOfRangeException();

      return columnMetaData[index].Name;
    }

    /// <summary>
    ///   Gets the name of the column type.
    /// </summary>
    /// <param name="index"> The column index. </param>
    /// <returns> </returns>
    public string GetColumnTypeName(int index)
    {
      if (index < 0 || index >= FieldCount)
        throw new IndexOutOfRangeException();

      return ColumnTypeName[columnMetaData[index].Type];
    }

    /// <summary>
    ///   Gets the type of the column.
    /// </summary>
    /// <param name="index"> The column index. </param>
    /// <returns> </returns>
    public Type GetColumnType(int index)
    {
      if (index >= FieldCount)
        throw new IndexOutOfRangeException();

      switch (columnMetaData[index].Type)
      {
        case CUBRIDDataType.CCI_U_TYPE_CHAR:
        case CUBRIDDataType.CCI_U_TYPE_NCHAR:
        case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
        case CUBRIDDataType.CCI_U_TYPE_STRING:
        case CUBRIDDataType.CCI_U_TYPE_ENUM:
          return typeof(String);

        case CUBRIDDataType.CCI_U_TYPE_SHORT:
          return typeof(Int16);

        case CUBRIDDataType.CCI_U_TYPE_INT:
          return typeof(Int32);

        case CUBRIDDataType.CCI_U_TYPE_BIGINT:
          return typeof(Int64);

        case CUBRIDDataType.CCI_U_TYPE_FLOAT:
          return typeof(float);

        case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
        case CUBRIDDataType.CCI_U_TYPE_MONETARY:
          return typeof(Double);

        case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
          return typeof(Decimal);

        case CUBRIDDataType.CCI_U_TYPE_DATE:
          return typeof(DateTime);

        // --- Added missing CUBRIDDateType:
        case CUBRIDDataType.CCI_U_TYPE_DATETIME:
          return typeof(DateTime);
        // ---

        case CUBRIDDataType.CCI_U_TYPE_TIME:
          return typeof(DateTime);

        case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
          return typeof(DateTime);

        case CUBRIDDataType.CCI_U_TYPE_RESULTSET:
        case CUBRIDDataType.CCI_U_TYPE_OBJECT:
          return typeof(Object);

        case CUBRIDDataType.CCI_U_TYPE_BIT:
        case CUBRIDDataType.CCI_U_TYPE_VARBIT:
          return typeof(Byte[]);

        case CUBRIDDataType.CCI_U_TYPE_SET:
        case CUBRIDDataType.CCI_U_TYPE_MULTISET:
        case CUBRIDDataType.CCI_U_TYPE_SEQUENCE:
          return typeof(Object[]);

        case CUBRIDDataType.CCI_U_TYPE_CLOB:
          return typeof(CUBRIDClob);

        case CUBRIDDataType.CCI_U_TYPE_BLOB:
          return typeof(CUBRIDBlob);

        default:
          return null;
      }
    }

    /// <summary>
    ///   Gets the column count.
    /// </summary>
    /// <returns> </returns>
    public int GetColumnCount()
    {
      return columnMetaData.Length;
    }
  }
}