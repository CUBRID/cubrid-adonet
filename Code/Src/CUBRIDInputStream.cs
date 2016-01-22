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
using System.Net;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
  internal partial class CUBRIDStream
  {
    private int readCursor;
    private byte[] responseBuffer;
    private int responseBufferCapacity;
    private int responseCode;

    internal int ResponseCode
    {
      get { return responseCode; }
    }

    internal int Receive()
    {
      readCursor = 0;

      //Read response length
      byte[] b = new byte[DATA_LENGTH_SIZEOF];
      baseStream.Read(b, 0, DATA_LENGTH_SIZEOF);
      int response_length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(b, 0));

      //Read CAS info
      baseStream.Read(casInfo, 0, CAS_INFO_SIZE);

      responseBufferCapacity = response_length;
      responseBuffer = new byte[responseBufferCapacity];

      FillBuffer();

      responseCode = ReadInt();
      if (responseCode < 0)
      {
        ReadInt();
        string errMsg = ReadString(responseBufferCapacity - readCursor, Encoding.Default);

        throw new CUBRIDException(errMsg);
      }

      return responseCode;
    }

    private void FillBuffer()
    {
      int read = 0;

      while (read < responseBufferCapacity)
      {
        int r = baseStream.Read(responseBuffer, read, responseBufferCapacity - read);
        if (r == 0)
        {
          responseBufferCapacity = read;
          break;
        }

        read += r;
      }
    }

    internal byte ReadByte()
    {
      if (readCursor >= responseBufferCapacity)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidBufferPosition));

      return responseBuffer[readCursor++];
    }

    internal void ReadBytes(byte[] value, int offset, int length)
    {
      if (value == null)
        return;

      if (readCursor + length > responseBufferCapacity)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidBufferPosition));

      Array.Copy(responseBuffer, readCursor, value, offset, length);
      readCursor += length;
    }

    internal void ReadBytes(byte[] value)
    {
      ReadBytes(value, 0, value.Length);
    }

    internal byte[] ReadBytes(int size)
    {
      byte[] value = new byte[size];
      ReadBytes(value, 0, size);

      return value;
    }

    internal short ReadShort()
    {
      if (readCursor + SHORT_SIZEOF > responseBufferCapacity)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidBufferPosition));

      short value = IPAddress.NetworkToHostOrder(BitConverter.ToInt16(responseBuffer, readCursor));
      readCursor += SHORT_SIZEOF;

      return value;
    }

    internal int ReadInt()
    {
      if (readCursor + INT_SIZEOF > responseBufferCapacity)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidBufferPosition));

      int value = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(responseBuffer, readCursor));
      readCursor += INT_SIZEOF;

      return value;
    }

    internal long ReadLong()
    {
      if (readCursor + LONG_SIZEOF > responseBufferCapacity)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidBufferPosition));

      long value = IPAddress.NetworkToHostOrder(BitConverter.ToInt64(responseBuffer, readCursor));
      readCursor += LONG_SIZEOF;

      return value;
    }

    internal float ReadFloat()
    {
      return BitConverter.ToSingle(BitConverter.GetBytes(ReadInt()), 0);
    }

    internal double ReadDouble()
    {
      return BitConverter.Int64BitsToDouble(ReadLong());
    }

    internal string ReadString(int size, Encoding encoding)
    {
      if (size <= 0)
        return null;

      if (readCursor + size > responseBufferCapacity)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidBufferPosition));

      string value = encoding.GetString(responseBuffer, readCursor, size - 1);
      readCursor += size;

      return value;
    }

    internal DateTime ReadDate()
    {
      int year = ReadShort();
      int month = ReadShort();
      int day = ReadShort();

      return new DateTime(year, month, day);
    }

    internal DateTime ReadTime()
    {
      int hour = ReadShort();
      int min = ReadShort();
      int sec = ReadShort();

      //DateTime(year, month, day, hour, min, sec) acceptable ranges are
      //year 1-9999
      //month 1-12
      //day 1-number of days in month
      //We need to convert CUBRID [yyy,mm,dd] return values to [1,1,1]
      return new DateTime(1, 1, 1, hour, min, sec);
    }

    internal DateTime ReadDateTime()
    {
      int year = ReadShort();
      int month = ReadShort();
      int day = ReadShort();

      int hour = ReadShort();
      int min = ReadShort();
      int sec = ReadShort();
      int millisec = ReadShort();

      return new DateTime(year, month, day, hour, min, sec, millisec);
    }

    internal DateTime ReadTimestamp()
    {
      int year = ReadShort();
      int month = ReadShort();
      int day = ReadShort();

      int hour = ReadShort();
      int min = ReadShort();
      int sec = ReadShort();

      return new DateTime(year, month, day, hour, min, sec);
    }

    internal CUBRIDOid ReadOid()
    {
      byte[] oid = ReadBytes(OID_SIZEOF);

      return new CUBRIDOid(oid);
    }

    internal int RemainedCapacity()
    {
      //position -> readCursor
      //capacity -> responseBufferCapacity
      return responseBufferCapacity - readCursor;
    }

    internal int ReadIntFromRaw()
    {
      byte[] b = new byte[4];
      baseStream.Read(b, 0, 4);

      return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(b, 0));
    }

    internal ResultInfo[] ReadResultInfo(int resultCount)
    {
      ResultInfo[] resultInfos = new ResultInfo[resultCount];

      for (int i = 0; i < resultCount; i++)
      {
        resultInfos[i] = new ResultInfo();

        resultInfos[i].StmtType = (CUBRIDStatementType)ReadByte();
        resultInfos[i].ResultCount = ReadInt();
        resultInfos[i].Oid = ReadOid();
        resultInfos[i].CacheTimeSec = ReadInt();
        resultInfos[i].CacheTimeUsec = ReadInt();
      }

      return resultInfos;
    }

    internal ColumnMetaData[] ReadColumnInfo(int count, bool extended_info = true)
    {
      ColumnMetaData[] infoArray = new ColumnMetaData[count];

      for (int i = 0; i < count; i++)
      {
        ColumnMetaData info = new ColumnMetaData();

        info.Type = (CUBRIDDataType)ReadByte();
        info.Scale = ReadShort();
        info.Precision = ReadInt();
        int len = ReadInt();
        info.Name = ReadString(len, Encoding.Default);

        if (extended_info)
        {
          len = ReadInt();
          info.RealName = ReadString(len, Encoding.Default);
          len = ReadInt();
          info.Table = ReadString(len, Encoding.Default);
          info.IsNullable = (ReadByte() == 0);
          len = ReadInt();
          info.DefaultValue = ReadString(len, Encoding.Default);
          info.IsAutoIncrement = (ReadByte() == 1);
          info.IsUniqueKey = (ReadByte() == 1);
          info.IsPrimaryKey = (ReadByte() == 1);
          info.IsReverseIndex = (ReadByte() == 1);
          info.IsReverseUnique = (ReadByte() == 1);
          info.IsForeignKey = (ReadByte() == 1);
          info.IsShared = (ReadByte() == 1);
        }

        infoArray[i] = info;
      }

      return infoArray;
    }

    internal void ReadSchemaProviderResultTuple(ResultTuple tuple, ColumnMetaData[] columnInfos, CUBRIDConnection conn)
    {
      tuple.Index = ReadInt();
      tuple.Oid = ReadOid();

      for (int j = 0; j < columnInfos.Length; j++)
      {
        int size = ReadInt();
        object val;

        if (size <= 0)
        {
          val = null;
        }
        else
        {
          CUBRIDDataType type = columnInfos[j].Type;
          val = ReadValue(j, type, size, conn);
        }

        tuple[j] = val;
        tuple[columnInfos[j].Name] = val;
      }
    }

    internal void ReadResultTuple(ResultTuple tuple, ColumnMetaData[] columnInfos, CUBRIDStatementType stmtType,
                                  CUBRIDConnection conn)
    {
      tuple.Index = ReadInt();
      tuple.Oid = ReadOid();

      for (int j = 0; j < columnInfos.Length; j++)
      {
        int size = ReadInt();
        object val;

        if (size <= 0)
        {
          val = null;
        }
        else
        {
          CUBRIDDataType type;

          if (stmtType == CUBRIDStatementType.CUBRID_STMT_CALL ||
              stmtType == CUBRIDStatementType.CUBRID_STMT_EVALUATE ||
              stmtType == CUBRIDStatementType.CUBRID_STMT_CALL_SP ||
              columnInfos[j].Type == CUBRIDDataType.CCI_U_TYPE_NULL)
          {
            type = (CUBRIDDataType)ReadByte();
            size--;
          }
          else
          {
            type = columnInfos[j].Type;
          }

          val = ReadValue(j, type, size, conn);
        }

        tuple[j] = val;
        tuple[columnInfos[j].Name] = val;
      }
    }

    internal void ReadResultTupleSP(ResultTuple tuple, int colCount, CUBRIDConnection conn)
    {
      tuple.Index = ReadInt();
      tuple.Oid = ReadOid();

      for (int i = 0; i < colCount; i++)
      {
        int size = ReadInt();
        object val;

        if (size <= 0)
        {
          val = null;
        }
        else
        {
          CUBRIDDataType type = (CUBRIDDataType)ReadByte();
          size--;

          val = ReadValue(i, type, size, conn);
        }

        tuple[i] = val;
      }
    }

    internal object ReadValue(int index, CUBRIDDataType type, int size, CUBRIDConnection conn)
    {
      switch (type)
      {
        case CUBRIDDataType.CCI_U_TYPE_CHAR:
        case CUBRIDDataType.CCI_U_TYPE_NCHAR:
        case CUBRIDDataType.CCI_U_TYPE_STRING:
        case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
        case CUBRIDDataType.CCI_U_TYPE_ENUM:
          return ReadString(size, conn.GetEncoding());

        case CUBRIDDataType.CCI_U_TYPE_SHORT:
          return ReadShort();

        case CUBRIDDataType.CCI_U_TYPE_INT:
          return ReadInt();

        case CUBRIDDataType.CCI_U_TYPE_BIGINT:
          return ReadLong();

        case CUBRIDDataType.CCI_U_TYPE_FLOAT:
          return ReadFloat();

        case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
        case CUBRIDDataType.CCI_U_TYPE_MONETARY:
          return ReadDouble();

        case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
          //return new BigDecimal(inBuffer.readString(dataSize, UJCIManager.sysCharsetName));
          //http://stackoverflow.com/questions/2863388/what-is-the-equivalent-of-the-java-bigdecimal-class-in-c
          string str = ReadString(size, conn.GetEncoding());
          decimal num = Decimal.Parse(str, CultureInfo.InvariantCulture);
          return num;

        case CUBRIDDataType.CCI_U_TYPE_DATE:
          return ReadDate();

        case CUBRIDDataType.CCI_U_TYPE_TIME:
          return ReadTime();

        case CUBRIDDataType.CCI_U_TYPE_DATETIME:
          return ReadDateTime();

        case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
          return ReadTimestamp();

        case CUBRIDDataType.CCI_U_TYPE_OBJECT:
          return ReadOid();

        case CUBRIDDataType.CCI_U_TYPE_BIT:
        case CUBRIDDataType.CCI_U_TYPE_VARBIT:
          return ReadBytes(size);

        case CUBRIDDataType.CCI_U_TYPE_SET:
        case CUBRIDDataType.CCI_U_TYPE_MULTISET:
        case CUBRIDDataType.CCI_U_TYPE_SEQUENCE:
          CUBRIDDataType baseType = (CUBRIDDataType)ReadByte();
          int count = ReadInt();
          object[] seq = new object[count];

          for (int i = 0; i < count; i++)
          {
            int elesize = ReadInt();
            if (elesize <= 0)
            {
              seq[i] = null;
            }
            else
            {
              seq[i] = ReadValue(i, baseType, elesize, conn);
            }
          }

          return seq;

        case CUBRIDDataType.CCI_U_TYPE_BLOB:
          CUBRIDBlob Blob = new CUBRIDBlob(responseBuffer, readCursor, size, conn);
          readCursor += size;

          return Blob;

        case CUBRIDDataType.CCI_U_TYPE_CLOB:
          CUBRIDClob Clob = new CUBRIDClob(responseBuffer, readCursor, size, conn);
          readCursor += size;

          return Clob;

        // [APIS-220] The CUBRID no longer support CAS_FC_MAKE_OUT_RS.
        //case CUBRIDDataType.CCI_U_TYPE_RESULTSET:
        //	int handle = ReadInt();
        //	CUBRIDCommand cmd = new CUBRIDCommand(conn, handle);
        //	return cmd.GetDataReaderFromStoredProcedure();

        default:
          throw new ArgumentException();
      }
    }

    // [APIS-221] Remove this function
    /*
    internal BatchResult[] ReadBatchResult(int count)
    {
      BatchResult[] results = new BatchResult[count];

      for (int i = 0; i < count; i++)
      {
        results[i].StmtType = (StmtType)ReadByte();
        results[i].Result = ReadInt(); //Execute result code

        if (results[i].Result < 0)
        {
          results[i].ErrorCode = results[i].Result;
          results[i].Result = -3; //TODO Document this value
          results[i].ErrorMessage = ReadString(ReadInt(), Encoding.Default);
        }
        else
        {
          ReadInt(); //TODO Document this value
          ReadShort(); //TODO Document this value
          ReadShort(); //TODO Document this value
        }
      }

      return results;
    }
        */
  }
}