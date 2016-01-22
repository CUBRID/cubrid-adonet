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
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Data.Common;

namespace CUBRID.Data.CUBRIDClient
{
	internal partial class CUBRIDStream
	{
		//TODO Make this configurable
		private const int BUFFER_COUNT = 1024;
		//TODO Make this configurable
		private const int BUFFER_CAPACITY = BUFFER_COUNT * 100;

		internal NetworkStream baseStream;

		private const short UNSPECIFIED_SIZEOF = 0;
		private const short BYTE_SIZEOF = 1;
		private const short BOOL_SIZEOF = 1;
		private const short INT_SIZEOF = 4;
		private const short LONG_SIZEOF = 8;
		private const short SHORT_SIZEOF = 2;
		private const short FLOAT_SIZEOF = 4;
		private const short DOUBLE_SIZEOF = 8;
		private const short DATE_SIZEOF = 14;
		private const short TIME_SIZEOF = 14;
		private const short DATETIME_SIZEOF = 14;
		private const short TIMESTAMP_SIZEOF = 12;
		private const short OID_SIZEOF = CUBRIDOid.OID_BYTE_SIZE;

		private byte[][] requestBuffer;
		private int writeCursor;
		private int writtenLength;
		private int requestBufferCursor;
		private int requestBufferCount;

		private const int CAS_INFO_SIZE = 4;
		byte[] casInfo = new byte[CAS_INFO_SIZE];

		private const int JDBC_INFO_SIZE = 4;
		private const short DATA_LENGTH_SIZEOF = 4;

		internal CUBRIDStream()
		{
			this.requestBuffer = new byte[BUFFER_COUNT][];
			this.requestBufferCount = 0;

			NewBuffer();
		}

		internal NetworkStream Stream
		{
			get { return this.baseStream; }
			set { this.baseStream = value; }
		}

		private void NewBuffer()
		{
			this.requestBuffer[this.requestBufferCount] = new byte[BUFFER_CAPACITY];
			this.requestBufferCount++;
		}

		internal void ClearBuffer()
		{
			byte[] jdbcinfo = new byte[JDBC_INFO_SIZE];

			jdbcinfo[0] = 0;
			jdbcinfo[1] = 0;
			jdbcinfo[2] = 0;
			jdbcinfo[3] = 0;

			writeCursor = 0;
			writtenLength = 0;
			requestBufferCursor = 0;

			WriteInt(0);
			WriteBytes(jdbcinfo, 0, jdbcinfo.Length);
		}

		internal void ClearBuffer(NetworkStream stream)
		{
			this.baseStream = stream;

			ClearBuffer();
		}

		internal void WriteCommand(CASFunctionCode commandCode)
		{
			//WriteInt(0); //8.2 First int is reserved for data length
			ClearBuffer(); //8.4

			WriteByte((byte)commandCode);
		}

		#region WriteDataTypeArguments

		internal void WriteShortArg(short value)
		{
			WriteInt(SHORT_SIZEOF);
			WriteShort(value);
		}

		internal void WriteIntArg(int value)
		{
			WriteInt(INT_SIZEOF);
			WriteInt(value);
		}

		internal void WriteLongArg(long value)
		{
			WriteInt(LONG_SIZEOF);
			WriteLong(value);
		}

		internal void WriteFloatArg(float value)
		{
			WriteInt(FLOAT_SIZEOF);
			WriteFloat(value);
		}

		internal void WriteDoubleArg(double value)
		{
			WriteInt(DOUBLE_SIZEOF);
			WriteDouble(value);
		}

		internal void WriteStringArg(string value, Encoding encoding)
		{
			byte[] b = encoding.GetBytes(value);

			WriteInt(b.Length + 1); //Size + 1
			WriteBytes(b, 0, b.Length);
			WriteByte(0); //Null terminate
		}

		internal void WriteByteArg(byte value)
		{
			WriteInt(BYTE_SIZEOF);
			WriteByte(value);
		}

		internal void WriteBooleanArg(bool value)
		{
			WriteInt(BOOL_SIZEOF);
			if (value == true)
				WriteByte((byte)1);
			else
				WriteByte((byte)0);
		}

		internal void WriteBytesArg(byte[] value)
		{
			WriteBytesArg(value, 0, value.Length);
		}

		internal void WriteBytesArg(byte[] value, int offset, int length)
		{
			WriteInt(length);
			WriteBytes(value, offset, length);
		}

		internal void WriteDateArg(DateTime value)
		{
			WriteInt(DATE_SIZEOF);
			WriteShort((short)Convert.ToInt16(value.Year));
			WriteShort((short)Convert.ToInt16(value.Month));
			WriteShort((short)Convert.ToInt16(value.Day));
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
		}

		internal void WriteTimeArg(DateTime value)
		{
			WriteInt(TIME_SIZEOF);
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
			WriteShort((short)Convert.ToInt16(value.Hour));
			WriteShort((short)Convert.ToInt16(value.Minute));
			WriteShort((short)Convert.ToInt16(value.Second));
			WriteShort(0);
		}

		internal void WriteDateTimeArg(DateTime value)
		{
			WriteInt(DATETIME_SIZEOF);
			WriteShort((short)Convert.ToInt16(value.Year));
			WriteShort((short)Convert.ToInt16(value.Month));
			WriteShort((short)Convert.ToInt16(value.Day));
			WriteShort((short)Convert.ToInt16(value.Hour));
			WriteShort((short)Convert.ToInt16(value.Minute));
			WriteShort((short)Convert.ToInt16(value.Second));
			WriteShort((short)Convert.ToInt16(value.Millisecond));
		}

		internal void WriteTimeStampArg(DateTime value)
		{
			WriteInt(TIMESTAMP_SIZEOF);
			WriteShort((short)Convert.ToInt16(value.Year));
			WriteShort((short)Convert.ToInt16(value.Month));
			WriteShort((short)Convert.ToInt16(value.Day));
			WriteShort((short)Convert.ToInt16(value.Hour));
			WriteShort((short)Convert.ToInt16(value.Minute));
			WriteShort((short)Convert.ToInt16(value.Second));
            //WriteShort((short)Convert.ToInt16(value.Millisecond)); // changed from 0
		}

		internal void WriteOidArg(CUBRIDOid value)
		{
			WriteInt(OID_SIZEOF);
			WriteBytes(value.Oid, 0, OID_SIZEOF);
		}

		internal void WriteNullArg()
		{
			WriteInt(0);
		}

		internal void WriteCollectionArg(object[] value, CUBRIDDataType type)
		{
			WriteCollection(value, type);
		}

		internal void WriteSequenceArg()
		{
			//TODO
			throw new NotImplementedException();
		}

		internal void WriteSetArg()
		{
			//TODO
			throw new NotImplementedException();
		}

		internal void WriteMultisetArg()
		{
			//TODO
			throw new NotImplementedException();
		}

		internal void WriteBlob(CUBRIDBlob value)
		{
			WriteBytesArg(value.getPackedLobHandle());
		}

		internal void WriteClob(CUBRIDClob value)
		{
			WriteBytesArg(value.getPackedLobHandle());
		}


		#endregion WriteDataTypeArguments


		#region AddDataType

		private int AddNull()
		{
			WriteInt(UNSPECIFIED_SIZEOF);

			return INT_SIZEOF;
		}

		private int AddInt(int intValue)
		{
			WriteInt(INT_SIZEOF);
			WriteInt(intValue);

			return INT_SIZEOF + INT_SIZEOF;
		}

		private int AddLong(long longValue)
		{
			WriteInt(LONG_SIZEOF);
			WriteLong(longValue);

			return INT_SIZEOF + LONG_SIZEOF;
		}

		private int AddBytes(byte[] value, int offset, int len)
		{
			WriteInt(len);
			WriteBytes(value, offset, len);

			return INT_SIZEOF + len;
		}

		private int AddBytes(byte[] value)
		{
			return AddBytes(value, 0, value.Length);
		}

		private int AddStringWithNull(String str)
		{
			byte[] b;

			try
			{
				b = System.Text.Encoding.Default.GetBytes(str);
			}
			catch
			{
				b = System.Text.Encoding.ASCII.GetBytes(str);
			}

			WriteInt(b.Length + 1);
			WriteBytes(b, 0, b.Length);
			WriteByte((byte)0); //Null terminate

			return INT_SIZEOF + b.Length + BYTE_SIZEOF;
		}

		private int AddDouble(double value)
		{
			WriteInt(DOUBLE_SIZEOF);
			WriteDouble(value);

			return INT_SIZEOF + DOUBLE_SIZEOF;
		}

		private int AddShort(short value)
		{
			WriteInt(SHORT_SIZEOF);
			WriteShort(value);

			return INT_SIZEOF + SHORT_SIZEOF;
		}

		private int AddFloat(float value)
		{
			WriteInt(FLOAT_SIZEOF);
			WriteFloat(value);

			return INT_SIZEOF + FLOAT_SIZEOF;
		}

		private int AddDate(DateTime value)
		{
			WriteInt(DATE_SIZEOF);
			WriteDate(value);

			return INT_SIZEOF + DATE_SIZEOF;
		}

		int AddTime(DateTime value)
		{
			WriteInt(TIME_SIZEOF);
			WriteTime(value);

			return INT_SIZEOF + TIME_SIZEOF;
		}

		int AddTimestamp(DateTime value)
		{
			WriteInt(TIMESTAMP_SIZEOF);
			WriteTimestamp(value);

			return INT_SIZEOF + TIMESTAMP_SIZEOF;
		}

		int AddDateTime(DateTime value)
		{
			WriteInt(DATETIME_SIZEOF);
			WriteDateTime(value);

			return INT_SIZEOF + DATETIME_SIZEOF;
		}

		int AddOID(CUBRIDOid value)
		{
			byte[] b = value.Oid;
			if (b == null || b.Length != OID_SIZEOF)
			{
				b = new byte[OID_SIZEOF];
			}

			WriteInt(OID_SIZEOF);
			WriteBytes(b, 0, b.Length);
			return INT_SIZEOF + OID_SIZEOF;
		}

		#endregion AddDataType


		#region WriteDataType

		internal void WriteBool(bool value)
		{
			if (value == true)
				WriteByte((byte)1);
			else
				WriteByte((byte)0);
		}

		internal void WriteCollection(object[] data, CUBRIDDataType type)
		{
			int collection_size = 0;
			int collection_size_msg_buffer_cursor = requestBufferCursor;
			int collection_size_msg_buffer_position = writeCursor;

			WriteInt(collection_size);
			WriteByte((byte)type);

			collection_size++;

			switch (type)
			{
				case CUBRIDDataType.CCI_U_TYPE_BIT:
				case CUBRIDDataType.CCI_U_TYPE_VARBIT:
					{
                        if (data != null)
                        {
                            byte[][] byteValues = new byte[data.Length][];
                            for (int i = 0; i < data.Length; i++)
                            {
                                if (data[i] != null)
                                {
                                    byteValues[i] = new byte[1];
                                    byteValues[i][0] = Convert.ToByte(data[i]);
                                }
                                else
                                {
                                    byteValues[i] = null;
                                }
                            }

                            for (int i = 0; byteValues != null && i < byteValues.Length; i++)
                            {
                                if (byteValues[i] == null)
                                    collection_size += AddNull();
                                else
                                    collection_size += AddBytes(byteValues[i]);
                            }
                        }
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_SHORT:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
                            if (data[i] == null)
                                collection_size += AddNull();
                            else
								collection_size += AddShort((short)Convert.ToInt16(data[i]));
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_INT:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
                            if (data[i] == null)
                                collection_size += AddNull();
                            else
								collection_size += AddInt((int)data[i]);
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_BIGINT:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
                            if (data[i] == null)
                                collection_size += AddNull();
                            else
								collection_size += AddLong((long)Convert.ToInt64(data[i]));
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_FLOAT:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
                            if (data[i] == null)
                                collection_size += AddNull();
                            else
								collection_size += AddFloat((float)Convert.ToDouble(data[i]));
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
				case CUBRIDDataType.CCI_U_TYPE_MONETARY:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
                            if (data[i] == null)
                                collection_size += AddNull();
                            else
								collection_size += AddDouble((double)Convert.ToDouble(data[i]));
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
							{
								collection_size += AddStringWithNull(data[i].ToString());
							}
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_DATE:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
								collection_size += AddDate((DateTime)data[i]);
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_TIME:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
								collection_size += AddTime((DateTime)data[i]);
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
								collection_size += AddTimestamp((DateTime)data[i]);
						}
					}
                    break;
				case CUBRIDDataType.CCI_U_TYPE_DATETIME:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
								collection_size += AddDateTime((DateTime)data[i]);
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_CHAR:
				case CUBRIDDataType.CCI_U_TYPE_NCHAR:
				case CUBRIDDataType.CCI_U_TYPE_STRING:
				case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
                case CUBRIDDataType.CCI_U_TYPE_ENUM:
					{
						for (int i = 0; data != null && i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
								collection_size += AddStringWithNull(data[i].ToString());
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_OBJECT:
					{
						for (int i = 0; i < data.Length; i++)
						{
							if (data[i] == null)
								collection_size += AddNull();
							else
								collection_size += AddOID((CUBRIDOid)data[i]);
						}
					}
					break;
				case CUBRIDDataType.CCI_U_TYPE_BLOB:
					{
						throw new NotImplementedException();
					}
				case CUBRIDDataType.CCI_U_TYPE_CLOB:
					{
						throw new NotImplementedException();
					}
				case CUBRIDDataType.CCI_U_TYPE_NULL:
				default:
					{
						for (int i = 0; data != null && i < data.Length; i++)
							collection_size += AddNull();
					}
					break;
			}

			WriteIntOverwrite(collection_size, collection_size_msg_buffer_cursor, collection_size_msg_buffer_position);
		}

		private void WriteShort(short value)
		{
			WriteBytes(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value)), 0, 2);
		}

		private void WriteInt(int value)
		{
			WriteBytes(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value)), 0, 4);
		}

		private void WriteLong(long value)
		{
            WriteByte((byte)((value >> 56) & 0xFF));
            WriteByte((byte)((value >> 48) & 0xFF));
            WriteByte((byte)((value >> 40) & 0xFF));
            WriteByte((byte)((value >> 32) & 0xFF));
            WriteByte((byte)((value >> 24) & 0xFF));
            WriteByte((byte)((value >> 16) & 0xFF));
            WriteByte((byte)((value >> 8) & 0xFF));
            WriteByte((byte)((value >> 0) & 0xFF));
		}

		private void WriteIntOverwrite(int value, int bufferCursor, int pos)
		{
			int savedLength = this.writtenLength;
			int savedBufferCursor = this.requestBufferCursor;
			int savedPos = this.writeCursor;

			this.requestBufferCursor = bufferCursor;
			this.writeCursor = pos;

			WriteInt(value);

			this.writtenLength = savedLength;
			this.requestBufferCursor = savedBufferCursor;
			this.writeCursor = savedPos;
		}

		private void WriteFloat(float value)
		{
			int i = BitConverter.ToInt32(BitConverter.GetBytes(value), 0);
			WriteBytes(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(i)), 0, 4);
		}

		private void WriteDouble(double value)
		{
			long l = BitConverter.DoubleToInt64Bits(value);
			WriteBytes(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(l)), 0, 8);
		}

		private void WriteByte(byte value)
		{
			if (writeCursor >= BUFFER_CAPACITY)
			{
				this.requestBufferCursor++;
				if (this.requestBufferCursor >= this.requestBufferCount)
				{
					NewBuffer();
				}
				this.writeCursor = 0;
			}

			this.requestBuffer[this.requestBufferCursor][this.writeCursor] = value;
			this.writeCursor++;
			this.writtenLength++;
		}

		private void WriteBytes(byte[] value, int offset, int length)
		{
			if (this.writeCursor + length <= BUFFER_CAPACITY)
			{
				Array.Copy(value, offset, this.requestBuffer[this.requestBufferCursor], this.writeCursor, length);
				this.writeCursor += length;
				this.writtenLength += length;

				return;
			}

			for (int i = 0; i < length; i++)
			{
				WriteByte(value[offset]);
				offset++;
			}
		}

		private void WriteBytesOverwrite(byte[] data, int msg_buffer_cursor, int msg_buffer_position)
		{
			int savedLength = this.writtenLength;
			int savedBufferCursor = this.requestBufferCursor;
			int savedPos = this.writeCursor;

			this.requestBufferCursor = msg_buffer_cursor;
			this.writeCursor = msg_buffer_position;

			WriteBytes(data, 0, data.Length);

			this.writtenLength = savedLength;
			this.requestBufferCursor = savedBufferCursor;
			this.writeCursor = savedPos;

		}

		private void WriteDate(DateTime data)
		{
            WriteShort((short)Convert.ToInt16(data.Year));
            WriteShort((short)Convert.ToInt16(data.Month));
            WriteShort((short)Convert.ToInt16(data.Day));
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
		}

		private void WriteTime(DateTime data)
		{
			String stringData = data.ToString("HH:mm:ss");

			WriteShort(0);
			WriteShort(0);
            WriteShort(0);
            WriteShort((short)Convert.ToInt16(data.Hour));
            WriteShort((short)Convert.ToInt16(data.Minute));
            WriteShort((short)Convert.ToInt16(data.Second));
			WriteShort(0);
		}

		private void WriteTimestamp(DateTime data)
		{
            WriteShort((short)Convert.ToInt16(data.Year));
            WriteShort((short)Convert.ToInt16(data.Month));
            WriteShort((short)Convert.ToInt16(data.Day));
            WriteShort((short)Convert.ToInt16(data.Hour));
            WriteShort((short)Convert.ToInt16(data.Minute));
            WriteShort((short)Convert.ToInt16(data.Second));
            //WriteShort((short)Convert.ToInt16(data.Millisecond));
		}

		private void WriteDateTime(DateTime data)
		{
            WriteShort((short)Convert.ToInt16(data.Year));
            WriteShort((short)Convert.ToInt16(data.Month));
            WriteShort((short)Convert.ToInt16(data.Day));
            WriteShort((short)Convert.ToInt16(data.Hour));
            WriteShort((short)Convert.ToInt16(data.Minute));
            WriteShort((short)Convert.ToInt16(data.Second));
            WriteShort((short)Convert.ToInt16(data.Millisecond));
		}

		#endregion WriteDataType


		public string GetDbVersion()
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_DB_VERSION);
			WriteByteArg((byte)1); //auto-commit = true

			Send();

			int responseCode = Receive();

			int len = responseBufferCapacity - readCursor;
			string ret = ReadString(len, Encoding.Default);

			return ret;
		}

		internal void WriteBytesToRaw(byte[] driverInfo, int offset, int count)
		{
			this.baseStream.Write(driverInfo, offset, count);
			this.baseStream.Flush();
		}

		internal void RequestPrepare(string sql, CCIPrepareOption prepareOption, Encoding encoding)
		{
			//outBuffer.newRequest(out, UFunctionCode.PREPARE);
			//outBuffer.addStringWithNull(sqlStatement);
			//outBuffer.addByte(prepareFlag);
			//outBuffer.addByte(getAutoCommit() ? (byte) 1 : (byte) 0);
			WriteCommand(CASFunctionCode.CAS_FC_PREPARE);
			WriteStringArg(sql, encoding);
			WriteByteArg((byte)prepareOption);
			//TODO Add support for auto commit
			WriteByteArg(0); //auto commit = false

			Send();

			Receive();
		}

		internal void RequestCloseHandle(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_CLOSE_REQ_HANDLE);
			WriteIntArg(handle);

			Send();

			Receive();
		}

		internal int RequestExecute(int handle, CCIExecutionOption executionFlag,
																CUBRIDParameter[] parameters, byte[] paramModes, byte fetchFlag, bool autoCommit)
		{
			WriteCommand(CASFunctionCode.CAS_FC_EXECUTE);

			//outBuffer.addInt(serverHandler);
			WriteIntArg(handle);
			//outBuffer.addByte(executeFlag);
			WriteByteArg((byte)executionFlag);
			WriteIntArg(0); //Max fields
			WriteIntArg(0); //Max fetch size

			if (paramModes != null)
			{
				WriteBytesArg(paramModes); //CUBRID_STMT_CALL_SP && paramModes
			}
			else
			{
				WriteNullArg(); //No parameters
			}

			//if (commandTypeIs == CUBRIDCommandType.CUBRID_STMT_SELECT)
			//  outBuffer.addByte((byte)1);
			//else
			//  outBuffer.addByte((byte)0); 
            WriteByteArg(fetchFlag);

			WriteByteArg(autoCommit ? (byte)1 : (byte)0); //Auto commit
            WriteByteArg(autoCommit ? (byte)1 : (byte)0); //forward_only_cursor
			//WriteByteArg(1); //Not scrollable
			WriteCacheTime(); //Cache time
            WriteIntArg(0);

			//BindParameter parameters
			if (parameters != null)
			{
				for (int i = 0; i < parameters.Length; i++)
				{
					parameters[i].Write(this);
				}
			}

			Send();

			return Receive();
		}

		internal int RequestNextResult(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_NEXT_RESULT);

			WriteIntArg(handle); //server handler
			WriteIntArg(0); //TODO: Document this

			Send();

			return Receive();
		}

		internal int RequestMoveCursor(int handle, int offset, CCICursorPosition origin)
		{
			/*
				outBuffer.newRequest(UFunctionCode.CURSOR);
				outBuffer.addInt(serverHandler);
				outBuffer.addInt(offset);
				outBuffer.addInt(origin);

				inBuffer = relatedConnection.send_recv_msg();
			}

			totalTupleNumber = inBuffer.readInt();			
			*/

			WriteCommand(CASFunctionCode.CAS_FC_CURSOR);
			WriteIntArg(handle);
			WriteIntArg(offset);
			WriteIntArg((int)origin);

			Send();

			Receive();

			return ReadInt();
		}

		internal int RequestFetch(int handle, int cursor_position = 0)
		{
			/*
			outBuffer.newRequest(UFunctionCode.FETCH);
			outBuffer.addInt(serverHandler);
			if (fetchDirection == ResultSet.FETCH_REVERSE)
			{
				int startPos = cursorPosition - fetchSize + 2;
				if (startPos < 1)
					startPos = 1;
				outBuffer.addInt(startPos);
			}
			else
			{
				outBuffer.addInt(cursorPosition + 1);
			}
			outBuffer.addInt(fetchSize);
			outBuffer.addByte((isSensitive == true) ? (byte)1 : (byte)0);
			// jci 3.0
			outBuffer.addInt(0);
			// outBuffer.addInt(resultset_index);

			inBuffer = relatedConnection.send_recv_msg();

			fetchedTupleNumber = inBuffer.readInt();
			if (fetchedTupleNumber < 0)
				fetchedTupleNumber = 0;
			*/

			WriteCommand(CASFunctionCode.CAS_FC_FETCH);

			WriteIntArg(handle); //outBuffer.addInt(serverHandler);
			WriteIntArg(cursor_position + 1); //Start position (= current cursor position + 1)
			WriteIntArg(100); //Fetch size; 0 = default; recommended = 100
			WriteByteArg(0); //Is case sensitive
			WriteIntArg(0); //Is the ResultSet index...?

			Send();

			return Receive();
		}

		internal int RequestBatchExecute(string[] sqls, bool autoCommit)
		{
			WriteCommand(CASFunctionCode.CAS_FC_EXECUTE_BATCH);
			WriteByteArg(autoCommit ? (byte)1 : (byte)0); //Auto commit

			for (int i = 0; i < sqls.Length; i++)
			{
				if (sqls[i] != null)
				{
					WriteStringArg(sqls[i], Encoding.Default);
				}
				else
				{
					WriteNullArg();
				}
			}

			Send();

			return Receive();
		}

		internal int RequestBatchExecute(int handle, CUBRIDParameterCollection[] paramCollection, bool autoCommit)
		{
			CUBRIDParameter parameter;

			WriteCommand(CASFunctionCode.CAS_FC_EXECUTE_ARRAY);
			WriteIntArg(handle);
			WriteByteArg(autoCommit ? (byte)1 : (byte)0);

			for (int i = 0; i < paramCollection.Length; i++)
			{
				for (int j = 0; j < paramCollection[i].Count; j++)
				{
					parameter = (CUBRIDParameter)paramCollection[i][j];
					if (parameter != null)
					{
						parameter.Write(this);
					}
				}
			}

			Send();

			return Receive();
		}

		internal DbDataReader GetGeneratedKeys(CUBRIDCommand cmd, int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_GENERATED_KEYS);
			WriteIntArg(handle);

			Send();

			Receive();

			CUBRIDStatementType commandTypeIs = (CUBRIDStatementType)ReadByte();
			int totalTupleNumber = ReadInt();
			bool isUpdatable = (ReadByte() == 1) ? true : false;
			int columnNumber = ReadInt();
			//StmtType statementType = StmtType.GET_AUTOINCREMENT_KEYS;
			ColumnMetaData[] columnMetadata = new ColumnMetaData[columnNumber];
			columnMetadata = ReadColumnInfo(columnNumber,false);
			//executeResult = totalTupleNumber;
            int TupleNumber = ReadInt();
			//read_fetch_data(inBuffer);
			DbDataReader dataReader = new CUBRIDDataReader(cmd, handle, totalTupleNumber, columnMetadata, TupleNumber);

			return dataReader;

		}
/*
 * REDO: not called
		internal string RequestQueryPlan(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_QUERY_INFO);
			WriteIntArg(handle);
			WriteByteArg(0x01); //QUERY_INFO_PLAN prepare option

			Send();

			Receive();

			return ReadString(responseBufferCapacity - INT_SIZEOF, Encoding.Default);
		}

		internal string RequestQueryPlan(string sql)
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_QUERY_INFO);
			WriteIntArg(0); //Handle
			WriteByteArg(0x01); //QUERY_INFO_PLAN prepare option
			WriteStringArg(sql, Encoding.Default);

			Send();

			Receive();

			return ReadString(responseBufferCapacity - INT_SIZEOF, Encoding.Default);
		}

		internal void RequestCloseConnection()
		{
			WriteCommand(CASFunctionCode.CAS_FC_CON_CLOSE);

			Send();

			Receive();
		}
*/

        /*
         * [APIS-220] The CUBRID no longer support CAS_FC_MAKE_OUT_RS.
		internal int RequestOutResultSet(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_MAKE_OUT_RS);
			WriteIntArg(handle);

			Send();

			Receive();

			return ReadInt(); //TODO Document this value
		}
        */

		internal bool RequestCheck()
		{
			WriteCommand(CASFunctionCode.CAS_FC_CHECK_CAS);

			Send();

			int result = ReadIntFromRaw(); //TODO Document this value

			if (result == 0)
				return true;

			if (result < 4) //TODO Document this value
				return false;

			result = ReadIntFromRaw(); //TODO Document this value

			if (result < 0)
				return false;

			return true;
		}

		internal void WriteCacheTime()
		{
			WriteInt(2 * INT_SIZEOF);
			WriteInt(0); //Seconds
			WriteInt(0); //USeconds
		}

		internal void Send()
		{
			//WriteIntOverwrite(writtenLength - 4, 0, 0); //8.2
			WriteIntOverwrite(this.writtenLength - (DATA_LENGTH_SIZEOF + CAS_INFO_SIZE), 0, 0);

			//Write CAS info
			WriteBytesOverwrite(this.casInfo, 0, CAS_INFO_SIZE);

			for (int i = 0; i < this.requestBufferCursor; i++)
			{
				this.baseStream.Write(this.requestBuffer[i], 0, this.requestBuffer[i].Length);
			}

			if (this.writeCursor > 0)
			{
				baseStream.Write(this.requestBuffer[this.requestBufferCursor], 0, this.writeCursor);
			}

			this.baseStream.Flush();
			ClearBuffer(this.baseStream);
		}

	}
}
