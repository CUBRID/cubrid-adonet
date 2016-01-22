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
 * - Neither the paramName of the <ORGANIZATION> nor the names of its contributors 
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
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	public class CUBRIDStream
	{
		private const int BUFFER_COUNT = 1024;
		private const int BUFFER_CAPACITY = 1024 * 100;

		private byte[][] requestBuffer;
		private int writeCursor;
		private int writtenLength;
		private int requestBufferCursor;
		private int requestBufferCount;

		private byte[] responseBuffer;
		private int readCursor;
		private int responseBufferCapacity;
		private int responseCode;

		private NetworkStream baseStream;

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
		private const short TIMESTAMP_SIZEOF = 14;
		private const short OID_SIZEOF = CUBRIDOid.OID_BYTE_SIZE;

		public NetworkStream Stream
		{
			get { return baseStream; }
			set { baseStream = value; }
		}

		public CUBRIDStream()
		{
			requestBuffer = new byte[BUFFER_COUNT][];
			requestBufferCount = 0;
			NewBuffer();
		}

		#region RequestWriter

		private void NewBuffer()
		{
			requestBuffer[requestBufferCount] = new byte[BUFFER_CAPACITY];
			requestBufferCount++;
		}

		public void Reset()
		{
			writeCursor = 0;
			writtenLength = 0;
			requestBufferCursor = 0;
		}

		public void Reset(NetworkStream stream)
		{
			this.baseStream = stream;
			Reset();
		}

		public void WriteCommand(CASFunctionCode command)
		{
			WriteInt(UNSPECIFIED_SIZEOF); //First byte is reserved for data length
			WriteByte((byte)command);
		}

		#region WriteDataTypeArguments

		public void WriteShortArg(short value)
		{
			WriteInt(SHORT_SIZEOF);
			WriteShort(value);
		}

		public void WriteIntArg(int value)
		{
			WriteInt(INT_SIZEOF);
			WriteInt(value);
		}

		public void WriteLongArg(long value)
		{
			WriteInt(LONG_SIZEOF);
			WriteLong(value);
		}

		public void WriteFloatArg(float value)
		{
			WriteInt(FLOAT_SIZEOF);
			WriteFloat(value);
		}

		public void WriteDoubleArg(double value)
		{
			WriteInt(DOUBLE_SIZEOF);
			WriteDouble(value);
		}

		public void WriteStringArg(string value, Encoding encoding)
		{
			byte[] b = encoding.GetBytes(value);

			WriteInt(b.Length + 1); //Size
			WriteBytes(b, 0, b.Length);
			WriteByte(0); //Null terminate
		}

		public void WriteByteArg(byte value)
		{
			WriteInt(BYTE_SIZEOF);
			WriteByte(value);
		}

		public void WriteBooleanArg(bool value)
		{
			WriteInt(BOOL_SIZEOF);
			if (value == true)
				WriteByte((byte)1);
			else
				WriteByte((byte)0);
		}

		public void WriteBytesArg(byte[] value)
		{
			WriteBytesArg(value, 0, value.Length);
		}

		public void WriteBytesArg(byte[] value, int offset, int length)
		{
			WriteInt(length);
			WriteBytes(value, offset, length);
		}

		public void WriteDateArg(DateTime value)
		{
			WriteInt(DATE_SIZEOF);
			WriteShort((short)value.Year);
			WriteShort((short)value.Month);
			WriteShort((short)value.Day);
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
		}

		public void WriteTimeArg(DateTime value)
		{
			WriteInt(TIME_SIZEOF);
			WriteShort(0);
			WriteShort(0);
			WriteShort(0);
			WriteShort((short)value.Hour);
			WriteShort((short)value.Minute);
			WriteShort((short)value.Second);
		}

		public void WriteDateTimeArg(DateTime value)
		{
			WriteInt(DATETIME_SIZEOF);
			WriteShort((short)value.Year);
			WriteShort((short)value.Month);
			WriteShort((short)value.Day);
			WriteShort((short)value.Hour);
			WriteShort((short)value.Minute);
			WriteShort((short)value.Second);
		}

		public void WriteOidArg(CUBRIDOid value)
		{
			WriteInt(OID_SIZEOF);
			WriteBytes(value.Oid, 0, OID_SIZEOF);
		}

		public void WriteNullArg()
		{
			WriteInt(0);
		}

		public void WriteCollectionArg()
		{
			//TODO
			throw new Exception("Not implemented yet!");
		}

		public void WriteSequenceArg()
		{
			//TODO
			throw new Exception("Not implemented yet!");
		}

		public void WriteSetArg()
		{
			//TODO
			throw new Exception("Not implemented yet!");
		}

		public void WriteMultisetArg()
		{
			//TODO
			throw new Exception("Not implemented yet!");
		}

		#endregion WriteDataTypeArguments

		#region AddDataType

		private int AddNull()
		{
			WriteInt(UNSPECIFIED_SIZEOF);
			return INT_SIZEOF;
		}

		int AddInt(int intValue)
		{
			WriteInt(INT_SIZEOF);
			WriteInt(intValue);
			return INT_SIZEOF + INT_SIZEOF;
		}

		int AddLong(long longValue)
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

		int AddStringWithNull(String str)
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

		int AddDouble(double value)
		{
			WriteInt(DOUBLE_SIZEOF);
			WriteDouble(value);
			return INT_SIZEOF + DOUBLE_SIZEOF;
		}

		int AddShort(short value)
		{
			WriteInt(SHORT_SIZEOF);
			WriteShort(value);
			return INT_SIZEOF + SHORT_SIZEOF;
		}

		int AddFloat(float value)
		{
			WriteInt(FLOAT_SIZEOF);
			WriteFloat(value);
			return INT_SIZEOF + FLOAT_SIZEOF;
		}

		int AddDate(DateTime value)
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

		public void WriteBool(bool value)
		{
			if (value == true)
				WriteByte((byte)1);
			else
				WriteByte((byte)0);
		}

		public void WriteCollection(object[] data, CUBRIDDataType type)
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
						byte[][] byteValues = null;
						if (data != null)
						{
							if (data is byte[][])
							{
								byteValues = (byte[][])data;
							}
							else if (data is bool[][])
							{
								byteValues = new byte[data.Length][];
								for (int i = 0; i < byteValues.Length; i++)
								{
									if (data[i] != null)
									{
										byteValues[i] = new byte[1];
										bool v = (bool)data[i];
										byteValues[i][0] = (v == true) ? (byte)1 : (byte)0;
									}
									else
									{
										byteValues[i] = null;
									}
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
								collection_size += AddShort((short)data[i]);
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
								collection_size += AddLong((long)data[i]);
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
								collection_size += AddFloat((float)data[i]);
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
								collection_size += AddDouble((double)data[i]);
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
				case CUBRIDDataType.CCI_U_TYPE_DATETIME:
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
				case CUBRIDDataType.CCI_U_TYPE_CHAR:
				case CUBRIDDataType.CCI_U_TYPE_NCHAR:
				case CUBRIDDataType.CCI_U_TYPE_STRING:
				case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
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
						//TODO
						throw new Exception("Not implemented yet!");
					}
				case CUBRIDDataType.CCI_U_TYPE_CLOB:
					{
						throw new Exception("Not implemented yet!");
					}
				case CUBRIDDataType.CCI_U_TYPE_NULL:
				default:
					{
						for (int i = 0; data != null && i < data.Length; i++)
							collection_size += AddNull();
					}
					break;
			}
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
			WriteByte((byte)((Utils.ushift(value, 56)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 48)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 40)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 32)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 24)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 16)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 8)) & 0xFF));
			WriteByte((byte)((Utils.ushift(value, 0)) & 0xFF));
		}

		private void WriteIntOverride(int value, int bufferCursor, int pos)
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
				requestBufferCursor++;
				if (requestBufferCursor >= requestBufferCount)
				{
					NewBuffer();
				}
				writeCursor = 0;
			}

			requestBuffer[requestBufferCursor][writeCursor] = value;
			writeCursor++;
			writtenLength++;
		}

		private void WriteBytes(byte[] value, int offset, int length)
		{
			if (writeCursor + length <= BUFFER_CAPACITY)
			{
				Array.Copy(value, offset, requestBuffer[requestBufferCursor], writeCursor, length);
				writeCursor += length;
				this.writtenLength += length;
			}
			else
			{
				for (int i = 0; i < length; i++)
				{
					WriteByte(value[offset]);
					offset++;
				}
			}
		}

		private void WriteDate(DateTime data)
		{
			String stringData = String.Format("0:yyyy-MM-dd", data);

			WriteShort(short.Parse(stringData.Substring(0, 4)));
			WriteShort(short.Parse(stringData.Substring(5, 7)));
			WriteShort(short.Parse(stringData.Substring(8, 10)));
			WriteShort((short)0);
			WriteShort((short)0);
			WriteShort((short)0);
			WriteShort((short)0);
		}

		private void WriteTime(DateTime data)
		{
			String stringData = String.Format("0:HH:mm:ss", data);

			WriteShort((short)0);
			WriteShort((short)0);
			WriteShort((short)0);
			WriteShort(short.Parse(stringData.Substring(0, 2)));
			WriteShort(short.Parse(stringData.Substring(3, 5)));
			WriteShort(short.Parse(stringData.Substring(6, 8)));
			WriteShort((short)0);
		}

		private void WriteTimestamp(DateTime data)
		{
			String stringData = String.Format("0:yyyy-MM-dd HH:mm:ss", data);

			WriteShort(short.Parse(stringData.Substring(0, 4)));
			WriteShort(short.Parse(stringData.Substring(5, 7)));
			WriteShort(short.Parse(stringData.Substring(8, 10)));
			WriteShort(short.Parse(stringData.Substring(11, 13)));
			WriteShort(short.Parse(stringData.Substring(14, 16)));
			WriteShort(short.Parse(stringData.Substring(17, 19)));
			WriteShort((short)0);
		}

		private void WriteDateTime(DateTime data)
		{
			String stringData = String.Format("0:yyyy-MM-dd HH:mm:ss.SSS", data);

			WriteShort(short.Parse(stringData.Substring(0, 4)));
			WriteShort(short.Parse(stringData.Substring(5, 7)));
			WriteShort(short.Parse(stringData.Substring(8, 10)));
			WriteShort(short.Parse(stringData.Substring(11, 13)));
			WriteShort(short.Parse(stringData.Substring(14, 16)));
			WriteShort(short.Parse(stringData.Substring(17, 19)));
			WriteShort(short.Parse(stringData.Substring(20, 23)));
		}

		#endregion WriteDataType

		internal void WriteBytesToRaw(byte[] driverInfo, int offset, int count)
		{
			baseStream.Write(driverInfo, offset, count);
			baseStream.Flush();
		}

		#endregion RequestWriter


		#region ResponseReader

		public int ResponseCode
		{
			get { return responseCode; }
		}

		public int Receive()
		{
			readCursor = 0;
			responseBufferCapacity = ReadStreamSize();
			responseBuffer = new byte[responseBufferCapacity];

			FillBuffer();

			responseCode = ReadInt();
			if (responseCode < 0)
			{
				string emsg = ReadString(responseBufferCapacity - 4, Encoding.Default);
				Trace.WriteLineIf(Utils.TraceState, String.Format("Receive::Error: {0}, {1}", responseCode, emsg));
				throw new CUBRIDException(emsg);
			}

			return responseCode;
		}

		private int ReadStreamSize()
		{
			byte[] b = new byte[4];

			baseStream.Read(b, 0, 4);
			return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(b, 0));
		}

		private void FillBuffer()
		{
			int read = 0, r;

			while (read < responseBufferCapacity)
			{
				r = baseStream.Read(responseBuffer, read, responseBufferCapacity - read);
				if (r == 0)
				{
					responseBufferCapacity = read;
					break;
				}

				read += r;
			}
		}

		public byte ReadByte()
		{
			if (readCursor >= responseBufferCapacity)
				throw new CUBRIDException("Invalid buffer position!");

			return responseBuffer[readCursor++];
		}

		public void ReadBytes(byte[] value, int offset, int length)
		{
			if (value == null)
				return;
			if (readCursor + length > responseBufferCapacity)
				throw new CUBRIDException("Invalid buffer position!");

			Array.Copy(responseBuffer, readCursor, value, offset, length);
			readCursor += length;
		}

		public void ReadBytes(byte[] value)
		{
			ReadBytes(value, 0, value.Length);
		}

		public byte[] ReadBytes(int size)
		{
			byte[] value = new byte[size];
			ReadBytes(value, 0, size);

			return value;
		}

		public short ReadShort()
		{
			if (readCursor + SHORT_SIZEOF > responseBufferCapacity)
				throw new CUBRIDException("Invalid buffer position!");

			short value = IPAddress.NetworkToHostOrder(BitConverter.ToInt16(responseBuffer, readCursor));
			readCursor += SHORT_SIZEOF;

			return value;
		}

		public int ReadInt()
		{
			if (readCursor + INT_SIZEOF > responseBufferCapacity)
				throw new CUBRIDException("Invalid buffer position!");

			int value = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(responseBuffer, readCursor));
			readCursor += INT_SIZEOF;

			return value;
		}

		public long ReadLong()
		{
			if (readCursor + LONG_SIZEOF > responseBufferCapacity)
				throw new CUBRIDException("Invalid buffer position!");

			long value = IPAddress.NetworkToHostOrder(BitConverter.ToInt64(responseBuffer, readCursor));
			readCursor += LONG_SIZEOF;

			return value;
		}

		public float ReadFloat()
		{
			return BitConverter.ToSingle(BitConverter.GetBytes(ReadInt()), 0);
		}

		public double ReadDouble()
		{
			return BitConverter.Int64BitsToDouble(ReadLong());
		}

		public string ReadString(int size, Encoding encoding)
		{
			string value;

			if (size <= 0)
				return null;
			if (readCursor + size > responseBufferCapacity)
				throw new CUBRIDException("Invalid buffer position!");

			value = encoding.GetString(responseBuffer, readCursor, size - 1);
			readCursor += size;

			return value;
		}

		public DateTime ReadDate()
		{
			int year, month, day;

			year = ReadShort();
			month = ReadShort();
			day = ReadShort();

			return new DateTime(year, month, day);
		}

		public DateTime ReadTime()
		{
			int hour, min, sec;

			hour = ReadShort();
			min = ReadShort();
			sec = ReadShort();

			return new DateTime(0, 0, 0, hour, min, sec);
		}

		public DateTime ReadDateTime()
		{
			int year, month, day, hour, min, sec, millisec;

			year = ReadShort();
			month = ReadShort();
			day = ReadShort();

			hour = ReadShort();
			min = ReadShort();
			sec = ReadShort();
			millisec = ReadShort();

			return new DateTime(year, month, day, hour, min, sec, millisec);
		}

		public DateTime ReadTimestamp()
		{
			int year, month, day, hour, min, sec;

			year = ReadShort();
			month = ReadShort();
			day = ReadShort();

			hour = ReadShort();
			min = ReadShort();
			sec = ReadShort();

			return new DateTime(year, month, day, hour, min, sec);
		}

		public CUBRIDOid ReadOid()
		{
			byte[] oid = ReadBytes(OID_SIZEOF);

			return new CUBRIDOid(oid);
		}

		/*
		CUBRIDBlob readBlob(int packedLobHandleSize, CUBRIDConnection conn)
		{
			try 
			{
				byte[] packedLobHandle = readBytes(packedLobHandleSize);
				return new CUBRIDBlob(conn, packedLobHandle);
			} 
			catch (Exception e) 
			{
				throw new UJciException(UErrorCode.ER_UNKNOWN);
			}
		}

		CUBRIDClob readClob(int packedLobHandleSize, CUBRIDConnection conn)
		{
			try 
			{
				byte[] packedLobHandle = readBytes(packedLobHandleSize);
				return new CUBRIDClob(conn, packedLobHandle, conn.getUConnection().getCharset());
			} 
			catch (Exception e) 
			{
				throw new UJciException(UErrorCode.ER_UNKNOWN);
			}
		}
		*/
		
		internal int ReadIntFromRaw()
		{
			byte[] b = new byte[4];
			baseStream.Read(b, 0, 4);

			return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(b, 0));
		}

		#endregion

		internal void RequestPrepare(string sql, CCIPrepareOption flag)
		{
			WriteCommand(CASFunctionCode.CAS_FC_PREPARE);
			WriteStringArg(sql, Encoding.Default);
			WriteByteArg((byte)flag);

			Flush();
			Receive();
		}

		internal void RequestCloseHandle(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_CLOSE_REQ_HANDLE);
			WriteIntArg(handle);

			Flush();
			Receive();
		}

		internal int RequestExecute(int handle, CCIExecutionOption flag,
																CUBRIDParameter[] parameters, byte[] paramModes, byte fetchFlag, bool autoCommit)
		{
			WriteCommand(CASFunctionCode.CAS_FC_EXECUTE);

			WriteIntArg(handle);
			WriteByteArg((byte)flag);
			WriteIntArg(0); /* max field */
			WriteIntArg(0); /* max fetch size */

			if (paramModes != null)
			{
				WriteBytesArg(paramModes);
			}
			else
			{
				WriteNullArg(); /* bind paramDirection */
			}

			WriteByteArg(fetchFlag); /* fetch flag */
			WriteByteArg(autoCommit ? (byte)1 : (byte)0); /* auto commit */
			WriteByteArg(1); /* not scrollable */
			WriteCacheTime(); /* cache time */

			/* bind parameter */
			if (parameters != null)
			{
				for (int i = 0; i < parameters.Length; i++)
				{
					parameters[i].Write(this);
				}
			}

			Flush();

			return Receive();
		}

		internal int RequestNextResult(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_NEXT_RESULT);

			WriteIntArg(handle);
			WriteIntArg(0);

			Flush();

			return Receive();
		}

		internal int RequestMoveCursor(int handle, int offset, CCICursorPosition origin)
		{
			WriteCommand(CASFunctionCode.CAS_FC_CURSOR);

			WriteIntArg(handle);
			WriteIntArg(offset);
			WriteIntArg((int)origin);

			Flush();
			Receive();

			return ReadInt();
		}

		internal int RequestFetch(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_FETCH);

			WriteIntArg(handle);
			WriteIntArg(1); /* position */ //TODO: check always 1 ??
			WriteIntArg(0); /* fetchsize */
			WriteByteArg(0); /* isSensitive */
			WriteIntArg(0); /* ? */

			Flush();

			return Receive();
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
				resultInfos[i].CacheTImeUsec = ReadInt();
			}

			return resultInfos;
		}

		internal ColumnMetaData[] ReadColumnInfo(int count)
		{
			ColumnMetaData[] infoArray = new ColumnMetaData[count];

			for (int i = 0; i < count; i++)
			{
				ColumnMetaData info = new ColumnMetaData();

				info.Type = (CUBRIDDataType)ReadByte();
				info.Scale = ReadShort();
				info.Precision = ReadInt();
				info.Name = ReadString(ReadInt(), Encoding.ASCII);
				info.RealName = ReadString(ReadInt(), Encoding.ASCII);
				info.Table = ReadString(ReadInt(), Encoding.ASCII);
				info.IsNull = (ReadByte() == 0);

				infoArray[i] = info;
			}

			return infoArray;
		}

		internal void ReadResultTuple(ResultTuple tuple, ColumnMetaData[] columnInfos, CUBRIDStatementType stmtType,
																	CUBRIDConnection con)
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
					CUBRIDDataType type = CUBRIDDataType.CCI_U_TYPE_NULL;

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

					val = ReadValue(j, type, size, con);
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
					CUBRIDDataType type;

					type = (CUBRIDDataType)ReadByte();
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
					return ReadString(size, Encoding.Default);

				case CUBRIDDataType.CCI_U_TYPE_SHORT:
					return ReadShort();

				case CUBRIDDataType.CCI_U_TYPE_INT:
					return ReadInt();

				case CUBRIDDataType.CCI_U_TYPE_FLOAT:
					return ReadFloat();

				case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
				case CUBRIDDataType.CCI_U_TYPE_MONETARY:
					return ReadDouble();

				case CUBRIDDataType.CCI_U_TYPE_DATE:
					return ReadDate();

				case CUBRIDDataType.CCI_U_TYPE_TIME:
					return ReadTime();

				case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
					return ReadDateTime();

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
					//TODO
					throw new Exception("Not implemented yet!");

				case CUBRIDDataType.CCI_U_TYPE_CLOB:
					//TODO
					throw new Exception("Not implemented yet!");

				case CUBRIDDataType.CCI_U_TYPE_RESULTSET:
					int handle = ReadInt();
					CUBRIDCommand cmd = new CUBRIDCommand(conn, handle);
					return cmd.GetDataReaderFromStoredProcedure();

				default:
					//return null;
					throw new ArgumentException();
			}
		}

		internal int RequestBatchExecute(string[] sqls)
		{
			WriteCommand(CASFunctionCode.CAS_FC_EXECUTE_BATCH);
			WriteIntArg(0); //auto commit

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

			Flush();

			return Receive();
		}

		internal int RequestBatchExecute(int handle, CUBRIDParameterCollection[] paramCollection)
		{
			CUBRIDParameter parameter;

			WriteCommand(CASFunctionCode.CAS_FC_EXECUTE_ARRAY);
			WriteIntArg(handle);
			WriteIntArg(0); //auto commit

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

			Flush();

			return Receive();
		}

		internal BatchResult[] ReadBatchResult(int count)
		{
			BatchResult[] results = new BatchResult[count];

			for (int i = 0; i < count; i++)
			{
				results[i].StmtType = (StmtType)ReadByte();
				results[i].Result = ReadInt(); //execute result

				if (results[i].Result < 0)
				{
					results[i].ErrorCode = results[i].Result;
					results[i].Result = -3; //???
					results[i].ErrorMessage = ReadString(ReadInt(), Encoding.Default);
				}
				else
				{
					ReadInt(); //dummy
					ReadShort();//dummy
					ReadShort();//dummy
				}
			}

			return results;
		}

		internal string RequestServerVersion()
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_DB_VERSION);
			WriteIntArg(0); //auto commit

			Flush();
			Receive();

			return ReadString(responseBufferCapacity - 4, Encoding.Default);
		}

		internal string RequestQueryPlan(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_QUERY_INFO);
			WriteIntArg(handle);
			WriteByteArg(0x01); //QUERY_INFO_PLAN flag

			Flush();
			Receive();

			return ReadString(responseBufferCapacity - 4, Encoding.Default);
		}

		internal string RequestQueryPlan(string sql)
		{
			WriteCommand(CASFunctionCode.CAS_FC_GET_QUERY_INFO);
			WriteIntArg(0); //no handle
			WriteByteArg(0x01); //QUERY_INFO_PLAN flag
			WriteStringArg(sql, Encoding.Default);

			Flush();
			Receive();

			return ReadString(responseBufferCapacity - 4, Encoding.Default);
		}

		internal void RequestCloseConnection()
		{
			WriteCommand(CASFunctionCode.CAS_FC_CON_CLOSE);

			Flush();
			Receive();
		}

		internal int RequestOutResultSet(int handle)
		{
			WriteCommand(CASFunctionCode.CAS_FC_MAKE_OUT_RS);
			WriteIntArg(handle);

			Flush();
			Receive();

			return ReadInt();
		}

		internal bool RequestCheck()
		{
			WriteCommand(CASFunctionCode.CAS_FC_CHECK_CAS);
			Flush();

			int result = ReadIntFromRaw();

			if (result == 0)
				return true;

			if (result < 4)
				return false;

			result = ReadIntFromRaw();
			if (result < 0)
				return false;

			return true;
		}

		public void WriteCacheTime()
		{
			WriteInt(8);
			WriteInt(0);
			WriteInt(0);
		}

		public void Flush()
		{
			WriteIntOverride(writtenLength - 4, 0, 0);

			for (int i = 0; i < requestBufferCursor; i++)
			{
				baseStream.Write(requestBuffer[i], 0, requestBuffer[i].Length);
			}

			if (writeCursor > 0)
			{
				baseStream.Write(requestBuffer[requestBufferCursor], 0, writeCursor);
			}

			baseStream.Flush();
			Reset(baseStream);
		}

	}
}
