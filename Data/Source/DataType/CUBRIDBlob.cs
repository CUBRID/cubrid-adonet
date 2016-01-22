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

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// Implementation of the CUBRIDBlob class.
	/// </summary>
	public class CUBRIDBlob
	{
		private static int CLOB_MAX_IO_LENGTH = 128 * 1024;
		private int lobSize;
		private int locatorSize;
		private String locator;
		private byte[] packedLobHandle;
		private CUBRIDConnection connection;

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDBlob"/> class.
		/// </summary>
		public CUBRIDBlob()
		{
			lobSize = 0;
			locator = "";
			locatorSize = 0;
			connection = null;
			packedLobHandle = null;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDBlob"/> class.
		/// </summary>
		/// <param name="conn">The conn.</param>
		public CUBRIDBlob(CUBRIDConnection conn)
		{
			this.connection = conn;

			packedLobHandle = connection.LOBNew(CUBRIDDataType.CCI_U_TYPE_BLOB);
			InitBlob(packedLobHandle, packedLobHandle.Length);

		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDBlob"/> class.
		/// </summary>
		/// <param name="responseBuffer">The response buffer.</param>
		/// <param name="responseBufferCapacity">The response buffer capacity.</param>
		/// <param name="readCursor">The read cursor.</param>
		/// <param name="size">The size.</param>
		/// <param name="conn">The conn.</param>
		public CUBRIDBlob(byte[] responseBuffer, int responseBufferCapacity, int readCursor, int size, CUBRIDConnection conn)
		{
			byte[] packedLobHandle = new byte[size];
			Array.Copy(responseBuffer, readCursor, packedLobHandle, 0, size);
			InitBlob(packedLobHandle, packedLobHandle.Length);
			this.connection = conn;
		}

		private void InitBlob(byte[] packedLobHandle, int size)
		{
			this.packedLobHandle = packedLobHandle;
			int pos = 0;
			pos += 4;

			lobSize = 0;
			for (int i = pos; i < pos + 8; i++)
			{
				lobSize <<= 8;
				lobSize |= (packedLobHandle[i] & 0xff);
			}
			pos += 8;

			locatorSize = 0;
			for (int i = pos; i < pos + 4; i++)
			{
				locatorSize <<= 8;
				locatorSize |= (packedLobHandle[i] & 0xff);
			}

			pos += 4;
			locator = System.Text.ASCIIEncoding.ASCII.GetString(packedLobHandle, pos, locatorSize - 1);
		}

		/// <summary>
		/// Gets the LOB content as bytes array.
		/// </summary>
		/// <param name="pos">The position.</param>
		/// <param name="length">The length.</param>
		/// <returns></returns>
		public byte[] getBytes(long pos, int length)
		{
			CUBRIDConnection con = new CUBRIDConnection();
			con.ConnectionString = this.connection.ConnectionString;
			con.Open();
			pos--;
			int real_read_len, read_len, total_read_len = 0;

			if ((pos + length) > BlobLength)
			{
				length = (int)(BlobLength - pos);
			}

			if (length <= 0)
				return new byte[0];

			byte[] buff = new byte[length];
			while (length > 0)
			{
				read_len = Math.Min(length, CLOB_MAX_IO_LENGTH);
				real_read_len = con.LOBRead(packedLobHandle, pos, buff, total_read_len, read_len);

				pos += real_read_len;
				length -= real_read_len;
				total_read_len += real_read_len;

				if (real_read_len == 0)
					break;
			}

			con.Close();

			if (total_read_len >= buff.Length)
				return buff;
			else
				return new byte[0];
		}

		/// <summary>
		/// Sets the LOB content as bytes array.
		/// </summary>
		/// <param name="pos">The position.</param>
		/// <param name="bytes">The bytes array.</param>
		/// <returns></returns>
		public long setBytes(long pos, byte[] bytes)
		{
			int len = bytes.Length;
			int offset = 0;

			if (BlobLength + 1 != pos)
				throw new CUBRIDException(Utils.GetStr(MsgId.InvalidLOBPosition));

			pos--;

			int real_write_len, write_len;
			long total_write_len = 0;

			while (len > 0)
			{
				write_len = Math.Min(len, CLOB_MAX_IO_LENGTH);
				real_write_len = connection.LOBWrite(packedLobHandle, pos, bytes, offset, write_len);

				pos += real_write_len;
				len -= real_write_len;
				offset += real_write_len;
				total_write_len += real_write_len;
			}

			if (pos > BlobLength)
			{
				lobSize = (int)pos;
				BlobLength = lobSize;
			}

			return total_write_len;
		}

		/// <summary>
		/// Gets or sets the length of the BLOB.
		/// </summary>
		/// <value>
		/// The length of the BLOB.
		/// </value>
		public long BlobLength
		{
			get
			{
				return lobSize;
			}
			set
			{
				int bitpos = 64;
				int pos = 4;
				for (int i = pos; i < pos + 8; i++)
				{
					bitpos -= 8;
					packedLobHandle[i] = (byte)((value >> bitpos) & 0xFF);
				}
			}
		}

		/// <summary>
		/// Gets the packed LOB handle.
		/// </summary>
		/// <returns></returns>
		public byte[] getPackedLobHandle()
		{
			return packedLobHandle;
		}
	}
}
