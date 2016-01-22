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
  ///   Implementation of the CUBRIDClob class.
  /// </summary>
  public class CUBRIDClob
  {
    private const int BLOB_MAX_IO_LENGTH = 128 * 1024;
    private readonly CUBRIDConnection connection;
    private int lobSize;
    private byte[] packedLobHandle;

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDClob" /> class.
    /// </summary>
    public CUBRIDClob()
    {
      lobSize = 0;
      connection = null;
      packedLobHandle = null;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDClob" /> class.
    /// </summary>
    /// <param name="conn"> The conn. </param>
    public CUBRIDClob(CUBRIDConnection conn)
    {
      connection = conn;

      packedLobHandle = connection.LOBNew(CUBRIDDataType.CCI_U_TYPE_CLOB);
      InitClob(packedLobHandle);
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDClob" /> class.
    /// </summary>
    /// <param name="responseBuffer"> The response buffer. </param>
    /// <param name="readCursor"> The read cursor. </param>
    /// <param name="size"> The size. </param>
    /// <param name="conn"> The connection. </param>
    public CUBRIDClob(byte[] responseBuffer, int readCursor, int size, CUBRIDConnection conn)
    {
      byte[] packedLobHandleBuffer = new byte[size];
      Array.Copy(responseBuffer, readCursor, packedLobHandleBuffer, 0, size);
      InitClob(packedLobHandleBuffer);
      connection = conn;
    }

    /// <summary>
    ///   Gets or sets the length of the CLOB.
    /// </summary>
    /// <value> The length of the CLOB. </value>
    public long ClobLength
    {
      get { return lobSize; }
      set
      {
        int bitpos = 64;
        const int pos = 4;
        for (int i = pos; i < pos + 8; i++)
        {
          bitpos -= 8;
          packedLobHandle[i] = (byte)((value >> bitpos) & 0xFF);
        }
      }
    }

    private void InitClob(byte[] packedLobHandleBuffer)
    {
      packedLobHandle = packedLobHandleBuffer;
      int pos = 0;
      pos += 4;

      lobSize = 0;
      for (int i = pos; i < pos + 8; i++)
      {
        lobSize <<= 8;
        lobSize |= (packedLobHandle[i] & 0xff);
      }
    }

    /// <summary>
    ///   Gets the LOB content as String.
    /// </summary>
    /// <param name="pos"> The position. </param>
    /// <param name="length"> The length. </param>
    /// <returns> </returns>
    public string GetString(long pos, int length)
    {
      using (CUBRIDConnection con = new CUBRIDConnection())
      {
        con.ConnectionString = connection.ConnectionString;
        con.Open();

        pos--;
        int totalReadLen = 0;

        if ((pos + length) > ClobLength)
        {
          length = (int)(ClobLength - pos);
        }

        if (length <= 0)
          return String.Empty;

        byte[] buff = new byte[length];
        while (length > 0)
        {
          int readLen = Math.Min(length, BLOB_MAX_IO_LENGTH);
          int realReadLen = con.LOBRead(packedLobHandle, pos, buff, totalReadLen, readLen);

          pos += realReadLen;
          length -= realReadLen;
          totalReadLen += realReadLen;

          if (realReadLen == 0)
            break;
        }

        con.Close();

        if (totalReadLen >= buff.Length)
        {
          return connection.GetEncoding().GetString(buff);
        }

        return String.Empty;
      }
    }

    /// <summary>
    ///   Sets the LOB content from String.
    /// </summary>
    /// <param name="pos"> The position. </param>
    /// <param name="str"> The string. </param>
    /// <returns> </returns>
    public long SetString(long pos, string str)
    {
      byte[] bytes = connection.GetEncoding().GetBytes(str);
      int len = bytes.Length;
      int offset = 0;

      if (ClobLength + 1 != pos)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidLOBPosition));

      pos--;

      long totalWriteLen = 0;

      while (len > 0)
      {
        int writeLen = Math.Min(len, BLOB_MAX_IO_LENGTH);
        int realWriteLen = connection.LOBWrite(packedLobHandle, pos, bytes, offset, writeLen);

        pos += realWriteLen;
        len -= realWriteLen;
        offset += realWriteLen;
        totalWriteLen += realWriteLen;
      }

      if (pos > ClobLength)
      {
        lobSize = (int)pos;
        ClobLength = lobSize;
      }

      return totalWriteLen;
    }

    /// <summary>
    ///   Gets the packed LOB handle.
    /// </summary>
    /// <returns> </returns>
    public byte[] GetPackedLobHandle()
    {
      return packedLobHandle;
    }
  }
}