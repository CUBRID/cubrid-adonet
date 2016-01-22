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
  ///   Implementation of the CUBRIDBlob class.
  /// </summary>
  public class CUBRIDBlob
  {
    private const int CLOB_MAX_IO_LENGTH = 128 * 1024;
    private readonly CUBRIDConnection connection;
    private int lobSize;
    private byte[] packedLobHandle;

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDBlob" /> class.
    /// </summary>
    public CUBRIDBlob()
    {
      lobSize = 0;
      connection = null;
      packedLobHandle = null;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDBlob" /> class.
    /// </summary>
    /// <param name="conn"> The conn. </param>
    public CUBRIDBlob(CUBRIDConnection conn)
    {
      connection = conn;

      packedLobHandle = connection.LOBNew(CUBRIDDataType.CCI_U_TYPE_BLOB);
      InitBlob(packedLobHandle);
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDBlob" /> class.
    /// </summary>
    /// <param name="responseBuffer"> The response buffer. </param>
    /// <param name="readCursor"> The read cursor. </param>
    /// <param name="size"> The size. </param>
    /// <param name="conn"> The connection </param>
    public CUBRIDBlob(byte[] responseBuffer, int readCursor, int size, CUBRIDConnection conn)
    {
      byte[] packedLobHandleBuffer = new byte[size];
      Array.Copy(responseBuffer, readCursor, packedLobHandleBuffer, 0, size);
      InitBlob(packedLobHandleBuffer);
      connection = conn;
    }

    /// <summary>
    ///   Gets or sets the length of the BLOB.
    /// </summary>
    /// <value> The length of the BLOB. </value>
    public long BlobLength
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

    /// <summary>
    ///   Gets the LOB content as bytes array.
    /// </summary>
    /// <param name="packedLobHandleBuffer"> Buffer containing a packedLobHandle. </param>
    private void InitBlob(byte[] packedLobHandleBuffer)
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
    ///   Gets the LOB content as bytes array.
    /// </summary>
    /// <param name="pos"> The position. </param>
    /// <param name="length"> The length. </param>
    /// <returns> A buffer containing the requested data </returns>
    public byte[] GetBytes(long pos, int length)
    {
      using (CUBRIDConnection con = new CUBRIDConnection())
      {
        con.ConnectionString = connection.ConnectionString;
        con.Open();
        pos--;
        int totalReadLen = 0;

        if ((pos + length) > BlobLength)
        {
          length = (int)(BlobLength - pos);
        }

        if (length <= 0)
          return new byte[0];

        byte[] buff = new byte[length];
        while (length > 0)
        {
          int readLen = Math.Min(length, CLOB_MAX_IO_LENGTH);
          int realReadLen = con.LOBRead(packedLobHandle, pos, buff, totalReadLen, readLen);

          pos += realReadLen;
          length -= realReadLen;
          totalReadLen += realReadLen;

          if (realReadLen == 0)
          {
            break;
          }
        }

        con.Close();

        return totalReadLen >= buff.Length ? buff : new byte[0];
      }
    }

    /// <summary>
    ///   Sets the LOB content as bytes array.
    /// </summary>
    /// <param name="pos"> The position. </param>
    /// <param name="bytes"> The bytes array. </param>
    /// <returns> The number of written bytes </returns>
    public long SetBytes(long pos, byte[] bytes)
    {
      int len = bytes.Length;
      int offset = 0;

      if (BlobLength + 1 != pos)
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidLOBPosition));

      pos--;

      long totalWriteLen = 0;

      while (len > 0)
      {
        int writeLen = Math.Min(len, CLOB_MAX_IO_LENGTH);
        int realWriteLen = connection.LOBWrite(packedLobHandle, pos, bytes, offset, writeLen);

        pos += realWriteLen;
        len -= realWriteLen;
        offset += realWriteLen;
        totalWriteLen += realWriteLen;
      }

      if (pos > BlobLength)
      {
        lobSize = (int)pos;
        BlobLength = lobSize;
      }

      return totalWriteLen;
    }

    /// <summary>
    ///   Gets the packed LOB handle.
    /// </summary>
    /// <returns> The packed LOB handle. </returns>
    public byte[] GetPackedLobHandle()
    {
      return packedLobHandle;
    }
  }
}