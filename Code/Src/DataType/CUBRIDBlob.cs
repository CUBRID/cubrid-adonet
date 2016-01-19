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
    private readonly CUBRIDConnection connection;
    IntPtr packedLobHandle;

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDBlob" /> class.
    /// </summary>
    /// <param name="conn"> The conn. </param>
    public CUBRIDBlob(CUBRIDConnection conn)
    {
      connection = conn;
      packedLobHandle = connection.LOBNew(CUBRIDDataType.CCI_U_TYPE_BLOB);
    }

    /// <summary>
    ///   Destructors of the <see cref="CUBRIDBlob" /> class.
    /// </summary>
    ~CUBRIDBlob()
    {
        CciInterface.cci_blob_free(packedLobHandle);
    }
    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDBlob" /> class.
    /// </summary>
    /// <param name="responseBuffer"> The response buffer. </param>
    /// <param name="conn"> The connection </param>
    public CUBRIDBlob(IntPtr responseBuffer, CUBRIDConnection conn)
    {
      packedLobHandle = responseBuffer;
      connection = conn;
    }

    /// <summary>
    ///   Gets or sets the length of the BLOB.
    /// </summary>
    /// <value> The length of the BLOB. </value>
    public long BlobLength
    {
      get 
      { 
          return (long)CciInterface.cci_blob_size(packedLobHandle);
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
      ulong lob_size = CciInterface.cci_blob_size(packedLobHandle);
      if(lob_size<0)
      {
          throw new CUBRIDException("Get lob size failed.");
      }
      pos--;
      lob_size = (lob_size - (ulong)pos < (ulong)length) ? lob_size - (ulong)pos : (ulong)length;

      byte[] buf = new byte[lob_size];
      T_CCI_ERROR err = new T_CCI_ERROR();
      int res = CciInterface.cci_blob_read(connection.Conection,packedLobHandle,(ulong)pos,(int)lob_size, buf,ref err);
      if (res < 0)
      {
          throw new CUBRIDException(err.err_code, err.err_msg);
      }
      return buf;
    }

    /// <summary>
    ///   Sets the LOB content as bytes array.
    /// </summary>
    /// <param name="pos"> The position. </param>
    /// <param name="bytes"> The bytes array. </param>
    /// <returns> The number of written bytes </returns>
    public long SetBytes(ulong pos, byte[] bytes)
    {
      int len = bytes.Length;
      T_CCI_ERROR err = new T_CCI_ERROR();

      pos--;

      int res  = CciInterface.cci_blob_write(
          connection.Conection, packedLobHandle,
          pos, bytes.GetLength(0), bytes, ref err);
      if (res < 0)
      {
          throw new CUBRIDException(err.err_code, err.err_msg);
      }

      return res;
    }

    /// <summary>
    ///   Gets the packed LOB handle.
    /// </summary>
    /// <returns> The packed LOB handle. </returns>
    public IntPtr GetPackedLobHandle()
    {
      return packedLobHandle;
    }
  }
}