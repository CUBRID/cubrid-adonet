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

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// CUBRID class that support OID manipulation.
	/// </summary>
	public class CUBRIDOid
	{
		/// <summary>
		/// The size of the OID in CUBRID
		/// </summary>
		public const int OID_BYTE_SIZE = 8;

		private byte[] oid = null;

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDOid"/> class.
		/// </summary>
		/// <param name="oid">The OID.</param>
		public CUBRIDOid(byte[] oid)
		{
			this.oid = oid;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDOid"/> class.
		/// </summary>
		/// <param name="oidStr">The OID.</param>
		public CUBRIDOid(String oidStr)
		{
			if (oidStr == null || oidStr.Substring(0, 1) != "@")
				throw new ArgumentException();

			String[] oidStringArray = oidStr.Substring(1).Split('|');
			int page = Int32.Parse(oidStringArray[0]);
			short slot = Int16.Parse(oidStringArray[1]);
			short vol = Int16.Parse(oidStringArray[2]);

			byte[] bOID = new byte[CUBRIDOid.OID_BYTE_SIZE];
			bOID[0] = ((byte)((page >> 24) & 0xFF));
			bOID[1] = ((byte)((page >> 16) & 0xFF));
			bOID[2] = ((byte)((page >> 8) & 0xFF));
			bOID[3] = ((byte)((page >> 0) & 0xFF));
			bOID[4] = ((byte)((slot >> 8) & 0xFF));
			bOID[5] = ((byte)((slot >> 0) & 0xFF));
			bOID[6] = ((byte)((vol >> 8) & 0xFF));
			bOID[7] = ((byte)((vol >> 0) & 0xFF));

			this.oid = bOID;
		}

		/// <summary>
		/// Gets the OID.
		/// </summary>
		public byte[] Oid
		{
			get { return this.oid; }
		}

		/// <summary>
		/// Gets the OID Page.
		/// </summary>
		/// <returns></returns>
		public int? Page()
		{
			if (this.oid == null) 
				return null;

			return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(oid, 0));
		}

		/// <summary>
		/// Gets the OID Slot.
		/// </summary>
		/// <returns></returns>
		public int? Slot()
		{
			if (this.oid == null) 
				return null;

			return IPAddress.NetworkToHostOrder(BitConverter.ToInt16(oid, 4));
		}

		/// <summary>
		/// Gets the OID Volume.
		/// </summary>
		/// <returns></returns>
		public int? Volume()
		{
			if (this.oid == null) 
				return null;
			
			return IPAddress.NetworkToHostOrder(BitConverter.ToInt16(oid, 6));
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			return "OID:@" + IPAddress.NetworkToHostOrder(BitConverter.ToInt32(oid, 0))
					+ "|" + IPAddress.NetworkToHostOrder(BitConverter.ToInt16(oid, 4))
					+ "|" + IPAddress.NetworkToHostOrder(BitConverter.ToInt16(oid, 6));
		}
	}

}
