using System;
using System.Collections.Generic;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	public class CUBRIDLobHandle
	{
		private CUBRIDDataType lobType; // U_TYPE_BLOB or U_TYPE_CLOB
		private long lobSize;
		private byte[] packedLobHandle;
		private String locator;

		public CUBRIDLobHandle(CUBRIDDataType lobType, byte[] packedLobHandle)
		{
			this.lobType = lobType;
			this.packedLobHandle = packedLobHandle;
			InitLob();
		}

		private void InitLob()
		{
			int pos = 0;

			if (packedLobHandle == null)
			{
				throw new NullReferenceException();
			}

			pos += 4; // skip db_type

			lobSize = 0;
			for (int i = pos; i < pos + 8; i++)
			{
				lobSize <<= 8;
				lobSize |= (packedLobHandle[i] & 0xff);
			}
			pos += 8; // lob_size

			int locatorSize = 0;
			for (int i = pos; i < pos + 4; i++)
			{
				locatorSize <<= 8;
				locatorSize |= (packedLobHandle[i] & 0xff);
			}
			pos += 4; // locator_size

			// remove terminating null character
			System.Text.Encoding enc = System.Text.Encoding.ASCII;
			string myString = enc.GetString(packedLobHandle);
			locator = myString.Substring(pos, locatorSize - 1);
		}

		public void SetLobSize(long size)
		{
			int pos = 0;

			if (packedLobHandle == null)
			{
				throw new NullReferenceException();
			}

			pos += 4; // skip db_type
			lobSize = size;
			int bitpos = 64;
			for (int i = pos; i < pos + 8; i++)
			{
				bitpos -= 8;
				packedLobHandle[i] = (byte)((lobSize >> bitpos) & 0xFF);
			}
		}

		public long GetLobSize()
		{
			return lobSize;
		}

		public byte[] GetPackedLobHandle()
		{
			return packedLobHandle;
		}

		public override String ToString()
		{
			return locator;
		}

		public override bool Equals(Object obj)
		{
			if (obj is CUBRIDLobHandle)
			{
				CUBRIDLobHandle that = (CUBRIDLobHandle)obj;
				return lobType == that.lobType && lobSize == that.lobSize && locator.Equals(that.locator);
			}

			return false;
		}
	}
}
