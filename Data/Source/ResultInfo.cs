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

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// Internal class storing execution result information.
	/// </summary>
	internal class ResultInfo
	{
		private CUBRIDStatementType stmtType;
		private int resultCount;
		private CUBRIDOid oid;
		private int cacheTimeSec;
		private int cacheTImeUsec;

		internal ResultInfo()
		{
		}

		internal ResultInfo(CUBRIDStatementType stmtType, int resultCount, CUBRIDOid oid, int cacheTimeSec, int cacheTImeUsec)
		{
			this.stmtType = stmtType;
			this.resultCount = resultCount;
			this.oid = oid;
			this.cacheTimeSec = cacheTimeSec;
			this.cacheTImeUsec = cacheTImeUsec;
		}

		internal CUBRIDStatementType StmtType
		{
			set { this.stmtType = value; }
			get { return this.stmtType; }
		}

		internal int ResultCount
		{
			set { this.resultCount = value; }
			get { return this.resultCount; }
		}

		internal CUBRIDOid Oid
		{
			set { this.oid = value; }
			get { return this.oid; }
		}

		internal int CacheTimeSec
		{
			set { this.cacheTimeSec = value; }
			get { return this.cacheTimeSec; }
		}

		internal int CacheTimeUsec
		{
			set { this.cacheTImeUsec = value; }
			get { return this.cacheTImeUsec; }
		}

	}
}
