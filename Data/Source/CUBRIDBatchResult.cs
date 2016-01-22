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
	/// Class storing multiple batches execution results.
	/// </summary>
	public sealed class CUBRIDBatchResult
	{
		private bool errorFlag;
		private int[] result;
		private int[] statementType;
		private int[] errorCode;
		private String[] errorMessage;
		private int count;

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDBatchResult"/> class.
		/// </summary>
		/// <param name="count">The count of results.</param>
		public CUBRIDBatchResult(int count)
		{
			if (count < 1)
				throw new ArgumentException();

			this.count = count;
			this.result = new int[count];
			this.statementType = new int[count];
			this.errorCode = new int[count];
			this.errorMessage = new String[count];
			this.errorFlag = false;
		}

		/// <summary>
		/// Gets the error codes.
		/// </summary>
		/// <returns></returns>
		public int[] getErrorCodes()
		{
			return this.errorCode;
		}

		/// <summary>
		/// Gets the error messages.
		/// </summary>
		/// <returns></returns>
		public String[] getErrorMessages()
		{
			return this.errorMessage;
		}

		/// <summary>
		/// Gets the results codes.
		/// </summary>
		/// <returns></returns>
		public int[] getResults()
		{
			return this.result;
		}

		/// <summary>
		/// The number of results.
		/// </summary>
		/// <returns></returns>
		public int Count()
		{
			return this.count;
		}

		/// <summary>
		/// Gets the type of the statements.
		/// </summary>
		/// <returns></returns>
		public int[] getStatementTypes()
		{
			return this.statementType;
		}

		/// <summary>
		/// Gets the error prepareOption.
		/// </summary>
		/// <returns></returns>
		public bool getErrorFlag()
		{
			return this.errorFlag;
		}

		internal void setResultCode(int index, int code)
		{
			if (index < 0 || index >= count)
				return;

			this.result[index] = code;
			this.errorCode[index] = 0;
			this.errorMessage[index] = null;
		}

		internal void setResultError(int index, int code, String message)
		{
			if (index < 0 || index >= count)
				return;

			this.result[index] = -3;
			this.errorCode[index] = code;
			this.errorMessage[index] = message;
			this.errorFlag = true;
		}

		internal void setStatementType(int index, int type)
		{
			if (index < 0 || index >= count)
				return;

			this.statementType[index] = type;
		}
	}

}
