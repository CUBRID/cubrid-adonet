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

using System.Data.Common;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID implementation of the <see cref="T:System.Data.Common.DbProviderFactory" /> class.
  /// </summary>
  public sealed class CUBRIDClientFactory : DbProviderFactory
  {
    /// <summary>
    ///   Gets an instance of the <see cref="CUBRIDClientFactory" />. 
    ///   This can be used to retrieve strongly typed data objects.
    /// </summary>
    public static readonly CUBRIDClientFactory Instance = new CUBRIDClientFactory();

    /// <summary>
    ///   Returns true if a <b>CUBRIDDataSourceEnumerator</b> can be created, or otherwise false.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator
    {
      get { return false; } //Not supported
    }

    /// <summary>
    ///   Returns a strongly typed <see cref="DbCommand" /> instance.
    /// </summary>
    /// <returns> A new strongly typed instance of <b>DbCommand</b> . </returns>
    public override DbCommand CreateCommand()
    {
      return new CUBRIDCommand();
    }

    /// <summary>
    ///   Returns a strongly typed <see cref="DbCommandBuilder" /> instance.
    /// </summary>
    /// <returns> A new strongly typed instance of <b>DbCommandBuilder</b> . </returns>
    public override DbCommandBuilder CreateCommandBuilder()
    {
      return new CUBRIDCommandBuilder();
    }

    /// <summary>
    ///   Returns a strongly typed <see cref="DbConnection" /> instance.
    /// </summary>
    /// <returns> A new strongly typed instance of <b>DbConnection</b> . </returns>
    public override DbConnection CreateConnection()
    {
      return new CUBRIDConnection();
    }

    /// <summary>
    ///   Returns a strongly typed <see cref="DbConnectionStringBuilder" /> instance.
    /// </summary>
    /// <returns> A new strongly typed instance of <b>DbConnectionStringBuilder</b> . </returns>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
      return new CUBRIDConnectionStringBuilder();
    }

    /// <summary>
    ///   Returns a strongly typed <see cref="DbDataAdapter" /> instance.
    /// </summary>
    /// <returns> A new strongly typed instance of <b>DbDataAdapter</b> . </returns>
    public override DbDataAdapter CreateDataAdapter()
    {
      return new CUBRIDDataAdapter();
    }

    /// <summary>
    ///   Returns a strongly typed <see cref="DbParameter" /> instance.
    /// </summary>
    /// <returns> A new strongly typed instance of <b>DbParameter</b> . </returns>
    public override DbParameter CreateParameter()
    {
      return new CUBRIDParameter();
    }
  }
}