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
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID implementation of the <see cref="T:System.Data.Common.DbTransaction" /> class.
  /// </summary>
  public sealed class CUBRIDTransaction : DbTransaction
  {
    private readonly CUBRIDConnection conn;
    private const CUBRIDIsolationLevel isolationLevel = CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE;
    private bool open;

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDTransaction" /> class.
    /// </summary>
    /// <param name="conn"> The connection. </param>
    /// <param name="isolationLevel"> The isolation level. </param>
    public CUBRIDTransaction(CUBRIDConnection conn, CUBRIDIsolationLevel isolationLevel)
    {
      if (isolationLevel == CUBRIDIsolationLevel.TRAN_UNKNOWN_ISOLATION)
        throw new ArgumentException(Utils.GetStr(MsgId.UnknownIsolationLevelNotSupported));

      this.conn = conn;
      conn.IsolationLevel = isolationLevel;
      conn.SetIsolationLevel(isolationLevel);
      open = true;
    }

    /// <summary>
    ///   Specifies the <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction.
    /// </summary>
    /// <returns> The <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction. </returns>
    public new CUBRIDConnection Connection
    {
      get { return conn; }
    }

    /// <summary>
    ///   Specifies the <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction.
    /// </summary>
    /// <returns> The <see cref="T:System.Data.Common.DbConnection" /> object associated with the transaction. </returns>
    protected override DbConnection DbConnection
    {
      get { return conn; }
    }

    /// <summary>
    ///   Gets the CUBRID isolation level.
    /// </summary>
    public CUBRIDIsolationLevel CUBRIDIsolationLevel
    {
        get { return conn.IsolationLevel; }
    }

    /// <summary>
    ///   Specifies the <see cref="T:System.Data.IsolationLevel" /> for this transaction.
    /// </summary>
    /// <returns> The <see cref="T:System.Data.IsolationLevel" /> for this transaction. </returns>
    public override IsolationLevel IsolationLevel
    {
      get
      {
        switch (isolationLevel)
        {
          case CUBRIDIsolationLevel.TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
            return IsolationLevel.ReadCommitted;
          case CUBRIDIsolationLevel.TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
            return IsolationLevel.ReadUncommitted;
          case CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE:
            return IsolationLevel.RepeatableRead;
          case CUBRIDIsolationLevel.TRAN_REP_CLASS_REP_INSTANCE:
            return IsolationLevel.RepeatableRead;
          case CUBRIDIsolationLevel.TRAN_SERIALIZABLE:
            return IsolationLevel.Serializable;
          case CUBRIDIsolationLevel.TRAN_UNKNOWN_ISOLATION:
            return IsolationLevel.Unspecified;
          default:
            return IsolationLevel.Unspecified;
        }
      }
    }

    /// <summary>
    ///   Commits the database transaction.
    /// </summary>
    public override void Commit()
    {
      if (conn == null || (conn.State != ConnectionState.Open))
        throw new InvalidOperationException(Utils.GetStr(MsgId.ConnectionMustBeValidAndOpenToCommit));

      if (!open)
        throw new InvalidOperationException(Utils.GetStr(MsgId.TransactionAlreadyCommittedOrNotPending));

      try
      {
        //TODO Verify best solution
        /*
        CUBRIDCommand columnMetadata = new CUBRIDCommand("COMMIT", conn);
        columnMetadata.ExecuteNonQuery();
        */
        conn.Commit();
        open = false;
      }
      catch (Exception ex)
      {
        Trace.WriteLineIf(Utils.TraceState, "Error occured during commit: " + ex.Message);
      }
    }

    /// <summary>
    ///   Rolls back a transaction from a pending state.
    /// </summary>
    public override void Rollback()
    {
      if (conn == null || (conn.State != ConnectionState.Open))
          throw new InvalidOperationException(Utils.GetStr(MsgId.ConnectionMustBeValidAndOpenToRollBack));

      if (!open)
        throw new InvalidOperationException(Utils.GetStr(MsgId.TransactionAlreadyCommittedOrNotPending));

      try
      {
        //TODO Verify best solution
        /*
        CUBRIDCommand columnMetadata = new CUBRIDCommand("ROLLBACK", conn);
        columnMetadata.ExecuteNonQuery();
        */
        conn.Rollback();
        open = false;
      }
      catch (Exception ex)
      {
        Trace.WriteLineIf(Utils.TraceState, "Error occured during rollback: " + ex.Message);
      }
    }

    /// <summary>
    ///   Releases the unmanaged resources used by the <see cref="T:System.Data.Common.DbTransaction" /> and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing"> If true, this method releases all resources held by any managed objects that this <see
    ///    cref="T:System.Data.Common.DbTransaction" /> references. </param>
    protected override void Dispose(bool disposing)
    {
      //Flush listeners
      foreach (TraceListener listener in Trace.Listeners)
      {
        listener.Flush();
      }

      if ((conn != null && conn.State == ConnectionState.Open) && open)
      {
        Rollback();
      }

      base.Dispose(disposing);
    }
  }
}