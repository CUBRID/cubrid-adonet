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
using System.Linq;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID implementation of the <see cref="T:System.Data.Common.DbCommand" /> class.
  /// </summary>
  public sealed class CUBRIDCommand : DbCommand, ICloneable
  {
    private readonly CUBRIDParameterCollection paramCollection;
    private int bindCount;
    private int cache_reusable;
    private string cmdText;
    private int cmdTimeout = 15; //seconds
    private CommandType cmdType;
    private int columnCount;
    private ColumnMetaData[] columnInfos;
    private CUBRIDConnection conn;
    private CUBRIDDataReader dataReader;
    private int handle;
    private bool isPrepared;
    private bool isUpdateable;
    private CUBRIDParameter[] parameters;
    private int resultCacheLifetime;
    private int resultCount;
    private ResultInfo[] resultInfo;
    private CUBRIDStatementType statementType;

    private StmtType stmtType = StmtType.NORMAL;
    private CUBRIDTransaction transaction;
    private UpdateRowSource updatedRowSource = UpdateRowSource.Both;

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDCommand" /> class.
    /// </summary>
    public CUBRIDCommand()
    {
      paramCollection = new CUBRIDParameterCollection();
      isPrepared = false;

      cmdType = CommandType.Text;
      cmdText = String.Empty;
      stmtType = StmtType.NORMAL;
    }

    /*
         * [APIS-220] The CUBRID no longer support CAS_FC_MAKE_OUT_RS.
    /// <summary>
    /// Initializes a new instance of the <see cref="CUBRIDCommand"/> class.
    /// </summary>
    /// <param name="conn">The connection.</param>
    /// <param name="handle">The command handle.</param>
    public CUBRIDCommand(CUBRIDConnection conn, int handle)
      : this()
    {
      this.conn = conn;
      this.handle = handle;
            this.paramCollection.SetParametersEncoding(conn.GetEncoding());

      GetOutResultSet(handle);
    }
        */

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDCommand" /> class.
    /// </summary>
    /// <param name="sql"> The SQL statement. </param>
    public CUBRIDCommand(string sql)
      : this()
    {
      cmdText = sql;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDCommand" /> class.
    /// </summary>
    /// <param name="sql"> The SQL statement. </param>
    /// <param name="conn"> The connection. </param>
    public CUBRIDCommand(string sql, CUBRIDConnection conn)
      : this(sql)
    {
      this.conn = conn;
      paramCollection.SetParametersEncoding(conn.GetEncoding());
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDCommand" /> class.
    /// </summary>
    /// <param name="sql"> The SQL statement. </param>
    /// <param name="conn"> The connection. </param>
    /// <param name="transaction"> The transaction. </param>
    public CUBRIDCommand(string sql, CUBRIDConnection conn, CUBRIDTransaction transaction)
      : this(sql, conn)
    {
      this.transaction = transaction;
    }

    /// <summary>
    ///   Gets the columns metadata.
    /// </summary>
    public ColumnMetaData[] ColumnInfos
    {
      get { return columnInfos; }
    }

    /// <summary>
    ///   Gets or sets the text command to run against the data source.
    /// </summary>
    /// <returns> The command SQL statement to execute. </returns>
    public override string CommandText
    {
      get
      {
        if (CommandType == CommandType.TableDirect)
        {
          if (cmdText.Length > 0 && cmdText.StartsWith("select * from `", StringComparison.InvariantCultureIgnoreCase))
          {
            string str = cmdText.Substring("select * from `".Length);
            str = str.Substring(0, str.Length - 1);

            return str;
          }
        }
        return cmdText;
      }
      set
      {
        cmdText = value.Trim();
        if (CommandType == CommandType.TableDirect)
        {
          if (cmdText.Length > 0 && !cmdText.StartsWith("select * from `", StringComparison.InvariantCultureIgnoreCase))
          {
            cmdText = "select * from `" + value.Trim() + "`";
          }
        }
        if (isPrepared)
        {
          paramCollection.Clear();
          isPrepared = false;
        }
      }
    }

    /// <summary>
    ///   Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
    /// </summary>
    /// <returns> The time in seconds to wait for the command to execute. </returns>
    public override int CommandTimeout
    {
      get { return cmdTimeout; }
      set { cmdTimeout = value; }
    }

    /// <summary>
    ///   Indicates or specifies how the <see cref="P:System.Data.Common.DbCommand.CommandText" /> property is interpreted.
    /// </summary>
    /// <returns> One of the <see cref="T:System.Data.CommandType" /> values. </returns>
    public override CommandType CommandType
    {
      get { return cmdType; }
      set
      {
        cmdType = value;
        if (CommandType == CommandType.TableDirect)
        {
          if (cmdText.Length > 0 && cmdText.StartsWith("select * from `", StringComparison.InvariantCultureIgnoreCase))
          {
            cmdText = "select * from `" + cmdText + "`";
          }
        }
      }
    }

    /// <summary>
    ///   Gets or sets the <see cref="T:System.Data.Common.DbConnection" /> used by this <see
    ///    cref="T:System.Data.Common.DbCommand" />.
    /// </summary>
    /// <returns> The connection to the data source. </returns>
    protected override DbConnection DbConnection
    {
      get { return conn; }
      set { conn = (CUBRIDConnection)value; }
    }

    internal CUBRIDConnection CUBRIDbConnection
    {
      get { return conn; }
      set
      {
        if (conn != value)
        {
          transaction = null;
          conn = value;
        }
      }
    }

    /// <summary>
    ///   Gets or sets the <see cref="P:CUBRID.Data.CUBRIDClient.CUBRIDCommand.DbTransaction" />.
    /// </summary>
    /// <returns> The transaction within which a Command object of a .NET Framework data provider executes. The default value is a null reference (Nothing in Visual Basic). </returns>
    protected override DbTransaction DbTransaction
    {
      get { return transaction; }
      set { transaction = (CUBRIDTransaction)value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether the command object should be visible in a customized interface control.
    /// </summary>
    /// <returns> true, if the command object should be visible in a control; otherwise false. The default is true. </returns>
    public override bool DesignTimeVisible { get; set; }

    /// <summary>
    ///   Gets or sets how command results are applied to the <see cref="T:System.Data.DataRow" /> when used by the Update method of a <see
    ///    cref="T:System.Data.Common.DbDataAdapter" />.
    /// </summary>
    /// <returns> One of the <see cref="T:System.Data.UpdateRowSource" /> values. The default is Both unless the command is automatically generated. Then the default is None. </returns>
    public override UpdateRowSource UpdatedRowSource
    {
      get { return updatedRowSource; }
      set { updatedRowSource = value; }
    }

    /// <summary>
    ///   Gets the collection of <see cref="T:System.Data.Common.DbParameter" /> objects.
    /// </summary>
    /// <returns> The parameters of the SQL statement or stored procedure. </returns>
    protected override DbParameterCollection DbParameterCollection
    {
      get { return paramCollection; }
    }

    /// <summary>
    ///   Gets the collection of <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter" /> objects.
    /// </summary>
    /// <returns> The parameters of the SQL statement or stored procedure. </returns>
    public new CUBRIDParameterCollection Parameters
    {
      get { return paramCollection; }
    }

    /// <summary>
    ///   Gets a value indicating whether this instance is prepared.
    /// </summary>
    /// <value> <c>true</c> if this instance is prepared; otherwise, <c>false</c> . </value>
    public bool IsPrepared
    {
      get { return isPrepared; }
    }

    internal CUBRIDStatementType StatementType
    {
      get { return statementType; }
      set { statementType = value; }
    }

    internal StmtType StmtType
    {
      get { return stmtType; }
      set { stmtType = value; }
    }

    internal int BindCount
    {
      get { return bindCount; }
    }

    private CCIPrepareOption GetPrepareOption()
    {
      switch (cmdType)
      {
        case CommandType.StoredProcedure:
          return CCIPrepareOption.CCI_PREPARE_CALL;
      }

      return CCIPrepareOption.CCI_PREPARE_NORMAL;
    }

    /// <summary>
    ///   Creates a prepared (or compiled) version of the command on the data source.
    /// </summary>
    public override void Prepare()
    {
      if (conn == null)
        throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionPropertyHasNotBeenSet));

      if (conn.State != ConnectionState.Open)
        throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

      if (cmdText == null || cmdText.Trim().Length == 0)
        return;

      PrepareInternal(cmdText, GetPrepareOption());

      isPrepared = true;
    }

    /// <summary>
    ///   Creates a new instance of a <see cref="T:System.Data.Common.DbParameter" /> object.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.Common.DbParameter" /> object. </returns>
    protected override DbParameter CreateDbParameter()
    {
      return new CUBRIDParameter();
    }

    private void BindParameters()
    {
      if (isPrepared == false)
      {
        Prepare();
      }

      //TODO Verify if other initializations are required
      if (parameters == null && paramCollection.Count > 0)
      {
        //Initialize parameters collection
        parameters = new CUBRIDParameter[paramCollection.Count];
      }

      for (int i = 0; i < paramCollection.Count; i++)
      {
        BindParameter(i, paramCollection[i]);
      }

      //TODO Verify if these initializations are required
      bindCount = paramCollection.Count;
    }

    /// <summary>
    ///   Executes the query and returns the first column of the first row in the result set returned by the query. All other columns and rows are ignored.
    /// </summary>
    /// <returns> The first column of the first row in the result set. </returns>
    public override object ExecuteScalar()
    {
      object ret = null;

      BindParameters();

      using (CUBRIDDataReader dr = ExecuteInternal())
      {
        if (dr.Read())
        {
          ret = dr.GetValue(0);
        }
        dr.Close();
      }

      return ret;
    }

    /// <summary>
    ///   Attempts to cancels the execution of a <see cref="T:System.Data.Common.DbCommand" />.
    ///   Not supported yet.
    /// </summary>
    public override void Cancel()
    {
      //TODO
      throw new NotSupportedException();
    }

    /*
         * [APIS-220] The CUBRID no longer support CAS_FC_MAKE_OUT_RS.
    /// <summary>
    /// Gets a data reader from stored procedure.
    /// </summary>
    /// <returns></returns>
    internal DbDataReader GetDataReaderFromStoredProcedure()
    {
      //int totalTupleNumber = conn.Stream.RequestMoveCursor(handle, 0, CCICursorPosition.CURSOR_SET);
      this.conn.Stream.RequestFetch(this.handle);
      int tupleCount = this.conn.Stream.ReadInt();

      return new CUBRIDDataReader(this, this.handle, this.resultCount, this.columnInfos, tupleCount);
    }
        */

    /// <summary>
    ///   Gets the generated keys (Auto-increment columns).
    /// </summary>
    /// <returns> A <see cref="T:System.Data.Common.DbDataReader" /> . </returns>
    public DbDataReader GetGeneratedKeys()
    {
      if (conn.AutoCommit)
        throw new CUBRIDException("AutoCommit must be false");
      return conn.Stream.GetGeneratedKeys(this, handle);
    }

    /// <summary>
    ///   Executes the command text against the connection.
    /// </summary>
    /// <param name="behavior"> An instance of <see cref="T:System.Data.CommandBehavior" /> . </param>
    /// <returns> A <see cref="T:System.Data.Common.DbDataReader" /> . </returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
      //WORKAROUND for Exception: DataReader is already open
      //if (this.dataReader != null)
      //	ResetDataReader();

      if (dataReader != null)
        throw new CUBRIDException(Utils.GetStr(MsgId.DataReaderIsAlreadyOpen));

      BindParameters();

      if (!IsQueryStatement())
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidQueryType));

      ExecuteInternal();

      return dataReader;
    }

    /// <summary>
    ///   Executes a SQL statement against a connection object.
    /// </summary>
    /// <returns> The number of rows affected. </returns>
    public override int ExecuteNonQuery()
    {
      BindParameters();

      if (IsQueryStatement())
        throw new CUBRIDException(Utils.GetStr(MsgId.InvalidQueryType));

      ExecuteInternal();

      // [APIS-223] Call a stored procedure's return value is not assigned to the return parameter.
      if (statementType == CUBRIDStatementType.CUBRID_STMT_CALL ||
          statementType == CUBRIDStatementType.CUBRID_STMT_CALL_SP)
      {
        conn.Stream.RequestFetch(handle);
        conn.Stream.ReadInt(); //It is always == 1? (it is the number of tuples returned by the server)

        int colCount = GetOutModeParameterCount() + 1;
        ResultTuple tuple = new ResultTuple(colCount);
        conn.Stream.ReadResultTupleSP(tuple, colCount, conn);

        int k = 1;
        foreach (CUBRIDParameter t in parameters)
        {
          if (t.Direction == ParameterDirection.Output ||
              t.Direction == ParameterDirection.InputOutput)
          {
            t.Value = tuple[k];
            k++;
          }
        }
      }

      return resultCount;
    }

    private int GetOutModeParameterCount()
    {
      return parameters.Count(t => t.Direction == ParameterDirection.Output || t.Direction == ParameterDirection.InputOutput);
    }

    /// <summary>
    /// Close Command
    /// </summary>
    public void Close()
    {
      conn.Stream.RequestCloseHandle(handle);
    }

    internal void PrepareInternal(string sql, CCIPrepareOption prepareOption)
    {
      conn.ReconnectIfNeeded();

      conn.Stream.RequestPrepare(sql, prepareOption, conn.GetEncoding());

      handle = conn.Stream.ResponseCode;
      resultCacheLifetime = conn.Stream.ReadInt();
      statementType = (CUBRIDStatementType)conn.Stream.ReadByte();
      bindCount = conn.Stream.ReadInt();
      isUpdateable = (conn.Stream.ReadByte() == 1);
      columnCount = conn.Stream.ReadInt();

      columnInfos = conn.Stream.ReadColumnInfo(columnCount);

      if (bindCount > 0)
      {
        parameters = new CUBRIDParameter[bindCount];
      }

      if (statementType == CUBRIDStatementType.CUBRID_STMT_CALL_SP)
      {
        columnCount = bindCount + 1;
      }

      if (conn.LogTraceApi)
      {
        CUBRIDTrace.WriteLine(string.Format("FLAG[{0}], SQL[{1}]", statementType, CommandText));
      }
    }

    internal CUBRIDDataReader ExecuteInternal()
    {
      if (parameters != null && IsAllParameterBound() == false)
        throw new CUBRIDException(Utils.GetStr(MsgId.NotAllParametersAreBound));

      byte[] paramModes = null;
      byte fetchFlag = 0;

      if (statementType == CUBRIDStatementType.CUBRID_STMT_CALL_SP && parameters != null)
      {
        paramModes = new byte[parameters.Length];
        for (int i = 0; i < parameters.Length; i++)
        {
          paramModes[i] = Convert.ToByte(parameters[i].Direction);
        }
      }

      if (statementType == CUBRIDStatementType.CUBRID_STMT_SELECT)
      {
        fetchFlag = 1;
      }

      int totalTupleCount = conn.Stream.RequestExecute(handle, CCIExecutionOption.CCI_EXEC_NORMAL, parameters,
                                                       paramModes, fetchFlag, conn.AutoCommit);
      if (conn.LogTraceApi)
      {
        CUBRIDTrace.WriteLine(string.Format("FLAG[{0}], MAX_COL_SIZE[{1}]", statementType, 0));
      }

      cache_reusable = conn.Stream.ReadByte();
      resultCount = conn.Stream.ReadInt();
      resultInfo = conn.Stream.ReadResultInfo(resultCount);

      if (statementType == CUBRIDStatementType.CUBRID_STMT_SELECT)
      {
        conn.Stream.ReadByte();
        conn.Stream.ReadInt();
        int tupleCount = conn.Stream.ReadInt();
        dataReader = new CUBRIDDataReader(this, handle, totalTupleCount, columnInfos, tupleCount);

        return dataReader;
      }

      return null;
    }

    internal bool NextResult()
    {
      int totalTupleCount = conn.Stream.RequestNextResult(handle);
      CUBRIDStatementType commandTypeIs = (CUBRIDStatementType)conn.Stream.ReadByte();
      conn.Stream.ReadByte();
      int columnCount = conn.Stream.ReadInt();
      columnInfos = conn.Stream.ReadColumnInfo(columnCount);

      if (commandTypeIs == CUBRIDStatementType.CUBRID_STMT_SELECT)
      {
        dataReader = new CUBRIDDataReader(this, handle, totalTupleCount, columnInfos);
      }

      return true;
    }

    private bool IsAllParameterBound()
    {
      return parameters.All(t => t != null);
    }

    internal void BindParameter(int index, CUBRIDDataType type, object value)
    {
      parameters[index] = new CUBRIDParameter();
      parameters[index].CUBRIDDataType = type;
      parameters[index].Value = value;
    }

    internal void BindParameter(int index, CUBRIDDataType type, object value, ParameterDirection direction)
    {
      BindParameter(index, type, value);
      parameters[index].Direction = direction;
    }

    internal void BindParameter(int index, CUBRIDParameter bindParameter)
    {
      parameters[index] = bindParameter;
    }

    /*
         * [APIS-220] The CUBRID no longer support CAS_FC_MAKE_OUT_RS.
    internal void GetOutResultSet(int handle)
    {
      this.handle = this.conn.Stream.RequestOutResultSet(handle); //TODO: check if we need to free "old" handle
      this.statementType = (CUBRIDStatementType)this.conn.Stream.ReadByte();
      this.resultCount = this.conn.Stream.ReadInt();
      this.isUpdateable = (this.conn.Stream.ReadByte() == 1);
      this.columnCount = this.conn.Stream.ReadInt();

      this.columnInfos = this.conn.Stream.ReadColumnInfo(columnCount);
    }
        */

    private bool IsQueryStatement()
    {
      switch (statementType)
      {
        case CUBRIDStatementType.CUBRID_STMT_SELECT:
        case CUBRIDStatementType.CUBRID_STMT_CALL:
        case CUBRIDStatementType.CUBRID_STMT_GET_STATS:
        case CUBRIDStatementType.CUBRID_STMT_EVALUATE:
          return true;
      }

      return false;
    }

    #region ICloneable

    /// <summary>
    ///   Creates a new object that is a copy of the current instance.
    /// </summary>
    /// <returns> A new object that is a copy of this instance. </returns>
    object ICloneable.Clone()
    {
      return Clone();
    }

    /// <summary>
    ///   Clones this instance.
    /// </summary>
    /// <returns> </returns>
    public CUBRIDCommand Clone()
    {
      using (CUBRIDCommand clone = new CUBRIDCommand(cmdText, conn, transaction))
      {
        clone.CommandType = CommandType;
        clone.cmdTimeout = cmdTimeout;
        clone.UpdatedRowSource = UpdatedRowSource;

        if (parameters != null)
        {
          foreach (CUBRIDParameter p in parameters)
          {
            clone.Parameters.Add(p.Clone());
          }
        }
        return clone;
      }
    }

    #endregion
  }
}