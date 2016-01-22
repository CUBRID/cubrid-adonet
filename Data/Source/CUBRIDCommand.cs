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
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// CUBRID implementation of the <see cref="T:System.Data.Common.DbCommand"/> class.
	/// </summary>
	public sealed class CUBRIDCommand : DbCommand, ICloneable
	{
		private CUBRIDConnection conn;
		private CUBRIDTransaction transaction;
		private string cmdText;
		private CUBRIDParameterCollection paramCollection;
		private bool isPrepared;
		private CUBRIDDataReader dataReader;
		private CommandType cmdType;

		private UpdateRowSource updatedRowSource = UpdateRowSource.Both;

		private int handle;
		private int resultCacheLifetime;
		private CUBRIDStatementType statementType;
		private int bindCount;
		private bool isUpdateable;
		private int columnCount;
		public ColumnMetaData[] columnInfos;
		private CUBRIDParameter[] parameters;
		private int cache_reusable;
		private int resultCount;
		private ResultInfo[] resultInfo;

		private int cmdTimeout = 15; //seconds
		private StmtType stmtType = StmtType.NORMAL;

		private bool designTimeVisible = false;

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDCommand"/> class.
		/// </summary>
		public CUBRIDCommand()
		{
			this.paramCollection = new CUBRIDParameterCollection();
			this.isPrepared = false;

			this.cmdType = CommandType.Text;
			this.cmdText = String.Empty;
			this.stmtType = StmtType.NORMAL;
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
		/// Initializes a new instance of the <see cref="CUBRIDCommand"/> class.
		/// </summary>
		/// <param name="sql">The SQL statement.</param>
		public CUBRIDCommand(string sql)
			: this()
		{
			this.cmdText = sql;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDCommand"/> class.
		/// </summary>
		/// <param name="sql">The SQL statement.</param>
		/// <param name="conn">The connection.</param>
		public CUBRIDCommand(string sql, CUBRIDConnection conn)
			: this(sql)
		{
			this.conn = conn;
            this.paramCollection.SetParametersEncoding(conn.GetEncoding());
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDCommand"/> class.
		/// </summary>
		/// <param name="sql">The SQL statement.</param>
		/// <param name="conn">The connection.</param>
		/// <param name="transaction">The transaction.</param>
		public CUBRIDCommand(string sql, CUBRIDConnection conn, CUBRIDTransaction transaction)
			: this(sql, conn)
		{
			this.transaction = transaction;
		}

		/// <summary>
		/// Gets the columns metadata.
		/// </summary>
		public ColumnMetaData[] ColumnInfos
		{
			get { return this.columnInfos; }
		}

		/// <summary>
		/// Gets or sets the text command to run against the data source.
		/// </summary>
		/// <returns>
		/// The command SQL statement to execute.
		///   </returns>
		public override string CommandText
		{
			get
			{
				if (this.CommandType == CommandType.TableDirect)
				{
					if (this.cmdText.Length > 0 && this.cmdText.StartsWith("select * from `", StringComparison.InvariantCultureIgnoreCase))
					{
						string str = this.cmdText.Substring("select * from `".Length);
						str = str.Substring(0, str.Length - 1);

						return str;
					}
				}
				return this.cmdText;
			}
			set
			{
				this.cmdText = value.Trim();
				if (this.CommandType == CommandType.TableDirect)
				{
					if (this.cmdText.Length > 0 && !this.cmdText.StartsWith("select * from `", StringComparison.InvariantCultureIgnoreCase))
					{
						this.cmdText = "select * from `" + value.Trim() + "`";
					}
				}
			}
		}

		/// <summary>
		/// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
		/// </summary>
		/// <returns>
		/// The time in seconds to wait for the command to execute.
		///   </returns>
		public override int CommandTimeout
		{
			get { return this.cmdTimeout; }
			set { this.cmdTimeout = value; }
		}

		/// <summary>
		/// Indicates or specifies how the <see cref="P:System.Data.Common.DbCommand.CommandText"/> property is interpreted.
		/// </summary>
		/// <returns>
		/// One of the <see cref="T:System.Data.CommandType"/> values.
		///   </returns>
		public override CommandType CommandType
		{
			get { return this.cmdType; }
			set
			{
				this.cmdType = value;
				if (this.CommandType == CommandType.TableDirect)
				{
					if (this.cmdText.Length > 0 && this.cmdText.StartsWith("select * from `", StringComparison.InvariantCultureIgnoreCase))
					{
						this.cmdText = "select * from `" + this.cmdText + "`";
					}
				}
			}
		}

		/// <summary>
		/// Gets or sets the <see cref="T:System.Data.Common.DbConnection"/> used by this <see cref="T:System.Data.Common.DbCommand"/>.
		/// </summary>
		/// <returns>
		/// The connection to the data source.
		///   </returns>
		protected override DbConnection DbConnection
		{
			get { return this.conn; }
			set { this.conn = (CUBRIDConnection)value; }
		}

		internal CUBRIDConnection CUBRIDbConnection
		{
			get { return this.conn; }
			set
			{
				if (this.conn != value)
				{
					this.transaction = null;
					this.conn = value;
				}
			}
		}

		/// <summary>
		/// Gets or sets the <see cref="P:CUBRID.Data.CUBRIDClient.DbCommand.CUBRIDTransaction"/>.
		/// </summary>
		/// <returns>
		/// The transaction within which a Command object of a .NET Framework data provider executes. The default value is a null reference (Nothing in Visual Basic).
		///   </returns>
		protected override DbTransaction DbTransaction
		{
			get { return this.transaction; }
			set { this.transaction = (CUBRIDTransaction)value; }
		}

		/// <summary>
		/// Gets or sets a value indicating whether the command object should be visible in a customized interface control.
		/// </summary>
		/// <returns>true, if the command object should be visible in a control; otherwise false. The default is true.
		///   </returns>
		public override bool DesignTimeVisible
		{
			get { return this.designTimeVisible; }
			set { this.designTimeVisible = value; }
		}

		/// <summary>
		/// Gets or sets how command results are applied to the <see cref="T:System.Data.DataRow"/> when used by the Update method of a <see cref="T:System.Data.Common.DbDataAdapter"/>.
		/// </summary>
		/// <returns>
		/// One of the <see cref="T:System.Data.UpdateRowSource"/> values. The default is Both unless the command is automatically generated. Then the default is None.
		///   </returns>
		public override UpdateRowSource UpdatedRowSource
		{
			get { return this.updatedRowSource; }
			set { this.updatedRowSource = value; }
		}

		/// <summary>
		/// Gets the collection of <see cref="T:System.Data.Common.DbParameter"/> objects.
		/// </summary>
		/// <returns>
		/// The parameters of the SQL statement or stored procedure.
		///   </returns>
		protected override DbParameterCollection DbParameterCollection
		{
			get { return this.paramCollection; }
		}

		/// <summary>
		/// Gets the collection of <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDParameter"/> objects.
		/// </summary>
		/// <returns>
		/// The parameters of the SQL statement or stored procedure.
		///   </returns>
		public new CUBRIDParameterCollection Parameters
		{
			get { return this.paramCollection; }
		}

		/// <summary>
		/// Gets a value indicating whether this instance is prepared.
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this instance is prepared; otherwise, <c>false</c>.
		/// </value>
		public bool IsPrepared
		{
			get { return this.isPrepared; }
		}

		internal CUBRIDStatementType StatementType
		{
			get { return this.statementType; }
			set { this.statementType = value; }
		}

		internal StmtType StmtType
		{
			get { return this.stmtType; }
			set { this.stmtType = value; }
		}

		private CCIPrepareOption GetPrepareOption()
		{
			switch (this.cmdType)
			{
				case CommandType.StoredProcedure:
					return CCIPrepareOption.CCI_PREPARE_CALL;
				default:
					break;
			}

			return CCIPrepareOption.CCI_PREPARE_NORMAL;
		}

		/// <summary>
		/// Creates a prepared (or compiled) version of the command on the data source.
		/// </summary>
		public override void Prepare()
		{
			if (this.conn == null)
				throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionPropertyHasNotBeenSet));

			if (this.conn.State != ConnectionState.Open)
				throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

			if (this.cmdText == null || this.cmdText.Trim().Length == 0)
				return;

			PrepareInternal(this.cmdText, GetPrepareOption());

			this.isPrepared = true;
		}

		/// <summary>
		/// Creates a new instance of a <see cref="T:System.Data.Common.DbParameter"/> object.
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.Data.Common.DbParameter"/> object.
		/// </returns>
		protected override DbParameter CreateDbParameter()
		{
			return new CUBRIDParameter();
		}

		private void BindParameters()
		{
			if (this.isPrepared == false)
			{
				Prepare();
			}

			//TODO Verify if other initializations are required
			if (this.parameters == null && this.paramCollection.Count > 0)
			{
				//Initialize parameters collection
				this.parameters = new CUBRIDParameter[this.paramCollection.Count];
			}

			for (int i = 0; i < this.paramCollection.Count; i++)
			{
				BindParameter(i, (CUBRIDParameter)this.paramCollection[i]);
			}

			//TODO Verify if these initializations are required
			this.bindCount = this.paramCollection.Count;
		}

		/// <summary>
		/// Executes the query and returns the first column of the first row in the result set returned by the query. All other columns and rows are ignored.
		/// </summary>
		/// <returns>
		/// The first column of the first row in the result set.
		/// </returns>
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
		/// Attempts to cancels the execution of a <see cref="T:System.Data.Common.DbCommand"/>.
		/// Not supported yet.
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
		/// Gets the generated keys (Auto-increment columns).
		/// </summary>
		/// <returns>
		/// A <see cref="T:System.Data.Common.DbDataReader"/>.
		/// </returns>
		public DbDataReader GetGeneratedKeys()
		{
            if (conn.AutoCommit == true)
                throw new CUBRIDException("AutoCommit must be false");
			return this.conn.Stream.GetGeneratedKeys(this, this.handle);
		}

		/// <summary>
		/// Resets the internal data reader.
		/// </summary>
		private void ResetDataReader()
		{
			if (this.dataReader != null)
			{
				this.dataReader.ForceClose();
				this.dataReader = null;
				Close();
			}
		}

        /// <summary>
		/// Executes the command text against the connection.
		/// </summary>
		/// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior"/>.</param>
		/// <returns>
		/// A <see cref="T:System.Data.Common.DbDataReader"/>.
		/// </returns>
		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
		{
			//WORKAROUND for Exception: DataReader is already open
			//if (this.dataReader != null)
			//	ResetDataReader();

			if (this.dataReader != null)
				throw new CUBRIDException(Utils.GetStr(MsgId.DataReaderIsAlreadyOpen));

			BindParameters();

			if (!IsQueryStatement())
				throw new CUBRIDException(Utils.GetStr(MsgId.InvalidQueryType));

			ExecuteInternal();

			return this.dataReader;
		}

		/// <summary>
		/// Executes a SQL statement against a connection object.
		/// </summary>
		/// <returns>
		/// The number of rows affected.
		/// </returns>
		public override int ExecuteNonQuery()
		{
			BindParameters();

			if (IsQueryStatement())
				throw new CUBRIDException(Utils.GetStr(MsgId.InvalidQueryType));

			ExecuteInternal();

            // [APIS-223] Call a stored procedure's return value is not assigned to the return parameter.
			if (this.statementType == CUBRIDStatementType.CUBRID_STMT_CALL ||
                this.statementType == CUBRIDStatementType.CUBRID_STMT_CALL_SP)
			{
				this.conn.Stream.RequestFetch(this.handle);
				this.conn.Stream.ReadInt(); //It is always == 1? (it is the number of tuples returned by the server)

				int colCount = GetOutModeParameterCount() + 1;
				ResultTuple tuple = new ResultTuple(colCount);
				this.conn.Stream.ReadResultTupleSP(tuple, colCount, this.conn);

				int k = 1;
				for (int i = 0; i < this.parameters.Length; i++)
				{
					if (this.parameters[i].Direction == ParameterDirection.Output ||
							this.parameters[i].Direction == ParameterDirection.InputOutput)
					{
						parameters[i].Value = tuple[k];
						k++;
					}
				}
			}

			return this.resultCount;
		}

		private int GetOutModeParameterCount()
		{
			int count = 0;

			for (int i = 0; i < parameters.Length; i++)
			{
				if (parameters[i].Direction == ParameterDirection.Output ||
						parameters[i].Direction == ParameterDirection.InputOutput)
				{
					count++;
				}
			}

			return count;
		}

		#region ICloneable

		/// <summary>
		/// Clones this instance.
		/// </summary>
		/// <returns></returns>
		public CUBRIDCommand Clone()
		{
            using (CUBRIDCommand clone = new CUBRIDCommand(this.cmdText, this.conn, this.transaction))
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

		/// <summary>
		/// Creates a new object that is a copy of the current instance.
		/// </summary>
		/// <returns>
		/// A new object that is a copy of this instance.
		/// </returns>
		object ICloneable.Clone()
		{
			return this.Clone();
		}

		#endregion

		public void Close()
		{
			this.conn.Stream.RequestCloseHandle(this.handle);
		}

		internal int BindCount
		{
			get { return this.bindCount; }
		}

		internal void PrepareInternal(string sql, CCIPrepareOption prepareOption)
		{
			this.conn.ReconnectIfNeeded();

			this.conn.Stream.RequestPrepare(sql, prepareOption, this.conn.GetEncoding());

			this.handle = this.conn.Stream.ResponseCode;
			this.resultCacheLifetime = this.conn.Stream.ReadInt();
			this.statementType = (CUBRIDStatementType)this.conn.Stream.ReadByte();
			this.bindCount = this.conn.Stream.ReadInt();
			this.isUpdateable = (this.conn.Stream.ReadByte() == 1);
			this.columnCount = this.conn.Stream.ReadInt();

			this.columnInfos = this.conn.Stream.ReadColumnInfo(columnCount);

			if (this.bindCount > 0)
			{
				this.parameters = new CUBRIDParameter[this.bindCount];
			}

			if (this.statementType == CUBRIDStatementType.CUBRID_STMT_CALL_SP)
			{
				this.columnCount = this.bindCount + 1;
			}

            if (this.conn.LogTraceAPI)
            {
                CUBRIDTrace.WriteLine(string.Format("FLAG[{0}], SQL[{1}]", this.statementType, this.CommandText));
            }
		}

		internal CUBRIDDataReader ExecuteInternal()
		{
			if (parameters != null && IsAllParameterBound() == false)
				throw new CUBRIDException(Utils.GetStr(MsgId.NotAllParametersAreBound));

			byte[] paramModes = null;
			byte fetchFlag = 0;

			if (this.statementType == CUBRIDStatementType.CUBRID_STMT_CALL_SP && this.parameters != null)
			{
				paramModes = new byte[this.parameters.Length];
				for (int i = 0; i < this.parameters.Length; i++)
				{
					paramModes[i] = (byte)Convert.ToByte(this.parameters[i].Direction);
				}
			}

			if (this.statementType == CUBRIDStatementType.CUBRID_STMT_SELECT)
			{
				fetchFlag = 1;
			}

			int totalTupleCount = this.conn.Stream.RequestExecute(this.handle, CCIExecutionOption.CCI_EXEC_NORMAL, parameters,
					paramModes, fetchFlag, conn.AutoCommit);
            if (this.conn.LogTraceAPI)
            {
                CUBRIDTrace.WriteLine(string.Format("FLAG[{0}], MAX_COL_SIZE[{1}]", this.statementType, 0));
            }

			this.cache_reusable = this.conn.Stream.ReadByte();
			this.resultCount = this.conn.Stream.ReadInt();
			this.resultInfo = this.conn.Stream.ReadResultInfo(resultCount);

			if (this.statementType == CUBRIDStatementType.CUBRID_STMT_SELECT)
			{
                this.conn.Stream.ReadByte();
				int fetchCode = this.conn.Stream.ReadInt();
				int tupleCount = this.conn.Stream.ReadInt();
				this.dataReader = new CUBRIDDataReader(this, this.handle, totalTupleCount, this.columnInfos, tupleCount);

				return this.dataReader;
			}

			return null;
		}

		internal bool NextResult()
		{
			int totalTupleCount = this.conn.Stream.RequestNextResult(handle);
			CUBRIDStatementType commandTypeIs = (CUBRIDStatementType)this.conn.Stream.ReadByte();
			bool isUpdatable = (this.conn.Stream.ReadByte() == 1) ? true : false;
			int columnCount = this.conn.Stream.ReadInt();
			this.columnInfos = this.conn.Stream.ReadColumnInfo(columnCount);

			if (commandTypeIs == CUBRIDStatementType.CUBRID_STMT_SELECT)
			{
				this.dataReader = new CUBRIDDataReader(this, this.handle, totalTupleCount, this.columnInfos);
			}

			return true;
		}

		private bool IsAllParameterBound()
		{
			for (int i = 0; i < this.parameters.Length; i++)
			{
				if (this.parameters[i] == null)
				{
					return false;
				}
			}

			return true;
		}

		internal void BindParameter(int index, CUBRIDDataType type, object value)
		{
			this.parameters[index] = new CUBRIDParameter();
			this.parameters[index].CUBRIDDataType = type;
			this.parameters[index].Value = value;
		}

		internal void BindParameter(int index, CUBRIDDataType type, object value, ParameterDirection direction)
		{
			BindParameter(index, type, value);
			this.parameters[index].Direction = direction;
		}

        internal void BindParameter(int index, CUBRIDParameter bindParameter)
		{
			this.parameters[index] = bindParameter;
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

				default:
					break;
			}

			return false;
		}

		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
		}

	}
}
