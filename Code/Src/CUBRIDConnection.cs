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
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace CUBRID.Data.CUBRIDClient
{
  //[ToolboxBitmap(typeof(CUBRIDConnection), "CUBRIDClient.resources.conn.bmp")]
  /// <summary>
  ///   CUBRID implementation of the <see cref="T:System.Data.Common.DbConnection" /> class.
  /// </summary>
  [DesignerCategory("Code")]
  [ToolboxItem(true)]
  public sealed class CUBRIDConnection : DbConnection, ICloneable
  {
    #region Delegates

    /// <summary>
    ///   Represents the method that will handle the <see cref="CUBRIDConnection.CUBRIDInfoMessageEventArgs" /> event of a 
    ///   <see cref="CUBRIDConnection" />.
    /// </summary>
    public delegate void CUBRIDInfoMessageEventHandler(object sender, CUBRIDInfoMessageEventArgs args);

    #endregion

    private const int _defaultPort = 33000;
    private readonly CUBRIDConnectionProperties connProperties;

    private bool autoCommit = true;
    private string connEncoding = "utf-8";
    private ConnectionState connState = ConnectionState.Closed;
    private string connString;
    private int connTimeout = 30; //seconds
    private string database = "";
    private string db_version = "";
    private CUBRIDIsolationLevel isolationLevel = CUBRIDIsolationLevel.TRAN_DEFAULT_ISOLATION;
    private int lockTimeout = -1; //the connection lock timeout as milliseconds.
    private const int maxStringLength = Int16.MaxValue;
    private string password = "";
    private int port = _defaultPort;
    private CUBRIDSchemaProvider schemaProvider;
    private string server = "";
    private const string serverVersion = "";
    private int sessionId = 0;
    private int con_id = 0;
    private string user = "";

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDConnection" /> class.
    /// </summary>
    public CUBRIDConnection()
    {
      Trace.WriteLineIf(Utils.TraceState, String.Format("CUBRIDConnection"));
      connProperties = new CUBRIDConnectionProperties();
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDConnection" /> class.
    /// </summary>
    /// <param name="connString"> The connection string. </param>
    public CUBRIDConnection(string connString)
      : this()
    {
      ConnectionString = connString;
    }

    #region ICloneable

    /// <summary>
    ///   Creates a new object that is a copy of the current instance.
    /// </summary>
    /// <returns> A new object that is a copy of this instance. </returns>
    object ICloneable.Clone()
    {
        CUBRIDConnection clone = new CUBRIDConnection();
        string connectionString = ConnectionString;
        if (connectionString != null) {
            clone.ConnectionString = connectionString;
        }
         
        return clone; 
    }

    /// <summary>
    ///   Creates a CUBRIDConnection clone
    /// </summary>
    /// <returns> CUBRIDConnection clone </returns>
    public CUBRIDConnection Clone()
    {
      return new CUBRIDConnection(connString);
      //We will let the user Open() the connection clone
    }

    #endregion

    #region IDisposeable

    /// <summary>
    ///   Releases the unmanaged resources used by the <see cref="T:System.ComponentModel.Component" /> and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing"> true to release both managed and unmanaged resources; false to release only unmanaged resources. </param>
    protected override void Dispose(bool disposing)
    {
      //Send listeners
      foreach (TraceListener listener in Trace.Listeners)
      {
        listener.Flush();
      }

      if (connState == ConnectionState.Open)
      {
        Trace.WriteLineIf(Utils.TraceState, String.Format("Dispose::Close conn"));
        Close();
      }
      base.Dispose(disposing);
    }

    #endregion

    /// <summary>
    ///   Gets or sets the session id.
    /// </summary>
    /// <value> The session id. </value>
    public int SessionId
    {
      get 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return sessionId;
      }
    }

    /// <summary>
    ///   Gets or sets the database version.
    /// </summary>
    /// <value> The database version. </value>
    public string DbVersion
    {
      get 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return db_version; 
      }
      set { db_version = value; }
    }

    /// <summary>
    ///   Gets conection.
    /// </summary>
    /// <value> The Conection. </value>
    public int Conection
    {
        get { return con_id; }
    }

    /// <summary>
    ///   Gets or sets the logTraceApi flag.
    /// </summary>
    /// <value> The value of the LogTraceApi flag. </value>
    public bool LogTraceApi
    {
      get { return connProperties.LogTraceApi; }
    }

    /// <summary>
    ///   Gets or sets the logTraceNetwork flag.
    /// </summary>
    /// <value> The value of the LogTraceNetwork flag. </value>
    public bool LogTraceNetwork
    {
      get { return connProperties.LogTraceNetwork; }
    }

    /// <summary>
    ///   Gets or sets the string used to open the connection.
    /// </summary>
    /// <returns> The connection string used to establish the initial connection. The exact contents of the connection string depend on the specific data source for this connection. The default value is an empty string. </returns>
    [Browsable(true)]
    [Category("Data")]
    [Description(
      "Information used to connect to a database, such as 'server=<xxx>;port=99999;user=<yyy>;password=<zzz>;database=<dbname>'."
      )]
    public override string ConnectionString
    {
      get { return connString; }
      set
      {
        if (connState != ConnectionState.Closed)
        {
          throw new CUBRIDException(Utils.GetStr(MsgId.NotAllowedToChangeConnectionStringWhenStateIs) + ": " + State +
                                    ".");
        }
        if (value == null)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.ConnectionStringIsNULL) + ": " + State + ".");
        }
        connString = value;
        connProperties.Reset(); // reset properties

        ParseConnectionString(); // parser connection string
      }
    }

    /// <summary>
    ///   Gets the time to wait while establishing a connection before terminating the attempt and generating an error.
    /// </summary>
    /// <returns> The time (in seconds) to wait for a connection to open. The default value is determined by the specific type of connection that you are using. </returns>
    [Browsable(true)]
    public override int ConnectionTimeout
    {
      get { return connTimeout; }
    }

    /// <summary>
    ///   Gets the database schema provider.
    /// </summary>
    /// <returns> The current database schema provider. </returns>
    public CUBRIDSchemaProvider SchemaProvider
    {
      get { return schemaProvider ?? (schemaProvider = new CUBRIDSchemaProvider(this)); }
    }

    /// <summary>
    ///   Gets/Sets the connection lock timeout(Unit: milliseconds).
    /// </summary>
    [Browsable(true)]
    public int LockTimeout
    {
      get { return lockTimeout; }
      set { lockTimeout = value; }
    }

    /*
         * Delete this method
         * http://jira.cubrid.org/browse/APIS-485
    /// <summary>
    /// Sets the lock timeout.
    /// </summary>
    /// <param name="value">The value.</param>
    public void SetLockTimeout(int value)
    {
      if (this.connState != ConnectionState.Closed)
      {
        throw new CUBRIDException(Utils.GetStr(MsgId.NotAllowedToChangeLockTimeoutWhenStateIs) + ": " + State + ".");
      }
      if (value >= 1 && value <= 99)
      {
        this.lockTimeout = value;
      }
      else
      {
        throw new Exception(Utils.GetStr(MsgId.InvalidTimeoutValue));
      }
    }
        */

    /// <summary>
    ///   Gets the name of the current database after a connection is opened, or the database name specified in the connection string before the connection is opened.
    /// </summary>
    /// <returns> The name of the current database or the name of the database to be used after a connection is opened. The default value is an empty string. </returns>
    [Browsable(true)]
    public override string Database
    {
      get { return database; }
    }

    /// <summary>
    ///   Gets a string that describes the state of the connection.
    /// </summary>
    /// <returns> The state of the connection. The format of the string returned depends on the specific type of connection you are using. </returns>
    [Browsable(false)]
    public override ConnectionState State
    {
      get { return connState; }
    }

    /// <summary>
    ///   Gets a string that represents the version of the server to which the object is connected.
    /// </summary>
    /// <returns> The version of the database. The format of the string returned depends on the specific type of connection you are using. </returns>
    [Browsable(false)]
    public override string ServerVersion
    {
        get 
        {
            if (State == ConnectionState.Closed)
            {
                throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
            }
            return db_version; 
        }
    }

    /// <summary>
    ///   Gets the name of the database server to which to connect.
    /// </summary>
    /// <returns> The name of the database server to which to connect. The default value is an empty string. </returns>
    [Browsable(true)]
    public override string DataSource
    {
      get 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return server; 
      }
    }

    /// <summary>
    ///   Gets the value indicating whether auto commit is set.
    /// </summary>
    /// <value> <c>true</c> if auto commit is set; otherwise, <c>false</c> . </value>
    [Browsable(true)]
    public bool AutoCommit
    {
      get 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return autoCommit; 
      }
    }

    /// <summary>
    ///   Gets or sets the length of the max string.
    /// </summary>
    /// <value> The length of the max string. </value>
    public int MaxStringLength
    {
      get 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return maxStringLength;
      }
    }

    /// <summary>
    ///   Gets the <see cref="T:System.Data.Common.DbProviderFactory" /> for this <see cref="T:System.Data.Common.DbConnection" />.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.Common.DbProviderFactory" /> . </returns>
    protected override DbProviderFactory DbProviderFactory
    {
      get
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return CUBRIDClientFactory.Instance; 
      }
    }

    /// <summary>
    ///   Gets or sets the isolation level.
    /// </summary>
    /// <value> The isolation level. </value>
    public CUBRIDIsolationLevel IsolationLevel
    {
      get 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          return GetIsolationLevel(); 
      }
      set 
      {
          if (State == ConnectionState.Closed)
          {
              throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
          }
          SetIsolationLevel(value);
      }
    }

    /// <summary>
    ///   Sets the connection timeout.The interface affect Open() only, can't affect OpenAsync().
    /// </summary>
    /// <param name="value"> The value. </param>
    public void SetConnectionTimeout(int value)
    {
      if (connState != ConnectionState.Closed)
      {
        throw new CUBRIDException(Utils.GetStr(MsgId.NotAllowedToChangeConnectionTimeoutWhenStateIs) + ": " + State +
                                  ".");
      }
      if (value <= Int32.MaxValue)
      {
        connTimeout = value;
      }
      else
      {
        throw new Exception(Utils.GetStr(MsgId.InvalidTimeoutValue));
      }
    }

    private void SetConnectionState(ConnectionState value)
    {
      if (connState != value)
      {
        connState = value;
      }
    }

    /// <summary>
    ///   Changes the current database for an open connection.
    /// </summary>
    /// <param name="databaseName"> Specifies the name of the database for the connection. </param>
    public override void ChangeDatabase(string databaseName)
    {
      throw new NotSupportedException(Utils.GetStr(MsgId.CantChangeDatabase));
    }

    /// <summary>
    ///   Starts a database transaction.
    /// </summary>
    /// <param name="isolationLevel"> Specifies the isolation level for the transaction. </param>
    /// <returns> An object representing the new transaction. </returns>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
      autoCommit = false;
      CUBRIDIsolationLevel level;
      //TODO Verify these mappings
      switch (isolationLevel)
      {
        case System.Data.IsolationLevel.Chaos:
          throw new CUBRIDException(Utils.GetStr(MsgId.NotSupportedInCUBRID));
        case System.Data.IsolationLevel.ReadCommitted:
          level = CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE;
          break;
        case System.Data.IsolationLevel.ReadUncommitted:
          throw new CUBRIDException(Utils.GetStr(MsgId.NotSupportedInCUBRID));
        case System.Data.IsolationLevel.RepeatableRead:
          level = CUBRIDIsolationLevel.TRAN_REP_CLASS_REP_INSTANCE;
          break;
        case System.Data.IsolationLevel.Serializable:
          level = CUBRIDIsolationLevel.TRAN_SERIALIZABLE;
          break;
        case System.Data.IsolationLevel.Snapshot:
          throw new CUBRIDException(Utils.GetStr(MsgId.NotSupportedInCUBRID));
        case System.Data.IsolationLevel.Unspecified:
          //Default value
          level = CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE;
          break;
        default:
          level = CUBRIDIsolationLevel.TRAN_REP_CLASS_REP_INSTANCE;
          break;
      }
      SetAutoCommit(false);
      return new CUBRIDTransaction(this, level);
    }

    /// <summary>
    ///   Begins the transaction.
    /// </summary>
    /// <returns> </returns>
    public new CUBRIDTransaction BeginTransaction()
    {
      return BeginTransaction(GetIsolationLevel());
    }

    /// <summary>
    ///   Begins the transaction.
    /// </summary>
    /// <param name="iso"> The isolation level. </param>
    /// <returns> </returns>
    public CUBRIDTransaction BeginTransaction(CUBRIDIsolationLevel iso)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        SetAutoCommit(false);
        CUBRIDTransaction t = new CUBRIDTransaction(this, iso);

        return t;
    }

    /// <summary>
    ///   Closes the connection to the database. This is the preferred method of closing any open connection.
    /// </summary>
    /// <exception cref="T:System.Data.Common.DbException">The connection-level error that occurred while opening the connection.</exception>
    public override void Close()
    {
      if (connState == ConnectionState.Closed)
        return;
      T_CCI_ERROR err = new T_CCI_ERROR();
      try
      {
          CciInterface.cci_disconnect(con_id, ref err);
      }
      finally
      {
          SetConnectionState(ConnectionState.Closed);
      }
    }

    /// <summary>
    ///   Creates and returns a <see cref="T:System.Data.Common.DbCommand" /> object associated with the current connection.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.Common.DbCommand" /> object. </returns>
    protected override DbCommand CreateDbCommand()
    {
      return CreateCommand();
    }

    /// <summary>
    ///   Creates a CUBRIDCommand.
    /// </summary>
    /// <returns> </returns>
    public new CUBRIDCommand CreateCommand()
    {
        if (State == ConnectionState.Closed)
        {
            /* no raising exception for iBatis environment */
        }

        CUBRIDCommand cmd = new CUBRIDCommand();
        cmd.Connection = this;

        return cmd;
    }
  
    /// <summary>
    ///   Opens a database connection with the settings specified by the <see
    ///    cref="P:System.Data.Common.DbConnection.ConnectionString" />.
    /// </summary>
    public override void Open()
    {
        Trace.WriteLineIf(Utils.TraceState,
                        String.Format(
                          "Open::Connection parameters: server: {0}, port: {1}, database: {2}, user: {3}, password: {4}",
                          server, port, database, user, password));
        if (connState != ConnectionState.Closed)
        {
            throw new CUBRIDException("Connection is already open!");
        }
        string connect_url = null;
        if (connProperties.PropertiesString == null || connProperties.PropertiesString.Length == 0)
        {
            connect_url = string.Format("cci:CUBRID:{0}:{1}:{2}:::", server, port, database);
        }
        else
        {
            connect_url = string.Format("cci:CUBRID:{0}:{1}:{2}:::?{3}", server, port, database, connProperties.PropertiesString);
        }
 

        T_CCI_ERROR err = new T_CCI_ERROR();
        con_id = CciInterface.cci_connect_with_url_ex(connect_url, user, password, ref err);
        if (con_id <= 0)
        {
            SetConnectionState(ConnectionState.Closed);
            throw new CUBRIDException(err.err_code, err.err_msg);            
        }
        else
        {
            SetConnectionState(ConnectionState.Open);
        }

        SetEncoding(connEncoding);
     }
     private void ParseConnectionString()
     {
      char[] delimiter = { ';' };
      string[] tokens = connString.Split(delimiter);

      server = "";
      database = "";
      port = 0;
      user = "";
      password = " ";

      if (tokens[0] == connString)
      {
        // URL mode
        // parse url string
        string[] tokenURL = connString.Split('?');
        if (tokenURL.Length > 0)
        {
          tokens = tokenURL[0].Split(':');
          {
            // default connection properties
            server = tokens[2]; // server
            port = Convert.ToInt32(tokens[3]); // broker port
            database = tokens[4]; // database
            user = tokens[5]; // user
            password = tokens[6]; // password
          }

          // parse connection properties
          if(tokenURL.Length>1)
            connProperties.SetConnectionProperties(tokenURL[1]);
          // auto-commit
          autoCommit = connProperties.AutoCommit;

          if (connProperties.LoginTimeout >= 1000)
            connTimeout = connProperties.LoginTimeout / 1000;

          if (connProperties.LogTraceApi)
          {
            CUBRIDTrace.WriteLine(String.Format("URL[{0}]", connString));
          }
        }
      }
      else
      {
        //If the connection string ends with (;) remove last token
        if (tokens[tokens.Length - 1] == String.Empty)
          Array.Resize(ref tokens, tokens.Length - 1);
        foreach (string t in tokens)
        {
          string[] pair = t.Split('=');
          if (pair.Length == 2)
          {
            if (pair[0].ToLower().Equals("server"))
            {
              server = pair[1];
            }
            else if (pair[0].ToLower().Equals("database"))
            {
              database = pair[1];
            }
            else if (pair[0].ToLower().Equals("port"))
            {
              port = Convert.ToInt32(pair[1]);
            }
            else if (pair[0].ToLower().Equals("user"))
            {
              user = pair[1];
            }
            else if (pair[0].ToLower().Equals("password"))
            {
              password = pair[1];
            }
            else if (pair[0].ToLower().Equals("charset"))
            {
              connEncoding = pair[1];
            }
            else if (pair[0].ToLower().Equals("autocommit"))
            {
              autoCommit = Convert.ToInt32(pair[1]) != 0;
            }
          }
          else
          {
            throw new CUBRIDException(Utils.GetStr(MsgId.InvalidConnectionParameter) + " : " + t);
          }
        }
      }

      if (database == string.Empty || database.Length == 0)
      {
        throw new CUBRIDException(Utils.GetStr(MsgId.DBNameIsEmpty));
      }
      if (server == string.Empty || server.Length == 0)
      {
          throw new CUBRIDException(Utils.GetStr(MsgId.ServerIsEmpty));
      }
      if (user == string.Empty || user.Length == 0)
      {
          throw new CUBRIDException(Utils.GetStr(MsgId.UserIsEmpty));
      }
      if (password == " ")
      {
        throw new CUBRIDException(Utils.GetStr(MsgId.PasswordIsEmpty));
      }
    }

    /// <summary>
    /// </summary>
    /// <param name="encoding"> </param>
    /// <returns> </returns>
    private Encoding EncodingFromString(string encoding)
    {
      encoding = encoding.ToLower();
      switch (encoding)
      {
        case "utf-8":
          return Encoding.UTF8;
        case "cp1252":
          return Encoding.GetEncoding("Windows-1252");
        case "iso-8859-1":
          return Encoding.GetEncoding("iso-8859-1");
        case "euc-kr":
          return Encoding.GetEncoding("euc-kr");
        case "euc-jp":
          return Encoding.GetEncoding("euc-jp");
        case "gb2312":
          return Encoding.GetEncoding("gb2312");
        case "gbk":
          return Encoding.GetEncoding("gbk");
        default:
          return Encoding.Default;
      }
    }

    /// <summary>
    ///   Sets the charset used to communicate with the database
    /// </summary>
    public void SetEncoding(string name)
    {
        if (connState != ConnectionState.Open)
        {
            connEncoding = name;
            return;
        }

      /* convert by driver, not cci
      int err_code = CciInterface.cci_set_charset(con_id, name);
      if (err_code < 0)
      {
          throw new CUBRIDException(err_code, Utils.GetStr(MsgId.ParameterNotFoundMissingPrefix));
      }
      */
      connEncoding = name;
    }

    /// <summary>
    /// Get Encoding
    /// </summary>
    /// <returns> Encoding</returns>
    public Encoding GetEncoding()
    {
        return EncodingFromString(connEncoding);
    }

    /// <summary>
    ///   Sets the isolation level.
    /// </summary>
    /// <param name="level"> The level. </param>
    public void SetIsolationLevel(CUBRIDIsolationLevel level)
    {
        T_CCI_ERROR err = new T_CCI_ERROR();

        if (connState != ConnectionState.Open)
            throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

        int ret = CciInterface.cci_set_db_parameter(con_id, T_CCI_DB_PARAM.CCI_PARAM_ISOLATION_LEVEL, (int)level, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }

        isolationLevel = level;
    }

    /// <summary>
    ///   Gets the isolation level.
    /// </summary>
    /// <returns> </returns>
    public CUBRIDIsolationLevel GetIsolationLevel()
    {
        T_CCI_ERROR err = new T_CCI_ERROR();
        int level = 0;

        if (connState != ConnectionState.Open)
            throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

        int ret = CciInterface.cci_get_db_parameter(con_id, T_CCI_DB_PARAM.CCI_PARAM_ISOLATION_LEVEL, ref level, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }
        isolationLevel = (CUBRIDIsolationLevel)level;
        return isolationLevel;
    }

    /// <summary>
    ///   Specifies the connection lock timeout as millisecond.
    /// </summary>
    /// <param name="value"> lock timeout value(Unit: millisecond). </param>
    public void SetLockTimeout(int value)
    {
        T_CCI_ERROR err = new T_CCI_ERROR();
        if (connState != ConnectionState.Open)
            throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

        int ret = CciInterface.cci_set_db_parameter(con_id, T_CCI_DB_PARAM.CCI_PARAM_LOCK_TIMEOUT, (int)value, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }

        lockTimeout = value;
    }

    /// <summary>
    ///   Gets system parameter: CCI_PARAM_LOCK_TIMEOUT.
    /// </summary>
    /// <returns> Parameter value </returns>
    public int GetLockTimeout()
    {
        T_CCI_ERROR err = new T_CCI_ERROR();
        int value = 0;

        if (connState != ConnectionState.Open)
            throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

        int ret = CciInterface.cci_get_db_parameter(con_id, T_CCI_DB_PARAM.CCI_PARAM_LOCK_TIMEOUT, ref value, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }
        lockTimeout = value;
        return lockTimeout;
    }


    /*
         * APIS-500: data will still be rollbacked after setting setautocommit(true)
         */

    /// <summary>
    ///   Sets the auto commit state of the connection.
    /// </summary>
    public void SetAutoCommit(bool autoCommit)
    {
      if (connState != ConnectionState.Open)
        throw new InvalidOperationException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));

      int ret = CciInterface.cci_set_autocommit(con_id, (CCI_AUTOCOMMIT_MODE)Convert.ToInt32(autoCommit));
      if (ret < 0)
      {
          throw new CUBRIDException(Utils.GetStr(MsgId.InvalidPropertyName));
      }

      this.autoCommit = autoCommit;
    }


    /// <summary>
    ///   Gets the auto commit state of the connection.
    /// </summary>
    public bool GetAutoCommit()
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        CCI_AUTOCOMMIT_MODE ret = CciInterface.cci_get_autocommit(con_id);
        if (ret < 0)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.InvalidPropertyName));
        }

        return Convert.ToBoolean(autoCommit);
    }

    /// <summary>
    ///   Sets the max length of the string used for communicating with the database.
    /// </summary>
    /// <param name="val"> The max length value. </param>
    public void SetMaxStringLength(int val)
    {
      //TODO
      throw new NotImplementedException();
    }

    /// <summary>
    ///   Gets the length of the max string.
    /// </summary>
    public int GetMaxStringLength()
    {
        T_CCI_ERROR err = new T_CCI_ERROR();
        int value = 0;
        int ret = CciInterface.cci_get_db_parameter(con_id, T_CCI_DB_PARAM.CCI_PARAM_MAX_STRING_LENGTH, ref value, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }
        return value;
    }

    /// <summary>
    ///   Batch executes sql statements, without returning any resultsets.
    /// </summary>
    /// <param name="sqls"> The SQL commands to be executed. </param>
    /// <returns> </returns>
    public int BatchExecuteNoQuery(string[] sqls)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }
        if (sqls == null || sqls.Length == 0)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.NotSupported));
            //return 0;
        }

        T_CCI_QUERY_RESULT[] query_result = null;
        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int n_executed = CciInterface.cci_execute_batch(con_id, sqls, ref query_result, ref err_buf);
        if (n_executed < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
        return n_executed;
    }

    /// <summary>
    ///   Commits the transaction.
    /// </summary>
    public void Commit()
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err = new T_CCI_ERROR();
        int ret = CciInterface.cci_end_tran(con_id, (char)CCI_TRAN_MODE.CCI_TRAN_COMMIT, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code,err.err_msg);
        }

        SetAutoCommit(true);
    }

    /// <summary>
    ///   Rollbacks the transaction.
    /// </summary>
    public void Rollback()
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err = new T_CCI_ERROR();
        int ret = CciInterface.cci_end_tran(con_id, (char)CCI_TRAN_MODE.CCI_TRAN_ROLLBACK, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }

        SetAutoCommit(true);
    }


    /// <summary>
    ///   Gets the table name from OID.
    /// </summary>
    /// <param name="oidStr"> The OID. </param>
    /// <returns> </returns>
    public string GetTableNameFromOID(string oidStr)
    {
        if (State == ConnectionState.Closed)
        {
          throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }
        byte[] buf = new byte[1024];
        T_CCI_ERROR err = new T_CCI_ERROR();
        int ret = CciInterface.cci_oid_get_class_name(con_id, oidStr, buf, 1024, ref err);
        if (ret < 0)
        {
            throw new CUBRIDException(err.err_code, err.err_msg);
        }
        return Encoding.UTF8.GetString(buf).TrimEnd('\0');
    }
    /// <summary>
    ///   Gets the current database.
    /// </summary>
    /// <returns> </returns>
    public string CurrentDatabase()
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }
        if (!string.IsNullOrEmpty(Database))
            return Database;

        using (CUBRIDCommand cmd = new CUBRIDCommand("select database()", this))
        {
            object _obj = cmd.ExecuteScalar();
            if (_obj != null)
            {
                return _obj.ToString();
            }
        }
        return string.Empty;
    }


    /// <summary>
    ///   Adds the element to set.
    /// </summary>
    /// <param name="oid"> The oid. </param>
    /// <param name="attributeName"> Name of the attribute. </param>
    /// <param name="value"> The value. </param>
    public void AddElementToSet(CUBRIDOid oid, String attributeName, Object value)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_col_set_add(Conection, oid.Get_OidStr(), attributeName, value.ToString(), ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
    }

    /// <summary>
    ///   Drops the element in set.
    /// </summary>
    /// <param name="oid"> The oid. </param>
    /// <param name="attributeName"> Name of the attribute. </param>
    /// <param name="value"> The value. </param>
    public void DropElementInSet(CUBRIDOid oid, String attributeName, Object value)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_col_set_drop(Conection, oid.Get_OidStr(), attributeName, value.ToString(), ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
    }
    /// <summary>
    ///   Adds the element in sequence.
    /// </summary>
    /// <param name="oid"> The oid. </param>
    /// <param name="attributeName"> Name of the attribute. </param>
    /// <param name="index"> The index. </param>
    /// <param name="value"> The value. </param>
    public void UpdateElementInSequence(CUBRIDOid oid, String attributeName, int index, Object value)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_col_seq_put(Conection, oid.Get_OidStr(), attributeName, index, value.ToString(), ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
    }

    /// <summary>
    ///   Inserts the element in sequence.
    /// </summary>
    /// <param name="oid"> The oid. </param>
    /// <param name="attributeName"> Name of the attribute. </param>
    /// <param name="index"> The index. </param>
    /// <param name="value"> The value. </param>
    public void InsertElementInSequence(CUBRIDOid oid, String attributeName, int index, Object value)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_col_seq_insert(Conection, oid.Get_OidStr(), attributeName, index, value.ToString(), ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
    }

    /// <summary>
    ///   Drops the element in sequence.
    /// </summary>
    /// <param name="oid"> The oid. </param>
    /// <param name="attributeName"> Name of the attribute. </param>
    /// <param name="index"> The index. </param>
    public void DropElementInSequence(CUBRIDOid oid, String attributeName, int index)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_col_seq_drop(Conection, oid.Get_OidStr(), attributeName, index, ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
    }

    /// <summary>
    ///   Gets the size of the collection.
    /// </summary>
    /// <param name="oid"> The oid. </param>
    /// <param name="attributeName"> Name of the attribute. </param>
    /// <returns> </returns>
    public int GetCollectionSize(CUBRIDOid oid, String attributeName)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }
        int size = 0;
        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_col_size(Conection, oid.Get_OidStr(), attributeName, ref size, ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
        return size;
    }

    /// <summary>
    ///   Gets the query plan only.
    /// </summary>
    /// <param name="sql"> The SQL. </param>
    /// <returns> </returns>
    public String GetQueryPlanOnly(String sql)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int req = CciInterface.cci_prepare(this, sql, ref err_buf);
        if (req < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }

        IntPtr query_buf_p = IntPtr.Zero;
        string query_plan = CciInterface.cci_get_query_plan(req, ref query_buf_p);

        int res = CciInterface.cci_query_info_free(query_buf_p);
        if (res < 0)
        {
            throw new CUBRIDException(res);
        }

        return query_plan;
    }

    /// <summary>
    ///   Gets the database version.
    /// </summary>
    /// <returns> </returns>
    [MethodImpl(MethodImplOptions.Synchronized)]
    public String GetDatabaseProductVersion()
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        return CciInterface.cci_get_db_version(this, 64); ;
    }

    /// <summary>
    ///   Create a new LOB.
    /// </summary>
    /// <param name="lob_type"> The LOB type. </param>
    /// <returns> The packed LOB handle. </returns>
    public IntPtr LOBNew(CUBRIDDataType lob_type)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        IntPtr packedLobHandle = IntPtr.Zero;
        T_CCI_ERROR err = new T_CCI_ERROR();

        if (lob_type == CUBRIDDataType.CCI_U_TYPE_BLOB)
        {
            int res = CciInterface.cci_blob_new(con_id, ref packedLobHandle, ref err);
            if (res < 0)
            {
                throw new CUBRIDException(err.err_code, err.err_msg);
            }
        }
        else if (lob_type == CUBRIDDataType.CCI_U_TYPE_CLOB)
        {
            int res = CciInterface.cci_clob_new(con_id, ref packedLobHandle, ref err);
            if (res < 0)
            {
                throw new CUBRIDException(err.err_code, err.err_msg);
            }
        }
        else
        {
            throw new ArgumentException(Utils.GetStr(MsgId.NotAValidLOBType));
        }
        return packedLobHandle;
    }

    /// <summary>
    ///   LOB read.
    /// </summary>
    /// <param name="Lob"> CUBRIDClob or CUBRIDBlob object. </param>
    /// <param name="offset"> The offset. </param>
    /// <param name="buf"> The buffer. </param>
    /// <param name="start"> The start positon. </param>
    /// <param name="len"> The length. </param>
    /// <returns> </returns>
    public int LOBRead(object Lob, long offset, byte[] buf, int start, int len)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        len = len < buf.Length - start ? len : buf.Length - start;
        byte[] byteArray;

        if (Lob.GetType().Name == "CUBRIDClob")
        {
            CUBRIDClob clob = (CUBRIDClob)Lob;
            byteArray = System.Text.Encoding.Default.GetBytes(clob.GetString(offset, len));
        }
        else if (Lob.GetType().Name == "CUBRIDBlob")
        {
            CUBRIDBlob blob = (CUBRIDBlob)Lob;
            byteArray = blob.GetBytes(offset, len);
        }
        else
        {
            throw new ArgumentException(Utils.GetStr(MsgId.NotAValidLOBType));
        }

        Array.Copy(byteArray, 0, buf, start, len);

        return byteArray.Length;
    }

    /// <summary>
    ///   LOB write.
    /// </summary>
    /// <param name="Lob">CUBRIDClob or CUBRIDBlob object. </param>
    /// <param name="offset"> The offset. </param>
    /// <param name="buf"> The buffer. </param>
    /// <param name="start"> The start position. </param>
    /// <param name="len"> The length. </param>
    /// <returns> </returns>
    public long LOBWrite(object Lob, ulong offset, byte[] buf, int start, int len)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }

        long res = 0;
        len = len < buf.Length - start ? len : buf.Length - start;
        byte[] byteArray = new byte[len];
        Array.Copy(buf, start, byteArray, 0, len);

        if (Lob.GetType().Name == "CUBRIDClob")
        {
            CUBRIDClob clob = (CUBRIDClob)Lob;
            res = clob.SetString(offset, System.Text.Encoding.Default.GetString(byteArray));
        }
        else if (Lob.GetType().Name == "CUBRIDBlob")
        {
            CUBRIDBlob blob = (CUBRIDBlob)Lob;
            res = blob.SetBytes(offset, byteArray);
        }
        else
        {
            throw new ArgumentException(Utils.GetStr(MsgId.NotAValidLOBType));
        }

        return res;
    }

    /// <summary>
    ///   Batch execute statements.
    /// </summary>
    /// <param name="batchSqlStmt"> The batch SQL statements. </param>
    /// <returns> CUBRIDBatchResult </returns>
    public CUBRIDBatchResult BatchExecute(string[] batchSqlStmt)
    {
        if (State == ConnectionState.Closed)
        {
            throw new CUBRIDException(Utils.GetStr(MsgId.TheConnectionIsNotOpen));
        }
        if (batchSqlStmt == null || batchSqlStmt.Length == 0)
        {
            return null;
        }

        T_CCI_QUERY_RESULT[] query_result = null;
        T_CCI_ERROR err_buf = new T_CCI_ERROR();
        int n_executed = CciInterface.cci_execute_batch(con_id, batchSqlStmt, ref query_result, ref err_buf);
        if (n_executed < 0)
        {
            throw new CUBRIDException(err_buf.err_code, err_buf.err_msg);
        }
        else 
        {
            CUBRIDBatchResult batchResult = new CUBRIDBatchResult(n_executed);
            for (int i = 0; i < n_executed; i++) 
            {
                batchResult.setStatementType(i, query_result[i].stmt_type);
                int result = query_result[i].result_count;
                if (result < 0)
                {
                    batchResult.setResultError(i, result, query_result[i].err_msg);
                }
                else 
                {
                    batchResult.setResultCode(i, result);
                }
            }
            return batchResult;
        }
    }

    #region GetSchema Support

    /// <summary>
    ///   Get the meta data of the supported collection names.
    /// </summary>
    /// <returns>A <see cref="DataTable" /> that contains the meta data of supported collection names, columns={"CollectionName", "NumberOfRestrictions"} </returns>
    public override DataTable GetSchema()
    {
      throw new CUBRIDException(Utils.GetStr(MsgId.NotSupportedInCUBRID));
    }

    /// <summary>
    ///   Returns schema information for the data source of this <see cref="DbConnection" /> using the specified string for the schema name. <para/>
    ///   Be equivalent to <see cref="GetSchema(string, string[])"/>
    /// </summary>
    /// <param name="collectionName">Specifies the name of the schema to return.</param>
    /// <returns> A <see cref="DataTable" /> that contains schema information. </returns>
    /// <exception cref="System.ArgumentException">collectionName is specified as null.</exception>
    public override DataTable GetSchema(string collectionName)
    {
      return GetSchema(collectionName, null);
    }

    /// <summary>
    ///   Returns schema information for the data source of this <see cref="DbConnection" /><para/>
    ///   using the specified string for the schema name and the specified string array <para/>
    ///   for the restriction values.
    /// </summary>
    /// <param name="collectionName"> Specifies the name of the schema to return. It could be any of the following name:<para/>
    /// <list type="bullet"><item>MetaDataCollections<para/></item><item>RESERVEDWORDS<para/></item><item>USERS<para/></item><item>DATABASES<para/></item><item>PROCEDURES<para/></item><item>TABLES<para/></item><item>VIEWS<para/></item>
    /// <item>COLUMNS<para/></item><item>INDEXES<para/></item><item>INDEX_COLUMNS<para/></item><item>FOREIGN_KEYS<para/></item>
    /// </list>
    /// </param>
    /// <param name="filters"> 
    /// Specifies a set of restriction values for the requested schema. It can supply n depth of values. For different <paramref name="collectionName"/>, the value is different.<para/>
    /// <list>
    /// <item>MetaDataCollections: <paramref name="filters"/> = null <para/></item>
    /// <item>RESERVEDWORDS: <paramref name="filters"/> = null <para/></item>
    /// <item>USERS: <paramref name="filters"/> = {"user name pattern"} <para/></item>
    /// <item>DATABASES: <paramref name="filters"/> = {"database name pattern"} <para/></item>
    /// <item>PROCEDURES: <paramref name="filters"/> = {"procedure name pattern"} <para/></item>
    /// <item>TABLES: <paramref name="filters"/> = {"table name"} <para/></item>
    /// <item>VIEWS: <paramref name="filters"/> = {"view name pattern"} <para/></item>
    /// <item>COLUMNS: <paramref name="filters"/> = {"table name pattern", "column name pattern"} <para/></item>
    /// <item>INDEXES: <paramref name="filters"/> = {"table name pattern", "column name pattern", "index name pattern"} <para/></item>
    /// <item>INDEX_COLUMNS: <paramref name="filters"/> = {"table name pattern", "index name pattern"} <para/></item>
    /// <item>FOREIGN_KEYS: <paramref name="filters"/> = {"table name pattern", "foreign key name pattern"} <para/></item>
    /// </list>
    /// </param>
    /// <returns> A <see cref="DataTable" /> that contains schema information. For different <paramref name="collectionName"/>, the columns are different. 
    /// <list>
    /// <item>RESERVEDWORDS: columns={<see cref="DbMetaDataColumnNames.ReservedWord"/>}<para/></item>
    /// <item>USERS: columns={"USERNAME"}<para/></item>
    /// <item>DATABASES: columns={"CATALOG_NAME", "SCHEMA_NAME"}<para/></item>
    /// <item>PROCEDURES: columns={"PROCEDURE_NAME", "PROCEDURE_TYPE", "RETURN_TYPE", "ARGUMENTS_COUNT", "LANGUAGE", "TARGET", "OWNER"}<para/></item>
    /// <item>TABLES: columns={"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"}<para/></item>
    /// <item>VIEWS: columns={"VIEW_CATALOG", "VIEW_SCHEMA", "VIEW_NAME"}<para/></item>
    /// <item>COLUMNS: columns={"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE", "CHARACTER_SET"}<para/></item>
    /// <item>INDEXES:columns={"INDEX_CATALOG", "INDEX_SCHEMA", "INDEX_NAME", "TABLE_NAME", "UNIQUE", "REVERSE", "PRIMARY", "FOREIGN_KEY", "DIRECTION"}<para/></item>
    /// <item>INDEX_COLUMNS: columns={"INDEX_CATALOG", "INDEX_SCHEMA", "INDEX_NAME", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DIRECTION"}<para/></item>
    /// <item>FOREIGN_KEYS: columns={"PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_ACTION", "DELETE_ACTION", "FK_NAME", "PK_NAME"}<para/></item>
    /// </list>
    /// </returns>
    /// <exception cref="System.ArgumentException">collectionName is specified as null.</exception>
    public override DataTable GetSchema(string collectionName, string[] filters)
    {
      if (collectionName == null)
      {
        throw new ArgumentException(Utils.GetStr(MsgId.collectionNameIsNull));
      }

      if (SchemaProvider != null)
      {
        string[] cleaned_filters = SchemaProvider.CleanFilters(filters);
        DataTable dt = SchemaProvider.GetSchema(collectionName, cleaned_filters);

        return dt;
      }

      return null;
    }

    #endregion

    #region Nested type: CUBRIDInfoMessageEventArgs

    /// <summary>
    ///   Provides data for the InfoMessage event.
    /// </summary>
    public class CUBRIDInfoMessageEventArgs : EventArgs
    {
      /// <summary>
      ///   Collection of errors
      /// </summary>
      public CUBRIDException[] errors;
    }

    #endregion
  }
}
