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
  class CUBRIDConnectionProperties
  {
    private const int MONITORING_INTERVAL = 60;

    private string[] altHost;
    private string logBaseDir = "";
    private string logFile = "CUBRID.ADO.NET.Log.txt";

    private int rcTime;
    private int loginTimeout;
    private int queryTimeout;
    private int slowQueryThresholdMillis = 60 * 1000;

    private bool disconnectOnQueryTimeout;
    private bool loadBalance;
    private bool logSlowQueries;
    private bool logTraceApi;
    private bool logTraceNetwork;
    private bool bAutoCommit = true;

    private string propertiesString;

    public CUBRIDConnectionProperties()
    {
    }

    public CUBRIDConnectionProperties(string strProperties) :
      this()
    {
      PropertiesString = strProperties;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="strValue"></param>
    /// <returns></returns>
    private bool GetBool(string strValue)
    {
      string[] accepts = 
      {
        "true", "false", "on", "off", "yes", "no"
      };

      if (strValue == string.Empty || strValue.Length == 0)
        return false;

      for (int i = 0; i < accepts.Length; i++)
      {
        if (accepts[i] == strValue)
        {
          return (i % 2) == 0;
        }
      }

      return false;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="strValue"></param>
    /// <returns></returns>
    private int GetInt(string strValue)
    {
      if (strValue == string.Empty || strValue.Length == 0)
        return -1;

      int nValue = Convert.ToInt32(strValue);

      return nValue;
    }

    /// <summary>
    /// Parse connection properties
    /// </summary>
    /// <param name="strProperties"></param>
    private void ParseProperties(string strProperties)
    {
      char[] delimiters = new[] { '?', '&' };
      string[] tokens = strProperties.Split(delimiters);

      foreach (string p in tokens)
      {
        string[] pair = p.Split('=');
        if (pair.Length == 2)
        {
          SetProperty(pair[0], pair[1]);
        }
      }

      if (logBaseDir == string.Empty || logBaseDir.Length == 0)
        logBaseDir = System.IO.Directory.GetCurrentDirectory();

      if (logTraceApi || logTraceNetwork)
      {
        CUBRIDTrace.SetTraceFile(logBaseDir, logFile);
      }
    }

    public void SetConnectionProperties(string properties)
    {
      PropertiesString = properties;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="strName"></param>
    /// <param name="strValue"></param>
    private void SetProperty(string strName, string strValue)
    {
      switch (strName.ToLower())
      {
        case "althosts":
          ParseAltHosts(strValue);
          break;
        case "loadbalance":
          loadBalance = GetBool(strValue.ToLower());
          break;
        case "rctime":
          rcTime = GetInt(strValue);
          break;
        case "logintimeout":
        case "login_timeout":
          loginTimeout = GetInt(strValue);
          break;
        case "querytimeout":
        case "query_timeout":
          queryTimeout = GetInt(strValue);
          break;
        case "disconnectonquerytimeout":
        case "disconnect_on_query_timeout":
          disconnectOnQueryTimeout = GetBool(strValue.ToLower());
          break;
        case "logfile":
          logFile = strValue;
          break;
        case "logslowqueries":
          logSlowQueries = GetBool(strValue.ToLower());
          break;
        case "slowquerythresholdmillis":
          slowQueryThresholdMillis = GetInt(strValue);
          break;
        case "logtraceapi":
          logTraceApi = GetBool(strValue.ToLower());
          break;
        case "logtracenetwork":
          logTraceNetwork = GetBool(strValue.ToLower());
          break;
        case "logbasedir":
          logBaseDir = strValue;
          break;
        case "autocommit":
          bAutoCommit = GetBool(strValue);
          break;
      }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="altHosts"></param>
    private void ParseAltHosts(string altHosts)
    {
      altHost = altHosts.Split(',');
    }

    /// <summary>
    /// Get Alternative hosts count
    /// </summary>
    /// <returns>Alternative hosts count</returns>
    public int AlterHostCount()
    {
      if (altHost == null)
        return 0;

      return altHost.Length;
    }

    /// <summary>
    /// altHosts=standby_broker1_host, standby_broker2_host, . . .: Specifies the broker information of the standby server, 
    /// which is used for failover when it is impossible to connect to the active server. 
    /// </summary>
    public string[] AltHosts
    {
      get { return altHost; }
    }

    /// <summary>
    /// A directory where a debug log file is created. 
    /// </summary>
    public string LogBaseDir
    {
      get { return logBaseDir; }
    }

    /// <summary>
    /// A log file name for debugging 
    /// </summary>
    public string LogFile
    {
      get { return logFile; }
    }

    /// <summary>
    /// An interval between the attempts to connect to the active broker in which failure occurred.
    /// </summary>
    public int RcTime
    {
      get { return rcTime; }
    }

    /// <summary>
    /// Timeout value (unit: msec.) for database login.
    /// </summary>
    public int LoginTimeout
    {
      get { return loginTimeout; }
    }

    /// <summary>
    /// Timeout value(unit: mesc.) for database query.
    /// </summary>
    public int QueryTimeout
    {
      get { return queryTimeout; }
    }

    /// <summary>
    ///  Timeout for slow query logging if slow query logging is enabled (default value: 60000, unit: milliseconds)
    /// </summary>
    public int SlowQueryThresholdMillis
    {
      get { return slowQueryThresholdMillis; }
    }

    public bool DisconnectOnQueryTimeout
    {
      get { return disconnectOnQueryTimeout; }
    }

    /// <summary>
    /// When this value is true, the applications try to connect with the main host and alternative hosts specified 
    /// with altHost property as random order. (default value: false).
    /// </summary>
    public bool LoadBalance
    {
      get { return loadBalance; }
    }

    //public bool LogOnException
    //{
    //    get { return this.logOnException; }
    //}

    /// <summary>
    /// Whether to log slow query for debugging (default value: false)
    /// </summary>
    public bool LogSlowQueries
    {
      get { return logSlowQueries; }
    }

    /// <summary>
    /// Whether to log the start and end of CCI functions
    /// </summary>
    public bool LogTraceApi
    {
      get { return logTraceApi; }
    }

    /// <summary>
    /// Whether to log network data content transferred of CCI functions
    /// </summary>
    public bool LogTraceNetwork
    {
      get { return logTraceNetwork; }
    }

    /// <summary>
    /// 
    /// </summary>
    public bool AutoCommit
    {
      get { return bAutoCommit; }
    }

    /// <summary>
    /// 
    /// </summary>
    public string PropertiesString
    {
      get { return propertiesString; }
      set
      {
        propertiesString = value;
        ParseProperties(propertiesString);

        if (rcTime < MONITORING_INTERVAL)
        {
          rcTime = MONITORING_INTERVAL;
        }
      }
    }


    /// <summary>
    /// Reset all properties.
    /// </summary>
    public void Reset()
    {
      propertiesString = "";
      altHost = null;
      logBaseDir = "";
      logFile = "CUBRID.ADO.NET.Log.txt";

      rcTime = 0;
      loginTimeout = 0;
      queryTimeout = 0;
      slowQueryThresholdMillis = 60000;

      disconnectOnQueryTimeout = false;
      loadBalance = false;
      logSlowQueries = false;
      logTraceApi = false;
      logTraceNetwork = false;
      bAutoCommit = true;
    }
  }
}
