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
using System.Text;
using System.IO;

namespace CUBRID.Data.CUBRIDClient
{
    class CUBRIDConnectionProperties
    {
        private static int MONITORING_INTERVAL = 60;

        private string[] altHost = null;
        private string logBaseDir = "";
        private string logFile = "CUBRID.Ado.log";
        
        private int rcTime = 0;
        private int loginTimeout = 0;
        private int queryTimeout = 0;
        private int slowQueryThresholdMillis = 60000;

        private bool disconnectOnQueryTimeout = false;
        private bool loadBalance = false;
        //private bool logOnException = false;
        private bool logSlowQueries = false;
        private bool logTraceApi = false;
        private bool logTraceNetwork = false;
        private bool bAutoCommint = true;

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
        /// <param name="bValue"></param>
        /// <returns></returns>
        private int GetBool(string strValue, ref bool bValue)
        {
            string[] accepts = {
            "true", "false", "on", "off", "yes", "no"
            };

            if (strValue == string.Empty || strValue.Length == 0)
                return -1;

            for (int i = 0; i < accepts.Length; i++)
            {
                if (accepts[i] == strValue)
	            {
                    bValue = (i % 2) == 0;
	                return 0;
	            }
            }

          return -1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="strValue"></param>
        /// <param name="nValue"></param>
        /// <returns></returns>
        private int GetInt(string strValue, ref int nValue)
        {
            if (strValue == string.Empty || strValue.Length == 0)
                return -1;

           nValue = Convert.ToInt32(strValue);

            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="strProperties"></param>
        private void ParserProperties(string strProperties)
        {
            char[] delimiters = new char[] { '?', '&' };
            string[] tokens = strProperties.Split(delimiters);

            foreach (string p in tokens)
            {
                string[] pair = p.Split('=');
                if (pair.Length == 2)
                {
                    set_property(pair[0].ToLower(), pair[1]);
                }
            }

            if(logBaseDir == string.Empty || logBaseDir.Length == 0)
                logBaseDir = System.IO.Directory.GetCurrentDirectory();

            if (this.logTraceApi || this.logTraceNetwork)
            {
                CUBRIDTrace.SetTraceFile(this.logBaseDir, this.logFile);
            }
        }

        public void SetConnectionProperties(string properties)
        {
            this.PropertiesString = properties;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="strName"></param>
        /// <param name="strValue"></param>
        private void set_property(string strName, string strValue)
        {
            switch (strName)
            {
                case "althosts":
                    ParserAltHosts(strValue);
                    break;
                case "loadbalance":
                    GetBool(strValue.ToLower(), ref this.loadBalance);
                    break;
                case "rctime":
                    GetInt(strValue, ref this.rcTime);
                    break;
                case "logintimeout":
                case "login_timeout":
                    GetInt(strValue, ref this.loginTimeout);
                    break;
                case "querytimeout":
                case "query_timeout":
                    GetInt(strValue, ref this.queryTimeout);
                    break;
                case "disconnectonquerytimeout":
                case "disconnect_on_query_timeout":
                    GetBool(strValue.ToLower(), ref this.disconnectOnQueryTimeout);
                    break;
                case "logfile":
                    this.logFile = strValue;
                    break;
            //{"logOnException",              CUBRIDConnectionPropertyType.BOOL_PROPERTY},
                case "logslowqueries":
                    GetBool(strValue.ToLower(), ref this.logSlowQueries);
                    break;
                case "slowquerythresholdmillis":
                    GetInt(strValue, ref this.slowQueryThresholdMillis);
                    break;
                case "logtraceapi":
                    GetBool(strValue.ToLower(), ref this.logTraceApi);
                    break;
                case "logtracenetwork":
                    GetBool(strValue.ToLower(), ref this.logTraceNetwork);
                    break;
                case "logbasedir":
                    this.logBaseDir = strValue;
                    break;
                case "autocommit":
                    GetBool(strValue, ref this.bAutoCommint);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="altHosts"></param>
        private void ParserAltHosts(string altHosts)
        {
            this.altHost = altHosts.Split(',');
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
            get { return this.altHost; }
        }

        /// <summary>
        /// A directory where a debug log file is created. 
        /// </summary>
        public string LogBaseDir
        {
            get { return this.logBaseDir; }
        }

        /// <summary>
        /// A log file name for debugging 
        /// </summary>
        public string LogFile
        {
            get { return this.logFile; }
        }

        /// <summary>
        /// An interval between the attempts to connect to the active broker in which failure occurred.
        /// </summary>
        public int RcTime
        {
            get { return this.rcTime; }
        }

        /// <summary>
        /// Timeout value (unit: msec.) for database login.
        /// </summary>
        public int LoginTimeout
        {
            get { return this.loginTimeout; }
        }

        /// <summary>
        /// Timeout value(unit: mesc.) for database query.
        /// </summary>
        public int QueryTimeout
        {
            get { return this.queryTimeout; }
        }

        /// <summary>
        ///  Timeout for slow query logging if slow query logging is enabled (default value: 60000, unit: milliseconds)
        /// </summary>
        public int SlowQueryThresholdMillis
        {
            get { return this.slowQueryThresholdMillis; }
        }

        public bool DisconnectOnQueryTimeout
        {
            get { return this.disconnectOnQueryTimeout; }
        }

        /// <summary>
        /// When this value is true, the applications try to connect with the main host and alternative hosts specified 
        /// with altHost property as random order. (default value: false).
        /// </summary>
        public bool LoadBalance
        {
            get { return this.loadBalance; }
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
            get { return this.logSlowQueries; }
        }
        
        /// <summary>
        /// Whether to log the start and end of CCI functions
        /// </summary>
        public bool LogTraceApi
        {
            get { return this.logTraceApi; }
        }

        /// <summary>
        /// Whether to log network data content transferred of CCI functions
        /// </summary>
        public bool LogTraceNetwork
        {
            get { return this.logTraceNetwork; }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool AutoCommit
        {
            get { return this.bAutoCommint; }
        }

        /// <summary>
        /// 
        /// </summary>
        public string PropertiesString
        {
            get { return this.propertiesString; }
            set
            {
                this.propertiesString = value;
                ParserProperties(this.propertiesString);

                if (this.rcTime < CUBRIDConnectionProperties.MONITORING_INTERVAL)
                {
                    this.rcTime = CUBRIDConnectionProperties.MONITORING_INTERVAL;
                }
            }
        }


        /// <summary>
        /// Reset all properties.
        /// </summary>
        public void Reset()
        {
            this.propertiesString = "";
            this.altHost = null;
            this.logBaseDir = "";
            this.logFile = "CUBRID.Ado.log";
        
            this.rcTime = 0;
            this.loginTimeout = 0;
            this.queryTimeout = 0;
            this.slowQueryThresholdMillis = 60000;

            this.disconnectOnQueryTimeout = false;
            this.loadBalance = false;
            //private bool logOnException = false;
            this.logSlowQueries = false;
            this.logTraceApi = false;
            this.logTraceNetwork = false;
            this.bAutoCommint = true;
        }
    }
}
