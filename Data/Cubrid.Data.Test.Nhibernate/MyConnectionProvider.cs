using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using CUBRID.Data.CUBRIDClient;
using NHibernate.Connection;
using NHibernate;
using Environment = NHibernate.Cfg.Environment;
using NHibernate.Driver;
using NHibernate.Util;
using System.Configuration;

namespace Cubrid.Data.Test.Nhibernate
{
    class MyConnectionProvider : IConnectionProvider
    {
        CUBRIDConnection conn;
        public string ConnectionString { get; set; }
        public int ConnectionTimeout { get{return 0;} }
        public string Database { get { return "Cubrid";} }
        public ConnectionState State { get{return ConnectionState.Open;} }
        private IDriver driver;

        public IDriver Driver
        {
            get { return driver; }
        }

        public IDbConnection GetConnection()
        {
            conn = new CUBRIDConnection();
            //conn.ConnectionString = "server=10.0.0.95;database=demodb;port=33000;user=public;password=";
            conn.ConnectionString = ConnectionString = ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
            conn.Open();
            conn.IsolationLevel = CUBRID.Data.CUBRIDClient.CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE;
            return conn;
        }
        public virtual void Configure(IDictionary<string, string> settings)
        {
            ConfigureDriver(settings);
        }

        public void Open()
        {
            conn.Open();
        }

        public void CloseConnection(IDbConnection c)
        {    
            conn.Close();
        }

        public IDbTransaction BeginTransaction()
        {
            IDbTransaction tran =  conn.BeginTransaction();
            return tran;
        }

        public IDbTransaction BeginTransaction(System.Data.IsolationLevel s)
        {
            IDbTransaction tran = conn.BeginTransaction(s);
            return tran;
        }

        public void ChangeDatabase(string s) { }
        public IDbCommand CreateCommand()
        { 
            return null;
        }

        public void Dispose()
        {
        }

        protected virtual void ConfigureDriver(IDictionary<string, string> settings)
        {
            string driverClass;
            if (!settings.TryGetValue(Environment.ConnectionDriver, out driverClass))
            {
                throw new HibernateException("The " + Environment.ConnectionDriver +
                                             " must be specified in the NHibernate configuration section.");
            }
            else
            {
                try
                {
                    driver =
                        (IDriver)Environment.BytecodeProvider.ObjectsFactory.CreateInstance(ReflectHelper.ClassForName(driverClass));
                    driver.Configure(settings);
                }
                catch (Exception e)
                {
                    throw new HibernateException("Could not create the driver from " + driverClass + ".", e);
                }
            }
        }
    }
}
