using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Xml;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CUBRID.Data.CUBRIDClient;
using ADOTest.TestHelper;


namespace ADOTest
{
    /// <summary>
    /// This is a test class for CUBRIDConnection
    /// </summary>
    [TestClass]
    public class CUBRIDConnectionTest : BaseTest
    {
        public CUBRIDConnectionTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        //private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        //public TestContext TestContext
        //{
        //    get
        //    {
        //        return testContextInstance;
        //    }
        //    set
        //    {
        //        testContextInstance = value;
        //    }
        //}


        // Use TestInitialize to run code before running each test 
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //    base.MyTestInitialize();
        //}

        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //

        /// <summary>
        ///Test Clone
        ///</summary>
        [TestMethod]
        public void Clone_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Clone a connection");
                conn.ConnectionString = DBHelper.connString;
                Log("change a property value of the original connection");
                conn.SetConnectionTimeout(45);
                conn.Open();

                Log("call the Clone method");
                CUBRIDConnection clonedConn = conn.Clone();
                Assert.IsTrue(clonedConn != null);

                Log("The property values are different between the original connection and the cloned connection");
                Assert.AreEqual(45, conn.ConnectionTimeout);
                Assert.AreEqual(30, clonedConn.ConnectionTimeout);

                try
                {
                    clonedConn.Open();

                    Log("Close the original connection, check the cloned connection works well");
                    conn.Close();
                    Assert.IsTrue(DBHelper.GetTableRowsCount("db_class", clonedConn) > 0);
                    clonedConn.Close();
                    LogStepPass();
                }
                catch (Exception ex)
                {
                    Log(ex.Message);
                    LogStepFail();
                }
                finally
                {
                    LogTestResult();
                    conn.Close();
                    clonedConn.Close();
                }
            }
        }


        /// <summary>
        /// Test ConnectionTest()
        /// </summary>
        [TestMethod]
        public void Connection_NoParam_Test()
        {
            CUBRIDConnection conn = null;

            try
            {
                LogTestStep("Create a CUBRID Connection instance");
                conn = new CUBRIDConnection();
                conn.ConnectionString = DBHelper.connString;
                conn.Open();
                LogStepPass();
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
                Log(ex.Message);
                LogStepFail();
            }
            finally
            {
                LogTestResult();
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        /// Test CUBRIDConnection(string connString)
        /// </summary>
        [TestMethod]
        public void Connection_WithConnString_Test()
        {
            CUBRIDConnection conn = null;

            try
            {
                LogTestStep("Create a CUBRID Connection instance with connection string");
                conn = new CUBRIDConnection(DBHelper.connString);
                conn.Open();
                LogStepPass();
            }
            catch (SystemException ex)
            {
                Assert.Fail(ex.Message);
                Log(ex.Message);
                LogStepFail();
            }
            finally
            {
                LogTestResult();
                if (conn != null)
                {
                    conn.Close();
                }
            }
        }

        /// <summary>
        ///Test Open
        ///</summary>
        [TestMethod]
        public void Conn_Open_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("open DB without specifying a data source or server");
                try
                {
                    conn.Open();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Log(ex.Message);
                    Console.WriteLine(ex.Message);
                    //Assert.AreEqual("some message", ex.Message);
                    LogStepPass();
                }

                LogTestStep("open a valid DB");
                try
                {
                    conn.ConnectionString = DBHelper.connString;
                    conn.Open();
                    Assert.IsTrue(DBHelper.GetTableRowsCount("db_class", conn) > 0);
                    LogStepPass();
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                    Log(ex.Message);
                    LogStepFail();
                }

                LogTestStep("open the DB twice");
                try
                {
                    conn.Open();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Connection is already open!", ex.Message);
                    LogStepPass();
                }
                finally
                {
                    conn.Close();
                }

                LogTestStep("open an invalid DB");
                try
                {
                    conn.ConnectionString = "server="+DBHelper.serverName+";database=another;port="+DBHelper.port+";user=public;password=";
                    conn.Open();
                    Assert.Fail("The DB name is invalid, the connection should not be opened");
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //Assert.AreEqual("Failed to connect to database server, 'another', on the following host(s): localhost:localhost", ex.Message);
                    LogStepPass();
                }
                finally
                {
                    LogTestResult();
                    conn.Close();
                }
            }
        }

        /// <summary>
        ///Test SetConnectionTimeout
        ///</summary>
        [TestMethod]
        public void SetConnectionTimeout_Test()
        {
            int timeout = 0;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Invalid timeout value");
                try
                {
                    timeout = 100;
                    conn.SetConnectionTimeout(timeout);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Assert.AreEqual("Invalid Timeout value! Expected a value between 1 and 99!", ex.Message);
                    LogStepPass();
                }

                LogTestStep("Valid timeout value, valid connection, set before open, check the timeout value is set successfuly");
                timeout = 40;
                conn.SetConnectionTimeout(timeout);

                //The database in the connection string does not exist
                conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;password=";
                var stopwatch = new Stopwatch();
                int elapseTime = 0;
                try
                {
                    stopwatch.Start();
                    conn.Open();
                    Log("The connection should not be opened");
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    elapseTime = (int)stopwatch.ElapsedMilliseconds / 1000;
                    int diffTime = elapseTime - timeout;
                    Log(ex.Message);
                    Assert.AreEqual(timeout, conn.ConnectionTimeout);
                    if (diffTime >= 0 && diffTime < 10)
                    {
                        LogStepPass();
                    }
                    else
                    {
                        LogStepFail();
                    }
                }

                LogTestStep("Valid timout value, valid connection, set after open");
                try
                {
                    conn.ConnectionString = DBHelper.connString;
                    conn.Open();
                    timeout = 20;
                    conn.SetConnectionTimeout(timeout);
                    Log("Not allowed to change the 'ConnectionTimeout' property while the connection state is!: Open.");
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    //Assert.AreEqual("Not allowed to change the 'ConnectionTimeout' property while the connection state is!: Open.", ex.Message);
                    Assert.AreEqual("Not allowed to change the 'ConnectionString' property while the connection state is!: Open.", ex.Message);
                    LogStepPass();
                }

                LogTestResult();

            }
        }

        /// <summary>
        /// Test SetLockTimeOut without param
        /// </summary>
        //[TestMethod]
        //public void SetLockTimeOutTest()
        //{
        //    using (CUBRIDConnection conn = new CUBRIDConnection())
        //    {
        //        conn.ConnectionString = DBHelper.connectionStr;

        //        LogTestStep("Connection is closed");
        //        //conn.Open();
        //        //conn.SetLockTimeout();

        //        Assert.AreEqual(30, conn.LockTimeout);
        //        LogStepPass();

        //        LogTestStep("Connection is open");
        //        try
        //        {
        //            conn.SetLockTimeout();
        //            Assert.IsTrue(false, "Not allowed to change the 'LockTimeout' property while the connection state is: Open.");
        //            LogStepFail();
        //            LogTestFail();
        //        }
        //        catch (Exception ex)
        //        {
        //            Assert.AreEqual("Not allowed to change the 'LockTimeout' property while the connection state is!: Open.", ex.Message);
        //            LogStepPass();
        //            LogTestPass();
        //        }
        //    }
        //}

        /// <summary>
        /// Test SetLockTimeOut with param
        /// </summary>
        [TestMethod]
        public void SetLockTimeOut_WithParam_Test()
        {
            int timeout;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                LogTestStep("connection is not open");
                try
                {
                    timeout = 2;
                    conn.SetLockTimeout(timeout);
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("The connection is not open!", ex.Message);
                    LogStepPass();
                }
                conn.Close();

                LogTestStep("Valid timout value, and connection is open");
                timeout = 35;
                conn.Open();
                conn.SetLockTimeout(35);
                Assert.AreEqual(35, conn.LockTimeout);
                LogStepPass();

                LogTestStep("Valid timout value, change the lock timeout");
                try
                {
                    Assert.AreEqual(35, conn.LockTimeout); 
                    conn.SetLockTimeout(40);
                    Assert.AreEqual(40, conn.LockTimeout);
                    LogStepPass();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //Assert.AreEqual("Not allowed to change the 'LockTimeout' property while the connection state is!: Open.", ex.Message);
                    LogStepFail();
                }
                finally
                {
                    LogTestResult();
                }
            }
        }

        /// <summary>
        /// Test SetLockTimeOut 
        /// </summary>
        [TestMethod]
        public void SetLockTimeOut_LockTime_Test()
        {
            LogTestStep("Test the lockTimeout is set successfully");
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            conn.Open();
            int timeout = 20;
            conn.SetLockTimeout(timeout);
            //conn.SetLockTimeout();
            DBHelper.ExecuteSQL("drop table if exists t", conn);
            DBHelper.ExecuteSQL("create table t(id integer)", conn);
            DBHelper.ExecuteSQL("insert into t values (1)", conn);

            conn.SetAutoCommit(false);
            CUBRIDConnection conn2 = null;
            double elapseTime = 0;
            var stopwatch = new Stopwatch();
            try
            {
                Thread thread2 = new Thread(delegate()
                    {
                        conn2 = new CUBRIDConnection();
                        conn2.ConnectionString = DBHelper.connString;
                        conn2.Open();
                        conn2.SetAutoCommit(false);

                        conn2.BeginTransaction();
                        DBHelper.ExecuteSQL("update t set id=2", conn2);
                    });
                conn.BeginTransaction();
                thread2.Start();
                Thread.Sleep(5000);

                stopwatch.Start();
                DBHelper.ExecuteSQL("update t set id=3", conn);
                thread2.Join();
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                elapseTime = (double)stopwatch.ElapsedMilliseconds / 1000;
                double diffTime = elapseTime - (double)(timeout/1000);
                Console.WriteLine("different time = " + stopwatch.ElapsedMilliseconds);
                Console.WriteLine("different time = " + diffTime);
                Log(ex.Message);
                Assert.AreEqual(timeout, conn.LockTimeout);

                if (diffTime >= 0 && diffTime < 10)
                {
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }
            }
            finally
            {
                LogTestResult();
                //DBHelper.ExecuteSQL("drop table t", conn);
                conn.Close();
                conn2.Close();
            }
        }


        /// <summary>
        ///Test GetSchema without parameter
        ///</summary>
        //[TestMethod()]
        //public void GetSchemaTest()
        //{
        //    using (CUBRIDConnection conn = new CUBRIDConnection())
        //    {
        //        try
        //        {
        //            conn.ConnectionString = DBHelper.connectionStr;
        //            conn.Open();

        //            //DataTable dt = new DataTable();
        //            {
        //                DataTable dt = conn.GetSchema("USERS");
        //                if (dt != null)
        //                {
        //                    Assert.IsTrue(dt.Rows.Count == 2);
        //                    Assert.IsTrue(dt.Rows[0]["USERNAME"].ToString() == "DBA");
        //                    Assert.IsTrue(dt.Rows[1]["USERNAME"].ToString() == "PUBLIC");
        //                }


        //                dt = conn.GetSchema("FOREIGN_KEYS", new String[] { "game", "fk_game_athlete_code" });
        //                if (dt != null)
        //                {
        //                    Assert.IsTrue(dt.Rows.Count == 2);
        //                    Assert.IsTrue(dt.Rows[0]["PKTABLE_NAME"].ToString() == "athlete");
        //                    Assert.IsTrue(dt.Rows[0]["PKCOLUMN_NAME"].ToString() == "code");
        //                    Assert.IsTrue(dt.Rows[0]["FKTABLE_NAME"].ToString() == "game");
        //                    Assert.IsTrue(dt.Rows[0]["FKCOLUMN_NAME"].ToString() == "athlete_code");
        //                    Assert.IsTrue((short)dt.Rows[0]["KEY_SEQ"] == (short)1);
        //                    Assert.IsTrue((short)dt.Rows[0]["UPDATE_ACTION"] == (short)1);
        //                    Assert.IsTrue((short)dt.Rows[0]["DELETE_ACTION"] == (short)1);
        //                    Assert.IsTrue(dt.Rows[0]["FK_NAME"].ToString() == "fk_game_athlete_code");
        //                    Assert.IsTrue(dt.Rows[0]["PK_NAME"].ToString() == "pk_athlete_code");
        //                }

        //                conn.Close();
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Assert.Fail(ex.ToString());
        //        }
        //    }
        //}

        /// <summary>
        ///Test ChangeDatabase
        ///</summary>
        [TestMethod]
        public void ChangeDatabase_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                LogTestStep("Change to a valid database");
                try
                {
                    conn.ChangeDatabase("demodb2");
                }
                catch (Exception ex)
                {
                    Log(ex.Message);
                    LogNotSuported();
                }

                LogTestStep("Change to an invalid database");
                try
                {
                    conn.ChangeDatabase("another");
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Changing database is not supported in CUBRID. Please open a new connection instead!", ex.Message);
                    LogStepPass();
                }
            }

            LogTestResult();
        }

        /// <summary>
        ///Test CurrentDatabase
        ///</summary>
        [TestMethod]
        public void CurrentDatabase_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("database name is not specified in the connection string");
                try
                {
                    conn.ConnectionString = "server=localhost;port=33000;user=public;password=;charset=utf-8";
                    conn.Open();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("The database name can't be empty!", ex.Message);
                    LogStepPass();
                }

                LogTestStep("database name is specified in the connection string");

                string expected = DBHelper.dbName;
                string actual;
                conn.ConnectionString = DBHelper.connString;
                conn.Open();
                actual = conn.CurrentDatabase();
                Assert.AreEqual(expected, actual);
                LogStepPass();
            }

            LogTestResult();
        }

        /// <summary>
        ///Test Close
        ///</summary>
        [TestMethod]
        public void Close_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Close DB before connected");
                try
                {
                    conn.ConnectionString = DBHelper.connString;
                    conn.Close();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("The connection is not open!", ex.Message);
                    LogStepPass();
                }

                LogTestStep("Close a valid DB Connection, and check: (1)the connection is closed (2)the transaction which is not committed is rolled back");
                Log("Connect to a DB");
                conn.Open();

                Log("Verify the Connection is OK by feching some data");
                Assert.IsTrue(DBHelper.GetTableRowsCount("db_class", conn) > 0);

                Log("Update the DB in a transaction, and close the connection before commit");
                DBHelper.ExecuteSQL("drop table if exists t", conn);

                conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);
                Int64 tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                conn.Close();

                Log("Verify the Connection is closed by feching some data");
                try
                {
                    Int64 count = DBHelper.GetTableRowsCount("db_class", conn);
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("The connection is not open!", ex.Message);
                }

                Log("Verify the transaction is rolled back");
                conn.Open();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(0, tablesCount);
                LogStepPass();
                conn.Close();

                LogTestStep("Close a valid DB Connection twice, check no exception is generated");
                conn.Close();
                LogStepPass();
                LogTestResult();
            }
        }

        /// <summary>
        ///Test BeginTransaction, Commit, Rollback with default isolation level
        ///</summary>
        [TestMethod]
        public void Transaction_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);

                LogTestStep("Begin a transaction, then rollback");
                conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);
                Int64 tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                conn.Rollback();
                //Verify the table does not exist
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(0, tablesCount);
                LogStepPass();

                LogTestStep("Begin a transaction, then commit, then rollback");
                conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                conn.Commit();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                conn.Rollback();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                LogStepPass();
                Console.WriteLine();
                //revert the test db                
                DBHelper.ExecuteSQL("drop table t", conn);
                conn.Commit();

                LogTestResult();
            }
        }

        ///// <summary>
        ///// Test BeginTransaction With IsolationLevel
        ///// </summary>
        //[TestMethod]
        //public void BeginTransaction_WithIsolationLevel_Test()
        //{
        //    CUBRIDTransaction tran = null;

        //    using (CUBRIDConnection conn = new CUBRIDConnection())
        //    {
        //        conn.ConnectionString = DBHelper.connectionStr;
        //        conn.Open();

        //        //create test table
        //        DBHelper.ExecuteSQL("drop table if exists t", conn);
        //        DBHelper.ExecuteSQL("create table t(a int, b char(10), c string, d float, e double, f date)", conn);

        //        tran = conn.BeginTransaction();

        //        string sql = "insert into t values(?, ?, ?, ?, ?, ?)";
        //        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        //        {
        //            CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
        //            p1.Value = 1;
        //            cmd.Parameters.Add(p1);

        //            CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
        //            p2.Value = 'A';
        //            cmd.Parameters.Add(p2);

        //            CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_STRING);
        //            p3.Value = "cubrid";
        //            cmd.Parameters.Add(p3);

        //            CUBRIDParameter p4 = new CUBRIDParameter("?p4", CUBRIDDataType.CCI_U_TYPE_FLOAT);
        //            p4.Value = 1.1f;
        //            cmd.Parameters.Add(p4);

        //            CUBRIDParameter p5 = new CUBRIDParameter("?p5", CUBRIDDataType.CCI_U_TYPE_DOUBLE);
        //            p5.Value = 2.2d;
        //            cmd.Parameters.Add(p5);

        //            CUBRIDParameter p6 = new CUBRIDParameter("?p6", CUBRIDDataType.CCI_U_TYPE_DATE);
        //            p6.Value = DateTime.Now;
        //            cmd.Parameters.Add(p6);

        //            cmd.ExecuteNonQuery();

        //            tran.Commit();
        //        }

        //        Assert.AreEqual(1, DBHelper.GetTableRowsCount("t", conn));
        //        DBHelper.ExecuteSQL("drop table if exists t", conn);
        //    }
        //}

        /// <summary>
        ///Test CreateCommand
        ///</summary>
        [TestMethod]
        public void CreateCommand_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();
                DBHelper.ExecuteSQL("drop table if exists t", conn);

                CUBRIDCommand cmd = conn.CreateCommand();
                cmd.CommandText = "create table t(idx integer)";

                cmd.ExecuteNonQuery();
                Int64 tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                //revert the test db
                DBHelper.ExecuteSQL("drop table t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        ///Test SetEncoding
        ///</summary>
        [TestMethod]
        public void SetEncoding_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;

                LogTestStep("set correct encoding for sql string");
                conn.SetEncoding("utf-8");
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t(a int, b varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values(1 ,'中文Goodこんにちは')", conn);

                String sql = "select * from t where b = '中文Goodこんにちは'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.AreEqual(1, reader.GetInt32(0));
                        Assert.AreEqual("中文Goodこんにちは", reader.GetString(1));
                    }
                }

                sql = "update t set b='中文Goodこんにちは'";
                DBHelper.ExecuteSQL(sql, conn);
                sql = "select * from t where b = '中文Goodこんにちは'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "中文Goodこんにちは");
                    }
                }
                LogStepPass();

                LogTestStep("set wrong encoding for sql string");
                conn.SetEncoding("cp1252");

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t(a int, b varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values(1 ,'中文Goodこんにちは')", conn);

                sql = "select * from t where b = '中文Goodこんにちは'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {

                        reader.Read(); //retrieve just one row

                        Assert.AreEqual(1, reader.GetInt32(0));
                        Assert.AreNotEqual("中文Goodこんにちは", reader.GetString(1));
                    }
                }
                LogStepPass();

                //revert the test db
                DBHelper.ExecuteSQL("drop table t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        ///Test GetEncoding
        ///</summary>
        [TestMethod]
        public void GetEncoding_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;

                LogTestStep("set encoding and execute a sql in this encoding, then verify the encoding value");
                conn.SetEncoding("utf-8");
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t(a int, b varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values(1 ,'中文')", conn);

                String sql = "select * from t where b = '中文'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.AreEqual(1, reader.GetInt32(0));
                        Assert.AreEqual("中文", reader.GetString(1));
                    }
                }
                Assert.AreEqual(Encoding.GetEncoding("utf-8"), conn.GetEncoding());
                LogStepPass();

                LogTestStep("test the other encodings");
                conn.SetEncoding("utf-8");
                Assert.AreEqual(conn.GetEncoding(), Encoding.UTF8);

                conn.SetEncoding("iso-8859-1");
                Assert.AreEqual(conn.GetEncoding(), Encoding.GetEncoding("iso-8859-1"));

                conn.SetEncoding("euc-kr");
                Assert.AreEqual(conn.GetEncoding(), Encoding.GetEncoding("euc-kr"));

                conn.SetEncoding("euc-jp");
                Assert.AreEqual(conn.GetEncoding(), Encoding.GetEncoding("euc-jp"));

                conn.SetEncoding("gb2312");
                Assert.AreEqual(conn.GetEncoding(), Encoding.GetEncoding("gb2312"));

                conn.SetEncoding("gbk");
                Assert.AreEqual(conn.GetEncoding(), Encoding.GetEncoding("gbk"));

                LogStepPass();

                //revert the test db
                DBHelper.ExecuteSQL("drop table t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        /// Test SetIsolationLevel 
        /// </summary>
        [TestMethod]
        public void SetIsolationLevel_Test()
        {
            string sqlTablesCount = "select count(*) from db_class";
            Int64 tablesCount, newTableCount;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                conn.SetAutoCommit(false);

                tablesCount = (Int64)DBHelper.GetSingleValue(sqlTablesCount, conn);
                DBHelper.ExecuteSQL("create table t(id int)", conn);
                newTableCount = (Int64)DBHelper.GetSingleValue(sqlTablesCount, conn);
                //Verify table was created
                Assert.AreEqual(tablesCount + 1, newTableCount);

                conn.Commit();
                newTableCount = (Int64)DBHelper.GetSingleValue(sqlTablesCount, conn);
                Assert.AreEqual(tablesCount + 1, newTableCount);

                DBHelper.ExecuteSQL("drop table if exists t", conn);

                LogTestResult();
            }
        }

        /// <summary>
        ///test GetSchema (string collectionName)
        ///</summary>
        [TestMethod()]
        public void GetSchema_SchemaName_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {

                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                Log("Test GetSchema with specified schema name");
                LogTestStep("Test USERS");
                DataTable dt = conn.GetSchema("USERS");
                if (dt != null)
                {
                    Assert.AreEqual(2, dt.Rows.Count);
                    Assert.AreEqual("DBA", dt.Rows[0]["USERNAME"].ToString());
                    Assert.AreEqual("PUBLIC", dt.Rows[1]["USERNAME"].ToString());
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                LogTestStep("Test DATABASES");
                dt = conn.GetSchema("DATABASES");
                if (dt != null)
                {
                    Assert.AreEqual(1, dt.Rows.Count);
                    Assert.AreEqual("demodb", dt.Rows[0]["CATALOG_NAME"].ToString());
                    Assert.AreEqual("demodb", dt.Rows[0]["SCHEMA_NAME"].ToString());
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                LogTestStep("Test VIEWS");
                dt = conn.GetSchema("VIEWS");
                if (dt != null)
                {
                    Assert.AreEqual(3, dt.Columns.Count);
                    Assert.AreEqual("VIEW_CATALOG", dt.Columns[0].ColumnName);
                    Assert.AreEqual("VIEW_SCHEMA", dt.Columns[1].ColumnName);
                    Assert.AreEqual("VIEW_NAME", dt.Columns[2].ColumnName);
                    Assert.AreEqual(11, dt.Rows.Count);
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                // question 1
                //LogTestStep("Test TABLES");
                //dt = conn.GetSchema("TABLES");
                //if (dt != null)
                //{
                //    Assert.AreEqual(3, dt.Columns.Count);
                //    Assert.AreEqual("TABLE_CATALOG", dt.Columns[0].ColumnName);
                //    Assert.AreEqual("TABLE_SCHEMA", dt.Columns[1].ColumnName);
                //    Assert.AreEqual("TABLE_NAME", dt.Columns[2].ColumnName);
                //    Assert.AreEqual(0, dt.Rows.Count);
                //}

                LogTestStep("Test RESERVED_WORDS");
                dt = conn.GetSchema("RESERVEDWORDS");
                dt.PrimaryKey = new DataColumn[] { dt.Columns[DbMetaDataColumnNames.ReservedWord] };
                if (dt != null)
                {
                    List<string> wordList = new List<string>();
                    using (StreamReader sr = new StreamReader(@"..\..\..\Test\QATest\ADOTest\Resource\ReservedKeyWords.txt"))
                    {
                        string s = null;
                        while ((s = sr.ReadLine()) != null)
                        {
                            wordList.Add(s.Trim());
                        }
                    }

                    Assert.AreEqual(wordList.Count, dt.Rows.Count);
                    foreach (string s in wordList)
                    {
                        if (!dt.Rows.Contains(s))
                        {
                            LogStepFail();
                            Assert.Fail(String.Format("Reservered word {0} is missing", s));
                        }
                    }
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                LogTestResult();
            }
        }

        /// <summary>
        ///test GetSchema (string collectionName)
        ///</summary>
        [TestMethod()]
        public void GetSchema_Negtive_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {

                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                LogTestStep("Test GetSchema() with collectionName as null");
                DataTable dt = null;
                string schemaName = null;

                try
                {
                    dt = conn.GetSchema(schemaName);
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //Assert.AreEqual("some message here", ex.Message);
                    LogStepPass();
                }

                Log("The ArgumentException is not thrown");

                LogTestStep("Test GetSchema(String, String[]) with collectionName as null");
                try
                {
                    dt = conn.GetSchema(schemaName, new String[] { "nation" });
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //Assert.AreEqual("some message here", ex.Message);
                    LogStepPass();
                }

                Log("The ArgumentException is not thrown");
                LogTestResult();
            }
        }

        /// <summary>
        ///test GetSchema(string collectionName, string collectionName)
        ///</summary>
        [TestMethod()]
        public void GetSchema_NameAndFilter_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {

                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                Log("Test GetSchema with specified schema name and restriction values");
                LogTestStep("Test TABLES");
                DataTable dt = conn.GetSchema("TABLES", new String[] { "nation" });
                if (dt != null)
                {
                    Assert.AreEqual(1, dt.Rows.Count);
                    Assert.AreEqual("demodb", dt.Rows[0]["TABLE_CATALOG"].ToString());
                    Assert.AreEqual("demodb", dt.Rows[0]["TABLE_SCHEMA"].ToString());
                    Assert.AreEqual("nation", dt.Rows[0]["TABLE_NAME"].ToString());
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                LogTestStep("Test COLUMNS");
                dt = conn.GetSchema("COLUMNS", new String[] { "game", "event_code" });
                if (dt != null)
                {
                    Assert.AreEqual(1, dt.Rows.Count);
                    Assert.AreEqual("demodb", dt.Rows[0]["TABLE_CATALOG"].ToString());
                    Assert.AreEqual("demodb", dt.Rows[0]["TABLE_SCHEMA"].ToString());
                    Assert.AreEqual("game", dt.Rows[0]["TABLE_NAME"].ToString());
                    Assert.AreEqual("event_code", dt.Rows[0]["COLUMN_NAME"].ToString());
                    Assert.AreEqual((uint)1, (uint)dt.Rows[0]["ORDINAL_POSITION"]);
                    Assert.AreEqual("", dt.Rows[0]["COLUMN_DEFAULT"].ToString());
                    Assert.AreEqual(false, (bool)dt.Rows[0]["IS_NULLABLE"]);
                    Assert.AreEqual("INTEGER", dt.Rows[0]["DATA_TYPE"].ToString());
                    Assert.AreEqual((uint)10, (uint)dt.Rows[0]["NUMERIC_PRECISION"]);
                    Assert.AreEqual((uint)0, (uint)dt.Rows[0]["NUMERIC_SCALE"]);
                    //Assert.AreEqual((byte)0, (byte)dt.Rows[0]["CHARACTER_SET"]);
                    Assert.AreEqual((string)"Not applicable", (string)dt.Rows[0]["CHARACTER_SET"]);
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                LogTestStep("Test INDEXES");
                dt = conn.GetSchema("INDEXES", new String[] { "nation", "code" });
                if (dt != null)
                {
                    Assert.AreEqual(1, dt.Rows.Count);
                    Assert.AreEqual("demodb", dt.Rows[0]["INDEX_CATALOG"].ToString());
                    Assert.AreEqual("demodb", dt.Rows[0]["INDEX_SCHEMA"].ToString());
                    Assert.AreEqual("pk_nation_code", dt.Rows[0]["INDEX_NAME"].ToString());
                    Assert.AreEqual("nation", dt.Rows[0]["TABLE_NAME"].ToString());
                    Assert.AreEqual(true, (bool)dt.Rows[0]["UNIQUE"]);
                    Assert.AreEqual(false, (bool)dt.Rows[0]["REVERSE"]);
                    Assert.AreEqual(true, (bool)dt.Rows[0]["PRIMARY"]);
                    Assert.AreEqual(false, (bool)dt.Rows[0]["FOREIGN_KEY"]);
                    Assert.AreEqual("ASC", dt.Rows[0]["DIRECTION"].ToString());
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                LogTestStep("Test INDEXE_COLUMNS");
                dt = conn.GetSchema("INDEX_COLUMNS", new String[] { "nation", "pk_nation_code" });
                if (dt != null)
                {
                    Assert.AreEqual(1, dt.Rows.Count);
                    Assert.AreEqual("demodb", dt.Rows[0]["INDEX_CATALOG"].ToString());
                    Assert.AreEqual("demodb", dt.Rows[0]["INDEX_SCHEMA"].ToString());
                    Assert.AreEqual("pk_nation_code", dt.Rows[0]["INDEX_NAME"].ToString());
                    Assert.AreEqual("nation", dt.Rows[0]["TABLE_NAME"].ToString());
                    Assert.AreEqual("code", dt.Rows[0]["COLUMN_NAME"].ToString());
                    Assert.AreEqual(0, (int)dt.Rows[0]["ORDINAL_POSITION"]);
                    Assert.AreEqual("ASC", dt.Rows[0]["DIRECTION"].ToString());
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                Log("Test FOREIGN_KEYS");
                dt = conn.GetSchema("FOREIGN_KEYS", new String[] { "public.game", "fk_game_athlete_code" });
                if (dt != null)
                {
                    Assert.AreEqual(2, dt.Rows.Count);
                    //Assert.AreEqual("athlete", dt.Rows[0]["PKTABLE_NAME"].ToString());
                    Assert.AreEqual("code", dt.Rows[0]["PKCOLUMN_NAME"].ToString());
                    Assert.AreEqual("public.game", dt.Rows[0]["FKTABLE_NAME"].ToString());
                    //Assert.AreEqual("athlete_code", dt.Rows[0]["FKCOLUMN_NAME"].ToString());
                    Assert.AreEqual((short)1, (short)dt.Rows[0]["KEY_SEQ"]);
                    Assert.AreEqual((short)1, (short)dt.Rows[0]["UPDATE_ACTION"]);
                    Assert.AreEqual((short)1, (short)dt.Rows[0]["DELETE_ACTION"]);
                    //Assert.AreEqual("fk_game_athlete_code", dt.Rows[0]["FK_NAME"].ToString());
                    //Assert.AreEqual("pk_athlete_code", dt.Rows[0]["PK_NAME"].ToString());
                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }


                try
                {
                    DBHelper.ExecuteSQL("drop function athlete_info", conn);
                }
                catch { }

                string sql = "CREATE FUNCTION athlete_info (string1 CHAR, string2 CHAR, string3 CHAR, string4 CHAR) RETURN INTEGER AS LANGUAGE JAVA NAME 'Athlete.Athlete_Insert(java.lang.String, java.lang.String, java.lang.String, java.lang.String) return int'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                LogTestStep("Test PROCEDURES");
                dt = conn.GetSchema("PROCEDURES", new String[] { "athlete_info" });
                if (dt != null && dt.Rows.Count > 0)
                {
                    Assert.AreEqual("athlete_info", dt.Rows[0]["PROCEDURE_NAME"].ToString());
                    Assert.AreEqual("FUNCTION", dt.Rows[0]["PROCEDURE_TYPE"].ToString());
                    Assert.AreEqual("INTEGER", dt.Rows[0]["RETURN_TYPE"].ToString());
                    Assert.AreEqual(4, (int)dt.Rows[0]["ARGUMENTS_COUNT"]);
                    Assert.AreEqual("JAVA", dt.Rows[0]["LANGUAGE"].ToString());
                    Assert.AreEqual("Athlete.Athlete_Insert(java.lang.String, java.lang.String, java.lang.String, java.lang.String) return int", dt.Rows[0]["TARGET"].ToString());
                    Assert.AreEqual("DBA", dt.Rows[0]["OWNER"].ToString());

                    LogStepPass();
                }
                else
                {
                    LogStepFail();
                }

                try
                {
                    DBHelper.ExecuteSQL("drop function athlete_info", conn);
                }
                catch { }

                LogTestResult();
            }
        }

        /// <summary>
        /// Test CUBRIDConnection AutoCommit property and Related menthods SetAutoCommit, GetAutoCommit
        /// </summary>
        [TestMethod()]
        public void AutoCommit_SetAfterOpen_Test()
        {
            Int64 tablesCount;

            Log("Test SetAutoCommit, GetAutoCommit, AutoCommit");

            LogTestStep("Set AutoCommit as false");
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                conn.SetAutoCommit(false);

                tablesCount = (Int64)DBHelper.GetSingleValue("select count(*) from db_class", conn);

                //Create table
                DBHelper.ExecuteSQL("create table xyz(id int)", conn);

                //Verify the current AutoCimmit value
                Assert.AreEqual(false, conn.GetAutoCommit());

            }

            using (CUBRIDConnection conn2 = new CUBRIDConnection())
            {
                conn2.ConnectionString = DBHelper.connString;
                conn2.Open();

                //Verify table was not created
                Assert.AreEqual(tablesCount, (Int64)DBHelper.GetSingleValue("select count(*) from db_class", conn2));

                LogStepPass();

                LogTestStep("Set AutoCommit as true");
                conn2.SetAutoCommit(true);
                //Create table
                DBHelper.ExecuteSQL("create table xyz(id int)", conn2);

                //Verify the current AutoCimmit value
                Assert.AreEqual(true, conn2.GetAutoCommit());
            }

            using (CUBRIDConnection conn3 = new CUBRIDConnection())
            {
                conn3.ConnectionString = DBHelper.connString;
                conn3.Open();

                //Verify table was created
                Assert.AreEqual(tablesCount + 1, (Int64)DBHelper.GetSingleValue("select count(*) from db_class", conn3));

                LogStepPass();

                LogTestStep("Set AutoCommit as default");
                //Leave the AutoCommit as default
                DBHelper.ExecuteSQL("drop table if exists xyz", conn3);
                Assert.AreEqual(true, conn3.GetAutoCommit());
            }

            using (CUBRIDConnection conn4 = new CUBRIDConnection())
            {
                conn4.ConnectionString = DBHelper.connString;
                conn4.Open();

                //Verify table was deleted
                Assert.AreEqual(tablesCount, (Int64)DBHelper.GetSingleValue("select count(*) from db_class", conn4));

                LogStepPass();
            }

            LogTestResult();
        }

        /// <summary>
        /// Test SetAutoCommit before open
        /// </summary>
        //[TestMethod()]
        //public void AutoCommit_SetBeforeOpen_Test()
        //{
        //    int tablesCount;

        //    Log("Test SetAutoCommit, GetAutoCommit, AutoCommit");

        //    LogTestStep("Set AutoCommit as false");
        //    using (CUBRIDConnection conn = new CUBRIDConnection())
        //    {
        //        conn.ConnectionString = DBHelper.connString;
        //        conn.SetAutoCommit(false);
        //        conn.Open();
                
        //        tablesCount = (int)DBHelper.GetSingleValue("select count(*) from db_class", conn);

        //        //Create table
        //        DBHelper.ExecuteSQL("create table xyz(id int)", conn);

        //        //Verify the current AutoCimmit value
        //        Assert.AreEqual(false, conn.GetAutoCommit());

        //    }

        //    using (CUBRIDConnection conn2 = new CUBRIDConnection())
        //    {
        //        conn2.ConnectionString = DBHelper.connString;
        //        conn2.Open();

        //        //Verify table was not created
        //        Assert.AreEqual(tablesCount, (int)DBHelper.GetSingleValue("select count(*) from db_class", conn2));

        //        LogStepPass();                
        //    }

        //    using (CUBRIDConnection conn3 = new CUBRIDConnection())
        //    {
        //        LogTestStep("Set AutoCommit as true");
        //        conn3.SetAutoCommit(true);
        //        //Create table
        //        DBHelper.ExecuteSQL("create table xyz(id int)", conn3);

        //        //Verify the current AutoCimmit value
        //        conn3.ConnectionString = DBHelper.connString;
        //        conn3.Open();

        //        //Verify table was created
        //        Assert.AreEqual(tablesCount + 1, (int)DBHelper.GetSingleValue("select count(*) from db_class", conn3));
        //        Assert.AreEqual(true, conn3.GetAutoCommit()); 

        //        LogStepPass();
        //    }

        //    using (CUBRIDConnection conn4 = new CUBRIDConnection())
        //    {
        //        LogTestStep("Set AutoCommit as default");
        //        //Leave the AutoCommit as default
        //        DBHelper.ExecuteSQL("drop table if exists xyz", conn4);
        //        Assert.AreEqual(true, conn4.GetAutoCommit());
        //        conn4.ConnectionString = DBHelper.connString;
        //        conn4.Open();

        //        //Verify table was deleted
        //        Assert.AreEqual(tablesCount, (int)DBHelper.GetSingleValue("select count(*) from db_class", conn4));

        //        LogStepPass();
        //    }

        //    LogTestResult();
        //}

        /// <summary>
        /// Test BatchExecuteNoQuery
        /// </summary>
        [TestMethod()]
        public void BatchExecuteNoQuery_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Test BatchExecute with serveral non-query sql statements");
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);

                string[] sql_arr = new string[4];
                sql_arr[0] = "create table t(id integer)";
                sql_arr[1] = "insert into t values(11)";
                sql_arr[2] = "insert into t values(22)";
                sql_arr[3] = "insert into t values(33)";
                int result = conn.BatchExecuteNoQuery(sql_arr);
                Assert.AreEqual(4, result);

                string sql = "select * from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        for (int i = 1; i < 4; i++)
                        {
                            reader.Read();
                            Assert.AreEqual(i * 11, reader.GetInt32(0));
                        }

                        LogStepPass();
                    }
                }

                LogTestStep("Test BatchExecute with null");
                try
                {
                    result = conn.BatchExecuteNoQuery(null);
                    Log("The expected exception is not thrown: Object reference not set to an instance of an object.");
                    LogStepFail();
                }
                catch (CUBRIDException ex)
                {
                    Log(ex.Message);
                    //Assert.AreEqual("Object reference not set to an instance of an object.", ex.Message);
                    Assert.AreEqual("Not supported!", ex.Message);
                    LogStepPass();
                }

                //revert the test db
                DBHelper.ExecuteSQL("drop table t;", conn);
                LogTestResult();
            }
        }

        /// <summary>
        /// Test BatchExecute
        /// </summary>
        [TestMethod()]
        public void BatchExecute_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Test BatchExecute with serveral non-query sql statements");
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);

                string[] sql_arr = new string[3];
                sql_arr[0] = "create table t(id integer)";
                sql_arr[1] = "insert into t values(11)";
                sql_arr[2] = "insert into t values(22)";

                CUBRIDBatchResult result = conn.BatchExecute(sql_arr);

                Log("Check the CUBRIDBatchResult returned by BatchExecute()");
                int[] statementType = result.getStatementTypes();
                int[] errorCodes = result.getErrorCodes();
                string[] errorMessage = result.getErrorMessages();
                int[] executeResults = result.getResults();

                Assert.AreEqual(3, result.Count());
                Assert.AreEqual(false, result.getErrorFlag());

                Assert.AreEqual(4, statementType[0]);
                Assert.AreEqual(20, statementType[1]);
                Assert.AreEqual(20, statementType[2]);

                Assert.AreEqual(0, executeResults[0]);
                Assert.AreEqual(1, executeResults[1]);
                Assert.AreEqual(1, executeResults[2]);

                for (int i = 0; i < 3; i++)
                {
                    Assert.AreEqual(0, errorCodes[i]);
                    Assert.AreEqual(null, errorMessage[i]);
                }

                string sql = "select * from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        for (int i = 1; i < 3; i++)
                        {
                            reader.Read();
                            Assert.AreEqual(i * 11, reader.GetInt32(0));
                        }
                    }
                }

                LogStepPass();

                LogTestStep("Test BatchExecute with null");
                result = conn.BatchExecute(null);
                Assert.AreEqual(null, result);
                LogStepPass();

                //revert the test db
                DBHelper.ExecuteSQL("drop table t;", conn);
                LogTestResult();
            }
        }

        /// <summary>
        /// Test GetTableNameFromOid
        /// </summary>
        [TestMethod()]
        public void GetTableNameFromOid_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Get table name from a specified Oid");
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                //string tableName = conn.GetTableNameFromOID("@620|1|0");
                //string tableName = conn.GetTableNameFromOID("@3841|1|0");
                string tableName = conn.GetTableNameFromOID("@4481|1|0");//11.2

                Assert.AreEqual("public.game", tableName);
                LogStepPass();

                LogTestResult();
            }
        }

        /// <summary>
        /// Test GetCollectionSize, AddElementToSet, DropElementInSet
        /// </summary>
        [TestMethod()]
        public void CollectionSet_Test()
        {
            Log("Test GetCollectionSize, AddElementToSet, DropElementInSet");

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Test GetCollectionSize");
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);

                //Create a new table with a sequence
                DBHelper.ExecuteSQL("CREATE TABLE t(seq SEQUENCE(int))", conn);
                //Insert some data in the sequence column
                DBHelper.ExecuteSQL("INSERT INTO t(seq) VALUES({0,1,2,3,4,5,6})", conn);
                CUBRIDOid oid = new CUBRIDOid("@0|0|0");

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT t FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            oid = (CUBRIDOid)reader[0];
                        }
                    }
                }

                String attributeName = "seq";

                int SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(7, SeqSize);
                LogStepPass();

                LogTestStep("Test AddElementToSet");
                conn.AddElementToSet(oid, attributeName, 10);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(8, SeqSize);
                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {

                        while (reader.Read())
                        {
                            int[] expected = { 0, 1, 2, 3, 4, 5, 6, 10 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.AreEqual(expected[i], Convert.ToInt32(o[i]));
                            }
                        }
                    }
                }
                LogStepPass();

                LogTestStep("Test DropElementInSet");
                conn.DropElementInSequence(oid, attributeName, 5);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(7, SeqSize);
                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {

                        while (reader.Read())
                        {
                            int[] expected = { 0, 1, 2, 3, 5, 6, 10 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.AreEqual(expected[i], Convert.ToInt32(o[i]));
                            }
                        }
                    }
                }
                LogStepPass();

                //revert test db
                DBHelper.ExecuteSQL("drop table t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        /// Test GetCollectionSize, UpdateElementInSequence, InsertElementInSequence, DropElementInSequence
        /// </summary>
        [TestMethod()]
        public void CollectionSequence_Test()
        {
            Log("Test GetCollectionSize, UpdateElementInSequence, InsertElementInSequence, DropElementInSequence");

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a sequence
                DBHelper.ExecuteSQL("CREATE TABLE t(seq SEQUENCE(int))", conn);
                //Insert some data in the sequence column
                DBHelper.ExecuteSQL("INSERT INTO t(seq) VALUES({0,1,2,3,4,5,6})", conn);
                CUBRIDOid oid = new CUBRIDOid("@0|0|0");

                LogTestStep("Test UpdateElementInSequence");
                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT t FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            oid = (CUBRIDOid)reader[0];
                        }
                    }
                }

                String attributeName = "seq";
                int SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(7, SeqSize);

                conn.UpdateElementInSequence(oid, attributeName, 1, 11);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(7, SeqSize);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 11, 1, 2, 3, 4, 5, 6 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.AreEqual(expected[i], Convert.ToInt32(o[i]));
                            }
                        }
                    }
                }
                LogStepPass();

                LogTestStep("Test InsertElementInSequence");
                conn.InsertElementInSequence(oid, attributeName, 5, 12);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(8, SeqSize);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 11, 1, 2, 3, 12, 4, 5, 6 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.AreEqual(expected[i], Convert.ToInt32(o[i]));
                            }
                        }
                    }
                }

                LogStepPass();

                LogTestStep("Test DropElementInSequence");
                conn.DropElementInSequence(oid, attributeName, 6);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.AreEqual(7, SeqSize);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 11, 1, 2, 3, 12, 5, 6 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.AreEqual(expected[i], Convert.ToInt32(o[i]));
                            }
                        }
                    }
                }
                LogStepPass();

                //revert test db
                DBHelper.ExecuteSQL("drop table t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        /// Test GetQueryPlanOnly
        /// </summary>
        [TestMethod()]
        public void QueryPlanOnly_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                LogTestStep("Test GetQueryPlanOnly");
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                string queryPlan = conn.GetQueryPlanOnly("select * from public.athlete order by 1 desc");

                //the string I get from Linux does not cotain tab and enter
                Console.WriteLine(queryPlan);
                Assert.IsTrue(queryPlan.Contains("Join graph segments (f indicates final):"));
                Assert.IsTrue(queryPlan.Contains("class: public.athlete node[0]"));
                Assert.IsTrue(queryPlan.Contains("select [public.athlete].code, [public.athlete].[name], [public.athlete].gender, [public.athlete].nation_code, [public.athlete].event from [public.athlete] [public.athlete] order by 1 desc"));
                LogStepPass();
                LogTestResult();
            }
        }

        /// <summary>
        ///Test GetDatabaseProductVersion
        ///</summary>
        [TestMethod()]
        public void GetDatabaseProductVersion_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                string expected = DBHelper.dbVersion;
                string actual;

                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                actual = conn.GetDatabaseProductVersion();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        ///Test ConnectionString
        ///</summary>
        [TestMethod()]
        public void ConnectionString_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();

            LogTestStep("Invalid connection string, server is not specified");
            try
            {
                conn.ConnectionString = "database=demodb;port=33000;user=public;password=";
                conn.Open();
                Log("There should be an exception when the server is not specified");
                LogStepFail();
            }
            catch (Exception ex)
            {
                Console.WriteLine("1. " + ex.Message);
                //Assert.AreEqual("The database name can't be empty!", ex.Message);
                LogStepPass();
            }

            LogTestStep("Invalid connection string, dbname is not specified");
            try
            {
                conn.ConnectionString = "server=localhost;port=33000;user=public;password=";
                conn.Open();
                Log("There should be an exception when the dbname is not specified");
                LogStepFail();
            }
            catch (CUBRIDException ex)
            {
                Console.WriteLine("2." + ex.Message);
                //Assert.AreEqual("The database name can't be empty!", ex.Message);
                LogStepPass();
            }

            LogTestStep("Invalid connection string, user is not specified");
            try
            {
                conn.ConnectionString = "server=localhost;database=demodb;port=33000;password=";
                conn.Open();
                Log("There should be an exception when the user is not specified");
                LogStepFail();
            }
            catch (Exception ex)
            {
                Console.WriteLine("3." + ex.Message);
                //Assert.AreEqual("The database name can't be empty!", ex.Message);
                LogStepPass();
            }

            LogTestStep("Invalid connection string, password is not specified");
            try
            {
                conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;";
                conn.Open();
                Log("There should be an exception when the password is not specified");
                LogStepFail();
            }
            catch (Exception ex)
            {
                Console.WriteLine("4." + ex.Message);
                //Assert.AreEqual("The database name can't be empty!", ex.Message);
                LogStepPass();
            }

            LogTestStep("Valid connection string");
            conn.ConnectionString = DBHelper.connString;
            conn.Open();
            conn.Close();
            LogStepPass();
            
            LogTestResult();            
        }

        /// <summary>
        ///Test Database
        ///</summary>
        [TestMethod()]
        public void Conn_Property_Database()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;

            LogTestStep("the database name specified in the connection string before the connection is opened.");
            string database = conn.Database;
            Assert.AreEqual(DBHelper.dbName, database);
            LogStepPass();

            LogTestStep("after a connection is opened");
            conn.Open();
            database = conn.Database;
            Assert.AreEqual(DBHelper.dbName, database);
            LogStepPass();
        }

        /// <summary>
        /// Test ServerVersion
        /// </summary>
        [TestMethod()]
        public void Conn_Property_ServerVersion()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            conn.Open();

            string serverVersion = "";

            LogTestStep("get ServerVersion when the connection is opened");
            try
            {
                serverVersion = conn.ServerVersion;
                LogStepPass();
            }
            catch (Exception ex)
            {
                //Assert.AreEqual("some", ex.Message);
                LogStepFail();
            }

            //TODO 
            //ServerVersion was called while the returned Task was not completed and the connection was not opened after a call to OpenAsync.
            conn.Close();

            LogTestStep("get ServerVersion when the connection is closed");
            try
            {
                serverVersion = conn.ServerVersion;
                LogStepFail();
            }
            catch (Exception ex)
            {
                Assert.AreEqual("The connection is not open!", ex.Message);
                LogStepPass();
            }

            LogTestResult();
        }

        /// <summary>
        /// Test State
        /// </summary>
        [TestMethod()]
        public void Conn_Property_State()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;            

            LogTestStep("State before open");
            Assert.AreEqual(ConnectionState.Closed, conn.State);
            LogStepPass();

            LogTestStep("State after open");
            conn.Open();
            Assert.AreEqual(ConnectionState.Open, conn.State);
            LogStepPass();

            LogTestStep("State after close");
            conn.Close();
            Assert.AreEqual(ConnectionState.Closed, conn.State);
            LogStepPass();
           
            //TODO 
            //State of Broken, Connecting, Executing, Fetching

            LogTestResult();
        }

        /// <summary>
        /// Test SessionId
        /// </summary>
        [TestMethod()]
        public void SessionId_Test()
        {
            //Asking ...
            //LogTestStep("SessionId");
        }

        /// <summary>
        ///test DataSource
        ///</summary>
        [TestMethod()]
        public void Conn_Property_DataSource()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;

            LogTestStep("get the DataSoure after open");
            conn.Open();
            Assert.AreEqual(DBHelper.serverName, conn.DataSource);
            LogStepPass();

            LogTestResult();
        }


        [TestMethod()]
        public void Conn_Invalid_Connstring()
        {
            CUBRIDConnection conn = new CUBRIDConnection();

            // ConnectionString is null
            try
            {
                conn.ConnectionString = null;
            }
            catch (CUBRIDException e)
            {
                Console.WriteLine("1. " + e.Message);
                Assert.AreEqual("Connection string is null!: Closed.", e.Message);
            }

            // database is not specified
            conn = new CUBRIDConnection();
            try
            {
                conn.ConnectionString = "server=localhost;port=33000;user=dba;password=";
            }
            catch (Exception e)
            {
                Console.WriteLine("2. " + e.Message);
                Assert.AreEqual("The database name can't be empty!", e.Message);
            }

            // server is not specified
            conn = new CUBRIDConnection();
            try
            {
                conn.ConnectionString = "database=demodb;port=33000;user=dba;password=";
            }
            catch (Exception e)
            {
                Console.WriteLine("3. " + e.Message);
                Assert.AreEqual("The Server can't be empty!", e.Message);
            }

            // user is not specified
            conn = new CUBRIDConnection();
            try
            {
                conn.ConnectionString = "server=localhost;database=demodb;port=33000;password=";
            }
            catch (Exception e)
            {
                Console.WriteLine("4. " + e.Message);
                Assert.AreEqual("The User can't be empty!", e.Message);
            }

            conn.Close();
        }
    }
}
