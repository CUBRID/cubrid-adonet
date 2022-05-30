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
    [TestClass]
    public class BTS_issue : BaseTest
    {
        [TestMethod]
        public void conn_Database()
        {
            string conn_string = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = conn_string;
            Console.WriteLine(conn.Database);
            conn.Open();
            Console.WriteLine(conn.Database);

            conn.Close();
        }

        [TestMethod]
        public void conn_setIsolationLevel()
        {
            string conn_string = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = conn_string;
            conn.Open();

            CUBRIDCommand cmd = new CUBRIDCommand();
            cmd.Connection = conn;
            cmd.CommandText = "drop table if exists test_isolation";
            cmd.ExecuteNonQuery();

            // open another session
            CUBRIDConnection conn2 = new CUBRIDConnection();
            conn2.ConnectionString = conn_string;
            conn2.Open();

            CUBRIDCommand cmd2 = new CUBRIDCommand();
            cmd2.Connection = conn2;


            // set up the isolation level to 
            conn.SetAutoCommit(false);
            conn.SetIsolationLevel(CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE);
            cmd.CommandText = "create table test_isolation(a int)";
            cmd.ExecuteNonQuery();

            conn.Commit();

            conn.Close();
        }

        [TestMethod]
        public void conn_dataAdapter_update()
        {
            string conn_string = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = conn_string;
            conn.Open();

            CUBRIDCommand cmd = new CUBRIDCommand();
            cmd.Connection = conn;
            cmd.CommandText = "drop table if exists tbl";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table tbl (id int, name varchar(100))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into tbl values (1, 'Nancy')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into tbl values (2, 'Peter')";
            cmd.ExecuteNonQuery();

            CUBRIDDataAdapter adapter = new CUBRIDDataAdapter();

            //SelectCommand
            string sql = "select * from tbl";
            CUBRIDCommand cmd2 = new CUBRIDCommand(sql, conn);
            adapter.SelectCommand = cmd2;

            sql = "insert into tbl values (3, 'Kitty')";
            cmd2 = new CUBRIDCommand(sql, conn);
            adapter.InsertCommand = cmd2;
            adapter.InsertCommand.ExecuteNonQuery();

            sql = "update tbl set name='Mandy' where id=1";
            cmd2 = new CUBRIDCommand(sql, conn);
            adapter.UpdateCommand = cmd2;
            adapter.UpdateCommand.ExecuteNonQuery();

            sql = "delete from tbl where name='Mandy'";
            cmd2 = new CUBRIDCommand(sql, conn);
            adapter.DeleteCommand = cmd2;
            adapter.DeleteCommand.ExecuteNonQuery();

            conn.Close();
        }

        [TestMethod]
        public void DataReader_NextResult_Test()
        {
            string conn_string = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = conn_string;
            conn.Open();

            String sql = "select * from public.nation;";
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            Console.WriteLine(reader.GetString(0));

            if (reader.NextResult())
            {
                reader.Read();
                Console.WriteLine(reader.GetString(0));
            }

            conn.Close();
        }

        [TestMethod]
        public void DataReader_MultiQuery_Test()
        {
            string conn_string = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = conn_string;
            conn.Open();

            String sql = "select s_name from public.code where f_name = 'Mixed'; select s_name from public.code where f_name = 'Woman';";
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            };

            while (reader.NextResult())
            {
                Console.WriteLine("=============================");

                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0));
                };
            }

            conn.Close();
        }

        [TestMethod]
        public void DataReader_MultiQuery_Test2()
        {
            string conn_string = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = conn_string;
            conn.Open();

            String sql = "select s_name from public.code where s_name='X'; select name from public.nation where name='Algeria';";
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            };

            while (reader.NextResult())
            {
                Console.WriteLine("=============================");

                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0));
                };
            }

            conn.Close();
        }

        [TestMethod]
        public void Conn_ConnectionString_Exception()
        {

            CUBRIDConnection conn = new CUBRIDConnection();

            // database is not specified
            try
            {
                conn.ConnectionString = "server=test-db-server;port=33000;user=dba;password=";
            }
            catch (Exception ex)
            {
                Console.WriteLine("1. " + ex.Message);
            }

            // ConnectionString is null
            try
            {
                conn.ConnectionString = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("2. " + ex.Message);
            }

            conn.Close();

        }

        [TestMethod]
        public void Conn_ConnectionString_Exception2()
        {

            CUBRIDConnection conn = new CUBRIDConnection();

            // server is not specified
            try
            {
                conn.ConnectionString = "database=demodb;port=33000;user=dba;password=";
                //conn.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine("1. " + ex.Message);
            }

            conn.Close();

        }

        [TestMethod]
        public void Conn_ConnectionString_Exception3()
        {

            CUBRIDConnection conn = new CUBRIDConnection();

            // user is not specified
            try
            {
                conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;password=";
                //conn.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine("1. " + ex.Message);
            }

            conn.Close();

        }

        [TestMethod]
        public void APIS_485()
        {
            LogTestStep("Test the lockTimeout is set successfully");
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            Assert.AreEqual(-1, conn.LockTimeout);
            
            conn.Open();
            int timeout = 20;
            conn.SetLockTimeout(timeout);
            Assert.AreEqual(timeout, conn.LockTimeout);

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
                double diffTime = elapseTime - (double)(timeout / 1000);
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

        [TestMethod]
        public void APIS_492()
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
                timeout = 3;
                conn.SetConnectionTimeout(timeout);
                

                //The database in the connection string does not exist
                conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=public;password=";
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
                    Console.WriteLine(ex.Message);
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
                    timeout = 20;
                    conn.SetConnectionTimeout(timeout);
                    Log("Not allowed to change the 'ConnectionTimeout' property while the connection state is!: Open.");
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Not allowed to change the 'ConnectionTimeout' property while the connection state is!: Open.", ex.Message);
                    LogStepPass();
                }

                LogTestResult();

            }
        }

        [TestMethod]
        public void APIS_493()
        {
            /*
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;password=";

            //conn.OpenAsync();
            while (conn.State != ConnectionState.Open)
            {
                Console.WriteLine("connecting....,{0}", conn.State);
            }
            Console.WriteLine(conn.State);

            Assert.AreEqual(ConnectionState.Open, conn.State);

            conn.Close();*/
        }

        [TestMethod]
        public void APIS_493_withToken()
        {
            /*CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;password=";

            CancellationTokenSource cts = new CancellationTokenSource();
            conn.OpenAsync(cts.Token);

            while (conn.State != ConnectionState.Open)
            {
                Console.WriteLine("connecting....,{0}", conn.State);
                cts.Cancel();
                if (conn.State == ConnectionState.Closed)
                {
                    Console.WriteLine("Cancel....");
                    break;
                }
            }

            Console.WriteLine(conn.State);
            Assert.AreEqual(ConnectionState.Closed, conn.State);*/

        }

        [TestMethod]
        public void APIS_501()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=public;password=";

            try
            {
                String sql = "select * from code;";
                CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Assert.AreEqual("The connection is not open!", e.Message);
            }
        }

        [TestMethod]
        public void OpenAsync()
        {
            /*
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;password=";

            try
            {
                conn.OpenAsync();
                Console.WriteLine(conn.State);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }*/
            //conn.Close();
        }

        [TestMethod]
        public void i18n_issue()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            conn.Open();

            CUBRIDCommand cmd = new CUBRIDCommand();
            cmd.Connection = conn;
            conn.SetEncoding("utf-8");

            cmd.CommandText = "drop table if exists 测试表;";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "create table 测试表 (名称 varchar);";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "insert into 测试表 value('小明');";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "select 名称 from 测试表;";
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            };

            conn.Close();
        }

        /*[TestMethod]
        public void APIS_729()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            conn.Open();

            conn.SetAutoCommit(false);
 
            CUBRIDTransaction transaction = conn.BeginTransaction();

            CUBRIDCommand cmd = new CUBRIDCommand("drop table if exists tkeys", conn);
            cmd.ExecuteNonQuery();

            cmd = new CUBRIDCommand("create table tkeys(id int auto_increment(300,1), str string)", conn);
            cmd.ExecuteNonQuery();

            //cmd.IsGeneratedKeys = true;
            cmd.CommandText = "insert into tkeys(str) values('xyz')";
            cmd.ExecuteNonQuery();
 
            DbDataReader reader = cmd.GetGeneratedKeys();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetInt32(0));
            };

            cmd.Close();
            transaction.Commit();

            conn.Close();

        }
        */
        [TestMethod]
        public void APIS_727()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
            conn.Open();

            try
            {
                DataTable dt = conn.GetSchema(null);
            }
            catch (ArgumentException e)
            {
                Console.WriteLine(e.Message);
                Assert.AreEqual("The collectionName is specified as null.!", e.Message);
            }

            conn.Close();
        }

        [TestMethod]
        public void APIS_728()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = "server=test-db-server;database=demodb;port=33000;user=dba;password=";

            try
            {
                DataTable dt = conn.GetSchema("TABLES");
            }
            catch (Exception e)
            {
                Assert.AreEqual("The connection is not open!", e.Message);
            }

            conn.Open();
            DataTable dt_test = conn.GetSchema("TABLES");
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("ReservedWords");
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("Users");
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("Databases");
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("Views");
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("Columns", new String[] { "public.game", "event_code" });
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("Indexes", new String[] { "public.nation", "code" });
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("Index_Columns", new String[] { "public.nation", "pk_nation_code" });
            Assert.IsNotNull(dt_test);
            dt_test = conn.GetSchema("FOREIGN_KEYS", new String[] { "public.game", "fk_game_athlete_code" });
            Assert.IsNotNull(dt_test);

            dt_test = conn.GetSchema("INVALID");
            Assert.IsNull(dt_test);

            conn.Close();
        }
    }
}
