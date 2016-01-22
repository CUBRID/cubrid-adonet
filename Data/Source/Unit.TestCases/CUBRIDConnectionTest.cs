using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data.Common;
using System.Data;
using System.Text;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDConnectionTest and is intended
    ///to contain all CUBRIDConnectionTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDConnectionTest
    {
        private static readonly string connString = "server=10.34.64.122;database=demodb;port=33510;user=public;password=";

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        /// <summary>
        ///A test for Open
        ///</summary>
        [TestMethod()]
        public void OpenTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                try
                {
                    target.ConnectionString = CUBRIDConnectionTest.connString;
                    target.Open();
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.ToString());
                }

                try
                {
                    target.Open();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Connection is already open!");
                }

                CUBRIDConnection _clone = target.Clone();
                Assert.AreEqual(_clone.State, ConnectionState.Closed);
            }
        }

        [TestMethod()]
        public void MultipleConnections_Test()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                try
                {
                    target.ConnectionString = CUBRIDConnectionTest.connString;
                    target.Open();

                    CUBRIDConnectionTest.ExecuteSQL("drop table if exists t", target);
                    CUBRIDConnectionTest.ExecuteSQL("create table t(idx integer)", target);

                    string sql = "select * from nation";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, target))
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            int count = 0;
                            while (reader.Read() && count++ < 3)
                            {
                                using (CUBRIDConnection conn2 = new CUBRIDConnection())
                                {
                                    conn2.ConnectionString = target.ConnectionString;
                                    conn2.Open();
                                    string sqlInsert = "insert into t values(" + count + ")";
                                    using (CUBRIDCommand cmdInsert = new CUBRIDCommand(sqlInsert, conn2))
                                    {
                                        cmdInsert.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }

                    using (CUBRIDConnection conn2 = new CUBRIDConnection())
                    {
                        conn2.ConnectionString = target.ConnectionString;
                        conn2.Open();
                        string sqlSelect = "select count(*) from t";
                        using (CUBRIDCommand cmd = new CUBRIDCommand(sqlSelect, conn2))
                        {
                            using (DbDataReader reader = cmd.ExecuteReader())
                            {
                                reader.Read();
                                Assert.IsTrue(reader.GetInt32(0) == 3);
                            }
                        }
                    }

                    CUBRIDConnectionTest.ExecuteSQL("drop table if exists t", target);
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.ToString());
                }
            }
        }


        private static void ExecuteSQL(string sql, CUBRIDConnection conn)
        {
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        ///A test for GetSchema
        ///</summary>
        [TestMethod()]
        public void DataTable_Basic_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDConnectionTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                using (DataTable dt = new DataTable("nation"))
                {
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                    {
                        da.SelectCommand = new CUBRIDCommand(sql, conn);
                        da.Fill(dt);

                        Assert.AreEqual(dt.Columns.Count, 4);
                        Assert.AreEqual(dt.Rows.Count, 215);
                        Assert.AreEqual(dt.Rows[1][1].ToString(), "Netherlands Antilles");
                        Assert.AreEqual(dt.Rows[3][2].ToString(), "Africa");
                    }
                }
            }
        }


        /// <summary>
        ///A test for GetSchema
        ///</summary>
        [TestMethod()]
        public void GetSchemaTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                try
                {
                    target.ConnectionString = CUBRIDConnectionTest.connString;
                    target.Open();

                    //DataTable dt = new DataTable();
                    {
                        DataTable dt = target.GetSchema("USERS");
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 2);
                            Assert.IsTrue(dt.Rows[0]["USERNAME"].ToString() == "DBA");
                            Assert.IsTrue(dt.Rows[1]["USERNAME"].ToString() == "PUBLIC");
                        }

                        dt = target.GetSchema("DATABASES");
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 1);
                            Assert.IsTrue(dt.Rows[0]["CATALOG_NAME"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["SCHEMA_NAME"].ToString() == "demodb");
                        }

                        dt = target.GetSchema("TABLES", new String[] { "nation" });
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 1);
                            Assert.IsTrue(dt.Rows[0]["TABLE_CATALOG"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["TABLE_SCHEMA"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["TABLE_NAME"].ToString() == "nation");
                        }

                        dt = target.GetSchema("VIEWS");
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Columns.Count == 3);
                            Assert.IsTrue(dt.Columns[0].ColumnName == "VIEW_CATALOG");
                            Assert.IsTrue(dt.Columns[1].ColumnName == "VIEW_SCHEMA");
                            Assert.IsTrue(dt.Columns[2].ColumnName == "VIEW_NAME");
                            Assert.IsTrue(dt.Rows.Count == 0);
                        }

                        dt = target.GetSchema("COLUMNS", new String[] { "game", "event_code" });
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 1);
                            Assert.IsTrue(dt.Rows[0]["TABLE_CATALOG"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["TABLE_SCHEMA"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["TABLE_NAME"].ToString() == "game");
                            Assert.IsTrue(dt.Rows[0]["COLUMN_NAME"].ToString() == "event_code");
                            Assert.IsTrue((uint)dt.Rows[0]["ORDINAL_POSITION"] == (uint)1);
                            Assert.IsTrue(dt.Rows[0]["COLUMN_DEFAULT"].ToString() == "");
                            Assert.IsTrue((bool)dt.Rows[0]["IS_NULLABLE"] == false);
                            Assert.IsTrue(dt.Rows[0]["DATA_TYPE"].ToString() == "INTEGER");
                            Assert.IsTrue((uint)dt.Rows[0]["NUMERIC_PRECISION"] == (uint)0);
                            Assert.IsTrue((uint)dt.Rows[0]["NUMERIC_SCALE"] == (uint)0);
                            Assert.IsTrue((byte)dt.Rows[0]["CHARACTER_SET"] == (byte)0);
                        }

                        dt = target.GetSchema("INDEXES", new String[] { "nation", "code" });
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 1);
                            Assert.IsTrue(dt.Rows[0]["INDEX_CATALOG"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["INDEX_SCHEMA"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["INDEX_NAME"].ToString() == "pk_nation_code");
                            Assert.IsTrue(dt.Rows[0]["TABLE_NAME"].ToString() == "nation");
                            Assert.IsTrue((bool)dt.Rows[0]["UNIQUE"] == true);
                            Assert.IsTrue((bool)dt.Rows[0]["REVERSE"] == false);
                            Assert.IsTrue((bool)dt.Rows[0]["PRIMARY"] == true);
                            Assert.IsTrue((bool)dt.Rows[0]["FOREIGN_KEY"] == false);
                            Assert.IsTrue(dt.Rows[0]["DIRECTION"].ToString() == "ASC");
                        }

                        dt = target.GetSchema("INDEX_COLUMNS", new String[] { "nation", "pk_nation_code" });
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 1);
                            Assert.IsTrue(dt.Rows[0]["INDEX_CATALOG"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["INDEX_SCHEMA"].ToString() == "demodb");
                            Assert.IsTrue(dt.Rows[0]["INDEX_NAME"].ToString() == "pk_nation_code");
                            Assert.IsTrue(dt.Rows[0]["TABLE_NAME"].ToString() == "nation");
                            Assert.IsTrue(dt.Rows[0]["COLUMN_NAME"].ToString() == "code");
                            Assert.IsTrue((int)dt.Rows[0]["ORDINAL_POSITION"] == 0);
                            Assert.IsTrue(dt.Rows[0]["DIRECTION"].ToString() == "ASC");
                        }

                        dt = target.GetSchema("FOREIGN_KEYS", new String[] { "game", "fk_game_athlete_code" });
                        if (dt != null)
                        {
                            Assert.IsTrue(dt.Rows.Count == 2);
                            Assert.IsTrue(dt.Rows[0]["PKTABLE_NAME"].ToString() == "athlete");
                            Assert.IsTrue(dt.Rows[0]["PKCOLUMN_NAME"].ToString() == "code");
                            Assert.IsTrue(dt.Rows[0]["FKTABLE_NAME"].ToString() == "game");
                            Assert.IsTrue(dt.Rows[0]["FKCOLUMN_NAME"].ToString() == "athlete_code");
                            Assert.IsTrue((short)dt.Rows[0]["KEY_SEQ"] == (short)1);
                            Assert.IsTrue((short)dt.Rows[0]["UPDATE_ACTION"] == (short)1);
                            Assert.IsTrue((short)dt.Rows[0]["DELETE_ACTION"] == (short)1);
                            Assert.IsTrue(dt.Rows[0]["FK_NAME"].ToString() == "fk_game_athlete_code");
                            Assert.IsTrue(dt.Rows[0]["PK_NAME"].ToString() == "pk_athlete_code");
                        }

                        target.Close();
                    }
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.ToString());
                }
            }
        }

        /// <summary>
        /// Test basic SQL statements execution
        /// </summary>
        [TestMethod()] 
        public void Test_Demo_Basic()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDConnectionTest.connString;
                conn.Open();

                using (CUBRIDCommand cmd = new CUBRIDCommand("drop table if exists t", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("create table t(id int, str string)", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("insert into t values(1, 'abc')", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt16(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "abc");
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("update t set str = 'xyz' where id = 1", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt16(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "xyz");
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("delete from t", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.HasRows == false);
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("drop table t", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Test basic SQL statements execution, using parameters
        /// </summary>
        [TestMethod()]
        public void Test_Demo_Basic_WithParameters()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDConnectionTest.connString;
                conn.Open();

                using (CUBRIDCommand cmd = new CUBRIDCommand("drop table if exists t", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("create table t(id int, str string)", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("insert into t values(?, ?)", conn))
                {
                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Value = "abc";
                    cmd.Parameters.Add(p2);

                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where id = ?", conn))
                {
                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt16(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "abc");
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("update t set str = ? where id = ?", conn))
                {
                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Value = "xyz";
                    cmd.Parameters.Add(p2);

                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where str = ?", conn))
                {
                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Value = "xyz";
                    cmd.Parameters.Add(p2);

                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt16(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "xyz");
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("delete from t where id = ? and str = ?", conn))
                {
                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Value = "xyz";
                    cmd.Parameters.Add(p2);

                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where str = ? and id = ?", conn))
                {
                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Value = "xyz";
                    cmd.Parameters.Add(p2);

                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p1.Value = 1;
                    cmd.Parameters.Add(p1);

                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.HasRows == false);
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("drop table t", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        ///A test for GetAutoCommit
        ///</summary>
        [TestMethod()]
        public void GetAutoCommitTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();

                bool bCommit = false;
                target.SetAutoCommit(bCommit);
                Assert.AreEqual(target.GetAutoCommit(), bCommit);
            }
        }

        /// <summary>
        ///A test for Clone
        ///</summary>
        [TestMethod()]
        public void CloneTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();

                CUBRIDConnection actual = target.Clone();
                
                Assert.IsTrue(actual != null);

                actual.SetEncoding("iso-8859-1");
                Assert.AreEqual(actual.GetEncoding(), Encoding.GetEncoding("iso-8859-1"));

                actual.SetEncoding("euc-kr");
                Assert.AreEqual(actual.GetEncoding(), Encoding.GetEncoding("euc-kr"));

                actual.SetEncoding("euc-jp");
                Assert.AreEqual(actual.GetEncoding(), Encoding.GetEncoding("euc-jp"));

                actual.SetEncoding("gb2312");
                Assert.AreEqual(actual.GetEncoding(), Encoding.GetEncoding("gb2312"));
                
                actual.SetEncoding("gbk");
                Assert.AreEqual(actual.GetEncoding(), Encoding.GetEncoding("gbk"));
                try
                {
                    actual.Open();
                    actual.ConnectionString = "server=localhost;database=demodb;port=33510;user=public;password=";
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Not allowed to change the 'ConnectionString' property while the connection state is!: Open.");
                }
            }
        }

        /// <summary>
        ///A test for SetConnectionTimeout
        ///</summary>
        [TestMethod()]
        public void SetConnectionTimeoutTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;

                try
                {
                    int value = 100;
                    target.SetConnectionTimeout(value);
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Invalid Timeout value! Expected a value between 1 and 99!");
                }

                target.SetConnectionTimeout(35);
                target.Open();

                Assert.AreEqual(target.ConnectionTimeout, 35);

                try
                {
                    target.SetConnectionTimeout(40);
                    Assert.IsTrue(false, "Not allowed to change the 'ConnectionTimeout' property while the connection state is: Open.");
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Not allowed to change the 'ConnectionTimeout' property while the connection state is!: Open.");
                }
            }
        }

        /// <summary>
        ///A test for SetLockTimeout
        ///</summary>
        [TestMethod()]
        public void SetLockTimeoutTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;

                try
                {
                    int value = 100;
                    target.SetLockTimeout(value);
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Invalid Timeout value! Expected a value between 1 and 99!");
                }

                target.SetLockTimeout(35);
                target.Open();
                Assert.AreEqual(target.LockTimeout, 35);
                target.SetLockTimeout();

                try
                {
                    target.SetLockTimeout(40);
                    Assert.IsTrue(false, "Not allowed to change the 'LockTimeout' property while the connection state is: Open.");
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Not allowed to change the 'LockTimeout' property while the connection state is!: Open.");
                }
            }
        }

        /// <summary>
        ///A test for ChangeDatabase
        ///</summary>
        [TestMethod()]
        public void ChangeDatabaseTest()
        {
            using(CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();

                try
                {
                    target.ChangeDatabase("another");
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "Changing database is not supported in CUBRID. Please open a new connection instead!");
                }
            }
        }

        /// <summary>
        ///A test for CurrentDatabase
        ///</summary>
        [TestMethod()]
        public void CurrentDatabaseTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                try
                {
                    target.ConnectionString = "server=10.34.64.122;port=33510;user=public;password=;charset=utf-8";
                    target.Open();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual(ex.Message, "The database name can't be empty!");
                }

                string expected = "demodb";
                string actual;

                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();
                actual = target.CurrentDatabase();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        ///A test for GetDatabaseProductVersion
        ///</summary>
        [TestMethod()]
        public void GetDatabaseProductVersionTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                string expected = "8.4.1.7007";
                string actual;

                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();

                actual = target.GetDatabaseProductVersion();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        ///A test for SetMaxStringLength
        ///</summary>
        [TestMethod()]
        public void SetMaxStringLengthTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();

                try
                {
                    int val = 2048;
                    target.SetMaxStringLength(val);
                }
                catch (Exception ex)
                {
                    Assert.IsTrue(true, ex.Message);
                }
            }
        }

        /// <summary>
        ///A test for GetMaxStringLength
        ///</summary>
        [TestMethod()]
        public void GetMaxStringLengthTest()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                target.ConnectionString = CUBRIDConnectionTest.connString;
                target.Open();

                Assert.AreEqual(target.MaxStringLength, 32767);

                try
                {
                    target.GetMaxStringLength();
                }
                catch (Exception ex)
                {
                    Assert.IsTrue(true, ex.Message);
                }
            }
        }

        /// <summary>
        ///A test for GetSchema
        ///</summary>
        [TestMethod()]
        public void GetSchemaTest1()
        {
            using (CUBRIDConnection target = new CUBRIDConnection())
            {
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetSchema();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test BatchExecute()
        /// </summary>
        [TestMethod()]
        public void Test_BatchExecute()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDConnectionTest.connString;
                conn.Open();

                string sql = "create table t(idx integer)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                CUBRIDBatchResult result = conn.BatchExecute(null);
                Assert.IsNull(result);
                /*
                string[] sql_arr = new string[3];
                sql_arr[0] = "insert into t values(1)";
                sql_arr[1] = "insert into t values(2)";
                sql_arr[2] = "insert into t values(3)";
                result = conn.BatchExecute(sql_arr);
                sql = "select count(*) from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt32(0) == 3);
                    }
                }
                */
                sql = "drop table t;";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        [TestMethod()]
        public void Test_ConnectionURL_And_Reset()
        {
            string strURL = "cci:cubrid:10.34.64.121:33510:demodb:public::?logSlowQueries=true" +
                            "&slowQueryThresholdMillis=1000&logTraceApi=true&logTraceNetwork=true" +
                            "&autoCommit=false&althosts=10.34.64.122,10.34.64.122:33510&" +
                            "querytimeout=10000&logintimeout=5000";
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = strURL;
                conn.Open();

                string sqlTablesCount = "select count(*) from db_class where is_system_class='NO' and class_type='CLASS'";
                int tablesCount = 0;
                using (CUBRIDCommand cmd = new CUBRIDCommand(sqlTablesCount, conn))
                {
                    tablesCount = (int)cmd.ExecuteScalar();
                }

                Assert.AreEqual(tablesCount, 10);

                conn.Close();

                try
                {
                    string strURL2 = "cci:cubrid:10.34.64.121:33510:demodb:public::?logSlowQueries=true" +
                                "&slowQueryThresholdMillis=1000&logTraceApi=false&logTraceNetwork=false&autoCommit=false";
                    conn.ConnectionString = strURL2;
                    conn.Open();

                    throw new Exception("Properties data are not reset");
                }
                catch { }
            }
        }

        [TestMethod()]
        public void TestConnectionURL_TraceLog()
        {
            string strURL = "cci:cubrid:10.34.64.122:33690:demodb:public::?logSlowQueries=true" +
                            "&slowQueryThresholdMillis=1000&logTraceApi=true&logfile=" +
                            "Cubrid_ad.net.log&logbasedir=c:\\temp\\doc&querytimeout=10000&logintimeout=5000";
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = strURL;
                conn.Open();

                string sqlTablesCount = "select count(*) from db_class where is_system_class='NO' and class_type='CLASS'";
                int tablesCount = 0;
                using (CUBRIDCommand cmd = new CUBRIDCommand(sqlTablesCount, conn))
                {
                    tablesCount = (int)cmd.ExecuteScalar();
                }

                Assert.AreEqual(tablesCount, 10);

                conn.Close();
            }
        }
    }
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     