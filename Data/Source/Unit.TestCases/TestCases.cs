using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data.Common;
using System.Data;
using System.Text;
using System.Runtime.InteropServices;

namespace Unit.TestCases
{
    /// <summary>
    ///</summary>
    [TestClass()]
    public class UnitTestCases
    {
        private static readonly string connString = "server=10.34.64.122;database=demodb;port=33690;user=public;password=";

        private TestContext testContextInstance;

        private static void CleanupTestTable(CUBRIDConnection conn)
        {
            UnitTestCases.ExecuteSQL("drop table if exists t", conn);
        }

        private static void CreateTestTable(CUBRIDConnection conn)
        {
            UnitTestCases.ExecuteSQL("drop table if exists t", conn);
            UnitTestCases.ExecuteSQL("create table t(a int, b char(10), c string, d float, e double, f date)", conn);
        }

        private static void ExecuteSQL(string sql, CUBRIDConnection conn)
        {
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private static object GetSingleValue(string sql, CUBRIDConnection conn)
        {
            object ret = null;

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                ret = cmd.ExecuteScalar();
            }

            return ret;
        }

        private static int GetTablesCount(string tableName, CUBRIDConnection conn)
        {
            int count = 0;
            string sql = "select count(*) from db_class where class_name = '" + tableName + "'";

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                count = (int)cmd.ExecuteScalar();
            }

            return count;
        }

        private static int GetTableRowsCount(string tableName, CUBRIDConnection conn)
        {
            int count = -1;
            string sql = "select count(*) from `" + tableName + "`";

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                count = (int)cmd.ExecuteScalar();
            }

            return count;
        }

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
        /// Test basic SQL statements execution, using DataSet
        /// </summary>
        [TestMethod()]
        public void Test_DataSet_Basic()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                {
                    da.SelectCommand = new CUBRIDCommand(sql, conn);
                    DataSet ds = new DataSet("nation");
                    da.Fill(ds);

                    DataTable dt0 = ds.Tables["Table"];
                    Assert.IsTrue(dt0 != null);

                    dt0 = ds.Tables[0];

                    Assert.IsTrue(dt0.Columns.Count == 4);
                    Assert.AreEqual(dt0.DefaultView.Count, 215);
                    Assert.IsTrue(dt0.DefaultView.AllowEdit == true);
                    Assert.IsTrue(dt0.DefaultView.AllowDelete == true);
                    Assert.IsTrue(dt0.DefaultView.AllowNew == true);
                    Assert.IsTrue(dt0.DataSet.DataSetName == "nation");

                    DataRow[] dataRow = dt0.Select("continent = 'Africa'");

                    Assert.IsTrue(dataRow.Length == 54);
                }
            }
        }

        /// <summary>
        /// Test exporting XML from DataSet
        /// </summary>
        [TestMethod()]
        public void Test_DataSet_ExportXML()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                using(CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                {
                    da.SelectCommand = new CUBRIDCommand(sql, conn);
                    DataSet ds = new DataSet();
                    da.Fill(ds, "nation");

                    string filename = @".\Test_DataSet_ExportXML.xml";
                    ds.WriteXml(filename);

                    if (!System.IO.File.Exists(filename))
                    {
                        throw new Exception("XML output file not found!");
                    }
                    else
                    {
                        System.IO.File.Delete(filename);
                    }
                }
            }
        }


        /// <summary>
        /// Test SQL statements execution, using DataView
        /// </summary>
        private static void Test_DataView_Basic()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                {
                    da.SelectCommand = new CUBRIDCommand(sql, conn);
                    DataTable dt = new DataTable("nation");
                    da.Fill(dt);

                    using (DataView dataView = new DataView(dt))
                    {
                        Assert.IsTrue(dataView.Count == 215);
                        Assert.IsTrue(dataView.Table.TableName == "nation");

                        foreach (DataRowView view in dataView)
                        {
                            Assert.IsTrue(dataView[0][0].ToString() == "AFG");
                            break; //retrieve just one row
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Test CUBRIDTransaction class
        /// </summary>
        [TestMethod()]
        public void Test_Transaction()
        {
            DbTransaction tran = null;
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);

                tran = conn.BeginTransaction();

                string sql = "create table t(idx integer)";
                using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
                {
                    command.ExecuteNonQuery();
                }

                int tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                tran.Rollback();

                //Verify the table does not exist
                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 0);

                conn.BeginTransaction();

                sql = "create table t(idx integer)";
                using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
                {
                    command.ExecuteNonQuery();
                }

                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                conn.Commit();

                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                conn.Rollback();

                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                conn.BeginTransaction();

                UnitTestCases.ExecuteSQL("drop table t", conn);

                conn.Commit();

                tablesCount = GetTablesCount("t", conn);
                Assert.IsTrue(tablesCount == 0);
            }
        }

        /// <summary>
        /// Test CUBRIDTransaction class, using parameters
        /// </summary>
        [TestMethod()]
        public void Test_Transaction_Parameters()
        {
            DbTransaction tran = null;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                CreateTestTable(conn);

                tran = conn.BeginTransaction();
                if (tran != null)
                {
                    string sql = "insert into t values(?, ?, ?, ?, ?, ?)";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                        p1.Value = 1;
                        cmd.Parameters.Add(p1);

                        CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
                        p2.Value = 'A';
                        cmd.Parameters.Add(p2);

                        CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_STRING);
                        p3.Value = "cubrid";
                        cmd.Parameters.Add(p3);

                        CUBRIDParameter p4 = new CUBRIDParameter("?p4", CUBRIDDataType.CCI_U_TYPE_FLOAT);
                        p4.Value = 1.1f;
                        cmd.Parameters.Add(p4);

                        CUBRIDParameter p5 = new CUBRIDParameter("?p5", CUBRIDDataType.CCI_U_TYPE_DOUBLE);
                        p5.Value = 2.2d;
                        cmd.Parameters.Add(p5);

                        CUBRIDParameter p6 = new CUBRIDParameter("?p6", CUBRIDDataType.CCI_U_TYPE_DATE);
                        p6.Value = DateTime.Now;
                        cmd.Parameters.Add(p6);

                        cmd.ExecuteNonQuery();

                        tran.Commit();
                    }

                    CUBRIDConnection cbConn = tran.Connection as CUBRIDConnection;
                    if (cbConn != null)
                        Assert.IsTrue(GetTableRowsCount("t", conn) == 1);

                    CUBRIDTransaction cbTran = (CUBRIDTransaction)tran;
                    Assert.AreEqual(cbTran.CUBRIDIsolationLevel, conn.GetIsolationLevel());
                    Assert.AreEqual(cbTran.IsolationLevel, IsolationLevel.RepeatableRead);
                    Assert.AreEqual(cbConn, conn);

                    CleanupTestTable(conn);

                    conn.Close();

                    try
                    {
                        tran.Commit();
                    }
                    catch (Exception ex)
                    {
                        Assert.AreEqual(ex.Message, "Connection must be valid and open to commit transaction!");
                    }
                }
            }
        }

        /// <summary>
        /// Test CUBRIDCommand ExecuteNonQuery() method
        /// </summary>
        private static void Test_ExecuteNonQuery()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                CreateTestTable(conn);

                string sql = "insert into t values(1, 'a', 'abc', 1.2, 2.1, '10/31/2008')";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                    cmd.Close();
                }
            }

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "select * from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    DbDataReader reader = cmd.ExecuteReader();

                    while (reader.Read()) //only one row will be available
                    {
                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "a         ");
                        Assert.IsTrue(reader.GetString(2) == "abc");
                        Assert.IsTrue(reader.GetFloat(3) == 1.2f);
                        Assert.IsTrue(reader.GetFloat(4) == (float)Convert.ToDouble(2.1));
                        Assert.IsTrue(reader.GetDateTime(5) == new DateTime(2008, 10, 31));
                    }

                    cmd.Close();
                }

                CleanupTestTable(conn);
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
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "create table t(idx integer)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                string[] sql_arr = new string[3];
                sql_arr[0] = "insert into t values(1)";
                sql_arr[1] = "insert into t values(2)";
                sql_arr[2] = "insert into t values(3)";
                CUBRIDBatchResult result = conn.BatchExecute(sql_arr);
                if (result != null)
                {
                    Assert.AreEqual(result.Count(), 3);
                    Assert.AreEqual(result.getErrorCodes().Length, 3);
                    Assert.AreEqual(result.getErrorFlag(), false);
                    Assert.AreEqual(result.getErrorMessages().Length, 3);
                    Assert.AreEqual(result.getResults().Length, 3);
                    Assert.AreEqual(result.getStatementTypes().Length, 3);
                }

                sql = "select count(*) from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt32(0) == 3);
                    }
                }

                sql = "drop table t;";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Test CUBRIDException class
        /// </summary>
        [TestMethod()]
        public void Test_CUBRIDException()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "select count(*) from xyz"; //Table xyz does not exist
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    try
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            reader.Read();
                        }
                    }
                    catch (Exception ex)
                    {
                        Assert.IsTrue(ex.Message == "Syntax: Unknown class \"xyz\". select count(*) from xyz");//todo
                    }
                }
            }

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "select count(*) from xyz"; //Table xyz does not exist
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    try
                    {
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            conn.Close();
                            reader.Read();
                        }
                    }
                    catch (Exception ex)
                    {
                        Assert.IsTrue(ex.Message == "Syntax: Unknown class \"xyz\". select count(*) from xyz"); //TODO
                    }
                }
            }
        }

        /// <summary>
        /// Test CUBRIDConnection BatchExecuteNoQuery() method
        /// </summary>
        [TestMethod()]
        public void Test_BatchExecuteNoQuery()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string[] sqls = new string[3];
                sqls[0] = "create table t(id int)";
                sqls[1] = "insert into t values(1)";
                sqls[2] = "insert into t values(2)";

                conn.BatchExecuteNoQuery(sqls);

                string sql = "select count(*) from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetInt32(0) == 2);
                    }
                }

                UnitTestCases.ExecuteSQL("drop table t", conn);
            }
        }

        /// <summary>
        /// Test CUBRIDDataReader GetOid() method
        /// </summary>
        [TestMethod()]
        public void Test_OID_Get()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "select * from nation limit 1";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        CUBRIDOid oid = reader.GetOid();
                        Assert.IsTrue(oid.ToString() == "OID:@0|0|0");
                    }
                }
            }
        }

        /// <summary>
        /// Test CUBRIDCommand GetGeneratedKeys() method
        /// </summary>
        [TestMethod()]
        public void Test_GetGeneratedKeys()
        {
            string sqlTablesCount = "select count(*) from db_class";
            int tablesCount, newTableCount;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists tkeys", conn);

                tablesCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                UnitTestCases.ExecuteSQL("create table tkeys(id int auto_increment, str string)", conn);
                newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                //Verify table was created
                Assert.IsTrue(newTableCount == tablesCount + 1);

                conn.BeginTransaction();
                using (CUBRIDCommand cmd = new CUBRIDCommand("insert into tkeys(str) values('xyz')", conn))
                {
                    cmd.ExecuteNonQuery();
                    DbDataReader keys = cmd.GetGeneratedKeys();

                    while (keys.Read())
                    {
                        //only on erow will be returned
                        Assert.IsTrue(keys.GetInt32(0) == 1);
                    }
                    conn.Commit();
                    cmd.Close();
                }

                UnitTestCases.ExecuteSQL("drop table if exists tkeys", conn);
                newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                Assert.IsTrue(newTableCount == tablesCount);
            }
        }


        /// <summary>
        /// Test ExecuteNonQuery() and ExecuteReader() methods
        /// </summary>
        [TestMethod()]
        public void Test_ExecuteNonQuery_Query()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "select count(*) from nation";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (CUBRIDException ex)
                    {
                        Assert.IsTrue(ex.Message == "Invalid query type!");
                    }
                }
            }

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string sql = "insert into nation values('x', 'x', 'x', 'x')";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    try
                    {
                        using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                        {
                            reader.Read();
                        }
                    }
                    catch (CUBRIDException ex)
                    {
                        Assert.IsTrue(ex.Message == "Invalid query type!");
                    }
                }
            }
        }

        /// <summary>
        /// Test CUBRIDOid class (which implements OID support)
        /// </summary>
        [TestMethod()] 
        public void Test_Oid_Basic()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                CUBRIDOid oid = new CUBRIDOid("@620|1|0");

                Assert.IsTrue(oid.Page() == 620);
                Assert.IsTrue(oid.Slot() == 1);
                Assert.IsTrue(oid.Volume() == 0);
            }
        }

        /// <summary>
        /// Test DateTime types
        /// </summary>
        [TestMethod()] 
        public void Test_DateTime_Types()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                CleanupTestTable(conn);
                UnitTestCases.ExecuteSQL("create table t(dt datetime)", conn);

                UnitTestCases.ExecuteSQL("insert into t values('10/31/2008 10:20:30.040')", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();

                    reader.Read();
                    Assert.IsTrue(reader.GetDateTime(0) == new DateTime(2008, 10, 31, 10, 20, 30, 040));
                    Assert.IsTrue(reader.GetDate(0) == "2008-10-31");
                    Assert.IsTrue(reader.GetDate(0, "yy-MM-dd") == "08-10-31");
                    Assert.IsTrue(reader.GetTime(0) == "10:20:30");
                    Assert.IsTrue(reader.GetTime(0, "HH") == "10");
                    Assert.IsTrue(reader.GetTimestamp(0) == "2008-10-31 10:20:30.040");
                    Assert.IsTrue(reader.GetTimestamp(0, "yyyy HH") == "2008 10");
                }

                CleanupTestTable(conn);
            }
        }

        /// <summary>
        /// Test read many rows in one SQL statement execution
        /// </summary>
        [TestMethod()] 
        public void Test_Read_ManyRows()
        {
            int curr_row = 0;
            int nCount = 0;
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                nCount = GetTableRowsCount("athlete", conn);

                string sql = "select * from athlete";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {

                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        curr_row++;
                    }
                    cmd.Close();
                }
            }

            Assert.AreEqual(curr_row, nCount);
        }

        [TestMethod()]
        public void Test_StringResources()
        {
            string str = CUBRID.Data.CUBRIDClient.CUBRIDException.GetStrTest();
            Assert.IsTrue(str == "Not implemented!");
        }

        /// <summary>
        /// Test CUBRID Isolation Levels
        /// </summary>
        [TestMethod()] 
        public void Test_IsolationLevel()
        {
            string sqlTablesCount = "select count(*) from db_class";
            int tablesCount, newTableCount;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists isol", conn);

                conn.SetIsolationLevel(CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);
                Assert.IsTrue(conn.GetIsolationLevel() == CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);

                tablesCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                UnitTestCases.ExecuteSQL("create table isol(id int)", conn);
                newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                //Verify table was created
                Assert.IsTrue(newTableCount == tablesCount + 1);

                using (CUBRIDConnection connOut = new CUBRIDConnection())
                {
                    connOut.ConnectionString = UnitTestCases.connString;
                    connOut.Open();

                    newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, connOut);
                    //CREATE TABLE is visible from another connection
                    Assert.IsTrue(newTableCount == tablesCount + 1);
                }

                UnitTestCases.ExecuteSQL("drop table if exists isol", conn);
            }
            /*
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                conn.SetIsolationLevel(CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE);
                Assert.IsTrue(conn.GetIsolationLevel() == CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE);

                tablesCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                conn.BeginTransaction(IsolationLevel.ReadCommitted);
                UnitTestCases.ExecuteSQL("create table isol(id int)", conn);
                newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                //Verify table was created
                Assert.IsTrue(newTableCount == tablesCount + 1);

                using (CUBRIDConnection connOut = new CUBRIDConnection())
                {
                    connOut.ConnectionString = UnitTestCases.connString;
                    connOut.Open();

                    newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, connOut);
                    //CREATE TABLE is NOT visible from another connection
                    Assert.IsTrue(newTableCount == tablesCount);
                }

                conn.Commit();

                newTableCount = (int)UnitTestCases.GetSingleValue(sqlTablesCount, conn);
                //Verify table was created
                Assert.IsTrue(newTableCount == tablesCount + 1);

                UnitTestCases.ExecuteSQL("drop table if exists isol", conn);
                Assert.IsTrue(newTableCount == tablesCount);
            }*/
        }

        /// <summary>
        /// Test CUBRIDConnection Auto-Commit property
        /// </summary>
        [TestMethod()] 
        public void Test_AutoCommit()
        {
            int tablesCount;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();
                UnitTestCases.ExecuteSQL("drop table if exists xyz", conn);

                conn.SetAutoCommit(false);

                tablesCount = (int)UnitTestCases.GetSingleValue("select count(*) from db_class", conn);

                //Create table
                UnitTestCases.ExecuteSQL("create table xyz(id int)", conn);
            }

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                //Verify table was not created
                Assert.IsTrue(tablesCount == (int)UnitTestCases.GetSingleValue("select count(*) from db_class", conn));

                //Create table
                UnitTestCases.ExecuteSQL("create table xyz(id int)", conn);
            }

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                //Verify table was created
                Assert.IsTrue(tablesCount == ((int)UnitTestCases.GetSingleValue("select count(*) from db_class", conn) - 1));

                UnitTestCases.ExecuteSQL("drop table if exists xyz", conn);
            }

        }

        /// <summary>
        /// Test CUBRID Connection properties
        /// </summary>
        [TestMethod()]
        public void Test_ConnectionProperties()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.SetEncoding("utf-8");
                conn.Open();

                Assert.IsTrue(conn.ConnectionTimeout == 30);
                Assert.IsTrue(conn.CurrentDatabase() == "demodb");
                Assert.IsTrue(conn.Database == "demodb");
                Assert.IsTrue(conn.DBVersion == "9.1.0.0087");
                Assert.IsTrue(conn.DataSource == "10.34.64.122");
                Assert.IsTrue(conn.AutoCommit == true);
                Assert.IsTrue(conn.LockTimeout == 30000);
                Assert.IsTrue(conn.ConnectionTimeout == 30);
                Assert.IsTrue(conn.IsolationLevel == CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);
                Assert.IsTrue(conn.ServerVersion == "");
                Assert.IsTrue(conn.State == ConnectionState.Open);
            }
        }

        /// <summary>
        /// Test the CUBRIDDataType.CCI_U_TYPE_NULL data type
        /// </summary>
        [TestMethod()] 
        public void Test_Null_WithParameters()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
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

                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_NULL);
                    p2.Value = null;
                    cmd.Parameters.Add(p2);

                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where id = 1", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.IsTrue(reader.GetValue(1) == null);
                    }
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("drop table t", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Test CUBRIDParameterCollection class
        /// </summary>
        [TestMethod()]
        public void Test_Parameters_Collection()
        {
            string errMsg;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                CUBRIDParameterCollection pcoll = new CUBRIDParameterCollection();

                CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                p1.Value = 1;
                pcoll.Add(p1);

                CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
                p2.Value = 'A';
                //pcoll.Add(p2);

                CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_NULL);
                p3.Value = null;
                pcoll.Add(p3);

                pcoll.Insert(1, p2);

                Assert.AreEqual(pcoll.IsFixedSize, false);
                Assert.AreEqual(pcoll.IsReadOnly, false);

                //Try to add again p1
                errMsg = "";
                try
                {
                    pcoll.Add(p1);
                    throw new Exception();
                }
                catch (Exception ex)
                {
                    errMsg = ex.Message;
                }
                Assert.IsTrue(errMsg == "Parameter already added to the collection!");

                Assert.IsTrue(pcoll.Count == 3);

                Assert.IsTrue(pcoll["?p1"].ParameterName == "?p1");
                Assert.IsTrue(pcoll[1].ParameterName == "?p2");

                Assert.IsTrue(pcoll["?p1"].DbType == DbType.Int32);
                Assert.IsTrue(pcoll[1].DbType == DbType.StringFixedLength);
                Assert.IsTrue(pcoll[2].DbType == DbType.Object);

                Assert.IsTrue((int)pcoll[0].Value == 1);
                Assert.IsTrue((char)pcoll[1].Value == 'A');
                Assert.IsTrue(pcoll[2].Value == null);

                //Try get non-existing parameter
                errMsg = "";
                try
                {
                    string str = pcoll["?p11"].ParameterName;
                }
                catch (Exception ex)
                {
                    errMsg = ex.Message;
                }
                Assert.IsTrue(errMsg == "?p11: Parameter not found in the collection!");

                //Try get non-existing parameter
                errMsg = "";
                int nErrCode = 0;
                try
                {
                    string str = pcoll[99].ParameterName;
                }
                catch (Exception ex)
                {
                    //errMsg = ex.Message;
                    nErrCode = Marshal.GetHRForException(ex);
                }
                Assert.AreEqual(nErrCode, -2146233080);
                //Assert.IsTrue(errMsg == "Index was outside the bounds of the array.");

                pcoll.RemoveAt(1);
                pcoll.Remove(p1);

                Assert.IsTrue(pcoll.Count == 1);

                pcoll.Clear();

                Assert.IsTrue(pcoll.Count == 0);
            }
        }

        /// <summary>
        /// Test CUBRID data types Get...()
        /// </summary>
        [TestMethod()] 
        public void Test_Various_DataTypes()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);

                string sql = "create table t(";
                sql += "c_integer_ai integer AUTO_INCREMENT, ";
                sql += "c_smallint smallint, ";
                sql += "c_integer integer, ";
                sql += "c_bigint bigint, ";
                sql += "c_numeric numeric(15,1), ";
                sql += "c_float float, ";
                sql += "c_decimal decimal(15,3), ";
                sql += "c_double double, ";
                sql += "c_char char(1), ";
                sql += "c_varchar character varying(4096), ";
                sql += "c_time time, ";
                sql += "c_date date, ";
                sql += "c_timestamp timestamp, ";
                sql += "c_datetime datetime, ";
                sql += "c_bit bit(1), ";
                sql += "c_varbit bit varying(4096), ";
                sql += "c_monetary monetary, ";
                sql += "c_string string";
                sql += ")";
                UnitTestCases.ExecuteSQL(sql, conn);

                sql = "insert into t values(";
                sql += "1, ";
                sql += "11, ";
                sql += "111, ";
                sql += "1111, ";
                sql += "1.1, ";
                sql += "1.11, ";
                sql += "1.111, ";
                sql += "1.1111, ";
                sql += "'a', ";
                sql += "'abcdfghijk', ";
                sql += "TIME '13:15:45 pm', ";
                sql += "DATE '00-10-31', ";
                sql += "TIMESTAMP '13:15:45 10/31/2008', ";
                sql += "DATETIME '13:15:45 10/31/2008', ";
                sql += "B'0', ";
                sql += "B'0', ";
                sql += "123456789, ";
                sql += "'qwerty'";
                sql += ")";

                UnitTestCases.ExecuteSQL(sql, conn);

                sql = "select * from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) //only one row will be available
                    {
                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetInt16(1) == 11);
                        Assert.IsTrue(reader.GetInt32(2) == 111);
                        Assert.IsTrue(reader.GetInt64(3) == 1111);
                        Assert.IsTrue(reader.GetDecimal(4) == (decimal)1.1);
                        Assert.IsTrue(reader.GetFloat(5) == (float)1.11);
                        Assert.IsTrue(reader.GetDecimal(6) == (decimal)1.111);
                        Assert.IsTrue(reader.GetDouble(7) == (double)1.1111);
                        Assert.IsTrue(reader.GetChar(8) == 'a');
                        Assert.IsTrue(reader.GetString(9) == "abcdfghijk");
                        Assert.IsTrue(reader.GetDateTime(10) == new DateTime(1, 1, 1, 13, 15, 45));
                        Assert.IsTrue(reader.GetDateTime(11) == new DateTime(2000, 10, 31));
                        Assert.IsTrue(reader.GetDateTime(12) == new DateTime(2008, 10, 31, 13, 15, 45));
                        Assert.IsTrue(reader.GetDateTime(13) == new DateTime(2008, 10, 31, 13, 15, 45));
                        Assert.IsTrue(reader.GetByte(14) == (byte)0);
                        Assert.IsTrue(reader.GetByte(15) == (byte)0);
                        Assert.IsTrue(reader.GetString(16) == "123456789");
                        Assert.IsTrue(reader.GetString(17) == "qwerty");
                    }
                }

                UnitTestCases.ExecuteSQL("drop table t", conn);
            }
        }

        /// <summary>
        /// Test CUBRID data types, using parameters
        /// </summary>
        [TestMethod()] 
        public void Test_Various_DataTypes_Parameters()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);

                string sql = "create table t(";
                sql += "c_integer_ai integer AUTO_INCREMENT, ";
                sql += "c_smallint smallint, ";
                sql += "c_integer integer, ";
                sql += "c_bigint bigint, ";
                sql += "c_numeric numeric(15,1), ";
                sql += "c_float float, ";
                sql += "c_decimal decimal(15,3), ";
                sql += "c_double double, ";
                sql += "c_char char(1), ";
                sql += "c_varchar character varying(4096), ";
                sql += "c_time time, ";
                sql += "c_date date, ";
                sql += "c_timestamp timestamp, ";
                sql += "c_datetime datetime, ";
                sql += "c_bit bit(8), ";
                sql += "c_varbit bit varying(4096), ";
                sql += "c_monetary monetary, ";
                sql += "c_string string, ";
                sql += "c_null string ";
                sql += ")";
                UnitTestCases.ExecuteSQL(sql, conn);

                sql = "insert into t values(";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "?, ";
                sql += "? ";
                sql += ")";

                using (CUBRIDCommand cmd_i = new CUBRIDCommand(sql, conn))
                {
                    //sql += "c_integer_ai integer AUTO_INCREMENT, ";
                    //sql += "1, ";
                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p1.Value = 1;
                    cmd_i.Parameters.Add(p1);
                    //sql += "c_smallint smallint, ";
                    //sql += "11, ";
                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_SHORT);
                    p2.Value = 11;
                    cmd_i.Parameters.Add(p2);
                    //sql += "c_integer integer, ";
                    //sql += "111, ";
                    CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_INT);
                    p3.Value = 111;
                    cmd_i.Parameters.Add(p3);
                    //sql += "c_bigint bigint, ";
                    //sql += "1111, ";
                    CUBRIDParameter p4 = new CUBRIDParameter("?p4", CUBRIDDataType.CCI_U_TYPE_BIGINT);
                    p4.Value = 1111;
                    cmd_i.Parameters.Add(p4);
                    //sql += "c_numeric numeric(15,0), ";
                    //sql += "1.1, ";
                    CUBRIDParameter p5 = new CUBRIDParameter("?p5", CUBRIDDataType.CCI_U_TYPE_NUMERIC);
                    p5.Value = 1.1;
                    cmd_i.Parameters.Add(p5);
                    //sql += "c_float float, ";
                    //sql += "1.11, ";
                    CUBRIDParameter p6 = new CUBRIDParameter("?p6", CUBRIDDataType.CCI_U_TYPE_FLOAT);
                    p6.Value = 1.11;
                    cmd_i.Parameters.Add(p6);
                    //sql += "c_decimal decimal, ";
                    //sql += "1.111, ";
                    CUBRIDParameter p7 = new CUBRIDParameter("?p7", CUBRIDDataType.CCI_U_TYPE_NUMERIC);
                    p7.Value = 1.111;
                    cmd_i.Parameters.Add(p7);
                    //sql += "c_double double, ";
                    //sql += "1.1111, ";
                    CUBRIDParameter p8 = new CUBRIDParameter("?p8", CUBRIDDataType.CCI_U_TYPE_DOUBLE);
                    p8.Value = 1.1111;
                    cmd_i.Parameters.Add(p8);
                    //sql += "c_char char(1), ";
                    //sql += "'a', ";
                    CUBRIDParameter p9 = new CUBRIDParameter("?p9", CUBRIDDataType.CCI_U_TYPE_CHAR);
                    p9.Size = 1;
                    p9.Value = 'a';
                    cmd_i.Parameters.Add(p9);
                    //sql += "c_varchar varchar(4096), ";
                    //sql += "'abcdfghijk', ";
                    CUBRIDParameter p10 = new CUBRIDParameter("?p10", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p10.Value = "abcdfghijk";//trebuie luat cap coada si vazut dc plm nu se trimite ok. S-ar putea sa fie de la n
                    cmd_i.Parameters.Add(p10);
                    //sql += "c_time time, ";
                    //sql += "TIME '13:15:45 pm', ";
                    CUBRIDParameter p11 = new CUBRIDParameter("?p11", CUBRIDDataType.CCI_U_TYPE_TIME);
                    p11.Value = new DateTime(2010, 1, 1, 13, 15, 45); //year/month/date is not relevant, only the time part is used
                    cmd_i.Parameters.Add(p11);
                    //sql += "c_date date, ";
                    //sql += "DATE '00-10-31', ";
                    CUBRIDParameter p12 = new CUBRIDParameter("?p12", CUBRIDDataType.CCI_U_TYPE_DATE);
                    p12.Value = new DateTime(2000, 10, 31);
                    cmd_i.Parameters.Add(p12);
                    //sql += "c_timestamp timestamp, ";
                    //sql += "TIMESTAMP '13:15:45 10/31/2008', ";
                    CUBRIDParameter p13 = new CUBRIDParameter("?p13", CUBRIDDataType.CCI_U_TYPE_TIMESTAMP);
                    p13.Value = new DateTime(2008, 10, 31, 13, 15, 45);
                    cmd_i.Parameters.Add(p13);
                    //sql += "c_datetime datetime, ";
                    //sql += "DATETIME '13:15:45 10/31/2008', ";
                    CUBRIDParameter p14 = new CUBRIDParameter("?p14", CUBRIDDataType.CCI_U_TYPE_DATETIME);
                    p14.Value = new DateTime(2008, 10, 31, 13, 15, 45);
                    cmd_i.Parameters.Add(p14);
                    //sql += "c_bit bit(1), ";
                    //sql += "B'1', ";
                    CUBRIDParameter p15 = new CUBRIDParameter("?p15", CUBRIDDataType.CCI_U_TYPE_BIT);
                    p15.Value = (byte)1;
                    cmd_i.Parameters.Add(p15);
                    //sql += "c_varbit bit varying(4096), ";
                    //sql += "B'1', ";
                    CUBRIDParameter p16 = new CUBRIDParameter("?p16", CUBRIDDataType.CCI_U_TYPE_VARBIT);
                    p16.Value = (byte)1;
                    cmd_i.Parameters.Add(p16);
                    //sql += "c_monetary monetary, ";
                    //sql += "123456789, ";
                    CUBRIDParameter p17 = new CUBRIDParameter("?p17", CUBRIDDataType.CCI_U_TYPE_MONETARY);
                    p17.Value = 123456789;
                    cmd_i.Parameters.Add(p17);
                    //sql += "c_string string ";
                    //sql += "'qwerty'";
                    CUBRIDParameter p18 = new CUBRIDParameter("?p18", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p18.Value = "qwerty";
                    cmd_i.Parameters.Add(p18);
                    //sql += "c_null string ";
                    //sql += "null";
                    CUBRIDParameter p19 = new CUBRIDParameter("?p19", CUBRIDDataType.CCI_U_TYPE_NULL);
                    p19.Value = null;
                    cmd_i.Parameters.Add(p19);

                    cmd_i.ExecuteNonQuery();

                    cmd_i.Close();

                    sql = "select * from t ";
                    sql += "where 1 = 1 ";
                    sql += "and c_integer_ai = ? ";
                    sql += "and c_smallint = ? ";
                    sql += "and c_integer = ? ";
                    sql += "and c_bigint = ? ";
                    sql += "and c_numeric = ? ";
                    sql += "and c_float = ? ";
                    sql += "and c_decimal = ? ";
                    sql += "and c_double = ? ";
                    sql += "and c_char = ? ";
                    sql += "and c_varchar = ? ";
                    sql += "and c_time = ? ";
                    sql += "and c_date = ? ";
                    sql += "and c_timestamp = ? ";
                    sql += "and c_datetime = ? ";
                    sql += "and c_bit = ? ";
                    sql += "and c_varbit = ? ";
                    sql += "and c_monetary = ? ";
                    sql += "and c_string = ? ";
                    sql += "and c_null IS NULL ";

                    using (CUBRIDCommand cmd_s = new CUBRIDCommand(sql, conn))
                    {
                        cmd_s.Parameters.Add(p1);
                        cmd_s.Parameters.Add(p2);
                        cmd_s.Parameters.Add(p3);
                        cmd_s.Parameters.Add(p4);
                        cmd_s.Parameters.Add(p5);
                        cmd_s.Parameters.Add(p6);
                        cmd_s.Parameters.Add(p7);
                        cmd_s.Parameters.Add(p8);
                        cmd_s.Parameters.Add(p9);
                        cmd_s.Parameters.Add(p10);
                        cmd_s.Parameters.Add(p11);
                        cmd_s.Parameters.Add(p12);
                        cmd_s.Parameters.Add(p13);
                        cmd_s.Parameters.Add(p14);
                        cmd_s.Parameters.Add(p15);
                        cmd_s.Parameters.Add(p16);
                        cmd_s.Parameters.Add(p17);
                        cmd_s.Parameters.Add(p18);
                        //cmd_s.Parameters.Add(p19);

                        DbDataReader reader = cmd_s.ExecuteReader();
                        while (reader.Read()) //only one row will be available
                        {
                            Assert.IsTrue(reader.GetInt32(0) == 1);
                            Assert.IsTrue(reader.GetInt16(1) == 11);
                            Assert.IsTrue(reader.GetInt32(2) == 111);
                            Assert.IsTrue(reader.GetInt64(3) == 1111);
                            Assert.IsTrue(reader.GetDecimal(4) == (decimal)1.1);
                            Assert.IsTrue(reader.GetFloat(5) == (float)1.11);
                            Assert.IsTrue(reader.GetDouble(6) == 1.111);
                            Assert.IsTrue(reader.GetDouble(7) == 1.1111);
                            Assert.IsTrue(reader.GetChar(8) == 'a');
                            Assert.IsTrue(reader.GetString(9) == "abcdfghijk");
                            Assert.IsTrue(reader.GetDateTime(10) == new DateTime(1, 1, 1, 13, 15, 45));
                            Assert.IsTrue(reader.GetDateTime(11) == new DateTime(2000, 10, 31));
                            Assert.IsTrue(reader.GetDateTime(12) == new DateTime(2008, 10, 31, 13, 15, 45));
                            Assert.IsTrue(reader.GetDateTime(13) == new DateTime(2008, 10, 31, 13, 15, 45, 00));
                            Assert.IsTrue(reader.GetByte(14) == 1);
                            Assert.IsTrue(reader.GetByte(15) == 1);
                            Assert.IsTrue(reader.GetString(16) == "123456789");
                            Assert.IsTrue(reader.GetString(17) == "qwerty");
                            Assert.IsTrue(reader.GetValue(18) == null);
                        }

                        cmd_s.Close();
                    }
                }
                UnitTestCases.ExecuteSQL("drop table t", conn);
            }
        }

        /// <summary>
        /// Test Table name from OID
        /// </summary>
        [TestMethod()] 
        public void Test_GetTableNameFromOid()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string tableName = conn.GetTableNameFromOID("@620|1|0");

                Assert.IsTrue(tableName == "record");
            }
        }

        /// <summary>
        /// Test CUBRIDConnection GetQueryPlanOnly() method
        /// </summary>
        [TestMethod()]
        public void Test_QueryPlanOnly()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                string queryPlan = conn.GetQueryPlanOnly("select * from athlete order by 1 desc");

                string expected = "Join graph segments (f indicates final):\nseg[0]: [0]\nseg[1]: code[0] (f)\nseg[2]: name[0] (f)\nseg[3]: gender[0] (f)\nseg[4]: nation_code[0] (f)\nseg[5]: event[0] (f)\nJoin graph nodes:\nnode[0]: athlete athlete(6677/27) (sargs 0)\nJoin graph terms:\nterm[0]: athlete.code range (-2147483648 ge_inf max) (sel 1) (rank 2) (sarg term) (not-join eligible) (indexable code[0]) (loc 0)\n\nQuery plan:\n\niscan\n    class: athlete node[0]\n    index: pk_athlete_code term[0] (desc_index)\n    sort:  1 asc\n    cost:  80 card 6677\n\nQuery stmt:\n\nselect athlete.code, athlete.[name], athlete.gender, athlete.nation_code, athlete.event from athlete athlete where (athlete.code>=-2147483648) order by 1 desc \n\n/* ---> skip ORDER BY */\n";
                Assert.AreEqual(queryPlan, expected);
            }
        }

        /// <summary>
        /// Test SEQUENCE operations
        /// </summary>
        [TestMethod()] 
        public void Test_SequenceOperations()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a sequence

                UnitTestCases.ExecuteSQL("CREATE TABLE t(seq SEQUENCE(int))", conn);
                //Insert some data in the sequence column
                UnitTestCases.ExecuteSQL("INSERT INTO t(seq) VALUES({0,1,2,3,4,5,6})", conn);
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
                int value = 7;

                int SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 7);

                conn.UpdateElementInSequence(oid, attributeName, 1, value);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 7);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 7, 1, 2, 3, 4, 5, 6 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.IsTrue(Convert.ToInt32(o[i]) == expected[i]);
                            }
                        }
                    }
                }

                conn.InsertElementInSequence(oid, attributeName, 5, value);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 8);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 7, 1, 2, 3, 7, 4, 5, 6 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.IsTrue(Convert.ToInt32(o[i]) == expected[i]);
                            }
                        }
                    }
                }

                conn.DropElementInSequence(oid, attributeName, 5);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 7);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 7, 1, 2, 3, 4, 5, 6 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.IsTrue(Convert.ToInt32(o[i]) == expected[i]);
                            }
                        }
                    }
                }

                UnitTestCases.ExecuteSQL("DROP t", conn);
            }
        }

        /// <summary>
        /// Test SET operations
        /// </summary>
        [TestMethod()] 
        public void Test_SetOperations()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                UnitTestCases.ExecuteSQL("CREATE TABLE t(s SET(int))", conn);
                //Insert some data in the sequence column
                UnitTestCases.ExecuteSQL("INSERT INTO t(s) VALUES({0,1,2,3,4,5,6})", conn);
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

                String attributeName = "s";
                object value = 7;

                int SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 7);

                conn.AddElementToSet(oid, attributeName, value);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 8);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 0, 1, 2, 3, 4, 5, 6, 7 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.IsTrue(Convert.ToInt32(o[i]) == expected[i]);
                            }
                        }
                    }
                }

                conn.DropElementInSet(oid, attributeName, 5);
                SeqSize = conn.GetCollectionSize(oid, attributeName);
                Assert.IsTrue(SeqSize == 7);

                using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int[] expected = { 0, 1, 2, 3, 4, 6, 7 };
                            object[] o = (object[])reader[0];
                            for (int i = 0; i < SeqSize; i++)
                            {
                                Assert.IsTrue(Convert.ToInt32(o[i]) == expected[i]);
                            }
                        }
                    }
                }

                UnitTestCases.ExecuteSQL("DROP t", conn);
            }
        }

        /// <summary>
        /// Test Encodings support
        /// </summary>
        [TestMethod()]
        public void Test_Encodings()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.SetEncoding("cp1252");
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);
                UnitTestCases.ExecuteSQL("create table t(a int, b varchar(100))", conn);

                String sql = "insert into t values(1 ,'¾Æ¹«°³')";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                sql = "select * from t where b = '¾Æ¹«°³'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "¾Æ¹«°³");
                    }
                }

                sql = "update t set b='¾Æ¹°³'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                sql = "select * from t where b = '¾Æ¹°³'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "¾Æ¹°³");
                    }
                }

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);
            }
        }

        /// <summary>
        /// Test Encodings support with parameters
        /// </summary>
        [TestMethod()] 
        public void Test_EncodingsWithParameters()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.SetEncoding("cp1252");
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);
                UnitTestCases.ExecuteSQL("create table t(a int, b varchar(100))", conn);

                String sql = "insert into t values(1 ,?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.SetParameterEncoding(Encoding.GetEncoding("Windows-1252"));
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                    param.Value = "¾Æ¹«°³";

                    cmd.Parameters.Add(param);
                    cmd.ExecuteNonQuery();

                    Assert.AreEqual(param.GetParameterEncoding().CodePage, 1252);
                    Assert.AreEqual(param.ToString(), "?");

                    param.IsNullable = false;
                    Assert.AreEqual(param.IsNullable, false);

                    param.ResetDbType();
                }

                sql = "select * from t where b = ?";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.SetParameterEncoding(Encoding.GetEncoding("Windows-1252"));
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                    param.Value = "¾Æ¹«°³";

                    cmd.Parameters.Add(param);
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "¾Æ¹«°³");
                    }

                    param.Scale = 6;
                    Assert.AreEqual(param.Scale, 6);
                }

                sql = "update t set b=?";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.SetParameterEncoding(Encoding.GetEncoding("Windows-1252"));
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                    param.Value = "¾Æ¹°³";

                    cmd.Parameters.Add(param);
                    cmd.ExecuteNonQuery();

                    CUBRIDCommand cmdClone = cmd.Clone();
                }

                sql = "select * from t where b = ?";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.SetParameterEncoding(Encoding.GetEncoding("Windows-1252"));
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                    param.Value = "¾Æ¹°³";

                    cmd.Parameters.Add(param);
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.IsTrue(reader.GetInt32(0) == 1);
                        Assert.IsTrue(reader.GetString(1) == "¾Æ¹°³");
                    }
                }

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);
            }
        }

        /// <summary>
        /// Test CREATE Database Stored Functions calls
        /// </summary>
        [TestMethod()] 
        public void Test_CreateFunction()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                try
                {
                    UnitTestCases.ExecuteSQL("drop function unit_hello", conn);
                }
                catch { }

                string sql = "CREATE FUNCTION unit_hello() RETURN string AS LANGUAGE JAVA NAME 'SpCubrid.HelloCubrid() return java.lang.String'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                sql = "? = CALL unit_hello()";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                
                    CUBRIDParameter p2 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(p2);
                    
                    cmd.ExecuteNonQuery();

                    Assert.IsTrue(cmd.Parameters[0].Value.ToString() == "Hello, Cubrid!!");
                }

                UnitTestCases.ExecuteSQL("drop function unit_hello", conn);
            }
        }

        /// <summary>
        /// Test CREATE Stored Procedures calls
        /// </summary>
        [TestMethod()]
        public void Test_CreateProcedure()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                try
                {
                    UnitTestCases.ExecuteSQL("DROP PROCEDURE UNIT_PHONE_INFO;", conn);
                }
                catch { }

                try
                {
                    UnitTestCases.ExecuteSQL("drop table if exists unit_phone", conn);
                }
                catch { }

                UnitTestCases.ExecuteSQL("CREATE TABLE unit_phone(NAME VARCHAR(32), phoneno VARCHAR(32));", conn);

                string sql = "create PROCEDURE UNIT_PHONE_INFO(name varchar, phoneno varchar)" +
                             "as language java name 'SpCubrid.AddPhone(java.lang.String,java.lang.String)';";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                sql = "CALL UNIT_PHONE_INFO(?, ?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p1.Direction = ParameterDirection.Input;
                    p1.Value = "NHN China";
                    cmd.Parameters.Add(p1);

                    CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
                    p2.Direction = ParameterDirection.Input;
                    p2.Value = "4000315315";
                    cmd.Parameters.Add(p2);

                    cmd.ExecuteNonQuery();
                }

                Assert.AreEqual(UnitTestCases.GetTableRowsCount("unit_phone", conn), 1);
                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from unit_phone", conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetValue(0).ToString(), "NHN China");
                        Assert.AreEqual(reader.GetValue(1).ToString(), "4000315315");
                    }
                }

                UnitTestCases.ExecuteSQL("DROP PROCEDURE UNIT_PHONE_INFO", conn);

                UnitTestCases.ExecuteSQL("drop unit_phone;", conn);
            }
        }

        [TestMethod()]
        public void Test_SetAutoCommit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = UnitTestCases.connString;
                conn.Open();

                UnitTestCases.ExecuteSQL("drop table if exists t", conn);

                conn.SetAutoCommit(true);

                string sql = "create table t(idx integer)";
                using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
                {
                    command.ExecuteNonQuery();                    
                }

                int tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                conn.Rollback();

                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                UnitTestCases.ExecuteSQL("drop table t", conn);

                tablesCount = GetTablesCount("t", conn);
                Assert.IsTrue(tablesCount == 0);

                conn.SetAutoCommit(false);
                using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
                {
                    command.ExecuteNonQuery();
                }

                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 1);

                conn.Rollback();

                tablesCount = GetTablesCount("t", conn);
                Assert.AreEqual(tablesCount, 0);
            }
        }
    }
}