using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Data;
using System.Data.Common;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDCommandTest and is intended
    ///to contain all CUBRIDCommandTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDCommandTest
    {
        private static readonly string connString = "server=10.34.64.122;database=demodb;port=33590;user=public;password=";

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

        private static void CreateTestTableLOB(CUBRIDConnection conn)
        {
            CUBRIDCommandTest.ExecuteSQL("drop table if exists t", conn);
            CUBRIDCommandTest.ExecuteSQL("create table t(b BLOB, c CLOB)", conn);
        }

        private static void CleanupTestTableLOB(CUBRIDConnection conn)
        {
            CUBRIDCommandTest.ExecuteSQL("drop table if exists t", conn);
        }

        private static void ExecuteSQL(string sql, CUBRIDConnection conn)
        {
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        ///A test for CUBRIDCommand Constructor
        ///</summary>
        [TestMethod()]
        public void CUBRIDCommandConstructorTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                String sql = "select * from nation";
                CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                {
                    da.SelectCommand = cmd;
                    DataTable dt = new DataTable("");
                    da.FillSchema(dt, SchemaType.Source);//To retrieve all the column properties you have to use the FillSchema() method

                    Assert.IsTrue(cmd.ColumnInfos[0].Name == "code");
                    Assert.IsTrue(cmd.ColumnInfos[0].IsPrimaryKey == true);
                    Assert.IsTrue(cmd.ColumnInfos[0].IsForeignKey == false);
                    Assert.IsTrue(cmd.ColumnInfos[0].IsNullable == false);
                    Assert.IsTrue(cmd.ColumnInfos[0].RealName == "");
                    Assert.IsTrue(cmd.ColumnInfos[0].Precision == 3);
                    Assert.IsTrue(cmd.ColumnInfos[0].Scale == 0);
                    Assert.IsTrue(cmd.ColumnInfos[0].IsAutoIncrement == false);
                    Assert.IsTrue(cmd.ColumnInfos[0].IsReverseIndex == false);
                    Assert.IsTrue(cmd.ColumnInfos[0].IsReverseUnique == false);
                    Assert.IsTrue(cmd.ColumnInfos[0].IsShared == false);
                    Assert.IsTrue(cmd.ColumnInfos[0].Type == CUBRIDDataType.CCI_U_TYPE_CHAR);
                    Assert.IsTrue(cmd.ColumnInfos[0].Table == "nation");
                }
            }
        }

        /// <summary>
        ///A test for CUBRIDCommand Constructor
        ///</summary>
        [TestMethod()]
        public void CUBRIDCommand_Clone_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                String sql = "select * from nation";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDCommand cmdClone = cmd.Clone();
                    {
                        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                        {
                            da.SelectCommand = cmdClone;
                            DataTable dt = new DataTable("");
                            da.FillSchema(dt, SchemaType.Source);//To retrieve all the column properties you have to use the FillSchema() method

                            Assert.IsTrue(cmdClone.ColumnInfos[0].Name == "code");
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsPrimaryKey == true);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsForeignKey == false);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsNullable == false);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].RealName == "");
                            Assert.IsTrue(cmdClone.ColumnInfos[0].Precision == 3);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].Scale == 0);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsAutoIncrement == false);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsReverseIndex == false);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsReverseUnique == false);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].IsShared == false);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].Type == CUBRIDDataType.CCI_U_TYPE_CHAR);
                            Assert.IsTrue(cmdClone.ColumnInfos[0].Table == "nation");
                        }
                    }
                }
            }
        }

        //http://msdn.microsoft.com/en-us/library/tf579hcz%28v=vs.80%29.aspx
        /// <summary>
        /// Test CUBRIDCommandBuilder class, and methods used to automatically get SQL commands
        /// </summary>
        [TestMethod()]
        public void Test_CommandBuilder_GetCommands()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn);

                using (CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(da))
                {
                    da.UpdateCommand = cmdBuilder.GetUpdateCommand();
                    Assert.AreEqual(da.UpdateCommand.CommandText, "UPDATE `nation` SET `code` = ?, `name` = ?, `continent` = ?, `capital` = ? WHERE ((`code` = ?) AND (`name` = ?) AND ((? = 1 AND `continent` IS NULL) OR (`continent` = ?)) AND ((? = 1 AND `capital` IS NULL) OR (`capital` = ?)))");

                    da.InsertCommand = cmdBuilder.GetInsertCommand();
                    Assert.AreEqual(da.InsertCommand.CommandText, "INSERT INTO `nation` (`code`, `name`, `continent`, `capital`) VALUES (?, ?, ?, ?)");

                    da.DeleteCommand = cmdBuilder.GetDeleteCommand();
                    Assert.AreEqual(da.DeleteCommand.CommandText, "DELETE FROM `nation` WHERE ((`code` = ?) AND (`name` = ?) AND ((? = 1 AND `continent` IS NULL) OR (`continent` = ?)) AND ((? = 1 AND `capital` IS NULL) OR (`capital` = ?)))");
                }
            }
        }

        /// <summary>
        /// Test BLOB INSERT
        /// </summary>
        [TestMethod()]
        public void Test_Blob_Insert()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);

                    byte[] bytes = new byte[256];
                    bytes[0] = 69;
                    bytes[1] = 98;
                    bytes[2] = 99;
                    bytes[255] = 122;

                    Blob.setBytes(1, bytes);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = Blob;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.ExecuteNonQuery();
                    cmd.Close();

                    string sql2 = "SELECT b from t";
                    using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                    {
                        DbDataReader reader = cmd2.ExecuteReader();
                        while (reader.Read())
                        {
                            CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
                            byte[] bytes2 = new byte[(int)bImage.BlobLength];
                            bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

                            Assert.IsTrue(bytes.Length == bytes2.Length, "The inserted BLOB length is not valid!");
                            bool ok = true;
                            for (int i = 0; i < bytes.Length; i++)
                            {
                                if (bytes[i] != bytes2[i])
                                    ok = false;
                            }

                            Assert.IsTrue(ok == true, "The BLOB was not inserted correctly!");
                        }

                        cmd2.Close();
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        ///<summary>
        /// Test BLOB SELECT
        /// </summary>
        [TestMethod()]
        public void Test_Blob_Select()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);

                    byte[] bytes1 = new byte[256];
                    bytes1[0] = 69;
                    bytes1[1] = 98;
                    bytes1[2] = 99;
                    bytes1[255] = 122;

                    Blob.setBytes(1, bytes1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    //param.DataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob;
                    cmd1.Parameters.Add(param);
                    cmd1.Parameters[0].DbType = DbType.Binary;
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "SELECT b from t";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
                            byte[] bytes = new byte[(int)bImage.BlobLength];
                            bytes = bImage.getBytes(1, (int)bImage.BlobLength);

                            Assert.IsTrue(bytes1.Length == bytes.Length, "The selected BLOB length is not valid!");
                            bool ok = true;
                            for (int i = 0; i < bytes.Length; i++)
                            {
                                if (bytes1[i] != bytes[i])
                                    ok = false;
                            }

                            Assert.IsTrue(ok == true, "The BLOB was not selected correctly!");
                        }

                        cmd.Close();
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test CLOB INSERT
        /// </summary>
        [TestMethod()]
        public void Test_Clob_Insert()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql = "insert into t (c) values(?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDClob Clob = new CUBRIDClob(conn);
                    String str = conn.ConnectionString; //Use ConnectionString content for testing
                    Clob.setString(1, str);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                    param.Value = Clob;
                    cmd.Parameters.Add(param);
                    cmd.ExecuteNonQuery();
                    cmd.Close();

                    string sql2 = "SELECT c from t";
                    using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                    {
                        DbDataReader reader = cmd2.ExecuteReader();

                        while (reader.Read())
                        {
                            CUBRIDClob cImage = (CUBRIDClob)reader[0];
                            string str2 = cImage.getString(1, (int)cImage.ClobLength);

                            Assert.IsTrue(str.Length == str2.Length, "The inserted CLOB length is not valid!");
                            Assert.IsTrue(str.Equals(str2), "The CLOB was not inserted correctly!");
                        }

                        cmd2.Close();
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test CLOB SELECT
        /// </summary>
        [TestMethod()]
        public void Test_Clob_Select()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (c) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDClob Clob1 = new CUBRIDClob(conn);
                    String str1 = conn.ConnectionString; //Use ConnectionString content for testing
                    Clob1.setString(1, str1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                    param.Value = Clob1;
                    cmd1.Parameters.Add(param);
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "SELECT c from t";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        DbDataReader reader = cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            CUBRIDClob cImage = (CUBRIDClob)reader[0];
                            string str = cImage.getString(1, (int)cImage.ClobLength);

                            Assert.IsTrue(str.Length == str1.Length, "The selected CLOB length is not valid!");
                            Assert.IsTrue(str.Equals(str1), "The CLOB was not selected correctly!");
                        }

                        cmd.Close();
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB SELECT, using CUBRIDDataAdapter and DataTable
        /// </summary>
        [TestMethod()]
        public void Test_Blob_SelectDataAdapter()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

                    byte[] bytes1 = new byte[256];
                    bytes1[0] = 69;
                    bytes1[1] = 98;
                    bytes1[2] = 99;
                    bytes1[255] = 122;

                    Blob1.setBytes(1, bytes1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob1;
                    cmd1.Parameters.Add(param);
                    cmd1.Parameters[0].DbType = DbType.Binary;
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "SELECT b from t";
                    DataTable dt = new DataTable("t");
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                    {
                        da.SelectCommand = new CUBRIDCommand(sql, conn);
                        da.Fill(dt);

                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            CUBRIDBlob bImage = (CUBRIDBlob)dt.Rows[j]["b"];
                            byte[] bytes = new byte[(int)bImage.BlobLength];
                            bytes = bImage.getBytes(1, (int)bImage.BlobLength);

                            Assert.IsTrue(bytes1.Length == bytes.Length, "The selected length is not valid!");
                            bool ok = true;
                            for (int i = 0; i < bytes.Length; i++)
                            {
                                if (bytes1[i] != bytes[i])
                                    ok = false;
                            }

                            Assert.IsTrue(ok == true, "The BLOB was not selected correctly!");
                        }
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB SELECT, using CUBRIDDataAdapter and DataSet
        /// </summary>
        [TestMethod()]
        public void Test_Blob_SelectDataAdapter2()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

                    byte[] bytes1 = new byte[256];
                    bytes1[0] = 69;
                    bytes1[1] = 98;
                    bytes1[2] = 99;
                    bytes1[255] = 122;

                    Blob1.setBytes(1, bytes1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob1;
                    cmd1.Parameters.Add(param);
                    cmd1.Parameters[0].DbType = DbType.Binary;
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "SELECT b from t";

                    DataSet ds = new DataSet("t");
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                    {
                        da.SelectCommand = new CUBRIDCommand(sql, conn);
                        da.Fill(ds);

                        DataTable dt = ds.Tables[0];
                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            CUBRIDBlob bImage = (CUBRIDBlob)dt.Rows[j]["b"];
                            byte[] bytes = new byte[(int)bImage.BlobLength];
                            bytes = bImage.getBytes(1, (int)bImage.BlobLength);

                            Assert.IsTrue(bytes1.Length == bytes.Length, "The selected BLOB length is not valid!");
                            bool ok = true;
                            for (int i = 0; i < bytes.Length; i++)
                            {
                                if (bytes1[i] != bytes[i])
                                    ok = false;
                            }

                            Assert.IsTrue(ok == true, "The BLOB was not selected correctly!");
                        }
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test CLOB SELECT, using CUBRIDDataAdapter and DataTable
        /// </summary>
        [TestMethod()]
        public void Test_Clob_SelectDataAdapter()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (c) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDClob Clob1 = new CUBRIDClob(conn);

                    String str1 = conn.ConnectionString; //Use ConnectionString content for testing
                    Clob1.setString(1, str1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                    param.Value = Clob1;
                    cmd1.Parameters.Add(param);
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "SELECT c from t";

                    DataTable dt = new DataTable("t");
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                    {
                        da.SelectCommand = new CUBRIDCommand(sql, conn);
                        da.Fill(dt);

                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            CUBRIDClob cImage = (CUBRIDClob)dt.Rows[j]["c"];
                            string str = cImage.getString(1, (int)cImage.ClobLength);

                            Assert.IsTrue(str.Length == str1.Length, "The selected CLOB length is not valid!");
                            Assert.IsTrue(str.Equals(str1), "The CLOB was not selected correctly!");
                        }
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test CLOB SELECT, using CUBRIDDataAdapter and DataSet
        /// </summary>
        [TestMethod()]
        public void Test_Clob_SelectDataAdapter2()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (c) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDClob Clob1 = new CUBRIDClob(conn);

                    String str1 = conn.ConnectionString; //Use ConnectionString content for testing
                    Clob1.setString(1, str1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                    param.Value = Clob1;
                    cmd1.Parameters.Add(param);
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "SELECT c from t";

                    DataSet ds = new DataSet();
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                    {
                        da.SelectCommand = new CUBRIDCommand(sql, conn);
                        da.Fill(ds);

                        DataTable dt = ds.Tables[0];
                        for (int j = 0; j < dt.Rows.Count; j++)
                        {
                            CUBRIDClob cImage = (CUBRIDClob)dt.Rows[j]["c"];
                            string str = cImage.getString(1, (int)cImage.ClobLength);

                            Assert.IsTrue(str.Length == str1.Length, "The selected CLOB length is not valid!");
                            Assert.IsTrue(str.Equals(str1), "The CLOB was not selected correctly!");
                        }
                    }
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB UPDATE
        /// </summary>
        [TestMethod()]
        public void Test_Blob_Update()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

                    byte[] bytes1 = new byte[256];
                    bytes1[0] = 69;
                    bytes1[1] = 98;
                    bytes1[2] = 99;
                    bytes1[255] = 122;

                    Blob1.setBytes(1, bytes1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob1;
                    cmd1.Parameters.Add(param);
                    cmd1.Parameters[0].DbType = DbType.Binary;
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "UPDATE t SET b = (?)";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        CUBRIDBlob Blob = new CUBRIDBlob(conn);
                        byte[] bytes = new byte[256];
                        bytes[0] = 0;
                        bytes[1] = 1;
                        bytes[2] = 2;
                        bytes[255] = 255;

                        Blob.setBytes(1, bytes);
                        CUBRIDParameter param2 = new CUBRIDParameter();
                        param2.ParameterName = "?";
                        param2.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                        param2.Value = Blob;
                        cmd.Parameters.Add(param2);
                        cmd.Parameters[0].DbType = DbType.Binary;
                        cmd.ExecuteNonQuery();
                        cmd.Close();

                        String sql2 = "SELECT b from t";
                        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                        {
                            DbDataReader reader = cmd2.ExecuteReader();
                            while (reader.Read())
                            {
                                CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
                                byte[] bytes2 = new byte[(int)bImage.BlobLength];
                                bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

                                Assert.IsTrue(bytes2.Length == bytes.Length, "The updated BLOB length is not valid!");

                                bool ok = true;
                                for (int i = 0; i < bytes.Length; i++)
                                {
                                    if (bytes2[i] != bytes[i])
                                        ok = false;
                                }

                                Assert.IsTrue(ok == true, "The BLOB was not updated correctly!");
                            }
                            cmd2.Close();
                        }
                    }
                }
                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test CLOB UPDATE
        /// </summary>
        [TestMethod()]
        public void Test_Clob_Update()
        {
            String str;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (c) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDClob Clob1 = new CUBRIDClob(conn);

                    String str1 = conn.ConnectionString; //Use ConnectionString content for testing
                    Clob1.setString(1, "test string to be inserted");

                    CUBRIDParameter param1 = new CUBRIDParameter();
                    param1.ParameterName = "?";
                    param1.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                    param1.Value = Clob1;
                    cmd1.Parameters.Add(param1);
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    string sql = "UPDATE t SET c = ?";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        CUBRIDClob Clob = new CUBRIDClob(conn);
                        str = conn.ConnectionString; //Use the ConnectionString for testing

                        Clob.setString(1, str);
                        CUBRIDParameter param = new CUBRIDParameter();
                        param.ParameterName = "?";
                        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                        param.Value = Clob;
                        cmd.Parameters.Add(param);
                        cmd.ExecuteNonQuery();
                    }
                }

                string sql2 = "SELECT c from t";
                using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                {
                    DbDataReader reader = cmd2.ExecuteReader();
                    while (reader.Read())
                    {
                        CUBRIDClob cImage = (CUBRIDClob)reader[0];
                        string str2 = cImage.getString(1, (int)cImage.ClobLength);

                        Assert.IsTrue(str.Length == str2.Length, "The selected CLOB length is not valid!");
                        Assert.IsTrue(str.Equals(str2), "The CLOB was not selected correctly!");
                    }
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB INSERT, using a jpg image input file
        /// </summary>
        [TestMethod()]
        public void Test_Blob_FromFile()
        {
            BinaryReader b;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);
                    byte[] bytes;
                    b = new BinaryReader(File.Open("..\\..\\..\\TestData\\1.jpg", FileMode.Open));
                    int length = (int)b.BaseStream.Length;
                    bytes = b.ReadBytes(length);

                    Blob.setBytes(1, bytes);
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.ExecuteNonQuery();
                }

                string sql2 = "SELECT b from t";
                using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                {
                    DbDataReader reader = cmd2.ExecuteReader();
                    while (reader.Read())
                    {
                        CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
                        byte[] bytes2 = new byte[(int)bImage.BlobLength];
                        bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

                        FileStream stream = new FileStream("1out.jpg", FileMode.Create);
                        BinaryWriter writer = new BinaryWriter(stream);
                        writer.Write(bytes2);
                        writer.Close();

                        BinaryReader b2 = new BinaryReader(File.Open("1out.jpg", FileMode.Open));
                        Assert.IsTrue(b2.BaseStream.Length == b.BaseStream.Length, "The inserted BLOB length is not valid!");
                        bool ok = true;
                        int file1byte, file2byte;
                        b.BaseStream.Position = 0;

                        do
                        {
                            file1byte = b.BaseStream.ReadByte();
                            file2byte = b2.BaseStream.ReadByte();
                            if (file1byte != file2byte)
                                ok = false;
                        }
                        while (file1byte != -1);

                        Assert.IsTrue(ok == true, "The BLOB was not inserted correctly!");

                        b.Close();
                        b2.Close();
                    }
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test CLOB INSERT, using a txt input file
        /// </summary>
        [TestMethod()]
        public void Test_Clob_FromFile()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql = "insert into t (c) values(?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDClob Clob = new CUBRIDClob(conn);
                    String str = conn.ConnectionString;

                    using (StreamReader r = new StreamReader("..\\..\\..\\TestData\\test.txt"))
                    {
                        string writestring = r.ReadToEnd();
                        r.Close();

                        Clob.setString(1, writestring);

                        CUBRIDParameter param = new CUBRIDParameter();
                        param.ParameterName = "?";
                        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                        param.Value = Clob;
                        cmd.Parameters.Add(param);
                        cmd.ExecuteNonQuery();
                        cmd.Close();

                        string sql2 = "SELECT c from t";
                        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                        {
                            DbDataReader reader = cmd2.ExecuteReader();

                            while (reader.Read())
                            {
                                CUBRIDClob cImage = (CUBRIDClob)reader[0];
                                string str2 = cImage.getString(1, (int)cImage.ClobLength);

                                using (StreamWriter w = new StreamWriter("testout.txt"))
                                {
                                    w.Write(str2);
                                    w.Close();
                                }

                                using (StreamReader r2 = new StreamReader("testout.txt"))
                                {
                                    string readstring = r2.ReadToEnd();
                                    r2.Close();
                                    Assert.IsTrue(writestring.Length == readstring.Length, "The inserted CLOB length is not valid!");
                                    Assert.IsTrue(writestring.Equals(readstring), "The CLOB was not inserted correctly!");
                                }
                            }
                        }
                    }
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB INSERT in a transaction
        /// </summary>
        [TestMethod()]
        public void Test_Blob_InsertTransaction()
        {
            DbTransaction tran = null;
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
                string sql = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);

                    byte[] bytes = new byte[256];
                    bytes[0] = 69;
                    bytes[1] = 98;
                    bytes[2] = 99;
                    bytes[255] = 122;

                    Blob.setBytes(1, bytes);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.ExecuteNonQuery();
                }

                tran.Rollback();
            }

            //We have to close and reopen connection. Otherwise we get an invalid buffer position.
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();
                string sql2 = "SELECT b from t";
                using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                {
                    DbDataReader reader = cmd2.ExecuteReader();
                    Assert.IsTrue(reader.HasRows == false, "Transaction did not rollback!");
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB UPDATE in a transaction
        /// </summary>
        [TestMethod()]
        public void Test_Blob_UpdateTransaction()
        {
            DbTransaction tran = null;
            byte[] bytes1 = new byte[256];

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

                    bytes1[0] = 69;
                    bytes1[1] = 98;
                    bytes1[2] = 99;
                    bytes1[255] = 122;

                    Blob1.setBytes(1, bytes1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob1;
                    cmd1.Parameters.Add(param);
                    cmd1.Parameters[0].DbType = DbType.Binary;
                    cmd1.ExecuteNonQuery();
                }

                tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
                string sql = "UPDATE t SET b = (?)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);
                    byte[] bytes = new byte[256];
                    bytes[0] = 0;
                    bytes[1] = 1;
                    bytes[2] = 2;
                    bytes[255] = 255;

                    Blob.setBytes(1, bytes);
                    CUBRIDParameter param2 = new CUBRIDParameter();
                    param2.ParameterName = "?";
                    param2.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param2.Value = Blob;
                    cmd.Parameters.Add(param2);
                    cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.ExecuteNonQuery();
                }

                tran.Rollback();
            }

            //We have to close and reopen connection. Otherwise we get an invalid buffer position.
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                String sql2 = "SELECT b from t";
                using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                {
                    DbDataReader reader = cmd2.ExecuteReader();
                    while (reader.Read())
                    {
                        CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
                        byte[] bytes2 = new byte[(int)bImage.BlobLength];
                        bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

                        Assert.IsTrue(bytes2.Length == bytes1.Length);

                        bool ok = true;
                        for (int i = 0; i < bytes1.Length; i++)
                        {
                            if (bytes2[i] != bytes1[i])
                                ok = false;
                        }

                        Assert.IsTrue(ok == true);
                    }
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        /// Test BLOB DELETE in a transaction
        /// </summary>
        [TestMethod()]
        public void Test_Blob_DeleteTransaction()
        {
            DbTransaction tran = null;
            byte[] bytes1 = new byte[256];

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                CreateTestTableLOB(conn);

                string sql1 = "insert into t (b) values(?)";
                using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);

                    bytes1[0] = 69;
                    bytes1[1] = 98;
                    bytes1[2] = 99;
                    bytes1[255] = 122;

                    Blob.setBytes(1, bytes1);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                    param.Value = Blob;
                    cmd1.Parameters.Add(param);
                    cmd1.Parameters[0].DbType = DbType.Binary;
                    cmd1.ExecuteNonQuery();
                    cmd1.Close();

                    using (CUBRIDCommand cmd3 = new CUBRIDCommand("Select * from t", conn))
                    {
                        using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd3.ExecuteReader())
                        {
                            reader.Read();

                            Assert.AreEqual(reader.GetColumnType(0), typeof(CUBRIDBlob));
                            Assert.AreEqual(reader.GetColumnType(1), typeof(CUBRIDClob));

                            //byte[] bits = new byte[4];
                            //long len = reader.GetBytes(0, 0, bits, 0, 4);
                            Assert.AreEqual(reader.GetColumnTypeName(0), "BLOB");
                            Assert.AreEqual(reader.GetColumnTypeName(1), "CLOB");

                        }
                    }

                    tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
                    string sql2 = "DELETE from t";
                    using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
                    {
                        cmd2.ExecuteNonQuery();
                    }
                }

                tran.Rollback();
            }

            //We have to close and reopen connection. Otherwise we get an invalid buffer position.
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                string sql = "SELECT b from t";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    DbDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        Assert.IsTrue(reader.HasRows == true);

                        CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
                        byte[] bytes = new byte[(int)bImage.BlobLength];
                        bytes = bImage.getBytes(1, (int)bImage.BlobLength);

                        Assert.IsTrue(bytes1.Length == bytes.Length);

                        bool ok = true;
                        for (int i = 0; i < bytes.Length; i++)
                        {
                            if (bytes1[i] != bytes[i])
                                ok = false;
                        }

                        Assert.IsTrue(ok == true, "The BLOB DELETE command was not rolled-back correctly!");
                    }
                }

                CleanupTestTableLOB(conn);
            }
        }

        /// <summary>
        ///A test for CommandText
        ///</summary>
        [TestMethod()]
        public void CommandTextTest()
        {
            using (CUBRIDCommand target = new CUBRIDCommand())
            {
                target.CommandType = CommandType.TableDirect;

                target.CommandText = "nation";

                Assert.AreEqual(target.CommandText, "nation");
            }
        }

        /// <summary>
        ///A test for ExecuteNonQuery
        ///</summary>
        [TestMethod()]
        public void ExecuteNonQueryTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop function hello", conn);
                }
                catch { }

                string sql = "CREATE FUNCTION hello(a int) RETURN int AS LANGUAGE JAVA NAME 'SpCubrid.SpInt(int) return java.lang.Integer;'";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                sql = "? = CALL hello(10)";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    //CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    //p1.Value = 10;
                    //cmd.Parameters.Add(p1);

                    CUBRIDParameter p2 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
                    p2.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(p2);

                    cmd.ExecuteNonQuery();

                    Assert.AreEqual(p2.Value, 11);
                    //Assert.IsTrue(cmd.Parameters[0].Value == "Hello, Cubrid !!");
                }

                CUBRIDCommandTest.ExecuteSQL("drop function hello", conn);
            }
        }

        [TestMethod()]
        public void Test_MultiCommand()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                string[] sqls ={ "drop table if exists table11;", 
                                "create table table11(c1 string , c2 string); ", 
                                "insert into table11(c1, c2) values('阿喔呃', 'this is test');",
                                "insert into table11(c2, c1) values('123', 'abc');",
                                "insert into table11(c2, c1) value(';create table table11(c1 string , c2 string);', '''Multi Line Command''');"};


                conn.BatchExecuteNoQuery(sqls);

                string sql = "select count(*) from table11";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetInt32(0), 3);
                    }
                }

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table11;", conn);
                }
                catch { }
            }
        }

        /*
        */
        [TestMethod()]
        public void Test_MultiValues()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table if exists table11;", conn);
                }
                catch { }

                /* create new table */
                string sql = "create table table11(a string , b string, c string);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                /* insert multi rows values */
                sql = "insert into table11 (a, b, c) values ('1', '2','3'),('a', 'b','c'),('!', '@', '#');";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                /* verify count */
                sql = "select count(*) from table11";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetInt32(0), 3);
                    }
                }

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table11;", conn);
                }
                catch { }
            }
        }

        /*
         */
        [TestMethod()]
        public void Test_DataType_Enum()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table if exists table11;", conn);
                }
                catch { }

                /* create new table */
                string sql = "create table table11(city enum('BeiJing', 'ChengDu', 'QingDao', 'DaLian'), nationality enum('China', 'Korea', 'Japan'));";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                /* insert multi rows values */
                sql = "insert into table11 (city, nationality) values ('BeiJing', 'Japan'),('ChengDu','China'),('QingDao', 'Korea');";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                /* verify count */
                sql = "select count(*) from table11";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetInt32(0), 3);
                    }
                }

                sql = "select * from table11";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetString(0), "BeiJing");
                    }
                }

                try
                {
                    /* 
                     Only thrown exception is the correct result
                     */
                    sql = "insert into table11 (city, nationality) values ('Peking', 'China');";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception exp)
                {
                    Assert.AreEqual(exp.Message, "Semantic: before ' , 'China');'\nCannot coerce 'Peking' to type enum. insert into table11 (city, nationality) values ( cast('Pekin...");
                }

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table11;", conn);
                }
                catch { }
            }
        }

        /*
         * Using wrong data
         */
        [TestMethod()]
        public void Test_WithWrongEnumData()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table if exists table11;", conn);
                }
                catch { }

                try
                {
                    /* create new table */
                    string sql = "create table table11(index enum(1, 2, 3, 4, 5, 6));";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception exp)
                {
                    Assert.AreEqual(exp.Message, "Syntax: In line 1, column 28 before '(1, 2, 3, 4, 5, 6));'\nSyntax error: unexpected 'enum' ");
                }
            }
        }

        [TestMethod()]
        public void Test_Big_Data()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDCommandTest.connString;
                conn.Open();

                string _create = "create table TBL_RAW_POWER(PDMU_ID int, CHANNEL_NUM int, REG_DATE datetime, AMPERE int," +
                                "ACTIVE_POWER int, POWER_ACT int, APPARENT_POWER int, REACTIVE_POWER int, POWER_REA int," +
                                "SYSTEM_STATUS int, FREQUENCY int, POWER_FACTOR int, POWER_STATUS int, VOLTAGE int);";

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop table if exists TBL_RAW_POWER;", conn);
                }
                catch { }

                /* create new table */
                using (CUBRIDCommand cmd = new CUBRIDCommand(_create, conn))
                {
                    cmd.ExecuteNonQuery();
                }

                string _insert = "INSERT INTO TBL_RAW_POWER(PDMU_ID, CHANNEL_NUM, REG_DATE, AMPERE, ACTIVE_POWER, " +
                                "POWER_ACT, APPARENT_POWER, REACTIVE_POWER, POWER_REA, SYSTEM_STATUS, FREQUENCY, " +
                                "POWER_FACTOR, POWER_STATUS, VOLTAGE ) VALUES " +
                                " (637, 12, '2013-01-18 13:34:19', 1316, 2268, 40729, 2804, 972, 40729, 1000, 596, 94, 0, 1011), " +
                                "(637, 14, '2013-01-18 13:34:19', 456, 942, 15605, 964, 294, 15605, 1000, 597, 95, 0, 1011), " +
                                "(637, 15, '2013-01-18 13:34:19', 4316, 2268, 15151, 2804, 972, 15151, 1000, 596, 94, 0, 1011), " +
                                "(637, 16, '2013-01-18 13:34:19', 1316, 2268, 15279, 2804, 972, 15279, 1000, 596, 94, 0, 1011), " +
                                "(637, 17, '2013-01-18 13:34:19', 4316, 2268, 15347, 2804, 972, 15347, 1000, 596, 94, 0, 1011), " +
                                "(637, 13, '2013-01-18 13:34:19', 456, 942, 15408, 964, 294, 15408, 1000, 597, 95, 0, 1011), " +
                                "(637, 31, '2013-01-18 13:34:19', 456, 942, 15282, 964, 294, 15282, 1000, 597, 95, 0, 1011), " +
                                "(637, 32, '2013-01-18 13:34:19', 1600, 3318, 15480, 3475, 979, 15480, 1000, 597, 95, 0, 1011), " +
                                "(637, 33, '2013-01-18 13:34:19', 1600, 3318, 15132, 3475, 979, 15132, 1000, 597, 95, 0, 1011), " +
                                "(637, 34, '2013-01-18 13:34:19', 4316, 2268, 15363, 2804, 972, 15363, 1000, 596, 94, 0, 1011), " +
                                "(637, 35, '2013-01-18 13:34:19', 0, 0, 15464, 0, 0, 15464, 1000, 597, 95, 0, 1011), " +
                                "(637, 36, '2013-01-18 13:34:19', 1600, 3318, 15415, 3475, 979, 15415, 1000, 597, 95, 0, 1011), " +
                                "(637, 37, '2013-01-18 13:34:19', 1316, 2268, 15251, 2804, 972, 15251, 1000, 596, 94, 0, 1011), " +
                                "(637, 38, '2013-01-18 13:34:19', 4316, 2268, 15299, 2804, 972, 15299, 1000, 596, 94, 0, 1011), " +
                                "(637, 39, '2013-01-18 13:34:19', 1600, 3318, 15384, 3475, 979, 15384, 1000, 597, 95, 0, 1011), " +
                                "(637, 40, '2013-01-18 13:34:19', 1316, 2268, 15305, 2804, 972, 15305, 1000, 596, 94, 0, 1011), " +
                                "(637, 41, '2013-01-18 13:34:19', 456, 942, 15431, 964, 294, 15431, 1000, 597, 95, 0, 1011), " +
                                "(637, 42, '2013-01-18 13:34:19', 1600, 3318, 15341, 3475, 979, 15341, 1000, 597, 95, 0, 1011), " +
                                "(637, 43, '2013-01-18 13:34:19', 1600, 3318, 15202, 3475, 979, 15202, 1000, 597, 95, 0, 1011), " +
                                "(637, 44, '2013-01-18 13:34:19', 1316, 2268, 15170, 2804, 972, 15170, 1000, 596, 94, 0, 1011), " +
                                "(637, 45, '2013-01-18 13:34:19', 456, 942, 15020, 964, 294, 15020, 1000, 597, 95, 0, 1011), " +
                                "(637, 46, '2013-01-18 13:34:19', 1316, 2268, 15268, 2804, 972, 15268, 1000, 596, 94, 0, 1011), " +
                                "(637, 47, '2013-01-18 13:34:19', 456, 942, 15253, 964, 294, 15253, 1000, 597, 95, 0, 1011), " +
                                "(637, 48, '2013-01-18 13:34:19', 1600, 3318, 15258, 3475, 979, 15258, 1000, 597, 95, 0, 1011), " +
                                "(637, 49, '2013-01-18 13:34:19', 1600, 3318, 15159, 3475, 979, 15159, 1000, 597, 95, 0, 1011), " +
                                "(637, 50, '2013-01-18 13:34:19', 1600, 3318, 15178, 3475, 979, 15178, 1000, 597, 95, 0, 1011), " +
                                "(637, 70, '2013-01-18 13:34:19', 1316, 2268, 7522, 2804, 972, 7522, 1000, 596, 94, 0, 1011), " +
                                "(637, 71, '2013-01-18 13:34:19', 1600, 3318, 7532, 3475, 979, 7532, 1000, 597, 95, 0, 1011), " +
                                "(637, 72, '2013-01-18 13:34:19', 456, 942, 7483, 964, 294, 7483, 1000, 597, 95, 0, 1011), " +
                                "(637, 18, '2013-01-18 13:34:19', 1600, 3318, 96721, 3475, 979, 96721, 1000, 597, 95, 0, 1011), " +
                                "(637, 19, '2013-01-18 13:34:19', 456, 942, 96110, 964, 294, 96110, 1000, 597, 95, 0, 1011), " +
                                "(637, 20, '2013-01-18 13:34:19', 0, 0, 56793, 0, 0, 56793, 1000, 597, 95, 0, 1011), " +
                                "(637, 1, '2013-01-18 13:34:19', 456, 942, 15250, 964, 294, 15250, 1000, 597, 95, 0, 1011), " +
                                "(637, 2, '2013-01-18 13:34:19', 456, 942, 15374, 964, 294, 15374, 1000, 597, 95, 0, 1011), " +
                                "(637, 30, '2013-01-18 13:34:19', 4316, 2268, 7595, 2804, 972, 7595, 1000, 596, 94, 0, 1011), " +
                                "(637, 28, '2013-01-18 13:34:19', 1600, 3318, 7713, 3475, 979, 7713, 1000, 597, 95, 0, 1011), " +
                                "(637, 29, '2013-01-18 13:34:19', 456, 942, 7487, 964, 294, 7487, 1000, 597, 95, 0, 1011)";

                /* insert multi rows values */
                using (CUBRIDCommand cmd = new CUBRIDCommand(_insert, conn))
                {
                    cmd.ExecuteNonQuery();
                }


                /* verify count */
                string sql = "select count(*) from TBL_RAW_POWER";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetInt32(0), 37);
                    }
                }

                try
                {
                    CUBRIDCommandTest.ExecuteSQL("drop TBL_RAW_POWER;", conn);
                }
                catch { }
            }
        }
    }
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             