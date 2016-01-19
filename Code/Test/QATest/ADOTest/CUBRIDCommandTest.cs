using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using CUBRID.Data.CUBRIDClient;
using ADOTest.TestHelper;

namespace ADOTest
{
    /// <summary>
    /// This is a test class for CUBRIDCommand
    /// </summary>
    [TestClass]
    public class CUBRIDCommandTest : BaseTest
    {
        /// <summary>
        /// Test CUBRIDCommand()
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_Constructor_NoParam_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;

            CUBRIDCommand cmd = new CUBRIDCommand();
            cmd.Connection = conn;
            cmd.CommandText = "select * from nation order by code asc";

            conn.Open();
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            Assert.AreEqual(4, reader.FieldCount);
            Assert.AreEqual("AFG", reader.GetString(0));
            Assert.AreEqual("Afghanistan", reader.GetString(1));
            Assert.AreEqual("Asia", reader.GetString(2));
            Assert.AreEqual("Kabul", reader.GetString(3));

            cmd.Close();
            reader.Close();
            conn.Close();
        }

        /// <summary>
        /// Test CUBRIDCommand()
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_Constructor_SQL_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;

            string sql = "select * from nation order by code asc";
            CUBRIDCommand cmd = new CUBRIDCommand(sql);
            cmd.Connection = conn;

            conn.Open();
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            Assert.AreEqual(4, reader.FieldCount);
            Assert.AreEqual("AFG", reader.GetString(0));
            Assert.AreEqual("Afghanistan", reader.GetString(1));
            Assert.AreEqual("Asia", reader.GetString(2));
            Assert.AreEqual("Kabul", reader.GetString(3));

            cmd.Close();
            reader.Close();
            conn.Close();
        }

        /// <summary>
        /// Test CUBRIDCommand()
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_Constructor_SQLAndConn_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;

            string sql = "select * from nation order by code asc";
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

            conn.Open();
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            Assert.AreEqual(4, reader.FieldCount);
            Assert.AreEqual("AFG", reader.GetString(0));
            Assert.AreEqual("Afghanistan", reader.GetString(1));
            Assert.AreEqual("Asia", reader.GetString(2));
            Assert.AreEqual("Kabul", reader.GetString(3));

            cmd.Close();
            reader.Close();
            conn.Close();
        }

        /// <summary>
        /// Test CUBRIDCommand() //APIS-512
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_Constructor_SQLAndConnAndTran_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            conn.Open();
            conn.SetAutoCommit(false);

            string sql = "drop table if exists t";
            CUBRIDTransaction transaction = new CUBRIDTransaction(conn, CUBRIDIsolationLevel.TRAN_DEFAULT_ISOLATION);
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn, transaction);

            conn.BeginTransaction();
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table t (id int, name varchar(50))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t values(1, 'Nancy')";
            cmd.ExecuteNonQuery();
            conn.Commit();

            cmd.CommandText = "select * from t";
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            Assert.AreEqual(2, reader.FieldCount);
            Assert.AreEqual("1", reader.GetString(0));
            Assert.AreEqual("Nancy", reader.GetString(1));

            cmd.Close();
            reader.Close();
            conn.Close();
        }

        /// <summary>
        /// Test CUBRIDCommand()
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_CreateParameter_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            conn.Open();

            DBHelper.ExecuteSQL("drop table if exists t", conn);
            DBHelper.ExecuteSQL("create table t (clsid bit(288))", conn);

            byte[] bytes = new byte[36]{55, 56, 50, 69, 55, 57, 67, 69,
                                            45, 50, 70, 68, 68, 45, 52, 68,
                                            50, 55, 45, 65, 51, 48, 48, 45,
                                            69, 48, 56, 56, 70, 56, 68, 68,
                                            55, 54, 66, 69};

            string sql = "INSERT INTO t VALUES (?);";

            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
            CUBRIDParameter param = (CUBRIDParameter)cmd.CreateParameter();
            param.ParameterName = "?p";
            param.Value = bytes;
            cmd.Parameters.Add(param);
            cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIT;
            cmd.ExecuteNonQuery();
            cmd.Close();

            sql = "select * from t";
            cmd = new CUBRIDCommand(sql, conn);
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            byte[] buffer = (byte[])reader.GetValue(0);
            string clsid = conn.GetEncoding().GetString(buffer);
            Assert.AreEqual(clsid, "782E79CE-2FDD-4D27-A300-E088F8DD76BE");
            reader.Close();
            cmd.Close();

            //Revert the test db
            DBHelper.ExecuteSQL("drop table if exists t", conn);
            conn.Close();
        }

        /// <summary>
        /// Test CUBRIDCommand()
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_ExecuteNonQuery_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                CUBRIDCommand cmd = new CUBRIDCommand("drop table if exists t", conn);
                int result = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, result);
                int tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(0, tablesCount);

                cmd.CommandText = "create table t(id integer)";
                result = cmd.ExecuteNonQuery();
                Assert.AreEqual(0, result);
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                //TODO: add new test cases for insert update delete sql statement

            }
        }

        /// <summary>
        /// Test ExecuteReader
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_ExecuteReader_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                string sql = "select * from nation order by code asc";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        //verify the first two results
                        reader.Read();
                        Assert.AreEqual(4, reader.FieldCount);
                        Assert.AreEqual("AFG", reader.GetString(0));
                        Assert.AreEqual("Afghanistan", reader.GetString(1));
                        Assert.AreEqual("Asia", reader.GetString(2));
                        Assert.AreEqual("Kabul", reader.GetString(3));

                        reader.Read();
                        Assert.AreEqual(4, reader.FieldCount);
                        Assert.AreEqual("AHO", reader.GetString(0));
                        Assert.AreEqual("Netherlands Antilles", reader.GetString(1));
                        Assert.AreEqual("Americas", reader.GetString(2));
                        Assert.AreEqual("Willemstad", reader.GetString(3));
                    }
                }
            }

        }

        /// <summary>
        /// Test ExecuteReader
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_ExecuteReader_CloseConnection_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                string sql = "select * from nation order by code asc";
                CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

                LogTestStep("Test CommandBehavior.CloseConnection");
                CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader(CommandBehavior.CloseConnection);

                //verify the first two results
                reader.Read();
                Assert.AreEqual(4, reader.FieldCount);
                Assert.AreEqual("AFG", reader.GetString(0));
                Assert.AreEqual("Afghanistan", reader.GetString(1));
                Assert.AreEqual("Asia", reader.GetString(2));
                Assert.AreEqual("Kabul", reader.GetString(3));

                reader.Read();
                Assert.AreEqual(4, reader.FieldCount);
                Assert.AreEqual("AHO", reader.GetString(0));
                Assert.AreEqual("Netherlands Antilles", reader.GetString(1));
                Assert.AreEqual("Americas", reader.GetString(2));
                Assert.AreEqual("Willemstad", reader.GetString(3));

                reader.Close();

                if (reader.IsClosed)
                {
                    try
                    {
                        cmd = new CUBRIDCommand("create table t(id int)", conn);
                        cmd.ExecuteNonQuery();
                        LogStepFail();
                    }
                    catch (Exception ex)
                    {
                        Assert.AreEqual("The connection is not open!", ex.Message);
                        LogStepPass();
                    }
                }
                else
                {
                    LogStepFail();
                }

                //TODO: Test CommandBehavior.Default, CommandBehavior.SchemaOnly, CommandBehavior.KeyInfo, CommandBehavior.SingleRow, CommandBehavior.SequentialAccess
                LogTestResult();
            }
        }

        /// <summary>
        /// Test ExecuteReader //APIS-514
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_ExecuteReader_SingleRow_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                string sql = "select * from nation order by code asc";
                CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

                LogTestStep("Test CommandBehavior.SingleRow");
                CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader(CommandBehavior.SingleRow);

                //verify the first two results
                reader.Read();
                Assert.AreEqual(4, reader.FieldCount);
                Assert.AreEqual("AFG", reader.GetString(0));
                Assert.AreEqual("Afghanistan", reader.GetString(1));
                Assert.AreEqual("Asia", reader.GetString(2));
                Assert.AreEqual("Kabul", reader.GetString(3));

                try
                {
                    if (reader.Read())
                    {
                        Assert.AreEqual(4, reader.FieldCount);
                        Assert.AreEqual("AHO", reader.GetString(0));
                        Assert.AreEqual("Netherlands Antilles", reader.GetString(1));
                        Assert.AreEqual("Americas", reader.GetString(2));
                        Assert.AreEqual("Willemstad", reader.GetString(3));
                        LogStepFail();
                    }
                    else
                    {
                        LogStepPass();
                    }
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("some message about there in no more result", ex.Message);
                }

                reader.Close();
                cmd.Close();
                //TODO: Test CommandBehavior.Default, CommandBehavior.SchemaOnly, CommandBehavior.KeyInfo, CommandBehavior.SingleRow, CommandBehavior.SequentialAccess
                LogTestResult();
            }
        }

        /// <summary>
        /// Test ExecuteScalar
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_ExecuteScalar_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                string sql = "select * from nation order by code asc";
                CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

                LogTestStep("Test ExecuteScalar - return a value");
                object obj = cmd.ExecuteScalar();

                Assert.AreEqual("AFG", obj.ToString());
                LogStepPass();

                LogTestStep("Test ExecuteScalar - return null");
                cmd.CommandText = "drop table if exists test_scalar;";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "create table test_scalar(a int);";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "insert into test_scalar values(NULL);";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "select * from test_scalar";
                obj = cmd.ExecuteScalar();
                Assert.AreEqual(DBNull.Value, obj);
                LogStepPass();

                cmd.Close();
                LogTestResult();
            }
        }

        /// <summary>
        /// Test Prepare
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_Prepare_Basic_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString))
            {
                conn.Open();
                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (id int, name varchar(50))", conn);

                CUBRIDCommand cmd = new CUBRIDCommand(null, conn);

                LogTestStep("Test Prepare");
                cmd.CommandText =
                    "insert into t (id, name) values (?, ?)";

                CUBRIDParameter idParam = new CUBRIDParameter("?", CUBRIDDataType.CCI_U_TYPE_INT, 8);
                CUBRIDParameter nameParam = new CUBRIDParameter("?", CUBRIDDataType.CCI_U_TYPE_STRING, 20);
                idParam.Value = 2;
                nameParam.Value = "Rachel Green";
                cmd.Parameters.Add(idParam);
                cmd.Parameters.Add(nameParam);


                Log("Call Prepare after setting the Commandtext and Parameters.");
                cmd.Prepare();
                cmd.ExecuteNonQuery();

                Log("Change parameter values and call ExecuteNonQuery.");
                cmd.Parameters[0].Value = 5;
                cmd.Parameters[1].Value = "Bill Gates";
                cmd.ExecuteNonQuery();

                Log("Verify the date are inserted by querying them from db");
                cmd = new CUBRIDCommand("select * from t", conn);
                CUBRIDDataAdapter adapter = new CUBRIDDataAdapter(cmd);
                DataTable dt = new DataTable();
                adapter.Fill(dt);

                Assert.AreEqual(2, dt.Rows.Count);
                Assert.AreEqual(2, (int)dt.Rows[0][0]);
                Assert.AreEqual("Rachel Green", dt.Rows[0][1].ToString());
                Assert.AreEqual(5, (int)dt.Rows[1][0]);
                Assert.AreEqual("Bill Gates", dt.Rows[1][1].ToString());
                LogStepPass();

                cmd.Close();

                Log("delete test table");
                DBHelper.ExecuteSQL("drop table if exists t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        /// Test Clone()
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_Clone_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString))
            {
                conn.Open();
                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (id int primary key, name varchar(50))", conn);
                DBHelper.ExecuteSQL("insert into t (id, name) values (2, 'Rachel Green')", conn);
                DBHelper.ExecuteSQL("insert into t (id, name) values (3, 'Rachel Green')", conn);
                DBHelper.ExecuteSQL("insert into t (id, name) values (5, 'Bill Gates')", conn);

                LogTestStep("Clone a CUBRIDCommand which has parameters");
                CUBRIDCommand cmd = new CUBRIDCommand(null, conn);
                cmd.CommandText = "select * from t where id = ?myId and name = ?myName";

                CUBRIDParameter idParam = new CUBRIDParameter("?myId", CUBRIDDataType.CCI_U_TYPE_INT, 8);
                CUBRIDParameter nameParam = new CUBRIDParameter("?myName", CUBRIDDataType.CCI_U_TYPE_STRING, 20);
                idParam.Value = 2;
                nameParam.Value = "Rachel Green";
                cmd.Parameters.Add(idParam);
                cmd.Parameters.Add(nameParam);

                CUBRIDCommand cmdClone = cmd.Clone();

                CUBRIDDataAdapter adapter = new CUBRIDDataAdapter();

                adapter.SelectCommand = cmdClone;

                Log("Verify the cloned command");
                DataTable dt = new DataTable("");
                adapter.Fill(dt);

                Assert.AreEqual(1, dt.Rows.Count);
                Assert.AreEqual(2, (int)dt.Rows[0][0]);
                Assert.AreEqual("Rachel Green", dt.Rows[0][1].ToString());

                adapter.FillSchema(dt, SchemaType.Source);//To retrieve all the column properties you have to use the FillSchema() method

                Assert.AreEqual(cmdClone.ColumnInfos[0].Name, "id");
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsPrimaryKey, true);
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsForeignKey, false);
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsNullable, false);
                Assert.AreEqual(cmdClone.ColumnInfos[0].RealName, "t");
                Assert.AreEqual(cmdClone.ColumnInfos[0].Precision, 10);
                Assert.AreEqual(cmdClone.ColumnInfos[0].Scale, 0);
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsAutoIncrement, false);
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsReverseIndex, false);
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsReverseUnique, false);
                Assert.AreEqual(cmdClone.ColumnInfos[0].IsShared, false);
                Assert.AreEqual(cmdClone.ColumnInfos[0].Type, CUBRIDDataType.CCI_U_TYPE_INT);
                Assert.AreEqual(cmdClone.ColumnInfos[0].Table, "t");
                LogStepPass();
                adapter.Dispose();
                cmd.Close();

                Log("delete test db");
                //DBHelper.ExecuteSQL("drop table if exists t", conn);
            }

            LogTestResult();
        }

        /// <summary>
        /// Test Close()
        ///</summary>
        [TestMethod]
        public void CUBRIDCommand_Close_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;

                string sql = "select * from nation order by code asc";
                CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

                conn.Open();
                CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
                reader.Read();
                Assert.AreEqual(4, reader.FieldCount);
                Assert.AreEqual("AFG", reader.GetString(0));
                Assert.AreEqual("Afghanistan", reader.GetString(1));
                Assert.AreEqual("Asia", reader.GetString(2));
                Assert.AreEqual("Kabul", reader.GetString(3));

                cmd.Close();

                try
                {
                    cmd.CommandText = "drop table if exists t";
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Some message about the cmd cannot be used", ex.Message);
                }

            }
        }

        /// <summary>
        /// Test GetGeneratedKeys
        /// </summary>
        [TestMethod]
        public void CUBRIDCommand_GetGeneratedKeys()
        {
            /*
            string sqlTablesCount = "select count(*) from db_class";
            int tablesCount, newTableCount;

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();
                conn.SetAutoCommit(false);

                CUBRIDTransaction transaction = conn.BeginTransaction();

                DBHelper.ExecuteSQL("drop table if exists tkeys", conn);
                tablesCount = (int)DBHelper.GetSingleValue(sqlTablesCount, conn);
                DBHelper.ExecuteSQL("create table tkeys(id int auto_increment, str string)", conn);
                newTableCount = (int)DBHelper.GetSingleValue(sqlTablesCount, conn);

                //Verify table was created
                Assert.IsTrue(newTableCount == tablesCount + 1);

                CUBRIDCommand cmd = new CUBRIDCommand("insert into tkeys(str) values('xyz')", conn);
                //cmd.IsGeneratedKeys = true;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "insert into tkeys(str) values('abcd')";
                cmd.ExecuteNonQuery();

                // get last generated key
                DbDataReader reader = cmd.GetGeneratedKeys();
             
                while (reader.Read())
                {
                    Assert.AreEqual(2, reader.GetInt32(0));
                }
                cmd.Close();
                transaction.Commit();
                conn.Close();

                //DBHelper.ExecuteSQL("drop table if exists tkeys", conn);                
            }
             * */
        }
    }
}