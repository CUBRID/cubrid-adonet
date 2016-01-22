using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;


namespace Unit.TestCases
{


    /// <summary>
    ///This is a test class for CUBRIDDataAdapterTest and is intended
    ///to contain all CUBRIDDataAdapterTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDDataAdapterTest
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


        /// <summary>
        ///A test for CUBRIDDataAdapter Constructor
        ///</summary>
        [TestMethod()]
        public void CUBRIDDataAdapterConstructorTest()
        {
            string selectCommandText = "select * from nation order by `code` asc";
            using (CUBRIDConnection connection = new CUBRIDConnection())
            {
                connection.ConnectionString = CUBRIDDataAdapterTest.connString;
                connection.Open();

                using (CUBRIDDataAdapter target = new CUBRIDDataAdapter(selectCommandText, connection))
                {
                    using (CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(selectCommandText, connection))
                    {
                        using (CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd))
                        {
                            target.UpdateCommand = cmdBuilder.GetUpdateCommand();
                            DataTable dt = new DataTable("nation");
                            target.Fill(dt);

                            //Update data
                            DataRow workRow = dt.Rows[0];

                            Assert.AreEqual(workRow["code"].ToString(), "AFG");
                            Assert.AreEqual(workRow["capital"].ToString(), "Kabul");

                            workRow.BeginEdit();
                            workRow["capital"] = "MyKabul";
                            workRow.EndEdit();
                            target.Update(dt);

                            Assert.AreEqual(workRow["capital"].ToString(), "MyKabul");
                            Assert.IsTrue(workRow.RowState.ToString() != "New");
                        }
                    }
                }

                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", connection), "MyKabul");
                //Revert changes
                ExecuteSQL("update nation set capital = 'Kabul' where capital = 'MyKabul'", connection);
                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", connection), "Kabul");
            }
        }

        /// <summary>
        ///A test for CUBRIDDataAdapter Constructor
        ///</summary>
        [TestMethod()]
        public void CUBRIDDataAdapterConstructorTest1()
        {
            string selectCommandText = "select * from nation order by `code` asc";
            using (CUBRIDDataAdapter target = new CUBRIDDataAdapter(selectCommandText, CUBRIDDataAdapterTest.connString))
            {
                using (CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(selectCommandText, CUBRIDDataAdapterTest.connString))
                {
                    using (CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd))
                    {
                        target.UpdateCommand = cmdBuilder.GetUpdateCommand();

                        DataTable dt = new DataTable("nation");
                        target.Fill(dt);

                        //Update data
                        DataRow workRow = dt.Rows[0];

                        Assert.AreEqual(workRow["code"].ToString(), "AFG");
                        Assert.AreEqual(workRow["capital"].ToString(), "Kabul");

                        workRow.BeginEdit();
                        workRow["capital"] = "MyKabul";
                        workRow.EndEdit();
                        target.Update(dt);

                        Assert.AreEqual(workRow["capital"].ToString(), "MyKabul");
                        Assert.IsTrue(workRow.RowState.ToString() != "New");
                    }
                }
            }

            using (CUBRIDConnection connection = new CUBRIDConnection())
            {
                connection.ConnectionString = CUBRIDDataAdapterTest.connString;
                connection.Open();
                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", connection), "MyKabul");
                //Revert changes
                ExecuteSQL("update nation set capital = 'Kabul' where capital = 'MyKabul'", connection);
                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", connection), "Kabul");
            }
        }

        /// <summary>
        /// Test DataTable column properties
        /// </summary>
        private static void Test_DataTable_ColumnProperties()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                String sql = "select * from nation";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                {
                    da.SelectCommand = new CUBRIDCommand(sql, conn);
                    DataTable dt = new DataTable("nation");
                    da.FillSchema(dt, SchemaType.Source);//To retrieve all the column properties you have to use the FillSchema() method

                    Assert.IsTrue(dt.Columns[0].ColumnName == "code");
                    Assert.IsTrue(dt.Columns[0].AllowDBNull == false);
                    Assert.IsTrue(dt.Columns[0].DefaultValue.ToString() == "");
                    Assert.IsTrue(dt.Columns[0].Unique == true);
                    Assert.IsTrue(dt.Columns[0].DataType == typeof(System.String));
                    Assert.IsTrue(dt.Columns[0].Ordinal == 0);
                    Assert.IsTrue(dt.Columns[0].Table == dt);
                }
            }
        }


        //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
        /// <summary>
        /// Test DataTable explicit DELETE
        /// </summary>
        [TestMethod()]
        public void Test_DataTable_DeleteExplicit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                //Insert a new row
                ExecuteSQL("insert into nation values('ZZZZ', 'Z', 'Z', 'Z')", conn);
                Assert.AreEqual(GetTableRowsCount("nation", conn), 216);

                String sql = "select * from nation order by `code` desc";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                {
                    //Initialize the command object that will be used as the DeleteCommand for the DataAdapter.
                    using (CUBRIDCommand daDelete = new CUBRIDCommand("delete from nation where code = ?", conn))
                    {
                        //Parameter: code
                        daDelete.Parameters.Add(new CUBRIDParameter("?p1", DbType.String));
                        daDelete.Parameters["?p1"].SourceVersion = DataRowVersion.Original;
                        daDelete.Parameters["?p1"].SourceColumn = "code";
                        daDelete.Parameters["?p1"].SourceColumnNullMapping = false;

                        daDelete.UpdatedRowSource = UpdateRowSource.OutputParameters;

                        //Assign the command to the DeleteCommand property of the DataAdapter.
                        da.DeleteCommand = daDelete;

                        DataTable dt = new DataTable("nation");
                        da.Fill(dt);

                        Assert.AreEqual(dt.Rows[0]["capital"].ToString(), "Z");

                        dt.Rows[0].Delete();
                        da.Update(dt);

                        Assert.AreEqual(dt.Rows.Count, 215);
                    }
                }

                Assert.AreEqual(GetTableRowsCount("nation", conn), 215);
            }
        }


        /// <summary>
        /// Test DataTable implicit DELETE
        /// </summary>
        [TestMethod()]
        public void Test_DataTable_DeleteImplicit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                //Insert a new row
                ExecuteSQL("insert into nation values('ZZZZ', 'Z', 'Z', 'Z')", conn);
                Assert.AreEqual(GetTableRowsCount("nation", conn), 216);

                String sql = "select * from nation order by `code` desc";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                {
                    CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(sql, conn);
                    using (CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd))
                    {
                        da.DeleteCommand = cmdBuilder.GetDeleteCommand();

                        DataTable dt = new DataTable("nation");
                        da.Fill(dt);

                        Assert.AreEqual(dt.Rows[0]["capital"].ToString(), "Z");

                        dt.Rows[0].Delete();
                        da.Update(dt);

                        Assert.AreEqual(dt.Rows.Count, 215);
                    }
                }

                Assert.AreEqual(GetTableRowsCount("nation", conn), 215);
            }
        }

        /// <summary>
        /// Test DataTable explicit INSERT
        /// </summary>
        [TestMethod()]
        public void Test_DataTable_InsertExplicit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` DESC LIMIT 10";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                {

                    //Initialize the command object that will be used as the UpdateCommand for the DataAdapter.
                    using (CUBRIDCommand daInsert = new CUBRIDCommand("insert into nation values(?,?,?,?)", conn))
                    {
                        daInsert.CommandType = CommandType.Text;

                        //Parameter: code
                        daInsert.Parameters.Add(new CUBRIDParameter("?p1", DbType.String));
                        daInsert.Parameters["?p1"].SourceVersion = DataRowVersion.Current;
                        daInsert.Parameters["?p1"].SourceColumn = "code";
                        daInsert.Parameters["?p1"].SourceColumnNullMapping = false;

                        //Parameter: name
                        daInsert.Parameters.Add(new CUBRIDParameter("?p2", DbType.String));
                        daInsert.Parameters["?p2"].SourceVersion = DataRowVersion.Original;
                        daInsert.Parameters["?p2"].SourceColumn = "name";
                        daInsert.Parameters["?p2"].SourceColumnNullMapping = false;

                        //Parameter: continent
                        daInsert.Parameters.Add(new CUBRIDParameter("?p3", DbType.String));
                        daInsert.Parameters["?p3"].SourceVersion = DataRowVersion.Current;
                        daInsert.Parameters["?p3"].SourceColumn = "continent";
                        daInsert.Parameters["?p3"].SourceColumnNullMapping = false;

                        //Parameter: capital
                        daInsert.Parameters.Add(new CUBRIDParameter("?p4", DbType.String));
                        daInsert.Parameters["?p4"].SourceVersion = DataRowVersion.Original;
                        daInsert.Parameters["?p4"].SourceColumn = "capital";
                        daInsert.Parameters["?p4"].SourceColumnNullMapping = false;

                        daInsert.UpdatedRowSource = UpdateRowSource.None;

                        //Assign the command to the InsertCommand property of the DataAdapter.
                        da.InsertCommand = daInsert;

                        DataTable dt = new DataTable("nation");
                        da.Fill(dt);
                        DataRow newRow = dt.NewRow();
                        newRow["code"] = "ZZZ";
                        newRow["name"] = "ABCDEF";
                        newRow["capital"] = "MyXYZ";
                        newRow["continent"] = "QWERTY";
                        dt.Rows.InsertAt(newRow, 0);
                        da.Update(dt);
                        dt.AcceptChanges();

                        Assert.AreEqual(dt.Rows[0]["capital"].ToString(), "MyXYZ");
                        Assert.IsTrue(newRow.RowState.ToString() != "New");
                    }
                }

                Assert.AreEqual(GetTableRowsCount("nation", conn), 216);
                //Revert changes
                ExecuteSQL("delete from nation where `code` = 'ZZZ'", conn);
                Assert.AreEqual(GetTableRowsCount("nation", conn), 215);
            }
        }


        //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
        /// <summary>
        /// Test DataTable implicit INSERT
        /// </summary>
        [TestMethod()]
        public void Test_DataTable_InsertImplicit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                {
                    using (CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(sql, conn))
                    {
                        using (CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd))
                        {
                            da.InsertCommand = cmdBuilder.GetInsertCommand();
                        }
                    }

                    DataTable dt = new DataTable("nation");
                    da.Fill(dt);

                    DataRow newRow = dt.NewRow();
                    newRow["code"] = "ZZZ";
                    newRow["name"] = "ABCDEF";
                    newRow["capital"] = "MyXYZ";
                    newRow["continent"] = "QWERTY";
                    dt.Rows.Add(newRow);

                    da.Update(dt);

                    Assert.AreEqual(dt.Rows.Count, 216);
                }

                Assert.AreEqual(GetTableRowsCount("nation", conn), 216);
                //Revert changes
                ExecuteSQL("delete from nation where `code` = 'ZZZ'", conn);
                Assert.AreEqual(GetTableRowsCount("nation", conn), 215);
            }
        }

        //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
        /// <summary>
        /// Test DataTable explicit UPDATE
        /// </summary>
        [TestMethod()]
        public void Test_DataTable_UpdateExplicit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc LIMIT 10";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                {
                    //Initialize the command object that will be used as the UpdateCommand for the DataAdapter.
                    using (CUBRIDCommand daUpdate = new CUBRIDCommand("update nation set capital = ? where code = ?", conn))
                    {
                        //Parameter: capital
                        daUpdate.Parameters.Add(new CUBRIDParameter("?p1", DbType.String));
                        daUpdate.Parameters[0].SourceVersion = DataRowVersion.Current;
                        daUpdate.Parameters[0].SourceColumn = "capital";
                        daUpdate.Parameters[0].SourceColumnNullMapping = false;

                        //Parameter: code
                        daUpdate.Parameters.Add(new CUBRIDParameter("?p2", DbType.String));
                        daUpdate.Parameters[1].SourceVersion = DataRowVersion.Original;
                        daUpdate.Parameters[1].SourceColumn = "code";
                        daUpdate.Parameters[1].SourceColumnNullMapping = false;

                        daUpdate.UpdatedRowSource = UpdateRowSource.None;
                        //Assign the command to the UpdateCommand property of the DataAdapter.
                        da.UpdateCommand = daUpdate;

                        DataTable dt = new DataTable("nation");
                        da.Fill(dt);
                        DataRow workRow = dt.Rows[0];
                        Assert.AreEqual(workRow["capital"].ToString(), "Kabul");
                        workRow.BeginEdit();
                        workRow["capital"] = "MyKabul";
                        workRow.EndEdit();
                        da.Update(dt);
                        dt.AcceptChanges();

                        Assert.AreEqual(workRow["capital"].ToString(), "MyKabul");
                        Assert.IsTrue(workRow.RowState.ToString() != "New");
                    }
                }

                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn), "MyKabul");
                //Revert changes
                ExecuteSQL("update nation set capital = 'Kabul' where capital = 'MyKabul'", conn);
                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn), "Kabul");
            }
        }

        //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
        /// <summary>
        /// Test DataTable implicit UPDATE
        /// </summary>
        [TestMethod()]
        public void Test_DataTable_UpdateImplicit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";
                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                {
                    using (CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(sql, conn))
                    {
                        using (CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd))
                        {
                            da.UpdateCommand = cmdBuilder.GetUpdateCommand();
                        }
                    }

                    DataTable dt = new DataTable("nation");
                    da.Fill(dt);

                    //Update data
                    DataRow workRow = dt.Rows[0];

                    Assert.AreEqual(workRow["code"].ToString(), "AFG");
                    Assert.AreEqual(workRow["capital"].ToString(), "Kabul");

                    workRow.BeginEdit();
                    workRow["capital"] = "MyKabul";
                    workRow.EndEdit();
                    da.Update(dt);

                    Assert.AreEqual(workRow["capital"].ToString(), "MyKabul");
                    Assert.IsTrue(workRow.RowState.ToString() != "New");
                }

                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn), "MyKabul");
                //Revert changes
                ExecuteSQL("update nation set capital = 'Kabul' where capital = 'MyKabul'", conn);
                Assert.AreEqual((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn), "Kabul");
            }
        }

        /// <summary>
        /// Test batch update, using DataAdapter
        /// </summary>
        [TestMethod()]
        public void Test_DataAdapter_BatchUpdate()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
                {
                    // Set the INSERT command and parameter.
                    da.InsertCommand = new CUBRIDCommand("insert into nation values ('A', 'B', 'C', 'D')");
                    da.InsertCommand.UpdatedRowSource = UpdateRowSource.None;

                    // Set the UPDATE command and parameters.
                    da.UpdateCommand = new CUBRIDCommand("update nation set capital = 'X' where `code` = 'A'");
                    da.UpdateCommand.UpdatedRowSource = UpdateRowSource.None;

                    // Set the DELETE command and parameter.
                    da.DeleteCommand = new CUBRIDCommand("delete from nation where `code` = 'A'");
                    da.DeleteCommand.UpdatedRowSource = UpdateRowSource.None;

                    // Set the batch size.
                    da.UpdateBatchSize = 3;

                    // Execute the update.
                    DataTable dt = new DataTable("nation");
                    da.Update(dt);
                }

                Assert.IsTrue(GetTableRowsCount("nation", conn) == 215);
            }
        }

        /// <summary>
        /// Test DataTableReader GetSchemaTable() method
        /// </summary>
        private static void Test_GetSchemaTable()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataAdapterTest.connString;
                conn.Open();

                string sql = "select * from athlete order by `code`";
                using (CUBRIDDataAdapter adapter = new CUBRIDDataAdapter(sql, conn))
                {
                    DataTable table = new DataTable();

                    //To retrieve the AlolowDBNull, IsUnique, IsKey, IsAutoIncrement and BaseTableName values from the Database Server
                    //you must use the FillSchema() method.
                    adapter.FillSchema(table, SchemaType.Source);

                    using (DataTableReader reader = new DataTableReader(table))
                    {
                        DataTable schemaTable = reader.GetSchemaTable();
                        DataRow row = schemaTable.Rows[0];

                        Assert.IsTrue(row["ColumnName"].ToString() == "code");
                        Assert.IsTrue(row["ColumnOrdinal"].ToString() == "0");
                        Assert.IsTrue(row["ColumnSize"].ToString() == "-1");
                        Assert.IsTrue(row["NumericPrecision"].ToString() == "");
                        Assert.IsTrue(row["NumericScale"].ToString() == "");
                        Assert.IsTrue(row["IsUnique"].ToString() == "True");
                        Assert.IsTrue(row["IsKey"].ToString() == "True");
                        Assert.IsTrue(row["BaseTableNamespace"].ToString() == "");
                        Assert.IsTrue(row["BaseColumnNamespace"].ToString() == "");
                        Assert.IsTrue(row["BaseCatalogName"].ToString() == "");
                        Assert.IsTrue(row["BaseColumnName"].ToString() == "code");
                        Assert.IsTrue(row["BaseSchemaName"].ToString() == "");
                        Assert.IsTrue(row["BaseTableName"].ToString() == "athlete");
                        Assert.IsTrue(row["DataType"].ToString() == "System.Int32");
                        Assert.IsTrue(row["AllowDBNull"].ToString() == "False");
                        Assert.IsTrue(row["ProviderType"].ToString() == "");
                        Assert.IsTrue(row["Expression"].ToString() == "");
                        Assert.IsTrue(row["AutoIncrementSeed"].ToString() == "0");
                        Assert.IsTrue(row["AutoincrementStep"].ToString() == "1");
                        Assert.IsTrue(row["IsAutoIncrement"].ToString() == "True");
                        Assert.IsTrue(row["IsRowVersion"].ToString() == "False");
                        Assert.IsTrue(row["IsLong"].ToString() == "False");
                        Assert.IsTrue(row["IsReadOnly"].ToString() == "False");
                        Assert.IsTrue(row["ColumnMapping"].ToString() == "1");
                        Assert.IsTrue(row["DefaultValue"].ToString() == "");
                    }
                }
            }
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

        private static object GetSingleValue(string sql, CUBRIDConnection conn)
        {
            object ret = null;

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                ret = cmd.ExecuteScalar();
            }

            return ret;
        }

        private static void ExecuteSQL(string sql, CUBRIDConnection conn)
        {
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              