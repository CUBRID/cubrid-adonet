
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
    /// This is a test class for CUBRIDSchemaProvider_Constrator
    /// </summary>
    [TestClass]
    public class CUBRIDSchemaProvider_Test : BaseTest
    {


    /*    /// <summary>
        /// Test CUBRIDSchemaProvider()
        ///</summary>
        [TestMethod]
        public void CUBRIDSchemaProvider_Constructor_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();

            conn.ConnectionString = DBHelper.connString;

            CUBRIDSchemaProvider csp = new CUBRIDSchemaProvider(conn);



            conn.Open();
            String[] fil = new String[] { "demodb", "demodb2" };
            {
                try
                {

                    csp.GetDatabases(null);
                }
                catch (ArgumentException e)
                {
                    Assert.AreEqual("ArgumentException", e.GetType);
                }




                conn.Close();
            }


        }
*/

        /// <summary>
        ///This is a test class for CUBRIDSchemaProviderTest and is intended
        ///to contain all CUBRIDSchemaProviderTest Unit Tests
        ///</summary>
        [TestClass()]
        public class CUBRIDSchemaProviderTest
        {


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
            ///A test for CUBRIDSchemaProvider Constructor
            ///</summary>
            [TestMethod()]
            public void CUBRIDSchemaProviderConstructorTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); ; // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection);
               

                CUBRIDCommand cmd = new CUBRIDCommand();
                cmd.Connection = connection;
            cmd.CommandText = "select * from nation order by code asc";

    
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
            reader.Read();
            Assert.AreEqual(4, reader.FieldCount);


            cmd.Close();
            reader.Close();
            connection.Close();

            }

         

      /*      /// <summary>
            ///A test for CleanFilters
            ///</summary>
            [TestMethod()]
            public void CleanFiltersTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                string[] expected = null; // TODO: Initialize to an appropriate value
                string[] actual;
                //   actual = target.CleanFilters(filters);
                //   Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");

            }

            /// <summary>
            ///A test for FillTable
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void FillTableTest()
            {
                DataTable dt = null; // TODO: Initialize to an appropriate value
                object[][] data = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor.FillTable(dt, data);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");
            }

            /// <summary>
            ///A test for FindTables
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void FindTablesTest()
            {
                PrivateObject param0 = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor target = new CUBRIDSchemaProvider_Accessor(param0); // TODO: Initialize to an appropriate value
                DataTable schemaTable = null; // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                target.FindTables(schemaTable, filters);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");
            }

            /// <summary>
            ///A test for FindViews
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void FindViewsTest()
            {
                PrivateObject param0 = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor target = new CUBRIDSchemaProvider_Accessor(param0); // TODO: Initialize to an appropriate value
                DataTable schemaTable = null; // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                target.FindViews(schemaTable, filters);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");

            }

            /// <summary>
            ///A test for GetColumns
            ///</summary>
            [TestMethod()]
            public void GetColumnsTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetColumns(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetCrossReferenceKeys
            ///</summary>
            [TestMethod()]
            public void GetCrossReferenceKeysTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetCrossReferenceKeys(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetDataTypes
            ///</summary>
            [TestMethod()]
            public void GetDataTypesTest()
            {
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = CUBRIDSchemaProvider.GetDataTypes();
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }
            */
            /// <summary>
            ///A test for GetDatabases
            ///</summary>
            [TestMethod()]
            public void GetDatabasesTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = new string[]{"lang"}; // TODO: Initialize to an appropriate value
                DataTable expected = new DataTable(); // TODO: Initialize to an appropriate value
         
                DataTable actual;
                actual = target.GetDatabases(filters);
                Assert.AreEqual(0,actual.Rows.Count);
                
            }

            /// <summary>
            ///A test for GetDatabases
            ///</summary>
            [TestMethod()]
            public void GetDatabasesNoFilterTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = new DataTable(); // TODO: Initialize to an appropriate value

                DataTable actual;
                actual = target.GetDatabases(filters);
                Assert.AreEqual(2, actual.Rows.Count);

            }

            /// <summary>
            ///A test for GetDatabases
            ///</summary>
            [TestMethod()]
            public void GetDatabasesOneResultTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = new string[] { "demodb" }; // TODO: Initialize to an appropriate value
                DataTable expected = new DataTable(); // TODO: Initialize to an appropriate value

                DataTable actual;
                actual = target.GetDatabases(filters);
                Assert.AreEqual(1, actual.Rows.Count);

            }


            /// <summary>
            ///A test for GetDatabases
            ///</summary>
            [TestMethod()]
            public void GetDatabasesTwoFilterExceptionTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = new string[] { "demodb","demodb2" }; // TODO: Initialize to an appropriate value
                DataTable expected = new DataTable(); // TODO: Initialize to an appropriate value

                DataTable actual;
                string ss = null;

                try
                {
                    actual = target.GetDatabases(filters);
                }
                catch (ArgumentException e)
                {
                    ss = e.Message;
                }
                Assert.IsNotNull(ss.Length);

            }


            /// <summary>
            ///A test for GetDatabases
            ///</summary>
            [TestMethod()]
            public void GetDatabasesPercentOneResultTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = new string[] { "%demodb%" }; // TODO: Initialize to an appropriate value
                DataTable expected = new DataTable(); // TODO: Initialize to an appropriate value

                DataTable actual;
                actual = target.GetDatabases(filters);
                Assert.AreEqual(1, actual.Rows.Count);


            }
            /// <summary>
            ///A test for GetDatabases
            ///</summary>
            [TestMethod()]
            public void GetDatabasesPercentTwoResultTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = new string[] { "%demo%" }; // TODO: Initialize to an appropriate value
                DataTable expected = new DataTable(); // TODO: Initialize to an appropriate value

                DataTable actual;
                actual = target.GetDatabases(filters);
                Assert.AreEqual(1, actual.Rows.Count);


            }

   /*         /// <summary>
            ///A test for GetExportedKeys
            ///</summary>
            [TestMethod()]
            public void GetExportedKeysTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetExportedKeys(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetForeignKeys
            ///</summary>
            [TestMethod()]
            public void GetForeignKeysTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetForeignKeys(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetForeignKeys
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void GetForeignKeysTest1()
            {
                PrivateObject param0 = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor target = new CUBRIDSchemaProvider_Accessor(param0); // TODO: Initialize to an appropriate value
                DataTable dt = null; // TODO: Initialize to an appropriate value
                string table = string.Empty; // TODO: Initialize to an appropriate value
                string keyName = string.Empty; // TODO: Initialize to an appropriate value
                target.GetForeignKeys(dt, table, keyName);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");
            }

            /// <summary>
            ///A test for GetIndexColumns
            ///</summary>
            [TestMethod()]
            public void GetIndexColumnsTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetIndexColumns(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetIndexes
            ///</summary>
            [TestMethod()]
            public void GetIndexesTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetIndexes(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetNumericFunctions
            ///</summary>
            [TestMethod()]
            public void GetNumericFunctionsTest()
            {
                string[] expected = null; // TODO: Initialize to an appropriate value
                string[] actual;
                actual = CUBRIDSchemaProvider.GetNumericFunctions();
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetProcedures
            ///</summary>
            [TestMethod()]
            public void GetProceduresTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetProcedures(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetReservedWords
            ///</summary>
            [TestMethod()]
            public void GetReservedWordsTest()
            {
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = CUBRIDSchemaProvider.GetReservedWords();
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetSchema
            ///</summary>
            [TestMethod()]
            public void GetSchemaTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string collection = string.Empty; // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetSchema(collection, filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetString
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void GetStringTest()
            {
                CUBRIDDataReader reader = null; // TODO: Initialize to an appropriate value
                int index = 0; // TODO: Initialize to an appropriate value
                string expected = string.Empty; // TODO: Initialize to an appropriate value
                string actual;
                actual = CUBRIDSchemaProvider_Accessor.GetString(reader, index);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }

            /// <summary>
            ///A test for GetStringFunctions
            ///</summary>
            [TestMethod()]
            public void GetStringFunctionsTest()
            {
                string[] expected = null; // TODO: Initialize to an appropriate value
                string[] actual;
                actual = CUBRIDSchemaProvider.GetStringFunctions();
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }
            */
            /// <summary>
            ///A test for GetTables
            ///</summary>
            [TestMethod()]
            public void GetTablesNoFilterTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters =null; // TODO: Initialize to an appropriate value
                DataTable actual;
                string ss=null;
                try
                {
                    actual = target.GetTables(filters);
                }
                catch (Exception e) {
                    ss = e.Message;
                }
                Assert.IsNotNull(ss.Length);
                connection.Close();
            }

            /// <summary>
            ///A test for GetTables
            ///</summary>
            [TestMethod()]
            public void GetTablesTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = new string[]{"%co%"}; // TODO: Initialize to an appropriate value
                DataTable actual;
                
                    actual = target.GetTables(filters);
                
                Assert.AreEqual(2,actual.Rows.Count);
                connection.Close();
            }
            /*
            /// <summary>
            ///A test for GetUsers
            ///</summary>
            [TestMethod()]
            public void GetUsersTest()
            {
                CUBRIDConnection connection = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable expected = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetUsers(filters);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }
            */
            /// <summary>
            ///A test for GetViews
            ///</summary>
            [TestMethod()]
            public void GetViewsTest_null()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString=DBHelper.connString;
                connection.Open();
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetViews(filters);
                Assert.AreEqual(0, actual.Rows.Count);
            }

            [TestMethod()]
            public void GetViewsTest()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                DBHelper.ExecuteSQL("drop view if exists test_view", connection);
                DBHelper.ExecuteSQL("create view test_view as select * from code;", connection);
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetViews(filters);
                Assert.AreEqual(1, actual.Rows.Count);
            }

            [TestMethod()]
            public void GetViewsTest2()
            {
                CUBRIDConnection connection = new CUBRIDConnection(); // TODO: Initialize to an appropriate value
                connection.ConnectionString = DBHelper.connString;
                connection.Open();
                DBHelper.ExecuteSQL("drop view if exists test_view", connection);
                DBHelper.ExecuteSQL("create view test_view as select * from code;", connection);
                CUBRIDSchemaProvider target = new CUBRIDSchemaProvider(connection); // TODO: Initialize to an appropriate value
                string[] filters = null; // TODO: Initialize to an appropriate value
                DataTable actual;
                actual = target.GetViews(filters);
                //Assert.AreEqual(1, actual.Rows.Count);
                Console.WriteLine(actual.Rows[0][0]);
            }

            /*
            /// <summary>
            ///A test for LoadTableColumns
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void LoadTableColumnsTest()
            {
                PrivateObject param0 = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor target = new CUBRIDSchemaProvider_Accessor(param0); // TODO: Initialize to an appropriate value
                DataTable dt = null; // TODO: Initialize to an appropriate value
                string schema = string.Empty; // TODO: Initialize to an appropriate value
                string tableName = string.Empty; // TODO: Initialize to an appropriate value
                string columnRestriction = string.Empty; // TODO: Initialize to an appropriate value
                target.LoadTableColumns(dt, schema, tableName, columnRestriction);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");
            }

            /// <summary>
            ///A test for QuoteDefaultValues
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void QuoteDefaultValuesTest()
            {
                PrivateObject param0 = null; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor target = new CUBRIDSchemaProvider_Accessor(param0); // TODO: Initialize to an appropriate value
                DataTable dt = null; // TODO: Initialize to an appropriate value
                target.QuoteDefaultValues(dt);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");
            }

            /// <summary>
            ///A test for SetDataTypeInfo
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void SetDataTypeInfoTest()
            {
                DataTable dsTable = null; // TODO: Initialize to an appropriate value
                string typeName = string.Empty; // TODO: Initialize to an appropriate value
                CUBRIDDataType cubridDataType = new CUBRIDDataType(); // TODO: Initialize to an appropriate value
                Type dataType = null; // TODO: Initialize to an appropriate value
                bool isAutoincrementable = false; // TODO: Initialize to an appropriate value
                bool isFixedLength = false; // TODO: Initialize to an appropriate value
                bool isFixedPrecisionScale = false; // TODO: Initialize to an appropriate value
                bool isLong = false; // TODO: Initialize to an appropriate value
                bool isNullable = false; // TODO: Initialize to an appropriate value
                CUBRIDSchemaProvider_Accessor.SetDataTypeInfo(dsTable, typeName, cubridDataType, dataType, isAutoincrementable, isFixedLength, isFixedPrecisionScale, isLong, isNullable);
                Assert.Inconclusive("A method that does not return a value cannot be verified.");
            }

            /// <summary>
            ///A test for ToBool
            ///</summary>
            [TestMethod()]
            [DeploymentItem("CUBRID.Data.dll")]
            public void ToBoolTest()
            {
                object val = null; // TODO: Initialize to an appropriate value
                bool expected = false; // TODO: Initialize to an appropriate value
                bool actual;
                actual = CUBRIDSchemaProvider_Accessor.ToBool(val);
                Assert.AreEqual(expected, actual);
                Assert.Inconclusive("Verify the correctness of this test method.");
            }
    
    * 
    */
        }
    }
}
