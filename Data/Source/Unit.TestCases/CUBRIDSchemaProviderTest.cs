using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDSchemaProviderTest and is intended
    ///to contain all CUBRIDSchemaProviderTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDSchemaProviderTest
    {
        private static readonly string connString = "server=10.34.64.122;database=demodb;port=33530;user=public;password=";

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
        ///A test for GetForeignKeys
        ///</summary>
        [TestMethod()]
        public void GetForeignKeysTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetForeignKeys(new string[] { "game" });

                Assert.IsTrue(dt.Columns.Count == 9);
                Assert.IsTrue(dt.Rows.Count == 2);

                Assert.IsTrue(dt.Rows[0][0].ToString() == "athlete");
                Assert.IsTrue(dt.Rows[0][1].ToString() == "code");
                Assert.IsTrue(dt.Rows[0][2].ToString() == "game");
                Assert.IsTrue(dt.Rows[0][3].ToString() == "athlete_code");
                Assert.IsTrue(dt.Rows[0][4].ToString() == "1");
                Assert.IsTrue(dt.Rows[0][5].ToString() == "1");
                Assert.IsTrue(dt.Rows[0][6].ToString() == "1");
                Assert.IsTrue(dt.Rows[0][7].ToString() == "fk_game_athlete_code");
                Assert.IsTrue(dt.Rows[0][8].ToString() == "pk_athlete_code");

                Assert.IsTrue(dt.Rows[1][0].ToString() == "event");
                Assert.IsTrue(dt.Rows[1][1].ToString() == "code");
                Assert.IsTrue(dt.Rows[1][2].ToString() == "game");
                Assert.IsTrue(dt.Rows[1][3].ToString() == "event_code");
                Assert.IsTrue(dt.Rows[1][4].ToString() == "1");
                Assert.IsTrue(dt.Rows[1][5].ToString() == "1");
                Assert.IsTrue(dt.Rows[1][6].ToString() == "1");
                Assert.IsTrue(dt.Rows[1][7].ToString() == "fk_game_event_code");
                Assert.IsTrue(dt.Rows[1][8].ToString() == "pk_event_code");
            }
        }

        /// <summary>
        ///A test for GetTables
        ///</summary>
        [TestMethod()]
        public void GetTablesTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetTables(new string[] { "%" });

                Assert.AreEqual(dt.Columns.Count, 3);
                Assert.AreEqual(dt.Rows.Count, 10);

                Assert.AreEqual(dt.Rows[0][0].ToString(), "demodb");
                Assert.AreEqual(dt.Rows[0][1].ToString(), "demodb");
                Assert.AreEqual(dt.Rows[0][2].ToString(), "stadium");
            }
        }

        /// <summary>
        ///A test for GetColumns
        ///</summary>
        [TestMethod()]
        public void GetColumnsTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetColumns(new string[] { "game" });

                Assert.IsTrue(dt.Columns.Count == 11);
                Assert.IsTrue(dt.Rows.Count == 7);

                Assert.IsTrue(dt.Rows[0][3].ToString() == "host_year");
                Assert.IsTrue(dt.Rows[1][3].ToString() == "event_code");
            }
        }

        /// <summary>
        ///A test for GetIndexes
        ///</summary>
        [TestMethod()]
        public void GetIndexesTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetIndexes(new string[] { "game" });

                Assert.IsTrue(dt.Columns.Count == 9);
                Assert.IsTrue(dt.Rows.Count == 5);

                Assert.IsTrue(dt.Rows[3][2].ToString() == "pk_game_host_year_event_code_athlete_code"); //Index name
                Assert.IsTrue(dt.Rows[3][4].ToString() == "True"); //Is PK?
            }
        }

        /// <summary>
        ///A test for GetUsers
        ///</summary>
        [TestMethod()]
        public void GetUsersTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetUsers(null);

                Assert.IsTrue(dt.Columns.Count == 1);
                Assert.IsTrue(dt.Rows.Count >= 2);

                Assert.IsTrue(dt.Rows[0][0].ToString().ToUpper() == "DBA");
                Assert.IsTrue(dt.Rows[1][0].ToString().ToUpper() == "PUBLIC");
            }
        }

        /// <summary>
        ///A test for GetViews
        ///</summary>
        [TestMethod()]
        public void GetViewsTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetViews(null);

                Assert.IsTrue(dt.Columns.Count == 3);
                Assert.IsTrue(dt.Rows.Count == 0);
            }
        }

        /// <summary>
        ///A test for GetDatabases
        ///</summary>
        [TestMethod()]
        public void GetDatabasesTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetDatabases(new string[] { "demo%" });

                Assert.IsTrue(dt.Columns.Count == 2);
                Assert.IsTrue(dt.Rows.Count >= 1);

                Assert.IsTrue(dt.Rows[0][0].ToString() == "demodb");
                Assert.IsTrue(dt.Rows[0][1].ToString() == "demodb");
            }
        }

        /// <summary>
        ///A test for GetProcedures
        ///</summary>
        [TestMethod()]
        public void GetProceduresTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetProcedures(null);

                Assert.AreEqual(dt.Columns.Count, 7);
                Assert.AreEqual(dt.Rows.Count, 0);

                //Assert.IsTrue(dt.Rows[0][0].ToString() == "sp1");
                //Assert.IsTrue(dt.Rows[1][0].ToString() == "sp2");
            }
        }

        /// <summary>
        ///A test for GetIndexColumns
        ///</summary>
        [TestMethod()]
        public void GetIndexColumnsTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
                DataTable dt = schema.GetIndexColumns(new string[] { "game" });

                Assert.IsTrue(dt.Columns.Count == 7);
                Assert.IsTrue(dt.Rows.Count == 5);

                Assert.IsTrue(dt.Rows[0][2].ToString() == "pk_game_host_year_event_code_athlete_code");
            }
        }

        /// <summary>
        /// Test CUBRIDSchemaProvider class
        /// </summary>
        [TestMethod()]
        public void Test_SchemaProvider_FunctionTypes()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                DataTable rw = CUBRIDSchemaProvider.GetReservedWords();
                string[] nf = CUBRIDSchemaProvider.GetNumericFunctions();
                string[] sf = CUBRIDSchemaProvider.GetStringFunctions();

                Assert.IsTrue(nf.GetValue(0).ToString() == "AVG");
                Assert.IsTrue(nf.GetValue(nf.Length - 1).ToString() == "VARIANCE");

                Assert.IsTrue(sf.GetValue(0).ToString() == "BIT_LENGTH");
                Assert.IsTrue(sf.GetValue(sf.Length - 1).ToString() == "UPPER");

                Assert.IsTrue(rw.Rows[0][0].ToString() == "ADD");
                Assert.IsTrue(rw.Rows[rw.Rows.Count - 1][0].ToString() == "TO_DATETIME");
            }
        }

        /// <summary>
        /// Test CUBRIDSchemaProvider data types functions
        /// </summary>
        [TestMethod()]
        public void Test_SchemaProvider_DataTypes()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDSchemaProviderTest.connString;
                conn.Open();

                DataTable dt = CUBRIDSchemaProvider.GetDataTypes();

                Assert.IsTrue(dt.Rows.Count == 19);

                //SetDataTypeInfo(dt, "BIGINT", CUBRIDDataType.CCI_U_TYPE_BIGINT, typeof(Int32), 
                //ToBool(IsAutoIncrementable.Yes), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.Yes), ToBool(IsNullable.Yes));
                Assert.IsTrue(dt.Rows[0]["TypeName"].ToString() == "BIGINT");
                Assert.IsTrue((CUBRIDDataType)dt.Rows[0]["ProviderDataType"] == CUBRIDDataType.CCI_U_TYPE_BIGINT);
                Assert.IsTrue((Type)dt.Rows[0]["DbType"] == typeof(Int32));
                Assert.IsTrue(dt.Rows[0]["Size"].ToString() == String.Empty);
                Assert.IsTrue((bool)dt.Rows[0]["IsLong"] == true);
                Assert.IsTrue((bool)dt.Rows[0]["IsFixedLength"] == true);
                Assert.IsTrue((bool)dt.Rows[0]["IsFixedPrecisionScale"] == true);
                Assert.IsTrue((bool)dt.Rows[0]["IsNullable"] == true);
                Assert.IsTrue((bool)dt.Rows[0]["IsAutoIncrementable"] == true);
            }
        }
    }
}
