using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDDataReaderTest and is intended
    ///to contain all CUBRIDDataReaderTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDDataReaderTest
    {
        private static readonly string connString = "server=10.34.64.122;database=demodb;port=33690;user=public;password=";

        private static void ExecuteSQL(string sql, CUBRIDConnection conn)
        {
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

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
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod()]
        public void DataReader_Basic_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";

                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.IsTrue(reader.FieldCount == 4);
                        Assert.IsTrue(reader.GetString(0) == "AFG");
                        Assert.IsTrue(reader.GetString(1) == "Afghanistan");
                        Assert.IsTrue(reader.GetString(2) == "Asia");
                        Assert.IsTrue(reader.GetString(3) == "Kabul");
                    }
                }
            }
        }

        /// <summary>
        /// Test SQL statements execution, using DataReader and parameters
        /// </summary>
        [TestMethod()]
        public void Test_DataReader_Parameters()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                using (CUBRIDCommand cmd = new CUBRIDCommand("select `code` from nation where capital = ?", conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter("?", CUBRIDDataType.CCI_U_TYPE_STRING);
                    //param.ParameterName = "?";
                    //param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                    param.Value = "Kabul";

                    cmd.Parameters.Add(param);

                    DbDataReader reader = cmd.ExecuteReader();

                    Assert.IsTrue(reader.FieldCount == 1);

                    while (reader.Read()) //only one row is available
                    {
                        Assert.IsTrue(reader.GetString(0) == "AFG");
                    }

                    cmd.Close();
                }
            }
        }

        /// <summary>
        /// Test CUBRIDDataReader getter methods
        /// </summary>
        [TestMethod()]
        public void Test_DataReader_Getxxx()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                string sql = "select * from nation;";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        Assert.IsTrue(reader.GetOrdinal("code") == 0);
                        Assert.IsTrue(reader.GetName(0) == "code");
                        Assert.IsTrue(reader.GetColumnName(0) == "code");
                        Assert.IsTrue(reader.GetColumnType(0) == typeof(System.String));
                        Assert.IsTrue(reader.GetDataTypeName(0) == "CHAR");

                        reader.Close();
                    }
                }
            }
        }

        /// <summary>
        ///A test for GetBytes
        ///</summary>
        [TestMethod()]
        public void GetBytesTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                /* 782E79CE-2FDD-4D27-A300-E088F8DD76BE */
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable (clsid BLOB);", conn);
                
                byte[] bytes = new byte[36]{55, 56, 50, 69, 55, 57, 67, 69,
                                            45, 50, 70, 68, 68, 45, 52, 68,
                                            50, 55, 45, 65, 51, 48, 48, 45,
                                            69, 48, 56, 56, 70, 56, 68, 68,
                                            55, 54, 66, 69};

                string sql = "INSERT INTO TestTable VALUES (?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDBlob Blob = new CUBRIDBlob(conn);
                    Blob.setBytes(1, bytes);

                    CUBRIDParameter param = new CUBRIDParameter(Blob);
                    param.ParameterName = "?p";
                    //param.Value = Blob;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].DbType = DbType.Binary;
                    //cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        byte[] buffer = new byte[34];
                        long len = reader.GetBytes(0, 2, buffer, 0, 34);
                        ASCIIEncoding encoding = new ASCIIEncoding();
                        string clsid = encoding.GetString(buffer);
                        Assert.AreEqual(clsid, "2E79CE-2FDD-4D27-A300-E088F8DD76BE");

                        len = reader.GetBytes(0, 0, null, 0, 36);
                        Assert.AreEqual(len, 0);

                        try
                        {
                            len = reader.GetBytes(0, 0, buffer, 37, 36);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            len = reader.GetBytes(0, 0, buffer, 0, 38);
                        }
                        catch (Exception ex)
                        {
                            Assert.AreEqual(ex.Message, "Buffer is not large enough to hold the requested data!");
                        }

                        try
                        {
                            len = reader.GetBytes(0, -1, buffer, 0, 36);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetBytes(0, 3, buffer, 0, 34);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        /// <summary>
        ///A test for GetBytes
        ///</summary>
        [TestMethod()]
        public void GetBytesTest2()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                /* 782E79CE-2FDD-4D27-A300-E088F8DD76BE */
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable (clsid string);", conn);

                CUBRIDDataReaderTest.ExecuteSQL("INSERT INTO TestTable VALUES ('" +
                                                "782E79CE-2FDD-4D27-A300-E088F8DD76BE');", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        byte[] buffer = new byte[36];
                        try
                        {
                            long len = reader.GetBytes(0, 0, buffer, 0, 36);
                        }
                        catch(Exception ex)
                        {
                            Assert.AreEqual(ex.Message, "GetBytes() can be called only on binary (BLOB, CLOB) columns!");
                        }

                        try
                        {
                            long len = reader.GetBytes(1, 2, buffer, 0, 36);
                        }
                        catch(Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        /// <summary>
        ///A test for GetBytes
        ///</summary>
        [TestMethod()]
        public void GetBytesTest3()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                /* 782E79CE-2FDD-4D27-A300-E088F8DD76BE */
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable (clsid CLOB);", conn);

                string strClsID = "782E79CE-2FDD-4D27-A300-E088F8DD76BE";

                string sql = "INSERT INTO TestTable VALUES (?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDClob Clob = new CUBRIDClob(conn);
                    Clob.setString(1, strClsID);

                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?";
                    param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                    param.Value = Clob;
                    cmd.Parameters.Add(param);
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        byte[] buffer = new byte[35];
                        long len = reader.GetBytes(0, 1, buffer, 0, 35);
                        string clsid = conn.GetEncoding().GetString(buffer);
                        Assert.AreEqual(clsid, "82E79CE-2FDD-4D27-A300-E088F8DD76BE");

                        try
                        {
                            reader.GetBytes(0, 2, buffer, 0, 35);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        /// <summary>
        ///A test for GetGuid
        ///</summary>
        [TestMethod()]
        public void GetGuidTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                /* 782E79CE-2FDD-4D27-A300-E088F8DD76BE */
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable (clsid string);", conn);

                CUBRIDDataReaderTest.ExecuteSQL("INSERT INTO TestTable VALUES " +
                                            "('782E79CE-2FDD-4D27-A300-E088F8DD76BE');", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        Guid guid= new Guid("782E79CE-2FDD-4D27-A300-E088F8DD76BE");
                        Assert.AreEqual(reader.GetGuid(0), guid);
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        /// <summary>
        ///A test for GetChars
        ///</summary>
        [TestMethod()]
        public void GetCharsTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable(ID integer AUTO_INCREMENT," +
                        "name character(255)," +
                        "Age bigint," +
                        "Age2 smallint," +
                        "Birth date," +
                        "Gender bit(1)," +
                        "Nationality national character(24)," +
                        "Province national character(32)," +
                        "City national character(32)," +
                        "Specialty national character varying(4096)," +
                        "Annual_salary double," +
                        "Luck float," +
                        "Status numeric(15,0)," +
                        "timeTest time," +
                        "Registered datetime," +
                        "Comment character varying(4096));", conn);

                CUBRIDDataReaderTest.ExecuteSQL("INSERT INTO TestTable (name, age, age2, birth, gender, nationality, province, city, " +
                     "specialty, annual_salary, Luck, status, timeTest, registered, comment) VALUES ('KaierGen', 28, 28, " +
                     "'1984-07-05', B'0', N'China', N'Beijing', N'Beijing', N'nothing', 70000, 67.90, 23000, " +
                     "'12:07:39', '2011-04-01 12:07:12', 'This IS test!');", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetColumnType(0), typeof(Int32));
                        Assert.AreEqual(reader.GetColumnType(1), typeof(String));
                        Assert.AreEqual(reader.GetColumnType(2), typeof(Int64));
                        Assert.AreEqual(reader.GetColumnType(3), typeof(Int16));
                        Assert.AreEqual(reader.GetColumnType(4), typeof(DateTime));
                        Assert.AreEqual(reader.GetColumnType(5), typeof(Byte[]));
                        Assert.AreEqual(reader.GetColumnType(6), typeof(String));
                        Assert.AreEqual(reader.GetColumnType(7), typeof(String));
                        Assert.AreEqual(reader.GetColumnType(8), typeof(String));
                        Assert.AreEqual(reader.GetColumnType(9), typeof(String));
                        Assert.AreEqual(reader.GetColumnType(10), typeof(Double));
                        Assert.AreEqual(reader.GetColumnType(11), typeof(float));
                        Assert.AreEqual(reader.GetColumnType(12), typeof(Decimal));
                        Assert.AreEqual(reader.GetColumnType(13), typeof(DateTime));
                        Assert.AreEqual(reader.GetColumnType(14), typeof(DateTime));
                        Assert.AreEqual(reader.GetColumnType(15), typeof(String));

                        Assert.AreEqual(reader.GetColumnTypeName(0), "INT");
                        Assert.AreEqual(reader.GetColumnTypeName(1), "CHAR");
                        Assert.AreEqual(reader.GetColumnTypeName(4), "DATE");
                        Assert.AreEqual(reader.GetColumnTypeName(5), "BIT");
                        Assert.AreEqual(reader.GetColumnTypeName(6), "NCHAR");
                        Assert.AreEqual(reader.GetColumnTypeName(7), "NCHAR");
                        Assert.AreEqual(reader.GetColumnTypeName(8), "NCHAR");
                        Assert.AreEqual(reader.GetColumnTypeName(9), "VARCHAR");
                        Assert.AreEqual(reader.GetColumnTypeName(10), "DOUBLE");
                        Assert.AreEqual(reader.GetColumnTypeName(11), "FLOAT");
                        Assert.AreEqual(reader.GetColumnTypeName(13), "TIME");
                        Assert.AreEqual(reader.GetColumnTypeName(14), "DATETIME");
                        Assert.AreEqual(reader.GetColumnTypeName(15), "STRING");

                        Assert.AreEqual(reader.GetTime(14), "12:07:12");
                        Assert.AreEqual(reader.GetDate(14), "2011-04-01");

                        DateTime dt = reader.GetDateTime(14);
                        Assert.AreEqual(dt.ToString("yyyy-MM-dd HH:mm:ss"), "2011-04-01 12:07:12");

                        Assert.AreEqual(reader.GetShort(3), 28);

                        char[] buffer = new char[256];
                        long length = reader.GetChars(1, 0, buffer, 0, 256);
                        System.Text.StringBuilder sb = new System.Text.StringBuilder();
                        sb.Append(buffer);
                        string value = sb.ToString().Substring(0, 8);
                        Assert.AreEqual(value, "KaierGen");

                        try
                        {
                            reader.GetChars(16, 0, buffer, 0, 256);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetChars(1, 0, buffer, -1, 256);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetChars(1, -1, buffer,0, 256);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetChars(1, 0, buffer, 0, 300);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as ArgumentException) != null);
                        }


                        value = reader.GetString(1).Substring(0, 8);
                        Assert.AreEqual(value, "KaierGen");

                        value = reader.GetString("name").Substring(0, 8);
                        Assert.AreEqual(value, "KaierGen");

                        try
                        {
                            value = reader.GetName(-1);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        reader.Close();
                        try
                        {
                            value = reader.GetName(0);
                        }
                        catch (Exception ex)
                        {
                            Assert.AreEqual(ex.Message, "Resultset is closed!");
                        }
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }



        /// <summary>
        ///A test for GetArray
        ///</summary>
        [TestMethod()]
        public void GetArrayTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable CLASS ATTRIBUTE(name character(10))" +
                                                "(id integer AUTO_INCREMENT, name character(10));", conn);

                CUBRIDDataReaderTest.ExecuteSQL("INSERT INTO TestTable(name) VALUES ('1234567890');", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        object[] buffer = reader.GetArray(1);
                        Assert.AreEqual(buffer, null);

                        Assert.AreEqual(reader.RecordsAffected, 1);
                        Assert.AreEqual(reader.Depth, 0);

                        bool gender = reader.GetBoolean(1);
                        Assert.AreEqual(gender, false);

                        try
                        {
                            reader.GetColumnName(3);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        Assert.IsFalse(reader.IsDBNull(0));
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        /// <summary>
        ///A test for GetColumnTypeName
        ///</summary>
        [TestMethod()]
        public void GetColumnTypeNameTest()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable(id integer AUTO_INCREMENT, name character(10), stamp timestamp);", conn);

                CUBRIDDataReaderTest.ExecuteSQL("INSERT INTO TestTable(name) VALUES ('1234567890');", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetColumnType(2), typeof(DateTime));
                        Assert.AreEqual(reader.GetColumnTypeName(2), "TIMESTAMP");
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }
        
        /// <summary>
        ///A test for DataType:SET
        ///</summary>
        [TestMethod()]
        public void DataType_Set_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE testtable(clsid set_of(character(36)));", conn);

                CUBRIDDataReaderTest.ExecuteSQL("INSERT INTO testtable VALUES ( {'782E79CE-2FDD-4D27-A300-E088F8DD76BE'} );", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(reader.GetColumnTypeName(0), "SET");
                        Assert.AreEqual(reader.GetColumnType(0), typeof(Object[]));
                        object[] vals = (object[])reader["clsid"];
                        Assert.AreEqual(vals[0], "782E79CE-2FDD-4D27-A300-E088F8DD76BE");
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        /// <summary>
        ///A test for DataType:MultiSet
        ///</summary>
        [TestMethod()]
        public void DataType_MultiSet_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE testtable(clsid multiset_of(INTEGER));", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        Assert.AreEqual(reader.HasRows, false);
                        Assert.AreEqual(reader.GetColumnType(0), typeof(Object[]));
                        Assert.AreEqual(reader.GetColumnTypeName(0), "MULTISET");

                        try
                        {
                            reader.GetColumnName(-1);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetColumnTypeName(-1);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetColumnTypeName(2);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetColumnType(-1);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }

                        try
                        {
                            reader.GetColumnType(2);
                        }
                        catch (Exception ex)
                        {
                            Assert.IsTrue((ex as IndexOutOfRangeException) != null);
                        }
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        [TestMethod()]
        public void NULL_Parameter_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable (clsid BLOB);", conn);

                string sql = "INSERT INTO TestTable VALUES (?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = null;
                    cmd.Parameters.Add(param);
                    //cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        [TestMethod()]
        public void NULL_Type_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDDataReaderTest.connString;
                conn.Open();

                CUBRIDDataReaderTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDDataReaderTest.ExecuteSQL("CREATE TABLE TestTable (clsid string);", conn);

                string sql = "INSERT INTO TestTable VALUES (?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter(CUBRIDDataType.CCI_U_TYPE_NULL);
                    param.ParameterName = "?p";
                    param.Value = "this is test";
                    cmd.Parameters.Add(param);
                    //cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NULL;
                    //cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.ExecuteNonQuery();

                    Assert.AreEqual(param.Size, 12);
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        Assert.AreEqual(reader.HasRows, true);
                    }
                }

                CUBRIDDataReaderTest.ExecuteSQL("drop TestTable;", conn);
            }
        }
    }
}
