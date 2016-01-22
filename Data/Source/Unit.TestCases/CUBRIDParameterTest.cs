using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDParameterTest and is intended
    ///to contain all CUBRIDParameterTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDParameterTest
    {
        private static readonly string connString = "server=10.34.64.122;database=demodb;port=33510;user=public;password=";

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
        ///A test for CUBRIDParameter Constructor
        ///</summary>
        [TestMethod()]
        public void CUBRIDParameterConstructorTest()
        {
            CUBRIDParameter target = new CUBRIDParameter();
            
            target.Value = true;
            Assert.AreEqual(target.DbType, DbType.Boolean);

            byte[] bytes = new byte[36]{55, 56, 50, 69, 55, 57, 67, 69,
                                            45, 50, 70, 68, 68, 45, 52, 68,
                                            50, 55, 45, 65, 51, 48, 48, 45,
                                            69, 48, 56, 56, 70, 56, 68, 68,
                                            55, 54, 66, 69};
            target.Value = bytes;
            Assert.AreEqual(target.DbType, DbType.Object);
            Assert.AreEqual(target.CUBRIDDataType, CUBRIDDataType.CCI_U_TYPE_OBJECT);

            target.Value = bytes[0];
            Assert.AreEqual(target.DbType, DbType.Byte);

            target.Value = Int16.MaxValue;
            Assert.AreEqual(target.DbType, DbType.Int16);

            target.Value = UInt16.MaxValue;
            Assert.AreEqual(target.DbType, DbType.UInt16);

            target.Value = Int32.MaxValue;
            Assert.AreEqual(target.DbType, DbType.Int32);            
            
            target.Value = UInt32.MaxValue;
            Assert.AreEqual(target.DbType, DbType.UInt32);

            target.Value = Int64.MaxValue;
            Assert.AreEqual(target.DbType, DbType.Int64);

            target.Value = UInt64.MaxValue;
            Assert.AreEqual(target.DbType, DbType.UInt64);

            target.Value = new DateTime(2012, 08, 02, 15, 05, 05);
            Assert.AreEqual(target.DbType, DbType.DateTime);

            target.Value = "this is test!!!!";
            Assert.AreEqual(target.DbType, DbType.String);

            target.Value = (double)12.667;
            Assert.AreEqual(target.DbType, DbType.Double);

            target.Value = (decimal)12.667;
            Assert.AreEqual(target.DbType, DbType.Decimal);

            DateTime dt1 = DateTime.Now;
            target.Value = new TimeSpan(dt1.Ticks);
            Assert.AreEqual(target.DbType, DbType.UInt64);
        }

        /// <summary>
        ///A test for DbType
        ///</summary>
        [TestMethod()]
        public void DbTypeTest()
        {
            CUBRIDParameter target = new CUBRIDParameter(); // TODO: Initialize to an appropriate value

            target.DbType = DbType.Binary;
            target.DbType = DbType.Boolean;
            target.DbType = DbType.Byte;
            target.DbType = DbType.Currency;
            target.DbType = DbType.Date;
            target.DbType = DbType.DateTime;
            target.DbType = DbType.Decimal;
            target.DbType = DbType.Double;
            target.DbType = DbType.Int16;
            target.DbType = DbType.Int32;
            target.DbType = DbType.Int64;

            target.DbType = DbType.Object;
            target.DbType = DbType.SByte;
            target.DbType = DbType.Single;
            target.DbType = DbType.String;
            target.DbType = DbType.StringFixedLength;
            target.DbType = DbType.Time;
            target.DbType = DbType.UInt16;
            target.DbType = DbType.UInt32;
            target.DbType = DbType.UInt64;
        }


        /// <summary>
        ///A test for GetBytes
        ///</summary>
        [TestMethod()]
        public void Write_Bytes_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                /* 782E79CE-2FDD-4D27-A300-E088F8DD76BE */
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("drop table if exists TestTable", conn);

                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE TestTable (clsid bit(288));", conn);

                byte[] bytes = new byte[36]{55, 56, 50, 69, 55, 57, 67, 69,
                                            45, 50, 70, 68, 68, 45, 52, 68,
                                            50, 55, 45, 65, 51, 48, 48, 45,
                                            69, 48, 56, 56, 70, 56, 68, 68,
                                            55, 54, 66, 69};

                string sql = "INSERT INTO TestTable VALUES (?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = bytes;
                    cmd.Parameters.Add(param);
                    //cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIT;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        byte[] buffer = (byte[])reader.GetValue(0);
                        string clsid = conn.GetEncoding().GetString(buffer);
                        Assert.AreEqual(clsid, "782E79CE-2FDD-4D27-A300-E088F8DD76BE");
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("drop TestTable;", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(int))", conn);
                //Insert some data in the sequence column
                int[] iArray = new int[7]{1, 2, 3, 4, 5, 6, 7};
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = iArray;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
                    cmd.Parameters.Add(param);
                    //cmd.Parameters[0].DbType = DbType.Binary;
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_String()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(string))", conn);
                //Insert some data in the sequence column
                string[] sArray = new string[7]{"Performance implications", 
                                                "Types of fragmentation", 
                                                "File fragmentation",
                                                "Free space fragmentation",
                                                "File scattering", 
                                                "File system fragmentation", 
                                                "Preventing fragmentation"};
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = sArray;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_Bit()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s set_of(bit(8)))", conn);
                //Insert some data in the sequence column
                byte[] bytes = new byte[7] { 55, 56, 50, 69, 51, 57, 67 };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = bytes;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIT;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using(CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using(CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_Bit2()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s set_of(bit(8)))", conn);
                try
                {
                    //Insert some data in the sequence column
                    byte[][] bytes = new byte[2][];
                    bytes[0] = new byte[7] { 55, 56, 50, 69, 51, 57, 67 };
                    bytes[1] = new byte[7] { 65, 66, 60, 79, 61, 67, 77 };
                    string sql = "INSERT INTO t(s) VALUES(?);";
                    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                    {
                        CUBRIDParameter param = new CUBRIDParameter();
                        param.ParameterName = "?p";
                        param.Value = bytes;
                        param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIT;
                        cmd.Parameters.Add(param);
                        cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    Assert.IsTrue((ex as  NotImplementedException) != null);
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_BigInt()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(BIGINT))", conn);
                //Insert some data in the sequence column
                Int64[] bytes = new Int64[7] { 55, 56, 50, 69, 55, 57, 67 };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = bytes;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIGINT;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 6);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_Short()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(SHORT))", conn);
                //Insert some data in the sequence column
                Int16[] bytes = new Int16[7] { 55, 56, 50, 69, 55, 57, 67 };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = bytes;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SHORT;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 6);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_Float()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(FLOAT))", conn);
                //Insert some data in the sequence column
                float[] floats = new float[7] { 55.55F, 56.8923F, 50.57F, 69.69F, 55.58F, 57.978F, 67.57689F };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = floats;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_FLOAT;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }
        
        [TestMethod()]
        public void Test_Write_Collection_Double()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(DOUBLE))", conn);
                //Insert some data in the sequence column
                double[] floats = new double[7] {55.55, 112367.67, 50.57, 69.69, 55.58, 57.978, 67.57689 };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = floats;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DOUBLE;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_NULL()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(DOUBLE))", conn);
                //Insert some data in the sequence column
                double[] floats = new double[7] { 55.55, 56.5, 50.57, 69.69, 55.58, 57.978, 67.57689 };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = floats;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NULL;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_Date()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(DATE))", conn);
                //Insert some data in the sequence column
                DateTime[] dates = new DateTime[7] { new DateTime(1989, 6, 4, 21, 9, 37), 
                                                     new DateTime(1976, 1, 8, 12, 19, 02), 
                                                     new DateTime(1984, 7, 5, 8, 17, 12), 
                                                     new DateTime(1977, 3, 28, 23, 23, 23), 
                                                     new DateTime(1980, 1, 12, 11, 11, 5), 
                                                     new DateTime(2012, 8, 10, 15, 13, 57), 
                                                     new DateTime(2034, 12, 12, 18, 01, 12)
                                                    };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = dates;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DATE;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_Time()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(TIME))", conn);
                //Insert some data in the sequence column
                DateTime[] dates = new DateTime[7] { new DateTime(1989, 6, 4, 21, 9, 37), 
                                                     new DateTime(1976, 1, 8, 12, 19, 02), 
                                                     new DateTime(1984, 7, 5, 8, 17, 12), 
                                                     new DateTime(1977, 3, 28, 23, 23, 23), 
                                                     new DateTime(1980, 1, 12, 11, 11, 5), 
                                                     new DateTime(2012, 8, 10, 15, 13, 57), 
                                                     new DateTime(2034, 12, 12, 18, 01, 12)
                                                    };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = dates;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_TIME;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        [TestMethod()]
        public void Test_Write_Collection_DateTime()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(DATETIME))", conn);
                //Insert some data in the sequence column
                DateTime[] dates = new DateTime[7] { new DateTime(1989, 6, 4, 21, 9, 37), 
                                                     new DateTime(1976, 1, 8, 12, 19, 02), 
                                                     new DateTime(1984, 7, 5, 8, 17, 12), 
                                                     new DateTime(1977, 3, 28, 23, 23, 23), 
                                                     new DateTime(1980, 1, 12, 11, 11, 5), 
                                                     new DateTime(2012, 8, 10, 15, 13, 57), 
                                                     new DateTime(2034, 12, 12, 18, 01, 12, 201)
                                                    };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = dates;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_DATETIME;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }


                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }
                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }

        /// <summary>
        /// UnitTest: CUBRIDStream WriteCollection, date type CCI_U_TYPE_TIMESTAMP
        /// </summary>
        [TestMethod()]
        public void Test_Write_Collection_TimeStamp()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = CUBRIDParameterTest.connString;
                conn.Open();

                // delete old table
                CUBRIDParameterTest.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

                //Create a new table with a collection
                CUBRIDParameterTest.ExecuteSQL("CREATE TABLE t(s SET(TIMESTAMP))", conn);
                //Insert some data in the sequence column
                DateTime[] dates = new DateTime[7] { new DateTime(1989, 6, 4, 21, 9, 37), 
                                                     new DateTime(1976, 1, 8, 12, 19, 02), 
                                                     new DateTime(1984, 7, 5, 8, 17, 12), 
                                                     new DateTime(1977, 3, 28, 23, 23, 23), 
                                                     new DateTime(1980, 1, 12, 11, 11, 5), 
                                                     new DateTime(2012, 8, 10, 15, 13, 57), 
                                                     new DateTime(2034, 12, 12, 18, 01, 12)
                                                    };
                string sql = "INSERT INTO t(s) VALUES(?);";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    CUBRIDParameter param = new CUBRIDParameter();
                    param.ParameterName = "?p";
                    param.Value = dates;
                    param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_TIMESTAMP;
                    cmd.Parameters.Add(param);
                    cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                    cmd.ExecuteNonQuery();
                }

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();

                        object objValue = reader.GetValue(0);
                        Array oArray = objValue as Array;
                        Assert.IsTrue(oArray != null);
                        if (oArray != null)
                            Assert.AreEqual(oArray.Length, 7);
                    }
                }

                // delete talble
                CUBRIDParameterTest.ExecuteSQL("DROP t", conn);
            }
        }
    }
}
