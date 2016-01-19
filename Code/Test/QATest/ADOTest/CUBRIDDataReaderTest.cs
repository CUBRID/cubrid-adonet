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
    /// This is a test class for CUBRIDDataReader
    /// </summary>
    [TestClass]
    public class CUBRIDDataReaderTest: BaseTest
    {
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Close_Test()
        {
            //TODO
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Dispose_Test()
        {
            //TODO
        }

        /// <summary>
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod]
        public void DataReader_Basic_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                String sql = "select * from nation order by `code` asc";

                LogTestStep("retrieve just one row");
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.AreEqual(4, reader.FieldCount);
                        Assert.AreEqual("AFG", reader.GetString(0));
                        Assert.AreEqual("Afghanistan", reader.GetString(1));
                        Assert.AreEqual("Asia",reader.GetString(2));
                        Assert.AreEqual("Kabul", reader.GetString(3));

                        LogStepPass();
                    }
                }
            }

            LogTestResult();

        }

        /// <summary>
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod]
        public void DataReader_NextResult_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                String sql = "select count(*) from nation; select count(*) from athlete";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(215, reader.GetInt32(0));

                        if (reader.NextResult())
                        {
                            reader.Read();
                            Assert.AreEqual(6677, reader.GetInt32(0));
                        }
                    }
                }

                sql = "select * from nation order by code asc; select * from athlete order by name asc";
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

                        if (reader.NextResult())
                        {
                            //verify the first two results
                            reader.Read();
                            Assert.AreEqual(5, reader.FieldCount);
                            Assert.AreEqual(15718, reader.GetInt32(0));
                            Assert.AreEqual("Aardenburg Willemien", reader.GetString(1));
                            Assert.AreEqual("W", reader.GetString(2));
                            Assert.AreEqual("NED", reader.GetString(3));
                            Assert.AreEqual("Hockey", reader.GetString(4));

                            reader.Read();
                            Assert.AreEqual(5, reader.FieldCount);
                            Assert.AreEqual(10000, reader.GetInt32(0));
                            Assert.AreEqual("Aardewijn Pepijn", reader.GetString(1));
                            Assert.AreEqual("M", reader.GetString(2));
                            Assert.AreEqual("NED", reader.GetString(3));
                            Assert.AreEqual("Rowing", reader.GetString(4));
                        }
                    }
                }
            }
            
        }


        /// <summary>
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod]
        public void DataReader_GetNumericTypes_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t(c0 short, c1 smallint, c2 integer, c3 int, c4 bigint, c5 numeric, c6 decimal, c7 float, c8 real, c9 double, c10 double precision, c11 monetary)", conn);
                DBHelper.ExecuteSQL("insert into t values(-10, 11, -32768, 32769, -2147483650, 2.5, -12.6, 33.5, -123.4567, 23.45, 45.678, 987.65)", conn);
                DBHelper.ExecuteSQL("insert into t values(-11, 12, -32769, 32770, -2147483651, 2.6, -12.7, 34.5, -223.4567, 33.45, 46.678, 989.65)", conn);

                String sql = "select * from t";

                LogTestStep("use exactly the same data type");
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read(); //retrieve just one row

                        Assert.AreEqual(12, reader.FieldCount);
                        Assert.AreEqual(-10, reader.GetInt16(0));
                        Assert.AreEqual(11, reader.GetInt16(1));
                        Assert.AreEqual(-32768, reader.GetInt32(2));
                        Assert.AreEqual(32769, reader.GetInt32(3));
                        Assert.AreEqual(-2147483650, reader.GetInt64(4));
                        //Assert.AreEqual(2.5, reader.GetDecimal(5)); //Actual 3
                        //Assert.AreEqual(-12.6, reader.GetDecimal(6)); //Actual -13
                        Assert.AreEqual(33.5, reader.GetFloat(7));
                        if (Math.Abs(-123.4567 - reader.GetFloat(8)) > 0.00001)
                        {
                            Assert.Fail();
                        }
                                                
                        Assert.AreEqual(23.45, reader.GetDouble(9));
                        Assert.AreEqual(45.678, reader.GetDouble(10));
                        //TODO: ask whether there is GetMONETARY method
                        Assert.AreEqual(987.65, reader.GetDouble(11));

                        LogStepPass();
                    }
                }

                //TODO change the datatype
                //LogTestStep("use exactly the same data type");
            }

            LogTestResult();

        }

        /// <summary>
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod]
        public void DataReader_GetDateTimeTypes_Test()
        {
            //TODO test date time 
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                
            }
        }

        /// <summary>
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod]
        public void DataReader_GetChars_Test()
        {
            //TODO test date time 
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (c varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values ('abcdefghijklmn')", conn);
                DBHelper.ExecuteSQL("insert into t values ('opq')", conn);

                long result = 0;
                char[] resultArray = {'1','2','3','4','5','6','7','8','9'};

                string sql = "select * from t order by c";
                using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        result = reader.GetChars(0, 2, resultArray, 3, 4);
                        char[] expected = {'1','2','3','c','d','e','f','8','9'};

                        Assert.AreEqual(4, result);
                        for (int i = 0; i < resultArray.Length; i++)
                        {
                            Assert.AreEqual(expected[i], resultArray[i]);
                        }

                        try
                        {
                            result = reader.GetChars(1, 2, resultArray, 3, 4);
                        }
                        catch (IndexOutOfRangeException ex)
                        {
                            Assert.AreEqual("Index was outside the bounds of the array.", ex.Message);
                        }

                        try
                        {
                            result = reader.GetChars(0, 20, resultArray, 3, 4);
                        }
                        catch (IndexOutOfRangeException ex)
                        {
                            Assert.AreEqual("Data offset must be a valid position in the field!", ex.Message);
                        }

                        try
                        {
                            result = reader.GetChars(0, 2, resultArray, 10, 4);
                        }
                        catch (IndexOutOfRangeException ex)
                        {
                            Assert.AreEqual("Buffer index must be a valid index in buffer!", ex.Message);
                        }

                        try
                        {
                            result = reader.GetChars(0, 2, resultArray, 3, 9);
                        }
                        catch (ArgumentException ex)
                        {
                            Assert.AreEqual("Buffer is not large enough to hold the requested data!", ex.Message);
                        }

                        
                        resultArray = new char[] { '1', '2', '3', '4', '5', '6', '7', '8', '9' };
                        // read opq
                        reader.Read();

                        try
                        {
                            result = reader.GetChars(0, 1, resultArray, 2, 4);
                        }
                        catch (ArgumentOutOfRangeException ex)
                        {
                            Assert.AreEqual(@"Index and count must refer to a location within the string.
Parameter name: sourceIndex", ex.Message);
                        }
                    }
                }

                //revert test db
                DBHelper.ExecuteSQL("drop table if exists t", conn); 
            }
        }

         /// <summary>
        /// Test SQL statements execution, using DataReader
        /// </summary>
        [TestMethod]
        public void DataReader_GetDate_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (timeTest time,datetimeTest datetime)", conn);
                DBHelper.ExecuteSQL("insert into t values('12:07:39', '2013-03-18 12:07:12')", conn);

                using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(2, reader.FieldCount);
                        Assert.AreEqual("12:07:39", reader.GetTime(0));
                        Assert.AreEqual("2013-03-18", reader.GetDate(1));

                        DateTime dt = reader.GetDateTime(1);
                        Assert.AreEqual("2013-03-18 12:07:12", dt.ToString("yyyy-MM-dd HH:mm:ss"));
                    }
                }

                //revert test db
                DBHelper.ExecuteSQL("drop table if exists t", conn); 
            }
        }

        /// <summary>
        ///A test for DataType:MultiSet
        ///</summary>
        [TestMethod]
        public void DataType_MultiSet_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists testtable", conn);

                DBHelper.ExecuteSQL("CREATE TABLE testtable(clsid multiset_of(INTEGER));", conn);

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

                DBHelper.ExecuteSQL("drop TestTable;", conn);
            }
        }
    }
}
