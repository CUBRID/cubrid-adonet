using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;

namespace Test.Functional
{
    public partial class TestCases
    {
        /// <summary>
        /// Test Encodings support
        /// </summary>
        ///
        public static void TestSequenceRun()
        {
            Test_Sequence_Default();
            Test_Sequence_String();
            Test_Sequence_Bit();
            Test_Sequence_Short();
            Test_Sequence_Int();
            Test_Sequence_Bigint();
            Test_Sequence_Float();
            Test_Sequence_Double();
            Test_Sequence_Numeric();
            Test_Sequence_Date();
            Test_Sequence_Time();
            Test_Sequence_Datetime();
            Test_Sequence_Timestamp();
            Test_Sequence_Object();
            Test_Sequence_Lob();
        }
        private static void Test_Sequence_Default()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(string))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[8]{"P", 
                                    "Types of fragmentation", 
                                    "File fragmentation",
                                    "Free space fragmentation",
                                    "File scattering", 
                                    "File system fragmentation", 
                                    "Preventing fragmentation",null};
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_LAST + 1;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                    cmd.ColumnInfos[0].ToString();
                }
            }
        }
        private static void Test_Sequence_String()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(string))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[8]{"P", 
                                    "Types of fragmentation", 
                                    "File fragmentation",
                                    "Free space fragmentation",
                                    "File scattering", 
                                    "File system fragmentation", 
                                    "Preventing fragmentation",null};
            string sql = "INSERT INTO t(s) VALUES( ?);";
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                    cmd.ColumnInfos[0].ToString();
                }
            }
        }
        private static void Test_Sequence_Bit()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(bit))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "1", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BIT;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Short()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(short))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "10", "512", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Int()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(int))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "1", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Bigint()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(bigint))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "100000", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Float()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(float))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "1.123", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Double()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(double))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "2.2", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Numeric()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(numeric))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "12345.6789", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_NUMERIC;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Date()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);
            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(date))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[2] { DateTime.Now.ToShortDateString(), null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Time()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(time))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[2] { DateTime.Now.ToLongTimeString(), null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Datetime()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(datetime))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[2] { DateTime.Now.ToLongDateString(), null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Timestamp()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(timestamp))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[2] { DateTime.Now.ToLongDateString(), null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Object()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(object))", conn);
            //Insert some data in the sequence column
            object[] sArray = new object[1] { null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
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
                    if (oArray != null)
                        Console.WriteLine(oArray.Length);
                }
            }
        }
        private static void Test_Sequence_Lob()
        {
            ExecuteSQL("DROP TABLE IF EXISTS t", conn);

            //Create a new table with a collection
            ExecuteSQL("CREATE TABLE t(s SET(object))", conn);
            //Insert some data in the sequence column
            string[] sArray = new string[3] { "1", "0", null };
            string sql = "INSERT INTO t(s) VALUES( ?);";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
                cmd.Parameters.Add(param);
                cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Debug.Assert(e.Message == "Not implemented");
                }
            }

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?p";
                param.Value = sArray;
                param.InnerCUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
                cmd.Parameters.Add(param);
                cmd.Parameters[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_SET;
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    Debug.Assert(e.Message == "Not implemented");
                }
            }
        }
    }
}