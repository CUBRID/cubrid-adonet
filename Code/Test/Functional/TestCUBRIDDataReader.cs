using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;
using System.Text;

namespace Test.Functional
{
  public partial class TestCases
  {
    /// <summary>
    /// Test Encodings support
    /// </summary>
    ///
    public static void TestCUBRIDDataReaderRun()
    {
        TestGetBytes();
        TestGetData();
        TestParameterCollection();
    }
    private static void TestParameterCollection()
    {
        string sql = "drop table if exists TestTable;";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }

        sql = "CREATE TABLE TestTable (clsid BLOB);";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }
        byte[] bytes = new byte[36] { 55, 56, 50, 69, 55, 57, 67, 69, 45, 50, 70, 68, 68, 45, 52, 68, 50, 55, 45, 65, 51, 48, 48, 45, 69, 48, 56, 56, 70, 56, 68, 68, 55, 54, 66, 69 };
        sql = "INSERT INTO TestTable VALUES(?);";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            CUBRIDBlob Blob = new CUBRIDBlob(conn);
            Blob.SetBytes(1, bytes);
            CUBRIDParameter param = new CUBRIDParameter();
            param.ParameterName = "?p";
            param.Value = Blob; cmd.Parameters.Add(param);
            cmd.Parameters[0].DbType = DbType.Binary;

            try
            {
                cmd.Parameters.Insert(0, param);
            }
            catch (Exception e)
            {
                Debug.Assert(e.Message == "Parameter already added to the collection!");
            }
            try
            {
                cmd.Parameters.Insert(0, null);
            }
            catch (Exception e)
            {
                string es = e.ToString();
                Debug.Assert(e.Message == "Only CUBRIDParameter objects are valid!");
            }
            cmd.ExecuteNonQuery();
        }
        using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
        {
            using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
            {
                reader.Read(); byte[] buffer = new byte[36];
                long len = reader.GetBytes(0, 0, buffer, 0, 36);
                ASCIIEncoding encoding = new ASCIIEncoding();
                string clsid = encoding.GetString(buffer);
                Debug.Assert(clsid == "782E79CE-2FDD-4D27-A300-E088F8DD76BE");
            }
        }
    }
    private static void TestGetBytes()
    {
        string sql = "drop table if exists TestTable;";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }

        sql = "CREATE TABLE TestTable (clsid BLOB);";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            CUBRIDDataAdapter ad1 = new CUBRIDDataAdapter(cmd);
            CUBRIDDataAdapter ad2 = new CUBRIDDataAdapter("Select * from TestTable",connString);
            cmd.ExecuteNonQuery(); 
        }
        byte[] bytes = new byte[36] { 55, 56, 50, 69, 55, 57, 67, 69, 45, 50, 70, 68, 68, 45, 52, 68, 50, 55, 45, 65, 51, 48, 48, 45, 69, 48, 56, 56, 70, 56, 68, 68, 55, 54, 66, 69 };
        sql = "INSERT INTO TestTable VALUES(?);";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        { 
            CUBRIDBlob Blob = new CUBRIDBlob(conn);
            Blob.SetBytes(1, bytes); 
            CUBRIDParameter param = new CUBRIDParameter(); 
            param.ParameterName = "?p"; 
            param.Value = Blob; cmd.Parameters.Add(param); 
            cmd.Parameters[0].DbType = DbType.Binary; 
            cmd.ExecuteNonQuery(); 
        }
        using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
        {
            using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
            { 
                reader.Read(); byte[] buffer = new byte[36]; 
                long len = reader.GetBytes(0, 0, buffer, 0, 36);
                ASCIIEncoding encoding = new ASCIIEncoding(); 
                string clsid = encoding.GetString(buffer); 
                Debug.Assert(clsid == "782E79CE-2FDD-4D27-A300-E088F8DD76BE"); 
            }
        }


        sql = "drop table if exists TestTable;";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }

        sql = "CREATE TABLE TestTable (clsid CLOB);";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }
        sql = "INSERT INTO TestTable VALUES('1234567890');";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }
        using (CUBRIDCommand cmd = new CUBRIDCommand("Select * from TestTable", conn))
        {
            using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
            {
                reader.Read(); 
                byte[] buffer = new byte[36];
                long len = reader.GetBytes(0, 0, buffer, 0, 8);

                try
                {
                    len = reader.GetBytes(0, 0, buffer, 0, 36);
                }
                catch (Exception e)
                { 
                }
            }
        }
    }

    private static void TestGetData()
    {
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
            conn.ConnectionString = TestCases.connString;
            conn.Open();

            ExecuteSQL("drop table if exists t", conn);

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
            ExecuteSQL(sql, conn);

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
            ExecuteSQL(sql, conn);

            sql = "select * from t";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();
                while (reader.Read()) //only one row will be available
                {
                    int Ordinal = reader.GetOrdinal("c_integer_ai");
                    Debug.Assert(Ordinal == 0);

                    try
                    {
                        Guid guid = reader.GetGuid(0);
                    }
                    catch (Exception e)
                    {
                        string error = e.ToString();
                        Debug.Assert(e.Message == "Value does not fall within the expected range.");
                    }

                    try
                    {
                        Guid guid = reader.GetGuid(9);
                    }
                    catch (Exception e)
                    {
                        string error = e.ToString();
                        Debug.Assert(e.Message == "Guid should contain 32 digits with 4 dashes (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).");
                    }               

                    int i = reader.GetInt32(0);
                    Type t = reader.GetColumnType(0);
                    Debug.Assert(i == 1);

                    short s = reader.GetInt16(1);
                    t = reader.GetColumnType(1);
                    Debug.Assert(s == 11);

                    i = reader.GetInt32(2);
                    t = reader.GetColumnType(2);
                    Debug.Assert(i == 111);

                    long i64 = reader.GetInt64(3);
                    t = reader.GetColumnType(3);
                    Debug.Assert(i64 == 1111);

                    decimal d = reader.GetDecimal(4);
                    t = reader.GetColumnType(4);
                    Debug.Assert(d == (decimal)1.1);

                    float f = reader.GetFloat(5);
                    t = reader.GetColumnType(5);
                    Debug.Assert(f == (float)1.11);

                    d = reader.GetDecimal(6);
                    t = reader.GetColumnType(6);
                    Debug.Assert(d == (decimal)1.111);

                    double db = reader.GetDouble(7);
                    t = reader.GetColumnType(7);
                    Debug.Assert(db == (double)1.1111);

                    char c = reader.GetChar(8);
                    t = reader.GetColumnType(8);
                    Debug.Assert(c == 'a');

                    string str = reader.GetString(9);
                    t = reader.GetColumnType(9);
                    Debug.Assert(str == "abcdfghijk");

                    DateTime dt = reader.GetDateTime(10);
                    t = reader.GetColumnType(10);
                    Debug.Assert(dt.Second == 45);

                    dt = reader.GetDateTime(11);
                    t = reader.GetColumnType(11);
                    string dt_s = reader.GetDate(11);
                    Debug.Assert(dt == new DateTime(2000, 10, 31));

                    dt = reader.GetDateTime(12);
                    t = reader.GetColumnType(12);
                    dt_s = reader.GetDate(12, "yyyy-MM-dd");
                    dt_s = reader.GetTimestamp(12);
                    dt_s = reader.GetTimestamp(12, "yyyy-MM-dd HH:mm:ss.fff");
                    Debug.Assert(dt == new DateTime(2008, 10, 31, 13, 15, 45));

                    dt = reader.GetDateTime(13);
                    t = reader.GetColumnType(13);
                    dt_s = reader.GetTime(13);
                    dt_s = reader.GetTime(13, "HH:mm:ss");
                
                    Debug.Assert(dt == new DateTime(2008, 10, 31, 13, 15, 45));

                    byte by= reader.GetByte(14);
                    t = reader.GetColumnType(14);
                    Debug.Assert(by == (byte)0);
                    Debug.Assert(reader.GetByte(15) == (byte)0);
                    t = reader.GetColumnType(15);
                    Debug.Assert(reader.GetString(16) == "123456789.0000000000000000");
                    t = reader.GetColumnType(16);
                    char[] buffer = new char[16];
                    reader.GetChars(16, 0, buffer, 0, 16);
                    Debug.Assert(reader.GetString(17) == "qwerty");
                    t = reader.GetColumnType(17);
                }
            }

            ExecuteSQL("drop table t", conn);
        }
    }
  }
}