using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using CUBRID.Data.CUBRIDClient;

namespace Test.Functional
{
  public partial class TestCases
  {
    /// <summary>
    /// Test CUBRIDParameterCollection class
    /// </summary>
    private static void Test_Parameters_Collection()
    {
      string errMsg;

      Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-us");

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDParameterCollection pcoll = new CUBRIDParameterCollection();

        CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
        p1.Value = 1;
        pcoll.Add(p1);

        CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
        p2.Value = 'A';
        pcoll.Add(p2);

        CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_NULL);
        p3.Value = null;
        pcoll.Add(p3);

        //Try to add again p1
        errMsg = "";
        try
        {
          pcoll.Add(p1);
          throw new Exception();
        }
        catch (Exception ex)
        {
          errMsg = ex.Message;
        }
        Debug.Assert(errMsg == "Parameter already added to the collection!");

        Debug.Assert(pcoll.Count == 3);

        Debug.Assert(pcoll["?p1"].ParameterName == "?p1");
        Debug.Assert(pcoll[1].ParameterName == "?p2");

        Debug.Assert(pcoll["?p1"].DbType == DbType.Int32);
        Debug.Assert(pcoll[1].DbType == DbType.StringFixedLength);
        Debug.Assert(pcoll[2].DbType == DbType.Object);

        Debug.Assert((int)pcoll[0].Value == 1);
        Debug.Assert((char)pcoll[1].Value == 'A');
        Debug.Assert(pcoll[2].Value == null);

        //Try get non-existing parameter
        errMsg = "";
        try
        {
          string str = pcoll["?p11"].ParameterName;
        }
        catch (Exception ex)
        {
          errMsg = ex.Message;
        }
        Debug.Assert(errMsg == "?p11: Parameter not found in the collection!");

        //Try get non-existing parameter
        errMsg = "";
        try
        {
          string str = pcoll[99].ParameterName;
        }
        catch (Exception ex)
        {
          errMsg = ex.Message;
        }
        Debug.Assert(errMsg == "Index was outside the bounds of the array.");

        pcoll.RemoveAt(1);
        pcoll.Remove(p1);

        Debug.Assert(pcoll.Count == 1);

        pcoll.Clear();

        Debug.Assert(pcoll.Count == 0);
      }
    }

    /// <summary>
    /// Test the CUBRIDDataType.CCI_U_TYPE_NULL data type
    /// </summary>
    private static void Test_Null_WithParameters()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        using (CUBRIDCommand cmd = new CUBRIDCommand("drop table if exists t", conn))
        {
          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("create table t(id int, str string)", conn))
        {
          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("insert into t values(?, ?)", conn))
        {
          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 1;
          cmd.Parameters.Add(p1);

          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_NULL);
          p2.Value = null;
          cmd.Parameters.Add(p2);

          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where id = 1", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetValue(1) == null);
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("drop table t", conn))
        {
          cmd.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Test CUBRID data types Get...()
    /// </summary>
    private static void Test_Various_DataTypes()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists t", conn);

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
        TestCases.ExecuteSQL(sql, conn);

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
        TestCases.ExecuteSQL(sql, conn);

        sql = "select * from t";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          DbDataReader reader = cmd.ExecuteReader();
          while (reader.Read()) //only one row will be available
          {
            Debug.Assert(reader.GetInt32(0) == 1);
            Debug.Assert(reader.GetInt16(1) == 11);
            Debug.Assert(reader.GetInt32(2) == 111);
            Debug.Assert(reader.GetInt64(3) == 1111);
            Debug.Assert(reader.GetDecimal(4) == (decimal)1.1);
            Debug.Assert(reader.GetFloat(5) == (float)1.11);
            Debug.Assert(reader.GetDecimal(6) == (decimal)1.111);
            Debug.Assert(reader.GetDouble(7) == (double)1.1111);
            Debug.Assert(reader.GetChar(8) == 'a');
            Debug.Assert(reader.GetString(9) == "abcdfghijk");
            Debug.Assert(reader.GetDateTime(10).Second == 45);
            Debug.Assert(reader.GetDateTime(11) == new DateTime(2000, 10, 31));
            Debug.Assert(reader.GetDateTime(12) == new DateTime(2008, 10, 31, 13, 15, 45));
            Debug.Assert(reader.GetDateTime(13) == new DateTime(2008, 10, 31, 13, 15, 45));
            //Debug.Assert(reader.GetByte(14) == (byte)0);
            //Debug.Assert(reader.GetByte(15) == (byte)0);
            Debug.Assert(reader.GetString(16) == "123456789.0000000000000000");
            Debug.Assert(reader.GetString(17) == "qwerty");
          }
        }

        TestCases.ExecuteSQL("drop table t", conn);
      }
    }

    /// <summary>
    /// Test CUBRID data types, using parameters
    /// </summary>
    private static void Test_Various_DataTypes_Parameters()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists t", conn);

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
        sql += "c_bit bit(8), ";
        sql += "c_varbit bit varying(4096), ";
        sql += "c_monetary monetary, ";
        sql += "c_string string, ";
        sql += "c_null string ";
        sql += ")";
        TestCases.ExecuteSQL(sql, conn);

        sql = "insert into t values(";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "?, ";
        sql += "? ";
        sql += ")";

        CUBRIDCommand cmd_i = new CUBRIDCommand(sql, conn);

        //sql += "c_integer_ai integer AUTO_INCREMENT, ";
        //sql += "1, ";
        CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
        p1.Value = 1;
        cmd_i.Parameters.Add(p1);
        //sql += "c_smallint smallint, ";
        //sql += "11, ";
        CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_SHORT);
        p2.Value = 11;
        cmd_i.Parameters.Add(p2);
        //sql += "c_integer integer, ";
        //sql += "111, ";
        CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_INT);
        p3.Value = 111;
        cmd_i.Parameters.Add(p3);
        //sql += "c_bigint bigint, ";
        //sql += "1111, ";
        CUBRIDParameter p4 = new CUBRIDParameter("?p4", CUBRIDDataType.CCI_U_TYPE_BIGINT);
        p4.Value = 1111;
        cmd_i.Parameters.Add(p4);
        //sql += "c_numeric numeric(15,0), ";
        //sql += "1.1, ";
        CUBRIDParameter p5 = new CUBRIDParameter("?p5", CUBRIDDataType.CCI_U_TYPE_NUMERIC);
        p5.Value = 1.1;
        cmd_i.Parameters.Add(p5);
        //sql += "c_float float, ";
        //sql += "1.11, ";
        CUBRIDParameter p6 = new CUBRIDParameter("?p6", CUBRIDDataType.CCI_U_TYPE_FLOAT);
        p6.Value = 1.11;
        cmd_i.Parameters.Add(p6);
        //sql += "c_decimal decimal, ";
        //sql += "1.111, ";
        CUBRIDParameter p7 = new CUBRIDParameter("?p7", CUBRIDDataType.CCI_U_TYPE_NUMERIC);
        p7.Value = 1.111;
        cmd_i.Parameters.Add(p7);
        //sql += "c_double double, ";
        //sql += "1.1111, ";
        CUBRIDParameter p8 = new CUBRIDParameter("?p8", CUBRIDDataType.CCI_U_TYPE_DOUBLE);
        p8.Value = 1.1111;
        cmd_i.Parameters.Add(p8);
        //sql += "c_char char(1), ";
        //sql += "'a', ";
        CUBRIDParameter p9 = new CUBRIDParameter("?p9", CUBRIDDataType.CCI_U_TYPE_CHAR);
        p9.Size = 1;
        p9.Value = 'a';
        cmd_i.Parameters.Add(p9);
        //sql += "c_varchar varchar(4096), ";
        //sql += "'abcdfghijk', ";
        CUBRIDParameter p10 = new CUBRIDParameter("?p10", CUBRIDDataType.CCI_U_TYPE_STRING);
        p10.Value = "abcdfghijk";//trebuie luat cap coada si vazut dc plm nu se trimite ok. S-ar putea sa fie de la n
        cmd_i.Parameters.Add(p10);
        //sql += "c_time time, ";
        //sql += "TIME '13:15:45 pm', ";
        CUBRIDParameter p11 = new CUBRIDParameter("?p11", CUBRIDDataType.CCI_U_TYPE_TIME);
        p11.Value = new DateTime(2010, 1, 1, 13, 15, 45); //year/month/date is not relevant, only the time part is used
        cmd_i.Parameters.Add(p11);
        //sql += "c_date date, ";
        //sql += "DATE '00-10-31', ";
        CUBRIDParameter p12 = new CUBRIDParameter("?p12", CUBRIDDataType.CCI_U_TYPE_DATE);
        p12.Value = new DateTime(2000, 10, 31);
        cmd_i.Parameters.Add(p12);
        //sql += "c_timestamp timestamp, ";
        //sql += "TIMESTAMP '13:15:45 10/31/2008', ";
        CUBRIDParameter p13 = new CUBRIDParameter("?p13", CUBRIDDataType.CCI_U_TYPE_TIMESTAMP);
        p13.Value = new DateTime(2008, 10, 31, 13, 15, 45);
        cmd_i.Parameters.Add(p13);
        //sql += "c_datetime datetime, ";
        //sql += "DATETIME '13:15:45 10/31/2008', ";
        CUBRIDParameter p14 = new CUBRIDParameter("?p14", CUBRIDDataType.CCI_U_TYPE_DATETIME);
        p14.Value = new DateTime(2008, 10, 31, 13, 15, 45);
        cmd_i.Parameters.Add(p14);
        //sql += "c_bit bit(1), ";
        //sql += "B'1', ";
        CUBRIDParameter p15 = new CUBRIDParameter("?p15", CUBRIDDataType.CCI_U_TYPE_BIT);
        p15.Value = (byte)1;
        cmd_i.Parameters.Add(p15);
        //sql += "c_varbit bit varying(4096), ";
        //sql += "B'1', ";
        CUBRIDParameter p16 = new CUBRIDParameter("?p16", CUBRIDDataType.CCI_U_TYPE_VARBIT);
        p16.Value = (byte)1;
        cmd_i.Parameters.Add(p16);
        //sql += "c_monetary monetary, ";
        //sql += "123456789, ";
        CUBRIDParameter p17 = new CUBRIDParameter("?p17", CUBRIDDataType.CCI_U_TYPE_MONETARY);
        p17.Value = 123456789;
        cmd_i.Parameters.Add(p17);
        //sql += "c_string string ";
        //sql += "'qwerty'";
        CUBRIDParameter p18 = new CUBRIDParameter("?p18", CUBRIDDataType.CCI_U_TYPE_STRING);
        p18.Value = "qwerty";
        cmd_i.Parameters.Add(p18);
        //sql += "c_null string ";
        //sql += "null";
        CUBRIDParameter p19 = new CUBRIDParameter("?p19", CUBRIDDataType.CCI_U_TYPE_NULL);
        p19.Value = null;
        cmd_i.Parameters.Add(p19);

        cmd_i.ExecuteNonQuery();

        cmd_i.Close();

        sql = "select * from t ";
        sql += "where 1 = 1 ";
        sql += "and c_integer_ai = ? ";
        sql += "and c_smallint = ? ";
        sql += "and c_integer = ? ";
        sql += "and c_bigint = ? ";
        sql += "and c_numeric = ? ";
        sql += "and c_float = ? ";
        sql += "and c_decimal = ? ";
        sql += "and c_double = ? ";
        sql += "and c_char = ? ";
        sql += "and c_varchar = ? ";
        sql += "and c_time = ? ";
        sql += "and c_date = ? ";
        sql += "and c_timestamp = ? ";
        sql += "and c_datetime = ? ";
        sql += "and c_bit = ? ";
        sql += "and c_varbit = ? ";
        sql += "and c_monetary = ? ";
        sql += "and c_string = ? ";
        sql += "and c_null IS NULL ";

        CUBRIDCommand cmd_s = new CUBRIDCommand(sql, conn);
        cmd_s.Parameters.Add(p1);
        cmd_s.Parameters.Add(p2);
        cmd_s.Parameters.Add(p3);
        cmd_s.Parameters.Add(p4);
        cmd_s.Parameters.Add(p5);
        cmd_s.Parameters.Add(p6);
        cmd_s.Parameters.Add(p7);
        cmd_s.Parameters.Add(p8);
        cmd_s.Parameters.Add(p9);
        cmd_s.Parameters.Add(p10);
        cmd_s.Parameters.Add(p11);
        cmd_s.Parameters.Add(p12);
        cmd_s.Parameters.Add(p13);
        cmd_s.Parameters.Add(p14);
        cmd_s.Parameters.Add(p15);
        cmd_s.Parameters.Add(p16);
        cmd_s.Parameters.Add(p17);
        cmd_s.Parameters.Add(p18);
        //cmd_s.Parameters.Add(p19);

        DbDataReader reader = cmd_s.ExecuteReader();
        while (reader.Read()) //only one row will be available
        {
          Debug.Assert(reader.GetInt32(0) == 1);
          Debug.Assert(reader.GetInt16(1) == 11);
          Debug.Assert(reader.GetInt32(2) == 111);
          Debug.Assert(reader.GetInt64(3) == 1111);
          Debug.Assert(reader.GetDecimal(4) == (decimal)1.1);
          Debug.Assert(reader.GetFloat(5) == (float)1.11);
          Debug.Assert(reader.GetDouble(6) == 1.111);
          Debug.Assert(reader.GetDouble(7) == 1.1111);
          Debug.Assert(reader.GetChar(8) == 'a');
          Debug.Assert(reader.GetString(9) == "abcdfghijk");
          Debug.Assert(reader.GetDateTime(10) == new DateTime(1, 1, 1, 13, 15, 45));
          Debug.Assert(reader.GetDateTime(11) == new DateTime(2000, 10, 31));
          Debug.Assert(reader.GetDateTime(12) == new DateTime(2008, 10, 31, 13, 15, 45));
          Debug.Assert(reader.GetDateTime(13) == new DateTime(2008, 10, 31, 13, 15, 45, 00));
          Debug.Assert(reader.GetByte(14) == 1);
          Debug.Assert(reader.GetByte(15) == 1);
          Debug.Assert(reader.GetString(16) == "123456789");
          Debug.Assert(reader.GetString(17) == "qwerty");
          Debug.Assert(reader.GetValue(18) == null);
        }

        cmd_s.Close();

        TestCases.ExecuteSQL("drop table t", conn);
      }
    }

    /// <summary>
    /// Test DateTime types
    /// </summary>
    private static void Test_DateTime_Types()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CleanupTestTable(conn);
        TestCases.ExecuteSQL("create table t(dt datetime)", conn);

        TestCases.ExecuteSQL("insert into t values('10/31/2008 10:20:30.040')", conn);

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
        {
          CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();

          reader.Read();
          Debug.Assert(reader.GetDateTime(0) == new DateTime(2008, 10, 31, 10, 20, 30, 040));
          Debug.Assert(reader.GetDate(0) == "2008-10-31");
          Debug.Assert(reader.GetDate(0, "yy/MM/dd") == "08/10/31");
          Debug.Assert(reader.GetTime(0) == "10:20:30");
          Debug.Assert(reader.GetTime(0, "HH") == "10");
          Debug.Assert(reader.GetTimestamp(0) == "2008-10-31 10:20:30.040");
          Debug.Assert(reader.GetTimestamp(0, "yyyy HH") == "2008 10");
        }

        CleanupTestTable(conn);
      }
    }

    /// <summary>
    /// Test SET operations
    /// </summary>
    private static void Test_SetOperations()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

        //Create a new table with a collection
        TestCases.ExecuteSQL("CREATE TABLE t(s SET(int))", conn);
        //Insert some data in the sequence column
        TestCases.ExecuteSQL("INSERT INTO t(s) VALUES({0,1,2,3,4,5,6})", conn);
        CUBRIDOid oid = new CUBRIDOid("@0|0|0");
        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT t FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              oid = (CUBRIDOid)reader[0];
            }
          }
        }

        String attributeName = "s";
        object value = 7;

        int SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 7);

        conn.AddElementToSet(oid, attributeName, value);
        SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 8);

        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              int[] expected = { 0, 1, 2, 3, 4, 5, 6, 7 };
              object[] o = (object[])reader[0];
              for (int i = 0; i < SeqSize; i++)
              {
                Debug.Assert(Convert.ToInt32(o[i]) == expected[i]);
              }
            }
          }
        }

        conn.DropElementInSet(oid, attributeName, 5);
        SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 7);

        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              int[] expected = { 0, 1, 2, 3, 4, 6, 7 };
              object[] o = (object[])reader[0];
              for (int i = 0; i < SeqSize; i++)
              {
                Debug.Assert(Convert.ToInt32(o[i]) == expected[i]);
              }
            }
          }
        }

        TestCases.ExecuteSQL("DROP t", conn);
      }
    }

    /// <summary>
    /// Test SEQUENCE operations
    /// </summary>
    private static void Test_SequenceOperations()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("DROP TABLE IF EXISTS t", conn);

        //Create a new table with a sequence

        TestCases.ExecuteSQL("CREATE TABLE t(seq SEQUENCE(int))", conn);
        //Insert some data in the sequence column
        TestCases.ExecuteSQL("INSERT INTO t(seq) VALUES({0,1,2,3,4,5,6})", conn);
        CUBRIDOid oid = new CUBRIDOid("@0|0|0");
        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT t FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              oid = (CUBRIDOid)reader[0];
            }
          }
        }

        String attributeName = "seq";
        int value = 7;

        int SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 7);

        conn.UpdateElementInSequence(oid, attributeName, 1, value);
        SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 7);

        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              int[] expected = { 7, 1, 2, 3, 4, 5, 6 };
              object[] o = (object[])reader[0];
              for (int i = 0; i < SeqSize; i++)
              {
                Debug.Assert(Convert.ToInt32(o[i]) == expected[i]);
              }
            }
          }
        }

        conn.InsertElementInSequence(oid, attributeName, 5, value);
        SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 8);

        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              int[] expected = { 7, 1, 2, 3, 7, 4, 5, 6 };
              object[] o = (object[])reader[0];
              for (int i = 0; i < SeqSize; i++)
              {
                Debug.Assert(Convert.ToInt32(o[i]) == expected[i]);
              }
            }
          }
        }

        conn.DropElementInSequence(oid, attributeName, 5);
        SeqSize = conn.GetCollectionSize(oid, attributeName);
        Debug.Assert(SeqSize == 7);

        using (CUBRIDCommand cmd = new CUBRIDCommand("SELECT * FROM t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            while (reader.Read())
            {
              int[] expected = { 7, 1, 2, 3, 4, 5, 6 };
              object[] o = (object[])reader[0];
              for (int i = 0; i < SeqSize; i++)
              {
                Debug.Assert(Convert.ToInt32(o[i]) == expected[i]);
              }
            }
          }
        }

        TestCases.ExecuteSQL("DROP t", conn);
      }
    }

    /// <summary>
    /// Test Enum data type
    /// </summary>
    public static void Test_DataType_Enum()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists table11;", conn);

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
            Debug.Assert(reader.GetInt32(0) == 3);
          }
        }

        sql = "select * from table11";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetString(0) == "BeiJing");
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
          string expected = exp.Message.Substring(0, exp.Message.LastIndexOf("["));
          Debug.Assert(expected == "Semantic: before ' , 'China');'\nCannot coerce 'Peking' to type enum. insert into [dba.table11] [dba.table11] ([dba.table11].city,...");
        }

        TestCases.ExecuteSQL("drop table11;", conn);
      }
    }

    /// <summary>
    /// Test Enum data type with wrong data
    /// </summary>
    public static void Test_WithWrongEnumData()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists table11;", conn);

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
          string expected = exp.Message.Substring(0, exp.Message.LastIndexOf("["));
          Debug.Assert(expected == "Syntax: In line 1, column 28 before '(1, 2, 3, 4, 5, 6));'\nSyntax error: unexpected 'enum' ");
        }
      }
    }
  }
}