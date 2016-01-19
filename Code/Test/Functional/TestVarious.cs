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
    /// Test CUBRIDCommand ExecuteNonQuery() method
    /// </summary>
    private static void Test_ExecuteNonQuery()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTable(conn);

        string sql = "insert into t values(1, 'a', 'abc', 1.2, 2.1, '10/31/2008')";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        cmd.ExecuteNonQuery();
        cmd.Close();
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select * from t";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        DbDataReader reader = cmd.ExecuteReader();

        while (reader.Read()) //only one row will be available
        {
          Debug.Assert(reader.GetInt32(0) == 1);
          Debug.Assert(reader.GetString(1) == "a         ");
          Debug.Assert(reader.GetString(2) == "abc");
          Debug.Assert(reader.GetFloat(3) == 1.2f);
          Debug.Assert(reader.GetFloat(4) == (float)Convert.ToDouble(2.1));
          Debug.Assert(reader.GetDateTime(5) == new DateTime(2008, 10, 31));
        }

        cmd.Close();

        CleanupTestTable(conn);
      }
    }

    /// <summary>
    /// Test CREATE Database Stored Functions calls
    /// </summary>
    private static void Test_CreateFunction()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        try
        {
          TestCases.ExecuteSQL("drop function sp1", conn);
        }
        catch { }

        string sql = "CREATE FUNCTION sp1(a int) RETURN string AS LANGUAGE JAVA NAME 'SpTest.test1(int) return java.lang.String'";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.ExecuteNonQuery();
        }

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetProcedures(null);

        Debug.Assert(dt.Rows.Count == 1);

        TestCases.ExecuteSQL("drop function sp1", conn);
      }
    }

    /// <summary>
    /// Test CREATE Stored Procedures calls
    /// </summary>
    private static void Test_CreateProcedure()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        try
        {
          TestCases.ExecuteSQL("drop function sp2", conn);
        }
        catch { }

        string sql = "CREATE PROCEDURE \"sp2\"() AS LANGUAGE JAVA NAME 'SpTest.test2()'";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.ExecuteNonQuery();
        }

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetProcedures(null);

        Debug.Assert(dt.Rows.Count == 1);

        TestCases.ExecuteSQL("drop procedure sp2", conn);
      }
    }

    /// <summary>
    /// Test BatchExecute()
    /// </summary>
    private static void Test_BatchExecute()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "create table t(idx integer)";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.ExecuteNonQuery();
        }

        string[] sql_arr = new string[3];
        sql_arr[0] = "insert into t values(1)";
        sql_arr[1] = "insert into t values(2)";
        sql_arr[2] = "insert into t values(3)";
        conn.BatchExecute(sql_arr);

        sql = "select count(*) from t";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt32(0) == 3);
          }
        }

        sql = "drop table t;";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Test CUBRIDException class
    /// </summary>
    private static void Test_CUBRIDException()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select count(*) from xyz"; //Table xyz does not exist
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          try
          {
            using (DbDataReader reader = cmd.ExecuteReader())
            {
              reader.Read();
            }
          }
          catch (Exception ex)
          {
            string r = "Syntax: Unknown class \"xyz\". select count(*) from xyz";
            Debug.Assert(ex.Message.Substring(0,r.Length) == r);//todo
          }
        }
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select count(*) from xyz"; //Table xyz does not exist
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          try
          {
            using (DbDataReader reader = cmd.ExecuteReader())
            {
              conn.Close();
              reader.Read();
            }
          }
          catch (Exception ex)
          {
              string r = "Syntax: Unknown class \"xyz\". select count(*) from xyz";
              Debug.Assert(ex.Message.Substring(0, r.Length) == r);//todo
          }
        }
      }
    }

    /// <summary>
    /// Test CUBRIDDataReader GetOid() method
    /// </summary>
    private static void Test_OID_Get()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select * from nation limit 1";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
          {
            reader.Read();
            CUBRIDOid oid = reader.GetOid();
            Debug.Assert(oid.ToString() == "OID:@0|0|0");
          }
        }
      }
    }

    /// <summary>
    /// Test CUBRIDConnection BatchExecuteNoQuery() method
    /// </summary>
    private static void Test_BatchExecuteNoQuery()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string[] sqls = new string[3];
        sqls[0] = "create table t(id int)";
        sqls[1] = "insert into t values(1)";
        sqls[2] = "insert into t values(2)";

        conn.BatchExecuteNoQuery(sqls);

        string sql = "select count(*) from t";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt32(0) == 2);
          }
        }

        TestCases.ExecuteSQL("drop table t", conn);
      }
    }

    /// <summary>
    /// Test CUBRIDCommand GetGeneratedKeys() method
    /// </summary>
    /*
    private static void Test_GetGeneratedKeys()
    {
      string sqlTablesCount = "select count(*) from db_class";
      int tablesCount, newTableCount;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists tkeys", conn);

        tablesCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        TestCases.ExecuteSQL("create table tkeys(id int auto_increment, str string)", conn);
        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        //Verify table was created
        Debug.Assert(newTableCount == tablesCount + 1);

        conn.BeginTransaction();
        CUBRIDCommand cmd = new CUBRIDCommand("insert into tkeys(str) values('xyz')", conn);
        cmd.ExecuteNonQuery();
        DbDataReader keys = cmd.GetGeneratedKeys();

        while (keys.Read())
        {
          //only on erow will be returned
          Debug.Assert(keys.GetInt32(0) == 1);
        }
        conn.Commit();
        cmd.Close();

        TestCases.ExecuteSQL("drop table if exists tkeys", conn);
        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        Debug.Assert(newTableCount == tablesCount);
      }
    }
    */
    /// <summary>
    /// Test ExecuteNonQuery() and ExecuteReader() methods
    /// </summary>
    private static void Test_ExecuteNonQuery_Query()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select count(*) from nation";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          try
          {
            cmd.ExecuteNonQuery();
          }
          catch (CUBRIDException ex)
          {
            Debug.Assert(ex.Message == "Invalid query type!");
          }
        }
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "insert into nation values('x', 'x', 'x', 'x')";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          try
          {
            using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
            {
              reader.Read();
            }
          }
          catch (CUBRIDException ex)
          {
            string r = "Operation would have caused one or more unique constraint violations.";
            Debug.Assert(ex.Message.Substring(0,r.Length) == r);
          }
          ExecuteSQL("delete from nation where code ='x'", conn);
        }
      }
    }

    /// <summary>
    /// Test CUBRIDOid class (which implements OID support)
    /// </summary>
    private static void Test_Oid_Basic()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDOid oid = new CUBRIDOid("@620|1|0");

        Debug.Assert(oid.Page() == 620);
        Debug.Assert(oid.Slot() == 1);
        Debug.Assert(oid.Volume() == 0);
      }
    }

    /// <summary>
    /// Test basic SQL statements execution
    /// </summary>
    private static void Test_Demo_Basic()
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

        using (CUBRIDCommand cmd = new CUBRIDCommand("insert into t values(1, 'abc')", conn))
        {
          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt16(0) == 1);
            Debug.Assert(reader.GetString(1) == "abc");
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("update t set str = 'xyz' where id = 1", conn))
        {
          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt16(0) == 1);
            Debug.Assert(reader.GetString(1) == "xyz");
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("delete from t", conn))
        {
          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t", conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            Debug.Assert(reader.HasRows == false);
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("drop table t", conn))
        {
          cmd.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Test basic SQL statements execution, using parameters
    /// </summary>
    private static void Test_Demo_Basic_WithParameters()
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

          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
          p2.Value = "abc";
          cmd.Parameters.Add(p2);

          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where id = ?", conn))
        {
          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 1;
          cmd.Parameters.Add(p1);

          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt16(0) == 1);
            Debug.Assert(reader.GetString(1) == "abc");
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("update t set str = ? where id = ?", conn))
        {
          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
          p2.Value = "xyz";
          cmd.Parameters.Add(p2);

          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 1;
          cmd.Parameters.Add(p1);

          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where str = ?", conn))
        {
          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
          p2.Value = "xyz";
          cmd.Parameters.Add(p2);

          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt16(0) == 1);
            Debug.Assert(reader.GetString(1) == "xyz");
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("delete from t where id = ? and str = ?", conn))
        {
          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 1;
          cmd.Parameters.Add(p1);

          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
          p2.Value = "xyz";
          cmd.Parameters.Add(p2);

          cmd.ExecuteNonQuery();
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("select * from t where str = ? and id = ?", conn))
        {
          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
          p2.Value = "xyz";
          cmd.Parameters.Add(p2);

          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 1;
          cmd.Parameters.Add(p1);

          using (DbDataReader reader = cmd.ExecuteReader())
          {
            Debug.Assert(reader.HasRows == false);
          }
        }

        using (CUBRIDCommand cmd = new CUBRIDCommand("drop table t", conn))
        {
          cmd.ExecuteNonQuery();
        }
      }
    }

    /// <summary>
    /// Test basic SQL statements execution, using parameters
    /// </summary>
    private static void Test_Command_Multiple_CommandText()
    {
      string sqlTablesCount = "select count(*) from db_class";
      int tablesCount, newTableCount;
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();
        using (CUBRIDCommand cmd = conn.CreateCommand())
        {
          tablesCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
          cmd.CommandText = "create table test(id int)";
          cmd.CommandType = CommandType.Text;
          cmd.ExecuteNonQuery();
          newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
          Debug.Assert(newTableCount == tablesCount + 1);
          cmd.CommandText = "drop table test";
          cmd.CommandType = CommandType.Text;
          cmd.ExecuteNonQuery();
          newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
          Debug.Assert(newTableCount == tablesCount);
        }
      }
    }
  }
}
