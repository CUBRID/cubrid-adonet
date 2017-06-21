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
    /// Test CUBRIDTransaction class
    /// </summary>
    private static void Test_Transaction()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists t", conn);

        conn.BeginTransaction();

        string sql = "create table t(idx integer)";
        using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
        {
          command.ExecuteNonQuery();
        }

        int tablesCount = GetTablesCount("t", conn);
        Debug.Assert(tablesCount == 1);

        conn.Rollback();

        //Verify the table does not exist
        tablesCount = GetTablesCount("t", conn);
        Debug.Assert(tablesCount == 0);

        conn.BeginTransaction();

        sql = "create table t(idx integer)";
        using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
        {
          command.ExecuteNonQuery();
        }

        tablesCount = GetTablesCount("t", conn);
        Debug.Assert(tablesCount == 1);

        conn.Commit();

        tablesCount = GetTablesCount("t", conn);
        Debug.Assert(tablesCount == 1);

        conn.BeginTransaction();

        TestCases.ExecuteSQL("drop table t", conn);

        conn.Commit();

        tablesCount = GetTablesCount("t", conn);
        Debug.Assert(tablesCount == 0);
      }
    }

    /// <summary>
    /// Test CUBRIDTransaction class, using parameters
    /// </summary>
    private static void Test_Transaction_Parameters()
    {
      DbTransaction tran = null;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTable(conn);

        tran = conn.BeginTransaction();

        string sql = "insert into t values(?, ?, ?, ?, ?, ?)";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 1;
          cmd.Parameters.Add(p1);

          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
          p2.Value = 'A';
          cmd.Parameters.Add(p2);

          CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_STRING);
          p3.Value = "cubrid";
          cmd.Parameters.Add(p3);

          CUBRIDParameter p4 = new CUBRIDParameter("?p4", CUBRIDDataType.CCI_U_TYPE_FLOAT);
          p4.Value = 1.1f;
          cmd.Parameters.Add(p4);

          CUBRIDParameter p5 = new CUBRIDParameter("?p5", CUBRIDDataType.CCI_U_TYPE_DOUBLE);
          p5.Value = 2.2d;
          cmd.Parameters.Add(p5);

          CUBRIDParameter p6 = new CUBRIDParameter("?p6", CUBRIDDataType.CCI_U_TYPE_DATE);
          p6.Value = DateTime.Now;
          cmd.Parameters.Add(p6);

          cmd.ExecuteNonQuery();

          tran.Commit();
        }

        Debug.Assert(GetTableRowsCount("t", conn) == 1);

        CleanupTestTable(conn);
      }
    }

    /// <summary>
    /// Test CUBRID Isolation Levels
    /// </summary>
    private static void Test_IsolationLevel()
    {
      string sqlTablesCount = "select count(*) from db_class";
      int tablesCount, newTableCount;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists isol", conn);

        conn.SetIsolationLevel(CUBRIDIsolationLevel.TRAN_REP_READ);
        Debug.Assert(conn.GetIsolationLevel() == CUBRIDIsolationLevel.TRAN_REP_READ);

        tablesCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        TestCases.ExecuteSQL("create table isol(id int)", conn);
        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        //Verify table was created
        Debug.Assert(newTableCount == tablesCount + 1);

        using (CUBRIDConnection connOut = new CUBRIDConnection())
        {
          connOut.ConnectionString = TestCases.connString;
          connOut.Open();

          newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, connOut);
          //CREATE TABLE is visible from another connection
          Debug.Assert(newTableCount == tablesCount + 1);
        }

        TestCases.ExecuteSQL("drop table if exists isol", conn);
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        conn.SetIsolationLevel(CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE);
        Debug.Assert(conn.GetIsolationLevel() == CUBRIDIsolationLevel.TRAN_REP_CLASS_COMMIT_INSTANCE);

        tablesCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        conn.BeginTransaction();
        TestCases.ExecuteSQL("create table isol(id int)", conn);
        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        //Verify table was created
        Debug.Assert(newTableCount == tablesCount + 1);

        using (CUBRIDConnection connOut = new CUBRIDConnection())
        {
          connOut.ConnectionString = TestCases.connString;
          connOut.Open();
          newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, connOut);
          Debug.Assert(newTableCount == tablesCount);
        }

        conn.Commit();

        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        //Verify table was created
        Debug.Assert(newTableCount == tablesCount + 1);

        TestCases.ExecuteSQL("drop table if exists isol", conn);
        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        Debug.Assert(newTableCount == tablesCount);
      }
    }

    /// <summary>
    /// Test CUBRIDConnection Auto-Commit property
    /// </summary>
    private static void Test_AutoCommit()
    {
      int tablesCount;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        conn.SetAutoCommit(false);

        tablesCount = (int)TestCases.GetSingleValue("select count(*) from db_class", conn);

        //Create table
        TestCases.ExecuteSQL("drop table if exists xyz", conn);
        TestCases.ExecuteSQL("create table xyz(id int)", conn);
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        //Verify table was not created
        Debug.Assert(tablesCount == (int)TestCases.GetSingleValue("select count(*) from db_class", conn));

        //Create table
        TestCases.ExecuteSQL("drop table if exists xyz", conn);
        TestCases.ExecuteSQL("create table xyz(id int)", conn);
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        //Verify table was created
        Debug.Assert(tablesCount == ((int)TestCases.GetSingleValue("select count(*) from db_class", conn) - 1));

        TestCases.ExecuteSQL("drop table if exists xyz", conn);
      }
    }
  }
}