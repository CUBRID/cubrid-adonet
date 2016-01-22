using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;

namespace CUBRID.Data.Test.Functional
{
  public partial class TestCases
  {
    /// <summary>
    /// Test ConnectionStringBuilder class
    /// </summary>
    private static void Test_ConnectionStringBuilder()
    {
      CUBRIDConnectionStringBuilder sb = new CUBRIDConnectionStringBuilder("localhost", 33000, "demodb", "public", "",
                                                                           "utf-8", false);
      //Note: Do not use sb.ConnectionString with empty password

      using (CUBRIDConnection conn = new CUBRIDConnection(sb.GetConnectionString()))
      {
        conn.Open();
      }

      sb = new CUBRIDConnectionStringBuilder("localhost", 33000, "demodb", "public", "wrong password", "utf-8", false);
      using (CUBRIDConnection conn = new CUBRIDConnection(sb.GetConnectionString()))
      {
        try
        {
          conn.Open();
        }
        catch (Exception ex)
        {
          Debug.Assert(ex.Message == "Incorrect or missing password.");
        }
      }

      sb = new CUBRIDConnectionStringBuilder(TestCases.connString);
      using (CUBRIDConnection conn = new CUBRIDConnection(sb.GetConnectionString()))
      {
        conn.Open();
      }

      sb = new CUBRIDConnectionStringBuilder();
      sb.User = "public";
      sb.Database = "demodb";
      sb.Port = "33000";
      sb.Server = "localhost";
      using (CUBRIDConnection conn = new CUBRIDConnection(sb.GetConnectionString()))
      {
        conn.Open();
      }
    }

    /// <summary>
    /// Test multiple connections
    /// </summary>
    private static void Test_MultipleConnections()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        TestCases.ExecuteSQL("drop table if exists t", conn);
        TestCases.ExecuteSQL("create table t(idx integer)", conn);

        string sql = "select * from nation";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            int count = 0;
            while (reader.Read() && count++ < 3)
            {
              using (CUBRIDConnection conn2 = new CUBRIDConnection())
              {
                conn2.ConnectionString = conn.ConnectionString;
                conn2.Open();
                string sqlInsert = "insert into t values(" + count + ")";
                using (CUBRIDCommand cmdInsert = new CUBRIDCommand(sqlInsert, conn2))
                {
                  cmdInsert.ExecuteNonQuery();
                }
              }
            }
          }
        }

        using (CUBRIDConnection conn2 = new CUBRIDConnection())
        {
          conn2.ConnectionString = conn.ConnectionString;
          conn2.Open();
          string sqlSelect = "select count(*) from t";
          using (CUBRIDCommand cmd = new CUBRIDCommand(sqlSelect, conn2))
          {
            using (DbDataReader reader = cmd.ExecuteReader())
            {
              reader.Read();
              Debug.Assert(reader.GetInt32(0) == 3);
            }
          }
        }

        TestCases.ExecuteSQL("drop table if exists t", conn);
      }
    }

    /// <summary>
    /// Test CUBRIDConnection GetSchema() method
    /// </summary>
    private static void Test_ConnectionGetSchema()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        DataTable dt = new DataTable();
        dt = conn.GetSchema("USERS");

        Debug.Assert(dt.Rows.Count == 2);
        Debug.Assert(dt.Rows[0]["USERNAME"].ToString() == "DBA");
        Debug.Assert(dt.Rows[1]["USERNAME"].ToString() == "PUBLIC");

        dt = conn.GetSchema("DATABASES");

        Debug.Assert(dt.Rows.Count == 1);
        Debug.Assert(dt.Rows[0]["CATALOG_NAME"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["SCHEMA_NAME"].ToString() == "demodb");

        dt = conn.GetSchema("PROCEDURES");

        Debug.Assert(dt.Rows.Count == 0);

        dt = conn.GetSchema("TABLES", new String[] { "nation" });

        Debug.Assert(dt.Rows.Count == 1);
        Debug.Assert(dt.Rows[0]["TABLE_CATALOG"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["TABLE_SCHEMA"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["TABLE_NAME"].ToString() == "nation");


        dt = conn.GetSchema("VIEWS");

        Debug.Assert(dt.Columns.Count == 3);
        Debug.Assert(dt.Columns[0].ColumnName == "VIEW_CATALOG");
        Debug.Assert(dt.Columns[1].ColumnName == "VIEW_SCHEMA");
        Debug.Assert(dt.Columns[2].ColumnName == "VIEW_NAME");
        Debug.Assert(dt.Rows.Count == 0);

        dt = conn.GetSchema("COLUMNS", new String[] { "game", "event_code" });

        Debug.Assert(dt.Rows.Count == 1);
        Debug.Assert(dt.Rows[0]["TABLE_CATALOG"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["TABLE_SCHEMA"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["TABLE_NAME"].ToString() == "game");
        Debug.Assert(dt.Rows[0]["COLUMN_NAME"].ToString() == "event_code");
        Debug.Assert((uint)dt.Rows[0]["ORDINAL_POSITION"] == (uint)1);
        Debug.Assert(dt.Rows[0]["COLUMN_DEFAULT"].ToString() == "");
        Debug.Assert((bool)dt.Rows[0]["IS_NULLABLE"] == false);
        Debug.Assert(dt.Rows[0]["DATA_TYPE"].ToString() == "INTEGER");
        Debug.Assert((uint)dt.Rows[0]["NUMERIC_PRECISION"] == (uint)10);
        Debug.Assert((uint)dt.Rows[0]["NUMERIC_SCALE"] == (uint)0);
        Debug.Assert((byte)dt.Rows[0]["CHARACTER_SET"] == (byte)0);

        dt = conn.GetSchema("INDEXES", new String[] { "nation", "code" });

        Debug.Assert(dt.Rows.Count == 1);
        Debug.Assert(dt.Rows[0]["INDEX_CATALOG"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["INDEX_SCHEMA"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["INDEX_NAME"].ToString() == "pk_nation_code");
        Debug.Assert(dt.Rows[0]["TABLE_NAME"].ToString() == "nation");
        Debug.Assert((bool)dt.Rows[0]["UNIQUE"] == true);
        Debug.Assert((bool)dt.Rows[0]["REVERSE"] == false);
        Debug.Assert((bool)dt.Rows[0]["PRIMARY"] == true);
        Debug.Assert((bool)dt.Rows[0]["FOREIGN_KEY"] == false);
        Debug.Assert(dt.Rows[0]["DIRECTION"].ToString() == "ASC");

        dt = conn.GetSchema("INDEX_COLUMNS", new String[] { "nation", "pk_nation_code" });

        Debug.Assert(dt.Rows.Count == 1);
        Debug.Assert(dt.Rows[0]["INDEX_CATALOG"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["INDEX_SCHEMA"].ToString() == "demodb");
        Debug.Assert(dt.Rows[0]["INDEX_NAME"].ToString() == "pk_nation_code");
        Debug.Assert(dt.Rows[0]["TABLE_NAME"].ToString() == "nation");
        Debug.Assert(dt.Rows[0]["COLUMN_NAME"].ToString() == "code");
        Debug.Assert((int)dt.Rows[0]["ORDINAL_POSITION"] == 0);
        Debug.Assert(dt.Rows[0]["DIRECTION"].ToString() == "ASC");

        dt = conn.GetSchema("FOREIGN_KEYS", new String[] { "game", "fk_game_athlete_code" });

        Debug.Assert(dt.Rows.Count == 2);
        Debug.Assert(dt.Rows[0]["PKTABLE_NAME"].ToString() == "event");
        Debug.Assert(dt.Rows[0]["PKCOLUMN_NAME"].ToString() == "code");
        Debug.Assert(dt.Rows[0]["FKTABLE_NAME"].ToString() == "game");
        Debug.Assert(dt.Rows[0]["FKCOLUMN_NAME"].ToString() == "event_code");
        Debug.Assert((short)dt.Rows[0]["KEY_SEQ"] == (short)1);
        Debug.Assert((short)dt.Rows[0]["UPDATE_ACTION"] == (short)1);
        Debug.Assert((short)dt.Rows[0]["DELETE_ACTION"] == (short)1);
        Debug.Assert(dt.Rows[0]["FK_NAME"].ToString() == "fk_game_event_code");
        Debug.Assert(dt.Rows[0]["PK_NAME"].ToString() == "pk_event_code");

        conn.Close();
      }
    }

    /// <summary>
    /// Test CUBRID Connection properties
    /// </summary>
    private static void Test_ConnectionProperties()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        Debug.Assert(conn.ConnectionTimeout == 30);
        Debug.Assert(conn.CurrentDatabase() == "demodb");
        Debug.Assert(conn.Database == "demodb");
        Debug.Assert(conn.DbVersion.StartsWith("9.1.0") == true);
        Debug.Assert(conn.DataSource == "localhost");
        Debug.Assert(conn.AutoCommit == true);
        Debug.Assert(conn.LockTimeout == 30000);
        Debug.Assert(conn.ConnectionTimeout == 30);
        Debug.Assert(conn.IsolationLevel == CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);
        Debug.Assert(conn.ServerVersion == "");
        Debug.Assert(conn.State == ConnectionState.Open);
      }
    }

    /// <summary>
    ///Test CUBRIDConnectionStringBuilder Constructor
    ///</summary>
    public static void Test_CUBRIDConnectionStringBuilderConstructor()
    {
      string connString = "server=localhost;database=demodb;port=33690;user=public;password=";
      CUBRIDConnectionStringBuilder target = new CUBRIDConnectionStringBuilder(connString);
      using (CUBRIDConnection conn = new CUBRIDConnection(target.GetConnectionString()))
      {
        try
        {
          conn.Open();
          conn.Close();
        }
        catch (Exception ex)
        {
          Debug.Assert(ex.Message == "No connection could be made because the target machine actively refused it 127.0.0.1:33690");
        }
      }
    }

    /// <summary>
    ///A test for GetConnectionString
    ///</summary>
    public static void Test_GetConnectionString()
    {
      string server = "localhost";
      int port = 33690;
      string database = "demodb";
      string user = "public";
      string password = "";
      string encoding = "utf-8";
      CUBRIDConnectionStringBuilder target = new CUBRIDConnectionStringBuilder(server, port, database, user, password,
                                                                               encoding, true);
      string expected =
        "server=localhost;port=33690;database=demodb;user=public;password=;charset=utf-8;autocommit=1";
      string actual = string.Empty;
      actual = target.GetConnectionString();
      Debug.Assert(expected == actual);

      Debug.Assert(target.Database == "demodb");
      Debug.Assert(target.Encoding == "utf-8");
      Debug.Assert(target.Password == "");
      Debug.Assert(target.Port == "33690");
      Debug.Assert(target.Server == "localhost");
      Debug.Assert(target.User == "public");
      Debug.Assert(target.AutoCommit == "1");

      target.Server = "192.168.0.1";
      Debug.Assert(target.Server == "192.168.0.1");
    }

    /// <summary>
    ///A connect with URL
    ///</summary>
    public static void Test_ConnectionURL_And_Reset()
    {
      string strURL = "cci:cubrid:localhost:33000:demodb:public::?logSlowQueries=true" +
                      "&slowQueryThresholdMillis=1000&logTraceApi=true&logTraceNetwork=true" +
                      "&autoCommit=false&althosts=10.34.64.122,10.34.64.122:33690&" +
                      "querytimeout=10000&logintimeout=5000";
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = strURL;
        conn.Open();

        string sqlTablesCount = "select count(*) from db_class where is_system_class='NO' and class_type='CLASS'";
        int tablesCount = 0;
        using (CUBRIDCommand cmd = new CUBRIDCommand(sqlTablesCount, conn))
        {
          tablesCount = (int)cmd.ExecuteScalar();
        }

        Debug.Assert(tablesCount == 10);

        conn.Close();

        try
        {
          string strURL2 = "cci:cubrid:localhost:33690:demodb:public::?logSlowQueries=true" +
                           "&slowQueryThresholdMillis=1000&logTraceApi=false&logTraceNetwork=false&autoCommit=false";
          conn.ConnectionString = strURL2;
          conn.Open();
        }
        catch(Exception ex)
        {
          Debug.Assert(ex.Message == "No connection could be made because the target machine actively refused it 127.0.0.1:33690");
        }
      }
    }
  }
}
