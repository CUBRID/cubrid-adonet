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
    public static void CUBRIDConnectionRun()
    {
        //Not support async connect
        //Test_ConnectionAsync();
        Test_CurrentDatabase();
        Test_GetTableNameFromOID();
        Test_GetDatabaseProductVersion();
        Test_GetAutoCommit();
        Test_GetLockTimeout();
        Test_SessionId();
        Test_ServerVersion();
        Test_MaxStringLength();
        Test_Isolation();
        Test_DbVersion();
        Test_Encoding();
        Test_ConnectionInit();
    }
    private static void Test_ConnectionInit()
    {
        CUBRIDConnectionStringBuilder cs =
            new CUBRIDConnectionStringBuilder("test-db-server", "demodb", "admin", "123456",
                                         "utf-8", false);

        Debug.Assert(cs.Database == "demodb");
        Debug.Assert(cs.Port == "33000");
        Debug.Assert(cs.AutoCommit=="0"); // wrong!
        Debug.Assert(cs.Password=="123456");
        Debug.Assert(cs.User=="admin");
        Debug.Assert(cs.Server=="test-db-server");
        Debug.Assert(cs.Encoding=="utf-8");
    }
    private static void Test_Encoding()
    {
        CUBRIDConnection conn = new CUBRIDConnection();

        conn.SetEncoding("cp1252");
        Debug.Assert(Encoding.GetEncoding("Windows-1252") == conn.GetEncoding());

        conn.SetEncoding("iso-8859-1");
        Debug.Assert(Encoding.GetEncoding("iso-8859-1") == conn.GetEncoding());

        conn.SetEncoding("euc-kr");
        Debug.Assert(Encoding.GetEncoding("euc-kr") == conn.GetEncoding());

        conn.SetEncoding("euc-jp");
        Debug.Assert(Encoding.GetEncoding("euc-jp") == conn.GetEncoding());

        conn.SetEncoding("gb2312");
        Debug.Assert(Encoding.GetEncoding("gb2312") == conn.GetEncoding());

        conn.SetEncoding("gbk");
        Debug.Assert(Encoding.GetEncoding("gbk") == conn.GetEncoding());

        conn.SetEncoding("xxxx");
        Debug.Assert(Encoding.Default == conn.GetEncoding());
    }
    private static void Test_CurrentDatabase()
    {
        Console.WriteLine(conn.CurrentDatabase());
    }
    /// <summary>
    /// Test ConnectionStringBuilder class
    /// </summary>
    /*
    private static void Test_ConnectionAsync()
    {
        CUBRIDConnection conn = new CUBRIDConnection();
        conn.ConnectionString = connString;
        conn.OpenAsync();
        int i = 0;

        while(true)
        {
            if (i++ > 10 ||  conn.State == ConnectionState.Open)
                break;

            System.Threading.Thread.Sleep(1000);
        }

        Debug.Assert(conn.State == ConnectionState.Open);
        try
        { conn.OpenAsync(); }
        catch
        { }
        conn.Close();


        string str ="ado:CUBRID:192.168.1.1:33000:demodb:dba::?althosts=192.168.137.207:33000";
        conn = new CUBRIDConnection();
        conn.ConnectionString = str;
        conn.OpenAsync();
        i = 0;
        while (true)
        {
            if (i++ > 10 || conn.State == ConnectionState.Open)
                break;

            System.Threading.Thread.Sleep(1000);
        }
        Debug.Assert(conn.State == ConnectionState.Open);
        conn.Close();

        str = "ado:CUBRID:192.168.1.1:33000:demodb:dba::?althosts=192.168.137.208:33000";
        conn = new CUBRIDConnection();
        conn.ConnectionString = str;
        conn.OpenAsync();
        i = 0;
        while (true)
        {
            if (i++ > 10 || conn.State == ConnectionState.Open)
                break;

            System.Threading.Thread.Sleep(1000);
        }
        Debug.Assert(conn.State == ConnectionState.Open);
        conn.Close();
    }
     * */
    private static void Test_GetTableNameFromOID()
    {
        try
        {
            conn.GetTableNameFromOID(null);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_GetDatabaseProductVersion()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            string source = conn.DataSource;
        }
        catch (Exception e)
        {
            Debug.Assert(e.Message == "The connection is not open!");
        }
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.GetDatabaseProductVersion();
        }
        catch (Exception e)
        {
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            conn.GetDatabaseProductVersion();
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        } 
    }
    private static void Test_SetConnectionTimeout()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.SetConnectionTimeout(30);
        }
        catch (Exception e)
        {
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            conn.SetConnectionTimeout(30);
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_GetAutoCommit()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.GetAutoCommit();
        }
        catch (Exception e)
        {
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            conn.GetAutoCommit();
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_GetLockTimeout()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.GetLockTimeout();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            conn.GetLockTimeout();
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_SessionId()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            int id = conn.SessionId;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            int id = conn.SessionId;
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_ServerVersion()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            string version = conn.ServerVersion;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            string version = conn.ServerVersion;
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_MaxStringLength()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            int max = conn.MaxStringLength;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            int max = conn.MaxStringLength;
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_Isolation()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            CUBRIDIsolationLevel level = conn.IsolationLevel;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            CUBRIDIsolationLevel level = conn.IsolationLevel;
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    private static void Test_DbVersion()
    {
        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            string version = conn.DbVersion;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
            Debug.Assert(e.Message == "The connection is not open!");
        }

        try
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = connString;
            conn.Open();
            string version = conn.DbVersion;
            conn.DbVersion = version;
            conn.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
    public static void Test_BeginDbTransaction()
    {
        try
        {
            conn.SetConnectionTimeout(30);
        }
        catch (Exception e)
        {
            Debug.Assert(e.ToString() == "Not allowed to change the 'ConnectionTimeout'");
        }
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
            conn.ConnectionString = TestCases.connString;
            conn.Open();

            string source = conn.DataSource;
            TestCases.ExecuteSQL("drop table if exists t", conn);

            conn.BeginTransaction();

            string sql = "create table t(idx integer)";
            using (CUBRIDCommand command = new CUBRIDCommand(sql, conn))
            {
                command.ExecuteNonQuery();
            }

            Int64 tablesCount = GetTablesCount("t", conn);
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


            //IsolationLevel.Chaos
            try
            {
                conn.BeginTransaction(IsolationLevel.Chaos);
            }
            catch (Exception e)
            {
                Debug.Assert(e.Message == "Not supported in CUBRID!");
            }


            //IsolationLevel.ReadCommitted
            conn.BeginTransaction(IsolationLevel.ReadCommitted);
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


            //IsolationLevel.RepeatableRead
            conn.BeginTransaction(IsolationLevel.RepeatableRead);
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

            //IsolationLevel.Serializable
            conn.BeginTransaction(IsolationLevel.Serializable);
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

            //IsolationLevel.Snapshot
            try
            {
                conn.BeginTransaction(IsolationLevel.Snapshot);
            }
            catch (Exception e)
            {
                Debug.Assert(e.Message == "Not supported in CUBRID!");
            }

            //IsolationLevel.Snapshot
            try
            {
                conn.BeginTransaction(IsolationLevel.Unspecified);
            }
            catch (Exception e)
            {
                Debug.Assert(e.Message == "Unknown isolation level is not supported!");
            }
            try
            {
                conn.BeginTransaction(0);
            }
            catch (Exception e)
            {
                Debug.Assert(e.Message == "Unknown isolation level is not supported!");
            }
        }
    }
  }
}
