/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using CUBRID.Data.CUBRIDClient;
using System.Text;

namespace CUBRID.ADO.NET.DataProvider.TestCases
{
  /// <summary>
  /// Implementation of test cases for the CUBRID ADO.NET Data Provider
  /// </summary>
  public class TestCases
  {
      private static readonly string connString = "server=localhost;database=demodb;port=33000;user=public;password=";

    static int executed = 0;
    static int passed = 0;
    static int testCasesCount = 0;

    //Specifiy the REGEX name patterns for the test cases to be executed
    //static string[] runFilters = new string[] { @"\w+" }; // "\w+" means: Match any test case name  

    //Specify what test cases to execute (use a regular expression):

    static bool matchExactName = true; //set to False if you want the runFilters to match ALL test cases with names that begin with ...
    static string[] runFilters = new string[] { @"Test_Encodings" };

    /* Documentation and examples for using ADO.NET:
    http://msdn.microsoft.com/en-us/library/e80y5yhx%28v=VS.80%29.aspx
    */

    public static void Main(string[] args)
    {
      Console.WriteLine("Test cases execution started...");
      Console.WriteLine();

      //Connection 
      TestCases.Run(TestCases.Test_ConnectionStringBuilder);
      TestCases.Run(TestCases.Test_MultipleConnections);
      TestCases.Run(TestCases.Test_ConnectionGetSchema);

      //DataTable 
      TestCases.Run(TestCases.Test_DataTable_Basic);
      TestCases.Run(TestCases.Test_DataTable_UpdateImplicit);
      TestCases.Run(TestCases.Test_DataTable_UpdateExplicit);
      TestCases.Run(TestCases.Test_DataTable_InsertImplicit);
      TestCases.Run(TestCases.Test_DataTable_InsertExplicit);
      TestCases.Run(TestCases.Test_DataTable_DeleteImplicit);
      TestCases.Run(TestCases.Test_DataTable_DeleteExplicit);
      TestCases.Run(TestCases.Test_DataTable_ColumnProperties);

      //Command 
      TestCases.Run(TestCases.Test_Command_ColumnProperties);
      TestCases.Run(TestCases.Test_CommandBuilder_GetCommands);

      //DataSet 
      TestCases.Run(TestCases.Test_DataSet_Basic);
      TestCases.Run(TestCases.Test_DataSet_ExportXML);

      //DataReader/DataAdapter/DataView
      TestCases.Run(TestCases.Test_DataReader_Basic);
      TestCases.Run(TestCases.Test_DataReader_Parameters);
      TestCases.Run(TestCases.Test_DataReader_Getxxx);
      TestCases.Run(TestCases.Test_DataAdapter_BatchUpdate);
      TestCases.Run(TestCases.Test_DataView_Basic);

      //Transaction 
      TestCases.Run(TestCases.Test_Transaction);
      TestCases.Run(TestCases.Test_Transaction_Parameters);

      //Various 
      TestCases.Run(TestCases.Test_ExecuteNonQuery);
      TestCases.Run(TestCases.Test_CreateFunction);
      TestCases.Run(TestCases.Test_CreateProcedure);
      TestCases.Run(TestCases.Test_BatchExecute);
      TestCases.Run(TestCases.Test_BatchExecuteNoQuery);
      TestCases.Run(TestCases.Test_CUBRIDException);
      TestCases.Run(TestCases.Test_OID_Get);
      TestCases.Run(TestCases.Test_GetGeneratedKeys);
      TestCases.Run(TestCases.Test_ExecuteNonQuery_Query);
      TestCases.Run(TestCases.Test_Oid_Basic);
      TestCases.Run(TestCases.Test_DateTime_Types);

      //Many results
      TestCases.Run(TestCases.Test_Read_ManyRows);

      //CUBRID Schema 
      TestCases.Run(TestCases.Test_GetForeignKeys);
      TestCases.Run(TestCases.Test_GetTables);
      TestCases.Run(TestCases.Test_GetColumns);
      TestCases.Run(TestCases.Test_GetIndexes);
      TestCases.Run(TestCases.Test_GetUsers);
      TestCases.Run(TestCases.Test_GetViews);
      TestCases.Run(TestCases.Test_GetDatabases);
      TestCases.Run(TestCases.Test_GetProcedures);
      TestCases.Run(TestCases.Test_GetIndexColumns);

      //CUBRID DataTableReader SchemaTable 
      TestCases.Run(TestCases.Test_GetSchemaTable);

      //LOB schema
      TestCases.Run(TestCases.Test_Blob_Insert);
      TestCases.Run(TestCases.Test_Blob_Select);
      TestCases.Run(TestCases.Test_Clob_Insert);
      TestCases.Run(TestCases.Test_Clob_Select);
      TestCases.Run(TestCases.Test_Blob_SelectDataAdapter);
      TestCases.Run(TestCases.Test_Blob_SelectDataAdapter2);
      TestCases.Run(TestCases.Test_Clob_SelectDataAdapter);
      TestCases.Run(TestCases.Test_Clob_SelectDataAdapter2);
      TestCases.Run(TestCases.Test_Blob_Update);
      TestCases.Run(TestCases.Test_Clob_Update);
      TestCases.Run(TestCases.Test_Blob_FromFile);
      TestCases.Run(TestCases.Test_Clob_FromFile);
      TestCases.Run(TestCases.Test_Blob_InsertTransaction);
      TestCases.Run(TestCases.Test_Blob_UpdateTransaction);
      TestCases.Run(TestCases.Test_Blob_DeleteTransaction);

      //Localization
      TestCases.Run(TestCases.Test_StringResources);

      //Demo test cases
      TestCases.Run(TestCases.Test_Demo_Basic);
      TestCases.Run(TestCases.Test_Demo_Basic_WithParameters);

      //Connection settings
      TestCases.Run(TestCases.Test_IsolationLevel);
      TestCases.Run(TestCases.Test_AutoCommit);
      TestCases.Run(TestCases.Test_ConnectionProperties);

      //NULL handling settings
      TestCases.Run(TestCases.Test_Null_WithParameters);

      //Parameters collection
      TestCases.Run(TestCases.Test_Parameters_Collection);

      //Data types
      TestCases.Run(TestCases.Test_Various_DataTypes);
      TestCases.Run(TestCases.Test_Various_DataTypes_Parameters);

      //Other
      TestCases.Run(TestCases.Test_GetTableNameFromOid);
      TestCases.Run(TestCases.Test_QueryPlanOnly);
      TestCases.Run(TestCases.Test_SchemaProvider_FunctionTypes);
      TestCases.Run(TestCases.Test_SchemaProvider_DataTypes);

      //Collections
      TestCases.Run(TestCases.Test_SequenceOperations);
      TestCases.Run(TestCases.Test_SetOperations);

      //Encodings
      TestCases.Run(TestCases.Test_Encodings);
      TestCases.Run(TestCases.Test_EncodingsWithParameters);


      Console.WriteLine();
      Console.WriteLine(String.Format("*** Results ***"));
      Console.WriteLine(String.Format("{0} test case(s) analyzed.", testCasesCount));
      Console.WriteLine(String.Format("{0} test case(s) executed.", executed));
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine(String.Format("{0} test case(s) passed.", passed));
      if (executed - passed > 0)
      {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(String.Format("{0} test case(s) failed.", executed - passed));
      }
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine("Press any key to continue...");
      Console.ReadKey();
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

        conn.SetIsolationLevel(CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);
        Debug.Assert(conn.GetIsolationLevel() == CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);

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
        conn.BeginTransaction(IsolationLevel.ReadCommitted);
        TestCases.ExecuteSQL("create table isol(id int)", conn);
        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        //Verify table was created
        Debug.Assert(newTableCount == tablesCount + 1);

        using (CUBRIDConnection connOut = new CUBRIDConnection())
        {
          connOut.ConnectionString = TestCases.connString;
          connOut.Open();

          newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, connOut);
          //CREATE TABLE is NOT visible from another connection
          Debug.Assert(newTableCount == tablesCount);
        }

        conn.Commit();

        newTableCount = (int)TestCases.GetSingleValue(sqlTablesCount, conn);
        //Verify table was created
        Debug.Assert(newTableCount == tablesCount + 1);

        TestCases.ExecuteSQL("drop table if exists isol", conn);
        Debug.Assert(newTableCount == tablesCount);
      }
    }

    /// <summary>
    /// Test CUBRIDCommand GetGeneratedKeys() method
    /// </summary>
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
        TestCases.ExecuteSQL("create table xyz(id int)", conn);
      }

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        //Verify table was not created
        Debug.Assert(tablesCount == (int)TestCases.GetSingleValue("select count(*) from db_class", conn));

        //Create table
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
	/// Test ConnectionStringBuilder class
	/// </summary>
    private static void Test_ConnectionStringBuilder()
    {
      CUBRIDConnectionStringBuilder sb = new CUBRIDConnectionStringBuilder("localhost", 33000, "demodb", "public", "", "utf-8");
      //Note: Do not use sb.ConnectionString with empty password

      using (CUBRIDConnection conn = new CUBRIDConnection(sb.GetConnectionString()))
      {
        conn.Open();
      }

      sb = new CUBRIDConnectionStringBuilder("localhost", 33000, "demodb", "public", "wrong password", "utf-8");
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
    /// Test CUBRIDParameterCollection class
    /// </summary>
    private static void Test_Parameters_Collection()
    {
      string errMsg;

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
    /// Test Table name from OID
    /// </summary>
    private static void Test_GetTableNameFromOid()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string tableName = conn.GetTableNameFromOID("@620|1|0");

        Debug.Assert(tableName == "history");
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
    /// Test CUBRIDConnection GetQueryPlanOnly() method
    /// </summary>
    private static void Test_QueryPlanOnly()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string queryPlan = conn.GetQueryPlanOnly("select * from athlete order by 1 desc");

        Debug.Assert(queryPlan == "Join graph segments (f indicates final):\r\nseg[0]: [0]\r\nseg[1]: code[0] (f)\r\nseg[2]: name[0] (f)\r\nseg[3]: gender[0] (f)\r\nseg[4]: nation_code[0] (f)\r\nseg[5]: event[0] (f)\r\nJoin graph nodes:\r\nnode[0]: athlete athlete(6677/27) (sargs 0)\r\nJoin graph terms:\r\nterm[0]: (athlete.code range (-2147483648 ge_inf max)) (sel 1) (rank 2) (sarg term) (not-join eligible) (indexable code[0]) (loc 0)\r\n\r\nQuery plan:\r\n\r\niscan\r\n    class: athlete node[0]\r\n    index: pk_athlete_code term[0] (desc_index)\r\n    sort:  1 asc\r\n    cost:  fixed 33(0.0/33.0) var 47(20.0/27.0) card 6677\r\n\r\nQuery stmt:\r\n\r\nselect athlete.code, athlete.[name], athlete.gender, athlete.nation_code, athlete.event from athlete athlete where ((athlete.code>=-2147483648)) order by 1 desc \r\n\r\n/* ---> skip ORDER BY */\r\n");
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
        Debug.Assert(conn.DBVersion == "8.4.0.0196");
        Debug.Assert(conn.DataSource == "localhost");
        Debug.Assert(conn.AutoCommit == true);
        Debug.Assert(conn.LockTimeout == 30);
        Debug.Assert(conn.ConnectionTimeout == 30);
        Debug.Assert(conn.IsolationLevel == CUBRIDIsolationLevel.TRAN_REP_CLASS_UNCOMMIT_INSTANCE);
        Debug.Assert(conn.ServerVersion == "");
        Debug.Assert(conn.State == ConnectionState.Open);
      }
    }

    /// <summary>
    /// Test basic SQL Statements execution, using DataTable
    /// </summary>
    private static void Test_DataTable_Basic()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        using (DataTable dt = new DataTable("nation"))
        {
          CUBRIDDataAdapter da = new CUBRIDDataAdapter();
          da.SelectCommand = new CUBRIDCommand(sql, conn);
          da.Fill(dt);

          Debug.Assert(dt.Columns.Count == 4);
          Debug.Assert(dt.Rows.Count == 215);
          Debug.Assert(dt.Rows[1][1].ToString() == "Netherlands Antilles");
          Debug.Assert(dt.Rows[3][2].ToString() == "Africa");
        }
      }
    }

    //http://msdn.microsoft.com/en-us/library/tf579hcz%28v=vs.80%29.aspx
    /// <summary>
    /// Test CUBRIDCommandBuilder class, and methods used to automatically get SQL commands
    /// </summary>
    private static void Test_CommandBuilder_GetCommands()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn);

        CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(da);
        da.UpdateCommand = cmdBuilder.GetUpdateCommand();
        Debug.Assert(da.UpdateCommand.CommandText == "UPDATE `nation` SET `code` = ?, `name` = ?, `continent` = ?, `capital` = ? WHERE ((`code` = ?) AND (`name` = ?) AND ((? = 1 AND `continent` IS NULL) OR (`continent` = ?)) AND ((? = 1 AND `capital` IS NULL) OR (`capital` = ?)))");
        da.InsertCommand = cmdBuilder.GetInsertCommand();
        Debug.Assert(da.InsertCommand.CommandText == "INSERT INTO `nation` (`code`, `name`, `continent`, `capital`) VALUES (?, ?, ?, ?)");
        da.DeleteCommand = cmdBuilder.GetDeleteCommand();
        Debug.Assert(da.DeleteCommand.CommandText == "DELETE FROM `nation` WHERE ((`code` = ?) AND (`name` = ?) AND ((? = 1 AND `continent` IS NULL) OR (`continent` = ?)) AND ((? = 1 AND `capital` IS NULL) OR (`capital` = ?)))");
      }
    }

    //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
    /// <summary>
    /// Test DataTable implicit UPDATE
    /// </summary>
    private static void Test_DataTable_UpdateImplicit()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
        {

          using (CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(sql, conn))
          {
            CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd);
            da.UpdateCommand = cmdBuilder.GetUpdateCommand();
          }

          DataTable dt = new DataTable("nation");
          da.Fill(dt);

          //Update data
          DataRow workRow = dt.Rows[0];

          Debug.Assert(workRow["code"].ToString() == "AFG");
          Debug.Assert(workRow["capital"].ToString() == "Kabul");

          workRow.BeginEdit();
          workRow["capital"] = "MyKabul";
          workRow.EndEdit();
          da.Update(dt);

          Debug.Assert(workRow["capital"].ToString() == "MyKabul");
          Debug.Assert(workRow.RowState.ToString() != "New");
        }

        Debug.Assert((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn) == "MyKabul");
        //Revert changes
        ExecuteSQL("update nation set capital = 'Kabul' where capital = 'MyKabul'", conn);
        Debug.Assert((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn) == "Kabul");
      }
    }

    //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
    /// <summary>
    /// Test DataTable explicit UPDATE
    /// </summary>
    private static void Test_DataTable_UpdateExplicit()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc LIMIT 10";
        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
        {
          //Initialize the command object that will be used as the UpdateCommand for the DataAdapter.
          CUBRIDCommand daUpdate = new CUBRIDCommand("update nation set capital = ? where code = ?", conn);

          //Parameter: capital
          daUpdate.Parameters.Add(new CUBRIDParameter("?p1", DbType.String));
          daUpdate.Parameters[0].SourceVersion = DataRowVersion.Current;
          daUpdate.Parameters[0].SourceColumn = "capital";
          daUpdate.Parameters[0].SourceColumnNullMapping = false;

          //Parameter: code
          daUpdate.Parameters.Add(new CUBRIDParameter("?p2", DbType.String));
          daUpdate.Parameters[1].SourceVersion = DataRowVersion.Original;
          daUpdate.Parameters[1].SourceColumn = "code";
          daUpdate.Parameters[1].SourceColumnNullMapping = false;

          daUpdate.UpdatedRowSource = UpdateRowSource.None;
          //Assign the command to the UpdateCommand property of the DataAdapter.
          da.UpdateCommand = daUpdate;

          DataTable dt = new DataTable("nation");
          da.Fill(dt);
          DataRow workRow = dt.Rows[0];
          Debug.Assert(workRow["capital"].ToString() == "Kabul");
          workRow.BeginEdit();
          workRow["capital"] = "MyKabul";
          workRow.EndEdit();
          da.Update(dt);
          dt.AcceptChanges();

          Debug.Assert(workRow["capital"].ToString() == "MyKabul");
          Debug.Assert(workRow.RowState.ToString() != "New");
        }

        Debug.Assert((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn) == "MyKabul");
        //Revert changes
        ExecuteSQL("update nation set capital = 'Kabul' where capital = 'MyKabul'", conn);
        Debug.Assert((string)GetSingleValue("select capital from nation where `code` = 'AFG'", conn) == "Kabul");
      }
    }

    //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
    /// <summary>
    /// Test DataTable implicit INSERT
    /// </summary>
    private static void Test_DataTable_InsertImplicit()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
        {
          using (CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(sql, conn))
          {
            CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd);
            da.InsertCommand = cmdBuilder.GetInsertCommand();
          }

          DataTable dt = new DataTable("nation");
          da.Fill(dt);

          DataRow newRow = dt.NewRow();
          newRow["code"] = "ZZZ";
          newRow["name"] = "ABCDEF";
          newRow["capital"] = "MyXYZ";
          newRow["continent"] = "QWERTY";
          dt.Rows.Add(newRow);

          da.Update(dt);

          Debug.Assert(dt.Rows.Count == 216);
        }

        Debug.Assert(GetTableRowsCount("nation", conn) == 216);
        //Revert changes
        ExecuteSQL("delete from nation where `code` = 'ZZZ'", conn);
        Debug.Assert(GetTableRowsCount("nation", conn) == 215);
      }
    }

    /// <summary>
    /// Test DataTable explicit INSERT
    /// </summary>
    private static void Test_DataTable_InsertExplicit()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` DESC LIMIT 10";
        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
        {

          //Initialize the command object that will be used as the UpdateCommand for the DataAdapter.
          CUBRIDCommand daInsert = new CUBRIDCommand("insert into nation values(?,?,?,?)", conn);
          daInsert.CommandType = CommandType.Text;

          //Parameter: code
          daInsert.Parameters.Add(new CUBRIDParameter("?p1", DbType.String));
          daInsert.Parameters["?p1"].SourceVersion = DataRowVersion.Current;
          daInsert.Parameters["?p1"].SourceColumn = "code";
          daInsert.Parameters["?p1"].SourceColumnNullMapping = false;

          //Parameter: name
          daInsert.Parameters.Add(new CUBRIDParameter("?p2", DbType.String));
          daInsert.Parameters["?p2"].SourceVersion = DataRowVersion.Original;
          daInsert.Parameters["?p2"].SourceColumn = "name";
          daInsert.Parameters["?p2"].SourceColumnNullMapping = false;

          //Parameter: continent
          daInsert.Parameters.Add(new CUBRIDParameter("?p3", DbType.String));
          daInsert.Parameters["?p3"].SourceVersion = DataRowVersion.Current;
          daInsert.Parameters["?p3"].SourceColumn = "continent";
          daInsert.Parameters["?p3"].SourceColumnNullMapping = false;

          //Parameter: capital
          daInsert.Parameters.Add(new CUBRIDParameter("?p4", DbType.String));
          daInsert.Parameters["?p4"].SourceVersion = DataRowVersion.Original;
          daInsert.Parameters["?p4"].SourceColumn = "capital";
          daInsert.Parameters["?p4"].SourceColumnNullMapping = false;

          daInsert.UpdatedRowSource = UpdateRowSource.None;

          //Assign the command to the InsertCommand property of the DataAdapter.
          da.InsertCommand = daInsert;

          DataTable dt = new DataTable("nation");
          da.Fill(dt);
          DataRow newRow = dt.NewRow();
          newRow["code"] = "ZZZ";
          newRow["name"] = "ABCDEF";
          newRow["capital"] = "MyXYZ";
          newRow["continent"] = "QWERTY";
          dt.Rows.InsertAt(newRow, 0);
          da.Update(dt);
          dt.AcceptChanges();

          Debug.Assert(dt.Rows[0]["capital"].ToString() == "MyXYZ");
          Debug.Assert(newRow.RowState.ToString() != "New");
        }

        Debug.Assert(GetTableRowsCount("nation", conn) == 216);
        //Revert changes
        ExecuteSQL("delete from nation where `code` = 'ZZZ'", conn);
        Debug.Assert(GetTableRowsCount("nation", conn) == 215);
      }
    }

    /// <summary>
    /// Test DataTable implicit DELETE
    /// </summary>
    private static void Test_DataTable_DeleteImplicit()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        //Insert a new row
        ExecuteSQL("insert into nation values('ZZZZ', 'Z', 'Z', 'Z')", conn);
        Debug.Assert(GetTableRowsCount("nation", conn) == 216);

        String sql = "select * from nation order by `code` desc";
        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
        {
          CUBRIDDataAdapter daCmd = new CUBRIDDataAdapter(sql, conn);
          CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(daCmd);
          da.DeleteCommand = cmdBuilder.GetDeleteCommand();

          DataTable dt = new DataTable("nation");
          da.Fill(dt);

          Debug.Assert(dt.Rows[0]["capital"].ToString() == "Z");

          dt.Rows[0].Delete();
          da.Update(dt);

          Debug.Assert(dt.Rows.Count == 215);
        }

        Debug.Assert(GetTableRowsCount("nation", conn) == 215);
      }
    }

    //http://msdn.microsoft.com/en-us/library/bbw6zyha%28v=vs.80%29.aspx
    /// <summary>
    /// Test DataTable explicit DELETE
    /// </summary>
    private static void Test_DataTable_DeleteExplicit()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        //Insert a new row
        ExecuteSQL("insert into nation values('ZZZZ', 'Z', 'Z', 'Z')", conn);
        Debug.Assert(GetTableRowsCount("nation", conn) == 216);

        String sql = "select * from nation order by `code` desc";
        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
        {
          //Initialize the command object that will be used as the DeleteCommand for the DataAdapter.
          CUBRIDCommand daDelete = new CUBRIDCommand("delete from nation where code = ?", conn);

          //Parameter: code
          daDelete.Parameters.Add(new CUBRIDParameter("?p1", DbType.String));
          daDelete.Parameters["?p1"].SourceVersion = DataRowVersion.Original;
          daDelete.Parameters["?p1"].SourceColumn = "code";
          daDelete.Parameters["?p1"].SourceColumnNullMapping = false;

          daDelete.UpdatedRowSource = UpdateRowSource.OutputParameters;

          //Assign the command to the DeleteCommand property of the DataAdapter.
          da.DeleteCommand = daDelete;

          DataTable dt = new DataTable("nation");
          da.Fill(dt);

          Debug.Assert(dt.Rows[0]["capital"].ToString() == "Z");

          dt.Rows[0].Delete();
          da.Update(dt);

          Debug.Assert(dt.Rows.Count == 215);
        }

        Debug.Assert(GetTableRowsCount("nation", conn) == 215);
      }
    }

    /// <summary>
    /// Test basic SQL statements execution, using DataSet
    /// </summary>
    private static void Test_DataSet_Basic()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        DataSet ds = new DataSet("nation");
        da.Fill(ds);

        DataTable dt0 = ds.Tables["Table"];
        Debug.Assert(dt0 != null);

        dt0 = ds.Tables[0];

        Debug.Assert(dt0.Columns.Count == 4);
        Debug.Assert(dt0.DefaultView.Count == 215);
        Debug.Assert(dt0.DefaultView.AllowEdit == true);
        Debug.Assert(dt0.DefaultView.AllowDelete == true);
        Debug.Assert(dt0.DefaultView.AllowNew == true);
        Debug.Assert(dt0.DataSet.DataSetName == "nation");

        DataRow[] dataRow = dt0.Select("continent = 'Africa'");

        Debug.Assert(dataRow.Length == 54);
      }
    }

    /// <summary>
    /// Test exporting XML from DataSet
    /// </summary>
    private static void Test_DataSet_ExportXML()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        DataSet ds = new DataSet();
        da.Fill(ds, "nation");

        string filename = @".\Test_DataSet_ExportXML.xml";
        ds.WriteXml(filename);

        if (!System.IO.File.Exists(filename))
        {
          throw new Exception("XML output file not found!");
        }
        else
        {
          System.IO.File.Delete(filename);
        }
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider class
    /// </summary>
    private static void Test_SchemaProvider_FunctionTypes()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        DataTable rw = CUBRIDSchemaProvider.GetReservedWords();
        string[] nf = CUBRIDSchemaProvider.GetNumericFunctions();
        string[] sf = CUBRIDSchemaProvider.GetStringFunctions();

        Debug.Assert(nf.GetValue(0).ToString() == "AVG");
        Debug.Assert(nf.GetValue(nf.Length - 1).ToString() == "VARIANCE");

        Debug.Assert(sf.GetValue(0).ToString() == "BIT_LENGTH");
        Debug.Assert(sf.GetValue(sf.Length - 1).ToString() == "UPPER");

        Debug.Assert(rw.Rows[0][0].ToString() == "ADD");
        Debug.Assert(rw.Rows[rw.Rows.Count - 1][0].ToString() == "TO_DATETIME");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider data types functions
    /// </summary>
    private static void Test_SchemaProvider_DataTypes()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        DataTable dt = CUBRIDSchemaProvider.GetDataTypes();

        Debug.Assert(dt.Rows.Count == 19);

        //SetDataTypeInfo(dt, "BIGINT", CUBRIDDataType.CCI_U_TYPE_BIGINT, typeof(Int32), 
        //ToBool(IsAutoIncrementable.Yes), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.Yes), ToBool(IsNullable.Yes));
        Debug.Assert(dt.Rows[0]["TypeName"].ToString() == "BIGINT");
        Debug.Assert((CUBRIDDataType)dt.Rows[0]["ProviderDataType"] == CUBRIDDataType.CCI_U_TYPE_BIGINT);
        Debug.Assert((Type)dt.Rows[0]["DbType"] == typeof(Int32));
        Debug.Assert(dt.Rows[0]["Size"].ToString() == String.Empty);
        Debug.Assert((bool)dt.Rows[0]["IsLong"] == true);
        Debug.Assert((bool)dt.Rows[0]["IsFixedLength"] == true);
        Debug.Assert((bool)dt.Rows[0]["IsFixedPrecisionScale"] == true);
        Debug.Assert((bool)dt.Rows[0]["IsNullable"] == true);
        Debug.Assert((bool)dt.Rows[0]["IsAutoIncrementable"] == true);
      }
    }

    /// <summary>
    /// Test DataTable column properties
    /// </summary>
    private static void Test_DataTable_ColumnProperties()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation";
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        DataTable dt = new DataTable("nation");
        da.FillSchema(dt, SchemaType.Source);//To retrieve all the column properties you have to use the FillSchema() method

        Debug.Assert(dt.Columns[0].ColumnName == "code");
        Debug.Assert(dt.Columns[0].AllowDBNull == false);
        Debug.Assert(dt.Columns[0].DefaultValue.ToString() == "");
        Debug.Assert(dt.Columns[0].Unique == true);
        Debug.Assert(dt.Columns[0].DataType == typeof(System.String));
        Debug.Assert(dt.Columns[0].Ordinal == 0);
        Debug.Assert(dt.Columns[0].Table == dt);
      }
    }

    /// <summary>
    /// Test CUBRIDCommand column properties
    /// </summary>
    private static void Test_Command_ColumnProperties()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = cmd;
        DataTable dt = new DataTable("");
        da.FillSchema(dt, SchemaType.Source);//To retrieve all the column properties you have to use the FillSchema() method

        Debug.Assert(cmd.ColumnInfos[0].Name == "code");
        Debug.Assert(cmd.ColumnInfos[0].IsPrimaryKey == true);
        Debug.Assert(cmd.ColumnInfos[0].IsForeignKey == false);
        Debug.Assert(cmd.ColumnInfos[0].IsNullable == false);
        Debug.Assert(cmd.ColumnInfos[0].RealName == "");
        Debug.Assert(cmd.ColumnInfos[0].Precision == 3);
        Debug.Assert(cmd.ColumnInfos[0].Scale == 0);
        Debug.Assert(cmd.ColumnInfos[0].IsAutoIncrement == false);
        Debug.Assert(cmd.ColumnInfos[0].IsReverseIndex == false);
        Debug.Assert(cmd.ColumnInfos[0].IsReverseUnique == false);
        Debug.Assert(cmd.ColumnInfos[0].IsShared == false);
        Debug.Assert(cmd.ColumnInfos[0].Type == CUBRIDDataType.CCI_U_TYPE_CHAR);
        Debug.Assert(cmd.ColumnInfos[0].Table == "nation");
      }
    }

    /// <summary>
    /// Test SQL statements execution, using DataReader and parameters
    /// </summary>
    private static void Test_DataReader_Parameters()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDCommand cmd = new CUBRIDCommand("select `code` from nation where capital = ?", conn);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
        param.Value = "Kabul";

        cmd.Parameters.Add(param);

        DbDataReader reader = cmd.ExecuteReader();

        Debug.Assert(reader.FieldCount == 1);

        while (reader.Read()) //only one row is available
        {
          Debug.Assert(reader.GetString(0) == "AFG");
        }

        cmd.Close();
      }
    }

    /// <summary>
    /// Test batch update, using DataAdapter
    /// </summary>
    private static void Test_DataAdapter_BatchUpdate()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        using (CUBRIDDataAdapter da = new CUBRIDDataAdapter())
        {
          // Set the INSERT command and parameter.
          da.InsertCommand = new CUBRIDCommand("insert into nation values ('A', 'B', 'C', 'D')");
          da.InsertCommand.UpdatedRowSource = UpdateRowSource.None;

          // Set the UPDATE command and parameters.
          da.UpdateCommand = new CUBRIDCommand("update nation set capital = 'X' where `code` = 'A'");
          da.UpdateCommand.UpdatedRowSource = UpdateRowSource.None;

          // Set the DELETE command and parameter.
          da.DeleteCommand = new CUBRIDCommand("delete from nation where `code` = 'A'");
          da.DeleteCommand.UpdatedRowSource = UpdateRowSource.None;

          // Set the batch size.
          da.UpdateBatchSize = 3;

          // Execute the update.
          DataTable dt = new DataTable("nation");
          da.Update(dt);
        }

        Debug.Assert(GetTableRowsCount("nation", conn) == 215);
      }
    }

    /// <summary>
    /// Test SQL statements execution, using DataView
    /// </summary>
    private static void Test_DataView_Basic()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        DataTable dt = new DataTable("nation");
        da.Fill(dt);

        DataView dataView = new DataView(dt);

        Debug.Assert(dataView.Count == 215);
        Debug.Assert(dataView.Table.TableName == "nation");

        foreach (DataRowView view in dataView)
        {
          Debug.Assert(dataView[0][0].ToString() == "AFG");
          break; //retrieve just one row
        }
      }
    }

    /// <summary>
    /// Test SQL statements execution, using DataReader
    /// </summary>
    private static void Test_DataReader_Basic()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql = "select * from nation order by `code` asc";

        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read(); //retrieve just one row

            Debug.Assert(reader.FieldCount == 4);
            Debug.Assert(reader.GetString(0) == "AFG");
            Debug.Assert(reader.GetString(1) == "Afghanistan");
            Debug.Assert(reader.GetString(2) == "Asia");
            Debug.Assert(reader.GetString(3) == "Kabul");
          }
        }
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
          Debug.Assert(reader.GetDate(0, "yy/MM/dd") == "08-10-31");
          Debug.Assert(reader.GetTime(0) == "10:20:30");
          Debug.Assert(reader.GetTime(0, "HH") == "10");
          Debug.Assert(reader.GetTimestamp(0) == "2008-10-31 10:20:30.040");
          Debug.Assert(reader.GetTimestamp(0, "yyyy HH") == "2008 10");
        }

        CleanupTestTable(conn);
      }
    }

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

        sql = "? = CALL sp1(?)";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.CommandType = CommandType.StoredProcedure;

          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_INT);
          p1.Value = 12345678;
          cmd.Parameters.Add(p1);

          CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_STRING);
          p2.Direction = ParameterDirection.Output;
          cmd.Parameters.Add(p2);

          cmd.ExecuteNonQuery();

          Debug.Assert((int)cmd.Parameters[0].Value == 0);
        }

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

        string sql = "CREATE FUNCTION sp2() RETURN cursor AS LANGUAGE JAVA NAME 'SpTest.test2() return java.sql.ResultSet'";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.ExecuteNonQuery();
        }

        sql = "? = CALL sp2()";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          cmd.CommandType = CommandType.StoredProcedure;

          CUBRIDParameter p1 = new CUBRIDParameter("?p1", CUBRIDDataType.CCI_U_TYPE_RESULTSET);
          p1.Direction = ParameterDirection.Output;
          cmd.Parameters.Add(p1);

          cmd.ExecuteNonQuery();

          DbDataReader reader = (DbDataReader)cmd.Parameters[0].Value;
          reader.Read();

          Debug.Assert(reader.GetInt32(0) == 0);
          Debug.Assert(reader.GetString(1) == "");
          Debug.Assert(reader.GetString(2) == "");
          Debug.Assert(reader.GetFloat(3) == 0.0f);
          Debug.Assert(reader.GetDouble(4) == 0.0d);
          Debug.Assert(reader.GetDateTime(5) == new DateTime(1, 1, 1, 0, 0, 0));
        }

        TestCases.ExecuteSQL("drop function sp2", conn);
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
            Debug.Assert(reader.GetDateTime(10) == new DateTime(1, 1, 1, 13, 15, 45));
            Debug.Assert(reader.GetDateTime(11) == new DateTime(2000, 10, 31));
            Debug.Assert(reader.GetDateTime(12) == new DateTime(2008, 10, 31, 13, 15, 45));
            Debug.Assert(reader.GetDateTime(13) == new DateTime(2008, 10, 31, 13, 15, 45));
            Debug.Assert(reader.GetByte(14) == (byte)0);
            Debug.Assert(reader.GetByte(15) == (byte)0);
            Debug.Assert(reader.GetString(16) == "123456789");
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
        Debug.Assert((uint)dt.Rows[0]["NUMERIC_PRECISION"] == (uint)0);
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
        Debug.Assert(dt.Rows[0]["PKTABLE_NAME"].ToString() == "athlete");
        Debug.Assert(dt.Rows[0]["PKCOLUMN_NAME"].ToString() == "code");
        Debug.Assert(dt.Rows[0]["FKTABLE_NAME"].ToString() == "game");
        Debug.Assert(dt.Rows[0]["FKCOLUMN_NAME"].ToString() == "athlete_code");
        Debug.Assert((short)dt.Rows[0]["KEY_SEQ"] == (short)1);
        Debug.Assert((short)dt.Rows[0]["UPDATE_ACTION"] == (short)1);
        Debug.Assert((short)dt.Rows[0]["DELETE_ACTION"] == (short)1);
        Debug.Assert(dt.Rows[0]["FK_NAME"].ToString() == "fk_game_athlete_code");
        Debug.Assert(dt.Rows[0]["PK_NAME"].ToString() == "pk_athlete_code");

        conn.Close();
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
            Debug.Assert(ex.Message == "Syntax: Unknown class \"xyz\". select count(*) from xyz");//todo
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
            Debug.Assert(ex.Message == "Syntax: Unknown class \"xyz\". select count(*) from xyz"); //TODO
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
    /// Test CUBRIDDataReader getter methods
    /// </summary>
    private static void Test_DataReader_Getxxx()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select * from nation;";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
          {
            reader.Read();

            Debug.Assert(reader.GetOrdinal("code") == 0);
            Debug.Assert(reader.GetName(0) == "code");
            Debug.Assert(reader.GetColumnName(0) == "code");
            Debug.Assert(reader.GetColumnType(0) == typeof(System.String));
            Debug.Assert(reader.GetDataTypeName(0) == "CHAR");
          }
        }
      }
    }

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
            Debug.Assert(ex.Message == "Invalid Query Type for ExecuteNonQuery");
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
            Debug.Assert(ex.Message == "Invalid Query Type for ExecuteDataReader");
          }
        }
      }
    }

    /// <summary>
    /// Test read many rows in one SQL statement execution
    /// </summary>
    private static void Test_Read_ManyRows()
    {
      int curr_row = 0;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select * from athlete";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {

          DbDataReader reader = cmd.ExecuteReader();
          while (reader.Read())
          {
            curr_row++;
          }
          cmd.Close();
        }
      }

      Debug.Assert(curr_row == 6677);
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetForeignKeys() method
    /// </summary>
    private static void Test_GetForeignKeys()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetForeignKeys(new string[] { "game" });

        Debug.Assert(dt.Columns.Count == 9);
        Debug.Assert(dt.Rows.Count == 2);

        Debug.Assert(dt.Rows[0][0].ToString() == "athlete");
        Debug.Assert(dt.Rows[0][1].ToString() == "code");
        Debug.Assert(dt.Rows[0][2].ToString() == "game");
        Debug.Assert(dt.Rows[0][3].ToString() == "athlete_code");
        Debug.Assert(dt.Rows[0][4].ToString() == "1");
        Debug.Assert(dt.Rows[0][5].ToString() == "1");
        Debug.Assert(dt.Rows[0][6].ToString() == "1");
        Debug.Assert(dt.Rows[0][7].ToString() == "fk_game_athlete_code");
        Debug.Assert(dt.Rows[0][8].ToString() == "pk_athlete_code");

        Debug.Assert(dt.Rows[1][0].ToString() == "event");
        Debug.Assert(dt.Rows[1][1].ToString() == "code");
        Debug.Assert(dt.Rows[1][2].ToString() == "game");
        Debug.Assert(dt.Rows[1][3].ToString() == "event_code");
        Debug.Assert(dt.Rows[1][4].ToString() == "1");
        Debug.Assert(dt.Rows[1][5].ToString() == "1");
        Debug.Assert(dt.Rows[1][6].ToString() == "1");
        Debug.Assert(dt.Rows[1][7].ToString() == "fk_game_event_code");
        Debug.Assert(dt.Rows[1][8].ToString() == "pk_event_code");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetTables() method
    /// </summary>
    private static void Test_GetTables()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetTables(new string[] { "%" });

        Debug.Assert(dt.Columns.Count == 3);
        Debug.Assert(dt.Rows.Count == 10);

        Debug.Assert(dt.Rows[0][0].ToString() == "demodb");
        Debug.Assert(dt.Rows[0][1].ToString() == "demodb");
        Debug.Assert(dt.Rows[0][2].ToString() == "stadium");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetColumns() method
    /// </summary>
    private static void Test_GetColumns()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetColumns(new string[] { "game" });

        Debug.Assert(dt.Columns.Count == 11);
        Debug.Assert(dt.Rows.Count == 7);

        Debug.Assert(dt.Rows[0][3].ToString() == "host_year");
        Debug.Assert(dt.Rows[1][3].ToString() == "event_code");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetIndexes() method
    /// </summary>
    private static void Test_GetIndexes()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetIndexes(new string[] { "game" });

        Debug.Assert(dt.Columns.Count == 9);
        Debug.Assert(dt.Rows.Count == 5);

        Debug.Assert(dt.Rows[3][2].ToString() == "pk_game_host_year_event_code_athlete_code"); //Index name
        Debug.Assert(dt.Rows[3][4].ToString() == "True"); //Is PK?
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetUsers() method
    /// </summary>
    private static void Test_GetUsers()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetUsers(null);

        Debug.Assert(dt.Columns.Count == 1);
        Debug.Assert(dt.Rows.Count >= 2);

        Debug.Assert(dt.Rows[0][0].ToString().ToUpper() == "DBA");
        Debug.Assert(dt.Rows[1][0].ToString().ToUpper() == "PUBLIC");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetViews() method
    /// </summary>
    private static void Test_GetViews()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetViews(null);

        Debug.Assert(dt.Columns.Count == 3);
        Debug.Assert(dt.Rows.Count == 0);
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetDatabases() method
    /// </summary>
    private static void Test_GetDatabases()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetDatabases(new string[] { "demo%" });

        Debug.Assert(dt.Columns.Count == 2);
        Debug.Assert(dt.Rows.Count >= 1);

        Debug.Assert(dt.Rows[0][0].ToString() == "demodb");
        Debug.Assert(dt.Rows[0][1].ToString() == "demodb");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetProcedures() method
    /// </summary>
    private static void Test_GetProcedures()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetProcedures(null);

        Debug.Assert(dt.Columns.Count == 7);
        Debug.Assert(dt.Rows.Count == 2);

        Debug.Assert(dt.Rows[0][0].ToString() == "p_myproc");
        Debug.Assert(dt.Rows[1][0].ToString() == "my_func");
      }
    }

    /// <summary>
    /// Test CUBRIDSchemaProvider GetIndexColumns() method
    /// </summary>
    private static void Test_GetIndexColumns()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CUBRIDSchemaProvider schema = new CUBRIDSchemaProvider(conn);
        DataTable dt = schema.GetIndexColumns(new string[] { "game" });

        Debug.Assert(dt.Columns.Count == 7);
        Debug.Assert(dt.Rows.Count == 5);

        Debug.Assert(dt.Rows[0][2].ToString() == "pk_game_host_year_event_code_athlete_code");
      }
    }

    /// <summary>
    /// Test DataTableReader GetSchemaTable() method
    /// </summary>
    private static void Test_GetSchemaTable()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "select * from athlete order by `code`";
        CUBRIDDataAdapter adapter = new CUBRIDDataAdapter(sql, conn);
        DataTable table = new DataTable();

        //To retrieve the AlolowDBNull, IsUnique, IsKey, IsAutoIncrement and BaseTableName values from the Database Server
        //you must use the FillSchema() method.
        adapter.FillSchema(table, SchemaType.Source);

        using (DataTableReader reader = new DataTableReader(table))
        {
          DataTable schemaTable = reader.GetSchemaTable();
          DataRow row = schemaTable.Rows[0];

          Debug.Assert(row["ColumnName"].ToString() == "code");
          Debug.Assert(row["ColumnOrdinal"].ToString() == "0");
          Debug.Assert(row["ColumnSize"].ToString() == "-1");
          Debug.Assert(row["NumericPrecision"].ToString() == "");
          Debug.Assert(row["NumericScale"].ToString() == "");
          Debug.Assert(row["IsUnique"].ToString() == "True");
          Debug.Assert(row["IsKey"].ToString() == "True");
          Debug.Assert(row["BaseTableNamespace"].ToString() == "");
          Debug.Assert(row["BaseColumnNamespace"].ToString() == "");
          Debug.Assert(row["BaseCatalogName"].ToString() == "");
          Debug.Assert(row["BaseColumnName"].ToString() == "code");
          Debug.Assert(row["BaseSchemaName"].ToString() == "");
          Debug.Assert(row["BaseTableName"].ToString() == "athlete");
          Debug.Assert(row["DataType"].ToString() == "System.Int32");
          Debug.Assert(row["AllowDBNull"].ToString() == "False");
          Debug.Assert(row["ProviderType"].ToString() == "");
          Debug.Assert(row["Expression"].ToString() == "");
          Debug.Assert(row["AutoIncrementSeed"].ToString() == "0");
          Debug.Assert(row["AutoincrementStep"].ToString() == "1");
          Debug.Assert(row["IsAutoIncrement"].ToString() == "True");
          Debug.Assert(row["IsRowVersion"].ToString() == "False");
          Debug.Assert(row["IsLong"].ToString() == "False");
          Debug.Assert(row["IsReadOnly"].ToString() == "False");
          Debug.Assert(row["ColumnMapping"].ToString() == "1");
          Debug.Assert(row["DefaultValue"].ToString() == "");
        }
      }
    }

    #region LOB

    /// <summary>
    /// Test BLOB INSERT
    /// </summary>
    private static void Test_Blob_Insert()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql = "insert into t (b) values(?)";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        CUBRIDBlob Blob = new CUBRIDBlob(conn);

        byte[] bytes = new byte[256];
        bytes[0] = 69;
        bytes[1] = 98;
        bytes[2] = 99;
        bytes[255] = 122;

        Blob.setBytes(1, bytes);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?p";
        param.Value = Blob;
        cmd.Parameters.Add(param);
        cmd.Parameters[0].DbType = DbType.Binary;
        cmd.ExecuteNonQuery();
        cmd.Close();

        string sql2 = "SELECT b from t";
        CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn);
        DbDataReader reader = cmd2.ExecuteReader();
        while (reader.Read())
        {
          CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
          byte[] bytes2 = new byte[(int)bImage.BlobLength];
          bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytes.Length == bytes2.Length, "The inserted BLOB length is not valid!");
          bool ok = true;
          for (int i = 0; i < bytes.Length; i++)
          {
            if (bytes[i] != bytes2[i])
              ok = false;
          }

          Debug.Assert(ok == true, "The BLOB was not inserted correctly!");
        }

        cmd2.Close();

        CleanupTestTableLOB(conn);
      }
    }

    ///<summary>
    /// Test BLOB SELECT
    /// </summary>
    private static void Test_Blob_Select()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (b) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);
        CUBRIDBlob Blob = new CUBRIDBlob(conn);

        byte[] bytes1 = new byte[256];
        bytes1[0] = 69;
        bytes1[1] = 98;
        bytes1[2] = 99;
        bytes1[255] = 122;

        Blob.setBytes(1, bytes1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        //param.DataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
        param.Value = Blob;
        cmd1.Parameters.Add(param);
        cmd1.Parameters[0].DbType = DbType.Binary;
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "SELECT b from t";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        DbDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
          CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
          byte[] bytes = new byte[(int)bImage.BlobLength];
          bytes = bImage.getBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytes1.Length == bytes.Length, "The selected BLOB length is not valid!");
          bool ok = true;
          for (int i = 0; i < bytes.Length; i++)
          {
            if (bytes1[i] != bytes[i])
              ok = false;
          }

          Debug.Assert(ok == true, "The BLOB was not selected correctly!");
        }

        cmd.Close();

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test CLOB INSERT
    /// </summary>
    private static void Test_Clob_Insert()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql = "insert into t (c) values(?)";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

        CUBRIDClob Clob = new CUBRIDClob(conn);
        String str = conn.ConnectionString; //Use ConnectionString content for testing
        Clob.setString(1, str);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
        param.Value = Clob;
        cmd.Parameters.Add(param);
        cmd.ExecuteNonQuery();
        cmd.Close();

        string sql2 = "SELECT c from t";
        CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn);
        DbDataReader reader = cmd2.ExecuteReader();

        while (reader.Read())
        {
          CUBRIDClob cImage = (CUBRIDClob)reader[0];
          string str2 = cImage.getString(1, (int)cImage.ClobLength);

          Debug.Assert(str.Length == str2.Length, "The inserted CLOB length is not valid!");
          Debug.Assert(str.Equals(str2), "The CLOB was not inserted correctly!");
        }

        cmd2.Close();

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test CLOB SELECT
    /// </summary>
    private static void Test_Clob_Select()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (c) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);

        CUBRIDClob Clob1 = new CUBRIDClob(conn);
        String str1 = conn.ConnectionString; //Use ConnectionString content for testing
        Clob1.setString(1, str1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
        param.Value = Clob1;
        cmd1.Parameters.Add(param);
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "SELECT c from t";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        DbDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
          CUBRIDClob cImage = (CUBRIDClob)reader[0];
          string str = cImage.getString(1, (int)cImage.ClobLength);

          Debug.Assert(str.Length == str1.Length, "The selected CLOB length is not valid!");
          Debug.Assert(str.Equals(str1), "The CLOB was not selected correctly!");
        }

        cmd.Close();

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB SELECT, using CUBRIDDataAdapter and DataTable
    /// </summary>
    private static void Test_Blob_SelectDataAdapter()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (b) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);
        CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

        byte[] bytes1 = new byte[256];
        bytes1[0] = 69;
        bytes1[1] = 98;
        bytes1[2] = 99;
        bytes1[255] = 122;

        Blob1.setBytes(1, bytes1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
        param.Value = Blob1;
        cmd1.Parameters.Add(param);
        cmd1.Parameters[0].DbType = DbType.Binary;
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "SELECT b from t";
        DataTable dt = new DataTable("t");
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        da.Fill(dt);

        for (int j = 0; j < dt.Rows.Count; j++)
        {
          CUBRIDBlob bImage = (CUBRIDBlob)dt.Rows[j]["b"];
          byte[] bytes = new byte[(int)bImage.BlobLength];
          bytes = bImage.getBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytes1.Length == bytes.Length, "The selected length is not valid!");
          bool ok = true;
          for (int i = 0; i < bytes.Length; i++)
          {
            if (bytes1[i] != bytes[i])
              ok = false;
          }

          Debug.Assert(ok == true, "The BLOB was not selected correctly!");
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB SELECT, using CUBRIDDataAdapter and DataSet
    /// </summary>
    private static void Test_Blob_SelectDataAdapter2()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (b) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);

        CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

        byte[] bytes1 = new byte[256];
        bytes1[0] = 69;
        bytes1[1] = 98;
        bytes1[2] = 99;
        bytes1[255] = 122;

        Blob1.setBytes(1, bytes1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
        param.Value = Blob1;
        cmd1.Parameters.Add(param);
        cmd1.Parameters[0].DbType = DbType.Binary;
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "SELECT b from t";

        DataSet ds = new DataSet("t");
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        da.Fill(ds);

        DataTable dt = ds.Tables[0];
        for (int j = 0; j < dt.Rows.Count; j++)
        {
          CUBRIDBlob bImage = (CUBRIDBlob)dt.Rows[j]["b"];
          byte[] bytes = new byte[(int)bImage.BlobLength];
          bytes = bImage.getBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytes1.Length == bytes.Length, "The selected BLOB length is not valid!");
          bool ok = true;
          for (int i = 0; i < bytes.Length; i++)
          {
            if (bytes1[i] != bytes[i])
              ok = false;
          }

          Debug.Assert(ok == true, "The BLOB was not selected correctly!");
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test CLOB SELECT, using CUBRIDDataAdapter and DataTable
    /// </summary>
    private static void Test_Clob_SelectDataAdapter()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (c) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);

        CUBRIDClob Clob1 = new CUBRIDClob(conn);

        String str1 = conn.ConnectionString; //Use ConnectionString content for testing
        Clob1.setString(1, str1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
        param.Value = Clob1;
        cmd1.Parameters.Add(param);
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "SELECT c from t";

        DataTable dt = new DataTable("t");
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        da.Fill(dt);

        for (int j = 0; j < dt.Rows.Count; j++)
        {
          CUBRIDClob cImage = (CUBRIDClob)dt.Rows[j]["c"];
          string str = cImage.getString(1, (int)cImage.ClobLength);

          Debug.Assert(str.Length == str1.Length, "The selected CLOB length is not valid!");
          Debug.Assert(str.Equals(str1), "The CLOB was not selected correctly!");
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test CLOB SELECT, using CUBRIDDataAdapter and DataSet
    /// </summary>
    private static void Test_Clob_SelectDataAdapter2()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (c) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);

        CUBRIDClob Clob1 = new CUBRIDClob(conn);

        String str1 = conn.ConnectionString; //Use ConnectionString content for testing
        Clob1.setString(1, str1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
        param.Value = Clob1;
        cmd1.Parameters.Add(param);
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "SELECT c from t";

        DataSet ds = new DataSet();
        CUBRIDDataAdapter da = new CUBRIDDataAdapter();
        da.SelectCommand = new CUBRIDCommand(sql, conn);
        da.Fill(ds);

        DataTable dt = ds.Tables[0];
        for (int j = 0; j < dt.Rows.Count; j++)
        {
          CUBRIDClob cImage = (CUBRIDClob)dt.Rows[j]["c"];
          string str = cImage.getString(1, (int)cImage.ClobLength);

          Debug.Assert(str.Length == str1.Length, "The selected CLOB length is not valid!");
          Debug.Assert(str.Equals(str1), "The CLOB was not selected correctly!");
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB UPDATE
    /// </summary>
    private static void Test_Blob_Update()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (b) values(?)";
        CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn);
        CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

        byte[] bytes1 = new byte[256];
        bytes1[0] = 69;
        bytes1[1] = 98;
        bytes1[2] = 99;
        bytes1[255] = 122;

        Blob1.setBytes(1, bytes1);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
        param.Value = Blob1;
        cmd1.Parameters.Add(param);
        cmd1.Parameters[0].DbType = DbType.Binary;
        cmd1.ExecuteNonQuery();
        cmd1.Close();

        string sql = "UPDATE t SET b = (?)";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

        CUBRIDBlob Blob = new CUBRIDBlob(conn);
        byte[] bytes = new byte[256];
        bytes[0] = 0;
        bytes[1] = 1;
        bytes[2] = 2;
        bytes[255] = 255;

        Blob.setBytes(1, bytes);
        CUBRIDParameter param2 = new CUBRIDParameter();
        param2.ParameterName = "?";
        param2.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
        param2.Value = Blob;
        cmd.Parameters.Add(param2);
        cmd.Parameters[0].DbType = DbType.Binary;
        cmd.ExecuteNonQuery();
        cmd.Close();

        String sql2 = "SELECT b from t";
        CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn);

        DbDataReader reader = cmd2.ExecuteReader();
        while (reader.Read())
        {
          CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
          byte[] bytes2 = new byte[(int)bImage.BlobLength];
          bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytes2.Length == bytes.Length, "The updated BLOB length is not valid!");

          bool ok = true;
          for (int i = 0; i < bytes.Length; i++)
          {
            if (bytes2[i] != bytes[i])
              ok = false;
          }

          Debug.Assert(ok == true, "The BLOB was not updated correctly!");
        }
        cmd2.Close();

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test CLOB UPDATE
    /// </summary>
    private static void Test_Clob_Update()
    {
      String str;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (c) values(?)";
        using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
        {
          CUBRIDClob Clob1 = new CUBRIDClob(conn);

          String str1 = conn.ConnectionString; //Use ConnectionString content for testing
          Clob1.setString(1, "test string to be inserted");

          CUBRIDParameter param1 = new CUBRIDParameter();
          param1.ParameterName = "?";
          param1.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
          param1.Value = Clob1;
          cmd1.Parameters.Add(param1);
          cmd1.ExecuteNonQuery();
          cmd1.Close();

          string sql = "UPDATE t SET c = ?";
          CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

          CUBRIDClob Clob = new CUBRIDClob(conn);
          str = conn.ConnectionString; //Use the ConnectionString for testing

          Clob.setString(1, str);
          CUBRIDParameter param = new CUBRIDParameter();
          param.ParameterName = "?";
          param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
          param.Value = Clob;
          cmd.Parameters.Add(param);
          cmd.ExecuteNonQuery();
        }

        string sql2 = "SELECT c from t";
        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
        {
          DbDataReader reader = cmd2.ExecuteReader();
          while (reader.Read())
          {
            CUBRIDClob cImage = (CUBRIDClob)reader[0];
            string str2 = cImage.getString(1, (int)cImage.ClobLength);

            Debug.Assert(str.Length == str2.Length, "The selected CLOB length is not valid!");
            Debug.Assert(str.Equals(str2), "The CLOB was not selected correctly!");
          }
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB INSERT, using a jpg image input file
    /// </summary>
    private static void Test_Blob_FromFile()
    {
      BinaryReader b;

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql = "insert into t (b) values(?)";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          CUBRIDBlob Blob = new CUBRIDBlob(conn);
          byte[] bytes;
          b = new BinaryReader(File.Open("1.jpg", FileMode.Open));
          int length = (int)b.BaseStream.Length;
          bytes = b.ReadBytes(length);

          Blob.setBytes(1, bytes);
          CUBRIDParameter param = new CUBRIDParameter();
          param.ParameterName = "?";
          param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
          param.Value = Blob;
          cmd.Parameters.Add(param);
          cmd.Parameters[0].DbType = DbType.Binary;
          cmd.ExecuteNonQuery();
        }

        string sql2 = "SELECT b from t";
        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
        {
          DbDataReader reader = cmd2.ExecuteReader();
          while (reader.Read())
          {
            CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
            byte[] bytes2 = new byte[(int)bImage.BlobLength];
            bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

            FileStream stream = new FileStream("1out.jpg", FileMode.Create);
            BinaryWriter writer = new BinaryWriter(stream);
            writer.Write(bytes2);
            writer.Close();

            BinaryReader b2 = new BinaryReader(File.Open("1out.jpg", FileMode.Open));
            Debug.Assert(b2.BaseStream.Length == b.BaseStream.Length, "The inserted BLOB length is not valid!");
            bool ok = true;
            int file1byte, file2byte;
            b.BaseStream.Position = 0;

            do
            {
              file1byte = b.BaseStream.ReadByte();
              file2byte = b2.BaseStream.ReadByte();
              if (file1byte != file2byte)
                ok = false;
            }
            while (file1byte != -1);

            Debug.Assert(ok == true, "The BLOB was not inserted correctly!");

            b.Close();
            b2.Close();
          }
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test CLOB INSERT, using a txt input file
    /// </summary>
    private static void Test_Clob_FromFile()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql = "insert into t (c) values(?)";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

        CUBRIDClob Clob = new CUBRIDClob(conn);

        String str = conn.ConnectionString;

        StreamReader r = new StreamReader("test.txt");
        string writestring = r.ReadToEnd();
        r.Close();

        Clob.setString(1, writestring);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?";
        param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_CLOB;
        param.Value = Clob;
        cmd.Parameters.Add(param);
        cmd.ExecuteNonQuery();
        cmd.Close();

        string sql2 = "SELECT c from t";
        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
        {
          DbDataReader reader = cmd2.ExecuteReader();

          while (reader.Read())
          {
            CUBRIDClob cImage = (CUBRIDClob)reader[0];
            string str2 = cImage.getString(1, (int)cImage.ClobLength);

            StreamWriter w = new StreamWriter("testout.txt");
            w.Write(str2);
            w.Close();

            StreamReader r2 = new StreamReader("testout.txt");
            string readstring = r2.ReadToEnd();
            r2.Close();

            Debug.Assert(writestring.Length == readstring.Length, "The inserted CLOB length is not valid!");
            Debug.Assert(writestring.Equals(readstring), "The CLOB was not inserted correctly!");
          }
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB INSERT in a transaction
    /// </summary>
    private static void Test_Blob_InsertTransaction()
    {
      DbTransaction tran = null;
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
        string sql = "insert into t (b) values(?)";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          CUBRIDBlob Blob = new CUBRIDBlob(conn);

          byte[] bytes = new byte[256];
          bytes[0] = 69;
          bytes[1] = 98;
          bytes[2] = 99;
          bytes[255] = 122;

          Blob.setBytes(1, bytes);

          CUBRIDParameter param = new CUBRIDParameter();
          param.ParameterName = "?";
          param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
          param.Value = Blob;
          cmd.Parameters.Add(param);
          cmd.Parameters[0].DbType = DbType.Binary;
          cmd.ExecuteNonQuery();
        }

        tran.Rollback();
      }

      //We have to close and reopen connection. Otherwise we get an invalid buffer position.
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();
        string sql2 = "SELECT b from t";
        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
        {
          DbDataReader reader = cmd2.ExecuteReader();
          Debug.Assert(reader.HasRows == false, "Transaction did not rollback!");
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB UPDATE in a transaction
    /// </summary>
    private static void Test_Blob_UpdateTransaction()
    {
      DbTransaction tran = null;
      byte[] bytes1 = new byte[256];

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (b) values(?)";
        using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
        {
          CUBRIDBlob Blob1 = new CUBRIDBlob(conn);

          bytes1[0] = 69;
          bytes1[1] = 98;
          bytes1[2] = 99;
          bytes1[255] = 122;

          Blob1.setBytes(1, bytes1);

          CUBRIDParameter param = new CUBRIDParameter();
          param.ParameterName = "?";
          param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
          param.Value = Blob1;
          cmd1.Parameters.Add(param);
          cmd1.Parameters[0].DbType = DbType.Binary;
          cmd1.ExecuteNonQuery();
        }

        tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
        string sql = "UPDATE t SET b = (?)";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          CUBRIDBlob Blob = new CUBRIDBlob(conn);
          byte[] bytes = new byte[256];
          bytes[0] = 0;
          bytes[1] = 1;
          bytes[2] = 2;
          bytes[255] = 255;

          Blob.setBytes(1, bytes);
          CUBRIDParameter param2 = new CUBRIDParameter();
          param2.ParameterName = "?";
          param2.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
          param2.Value = Blob;
          cmd.Parameters.Add(param2);
          cmd.Parameters[0].DbType = DbType.Binary;
          cmd.ExecuteNonQuery();
        }

        tran.Rollback();
      }

      //We have to close and reopen connection. Otherwise we get an invalid buffer position.
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        String sql2 = "SELECT b from t";
        using (CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn))
        {
          DbDataReader reader = cmd2.ExecuteReader();
          while (reader.Read())
          {
            CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
            byte[] bytes2 = new byte[(int)bImage.BlobLength];
            bytes2 = bImage.getBytes(1, (int)bImage.BlobLength);

            Debug.Assert(bytes2.Length == bytes1.Length);

            bool ok = true;
            for (int i = 0; i < bytes1.Length; i++)
            {
              if (bytes2[i] != bytes1[i])
                ok = false;
            }

            Debug.Assert(ok == true);
          }
        }

        CleanupTestTableLOB(conn);
      }
    }

    /// <summary>
    /// Test BLOB DELETE in a transaction
    /// </summary>
    private static void Test_Blob_DeleteTransaction()
    {
      DbTransaction tran = null;
      byte[] bytes1 = new byte[256];

      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        CreateTestTableLOB(conn);

        string sql1 = "insert into t (b) values(?)";
        using (CUBRIDCommand cmd1 = new CUBRIDCommand(sql1, conn))
        {
          CUBRIDBlob Blob = new CUBRIDBlob(conn);

          bytes1[0] = 69;
          bytes1[1] = 98;
          bytes1[2] = 99;
          bytes1[255] = 122;

          Blob.setBytes(1, bytes1);

          CUBRIDParameter param = new CUBRIDParameter();
          param.ParameterName = "?";
          param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_BLOB;
          param.Value = Blob;
          cmd1.Parameters.Add(param);
          cmd1.Parameters[0].DbType = DbType.Binary;
          cmd1.ExecuteNonQuery();
          cmd1.Close();

          tran = conn.BeginTransaction(IsolationLevel.ReadUncommitted);
          string sql2 = "DELETE from t";
          CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn);
          cmd2.ExecuteNonQuery();
        }

        tran.Rollback();
      }

      //We have to close and reopen connection. Otherwise we get an invalid buffer position.
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string sql = "SELECT b from t";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          DbDataReader reader = cmd.ExecuteReader();
          while (reader.Read())
          {
            Debug.Assert(reader.HasRows == true);

            CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
            byte[] bytes = new byte[(int)bImage.BlobLength];
            bytes = bImage.getBytes(1, (int)bImage.BlobLength);

            Debug.Assert(bytes1.Length == bytes.Length);

            bool ok = true;
            for (int i = 0; i < bytes.Length; i++)
            {
              if (bytes1[i] != bytes[i])
                ok = false;
            }

            Debug.Assert(ok == true, "The BLOB DELETE command was not rolled-back correctly!");
          }
        }

        CleanupTestTableLOB(conn);
      }
    }

    private static void Test_StringResources()
    {
      string str = CUBRID.Data.CUBRIDClient.CUBRIDException.GetStrTest();
      Debug.Assert(str == "Not implemented!");
    }

    #endregion LOB

    /// <summary>
    /// Test Encodings support
    /// </summary>
    private static void Test_Encodings()
    {
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
            conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;password=;charset=utf-8";
            conn.Open();

            TestCases.ExecuteSQL("drop table if exists t", conn);
            TestCases.ExecuteSQL("create table t(a int, b varchar(100))", conn);

            String sql = "insert into t values(1 ,'¾Æ¹«°³')";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }

            sql = "select * from t where b = '¾Æ¹«°³'";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read(); //retrieve just one row

                    Debug.Assert(reader.GetInt32(0) == 1);
                    Debug.Assert(reader.GetString(1) == "¾Æ¹«°³");
                }
            }

            sql = "update t set b='¾Æ¹°³'";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }

            sql = "select * from t where b = '¾Æ¹°³'";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read(); //retrieve just one row

                    Debug.Assert(reader.GetInt32(0) == 1);
                    Debug.Assert(reader.GetString(1) == "¾Æ¹°³");
                }
            }

            TestCases.ExecuteSQL("drop table if exists t", conn);
        }
    }

    /// <summary>
    /// Test Encodings support with parameters
    /// </summary>
    private static void Test_EncodingsWithParameters()
    {
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
            conn.ConnectionString = "server=localhost;database=demodb;port=33000;user=public;password=;charset=utf-8";
            conn.Open();

            TestCases.ExecuteSQL("drop table if exists t", conn);
            TestCases.ExecuteSQL("create table t(a int, b varchar(100))", conn);

            String sql = "insert into t values(1 ,?)";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?";
                param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                param.Value = "¾Æ¹«°³";

                cmd.Parameters.Add(param);
                cmd.ExecuteNonQuery();
            }

            sql = "select * from t where b = ?";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?";
                param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                param.Value = "¾Æ¹«°³";

                cmd.Parameters.Add(param);
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read(); //retrieve just one row

                    Debug.Assert(reader.GetInt32(0) == 1);
                    Debug.Assert(reader.GetString(1) == "¾Æ¹«°³");
                }
            }

            sql = "update t set b=?";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?";
                param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                param.Value = "¾Æ¹°³";

                cmd.Parameters.Add(param);
                cmd.ExecuteNonQuery();
            }

            sql = "select * from t where b = ?";
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                CUBRIDParameter param = new CUBRIDParameter();
                param.ParameterName = "?";
                param.CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_STRING;
                param.Value = "¾Æ¹°³";

                cmd.Parameters.Add(param);
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read(); //retrieve just one row

                    Debug.Assert(reader.GetInt32(0) == 1);
                    Debug.Assert(reader.GetString(1) == "¾Æ¹°³");
                }
            }

            TestCases.ExecuteSQL("drop table if exists t", conn);
        }
    }

    #region Helpers

    private static CUBRIDConnection GetDemodbConnection()
    {
      CUBRIDConnection conn = new CUBRIDConnection(TestCases.connString);
      conn.Open();

      if (conn == null)
      {
        throw new Exception("Can't connect to the [demodb] database!");
      }

      return conn;
    }

    private static void ExecuteSQL(string sql, CUBRIDConnection conn)
    {
      using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
      {
        cmd.ExecuteNonQuery();
      }
    }

    private static void CreateTestTable(CUBRIDConnection conn)
    {
      TestCases.ExecuteSQL("drop table if exists t", conn);
      TestCases.ExecuteSQL("create table t(a int, b char(10), c string, d float, e double, f date)", conn);
    }

    private static void CleanupTestTable(CUBRIDConnection conn)
    {
      TestCases.ExecuteSQL("drop table if exists t", conn);
    }

    private static void CreateTestTableLOB(CUBRIDConnection conn)
    {
      TestCases.ExecuteSQL("drop table if exists t", conn);
      TestCases.ExecuteSQL("create table t(b BLOB, c CLOB)", conn);
    }

    private static void CleanupTestTableLOB(CUBRIDConnection conn)
    {
      TestCases.ExecuteSQL("drop table if exists t", conn);
    }

    private static int GetTableRowsCount(string tableName, CUBRIDConnection conn)
    {
      int count = -1;
      string sql = "select count(*) from `" + tableName + "`";

      using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
      {
        count = (int)cmd.ExecuteScalar();
      }

      return count;
    }

    private static int GetTablesCount(string tableName, CUBRIDConnection conn)
    {
      int count = 0;
      string sql = "select count(*) from db_class where class_name = '" + tableName + "'";

      using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
      {
        count = (int)cmd.ExecuteScalar();
      }

      return count;
    }

    private static object GetSingleValue(string sql, CUBRIDConnection conn)
    {
      object ret = null;

      using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
      {
        ret = cmd.ExecuteScalar();
      }

      return ret;
    }

    private static void RunByName(string testCaseName)
    {
      foreach (string regexFilter in TestCases.runFilters)
      {
        Match match = Regex.Match(testCaseName, regexFilter, RegexOptions.IgnoreCase);
        if (match.Success && (TestCases.matchExactName == false || (TestCases.matchExactName && testCaseName == regexFilter)))
        {
          Console.WriteLine("Executing: [" + testCaseName + "]" + "...");
          executed++;

          try
          {
            Type t = typeof(TestCases);
            MethodInfo method = t.GetMethod(testCaseName, BindingFlags.Static | BindingFlags.NonPublic);
            if (method == null)
            {
              Console.WriteLine("Error - Method not found: " + testCaseName + "!");
              return;
            }

            method.Invoke(null, null);

            passed++;
          }
          catch (TargetInvocationException ex)
          {
            //Be aware, this is not working by default in Debugger
            //Read more here (including how to setup the VS debugger):
            //http://stackoverflow.com/questions/2658908/why-is-targetinvocationexception-treated-as-uncaught-by-the-ide
            Console.WriteLine("Error: " + ex.InnerException.Message);
          }
          catch (Exception ex)
          {
            Console.WriteLine("Error: " + ex.Message);
          }

          Console.WriteLine("Completed.");

          break; //exit foreach, as test case name might match multiple filters
        }
        else
        {
          //Console.WriteLine("Skipped: [" + testCaseName + "]");
        }
      }
    }

    //This method wil ALWAYS be executed, for each test case, BEFORE the test case execution starts
    //It is recommended to put in here all the setup required by the test cases
    private static void TestSetup()
    {
      try
      {
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
          conn.ConnectionString = TestCases.connString;
          conn.Open();

          ExecuteSQL("drop table if exists t", conn);
        }
      }
      catch { }
    }

    //This method wil ALWAYS be executed, for each test case, AFTER the test case execution end
    //It is recommended to put in here all the cleanup that should be done, to leave the system as it was in the beginning
    private static void TestCleanup()
    {
      try
      {
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
          conn.ConnectionString = TestCases.connString;
          conn.Open();

          ExecuteSQL("drop table if exists t", conn);
        }
      }
      catch { }
    }

    //This method wil ALWAYS be executed, one time only, BEFORE the test cases execution starts
    //It is recommended to put in here all the setup required BEFORE starting executing the test cases
    private static void SuiteSetup()
    {
    }

    //This method wil ALWAYS be executed, one time only, AFTER all the test cases execution ended
    //It is recommended to put in here all the setup required AFTER starting executing the test cases in the suite
    private static void SuiteCleanup()
    {
    }

    private static void Run(Action f)
    {
      try
      {
        TestCases.SuiteSetup();

        string testCaseName = f.Method.ToString().Replace("Void ", "").Replace("()", "");

        if (testCaseName.StartsWith("Test_"))
          testCasesCount++;

        foreach (string regexFilter in TestCases.runFilters)
        {
          Match match = Regex.Match(testCaseName, regexFilter, RegexOptions.IgnoreCase);
          if (match.Success && (TestCases.matchExactName == false || (TestCases.matchExactName && testCaseName == regexFilter)))
          {
            Console.WriteLine("Executing: [" + testCaseName + "]" + "...");
            executed++;

            try
            {
              TestCases.TestSetup();
              f();
              TestCases.TestCleanup();

              passed++;
            }
            catch (Exception ex)
            {
              Console.WriteLine("Error: " + ex.Message);
              Console.WriteLine("Details: " + ex.StackTrace);
            }

            break; //exit foreach, as test case name might match multiple filters
          }
          else
          {
            //Console.WriteLine("Skipped: [" + testCaseName + "]");
          }
        }
      }
      finally
      {
        TestCases.SuiteCleanup();
      }
    }

    #endregion

  }
}
