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
using System.Reflection;
using System.Text.RegularExpressions;
using CUBRID.Data.CUBRIDClient;

namespace Test.Functional
{
  /// <summary>
  /// Implementation of test cases for the CUBRID ADO.NET Data Provider
  /// </summary>
  public partial class TestCases
  {
    private const string connString = "server=test-db-server;database=demodb;port=33000;user=dba;password=";
    private const string ip = "test-db-server";
    static int executed = 0;
    static int passed = 0;
    static int testCasesCount = 0;
    static CUBRIDConnection conn = new CUBRIDConnection();

    //Specify the REGEX name patterns for the test cases to be executed
    //static string[] runFilters = new string[] { @"\w+" }; // "\w+" means: Match any test case name  

    //Specify what test cases to execute (use a regular expression):

    static bool matchExactName = false; //set to False if you want the runFilters to match ALL test cases with names that begin with ...
    static string[] runFilters = new string[] { @"Test_Blob_FromFile" };

    /* Documentation and examples for using ADO.NET:
    http://msdn.microsoft.com/en-us/library/e80y5yhx%28v=VS.80%29.aspx
    */
    public static void Test_init()
    {
        conn.ConnectionString = TestCases.connString;
        conn.SetConnectionTimeout(300);
        conn.Open();
    }
    public static void Test_dinit()
    {
        conn.Close();
    }
    public static void Run()
    {
      Console.WriteLine("Test cases execution started...");
      Console.WriteLine();

      //Connection 
      TestCases.Run(TestCases.Test_ConnectionStringBuilder);
      TestCases.Run(TestCases.Test_MultipleConnections);
      TestCases.Run(TestCases.Test_ConnectionGetSchema);
      TestCases.Run(TestCases.Test_CUBRIDConnectionStringBuilderConstructor);
      TestCases.Run(TestCases.Test_GetConnectionString);
      TestCases.Run(TestCases.Test_ConnectionURL_And_Reset);

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
      TestCases.Run(TestCases.Test_Big_Data);

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
      //TestCases.Run(TestCases.Test_GetGeneratedKeys);
      TestCases.Run(TestCases.Test_ExecuteNonQuery_Query);
      TestCases.Run(TestCases.Test_Oid_Basic);
      TestCases.Run(TestCases.Test_DateTime_Types);
      TestCases.Run(TestCases.Test_Command_Multiple_CommandText);

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
      TestCases.Run(TestCases.Test_DataType_Enum);
      TestCases.Run(TestCases.Test_WithWrongEnumData);

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
          if (TestCases.matchExactName == false || (match.Success &&(TestCases.matchExactName && testCaseName == regexFilter)))
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
