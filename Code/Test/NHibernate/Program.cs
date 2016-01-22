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
using System.Text.RegularExpressions;
using CUBRID.Data.CUBRIDClient;
using System.Collections.Generic;
using System.Data.Common;

namespace CUBRID.Data.TestNHibernate
{
  /// <summary>
  /// Implementation of test cases for the CUBRID ADO.NET Data Provider
  /// </summary>
  public partial class TestCases
  {
    private const string connString = "server=localhost;database=demodb;port=33000;user=public;password=";

    static int executed;
    static int passed;
    static int testCasesCount;

    //Specify the REGEX name patterns for the test cases to be executed
    //static string[] runFilters = new string[] { @"\w+" }; // "\w+" means: Match any test case name  

    //Specify what test cases to execute (use a regular expression):

    private const bool matchExactName = false; //set to False if you want the runFilters to match ALL test cases with names that begin with ...
    static readonly string[] runFilters = new[] { @"Test_OneToManySelect" };

    /* Documentation and examples for using ADO.NET:
    http://msdn.microsoft.com/en-us/library/e80y5yhx%28v=VS.80%29.aspx
    */

    public static void Main(string[] args)
    {
      Console.WriteLine(@"Test cases execution started...");
      Console.WriteLine();

      //Test standard data types
      TestCases.Run(Test_DataTypesStandard_Insert);
      TestCases.Run(Test_DataTypesStandard_Select);
      TestCases.Run(Test_DataTypesStandard_Update);
      TestCases.Run(Test_DataTypesStandard_Delete);

      //Test CUBRID specific data types
      TestCases.Run(Test_CUBRIDBlob_Insert);
      TestCases.Run(Test_CUBRIDBlob_Select);
      TestCases.Run(Test_CUBRIDBlob_Update);
      TestCases.Run(Test_CUBRIDBlob_Delete);
      TestCases.Run(Test_CUBRIDClob_Insert);
      TestCases.Run(Test_CUBRIDClob_Select);
      TestCases.Run(Test_CUBRIDClob_Update);
      TestCases.Run(Test_CUBRIDClob_Delete);
      TestCases.Run(Test_CUBRIDCollections_Select);
      TestCases.Run(Test_CUBRIDCollections_Insert);
      TestCases.Run(Test_CUBRIDCollections_Update);
      TestCases.Run(Test_CUBRIDCollections_Delete);

      //Test Relationships
      TestCases.Run(Test_OneToManySelect);
      TestCases.Run(Test_OneToManyInsert);
      TestCases.Run(Test_OneToManyUpdate);
      TestCases.Run(Test_OneToManyDelete);
      TestCases.Run(Test_ManyToManySelect);
      TestCases.Run(Test_ManyToManyInsert);
      TestCases.Run(Test_ManyToManyUpdate);
      TestCases.Run(Test_ManyToManyDelete);
      TestCases.Run(Test_OneToOneSelect);
      TestCases.Run(Test_OneToOneInsert);
      TestCases.Run(Test_OneToOneUpdate);
      TestCases.Run(Test_OneToOneDelete);

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

      Console.WriteLine(@"Press any key to continue...");
      Console.ReadKey();
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

    private static List<object> GetTableValues(string tableName, int indexPosition, string[] columnNames)
    {
      List<object> columnValues = new List<object>();
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        using(CUBRIDCommand cmd  = new CUBRIDCommand("select * from " + tableName, conn))
        {
          DbDataReader reader = cmd.ExecuteReader();
          for (int i = 0; i < indexPosition; i++)
          {
            reader.Read();
          }

          for (int i = 0; i < columnNames.Length; i++)
          {
            columnValues.Add(reader[columnNames[i]]);
          }
        }
      }

      return columnValues;
    }

    private static void ExecuteSQL(string sql, CUBRIDConnection conn)
    {
      using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
      {
        cmd.ExecuteNonQuery();
      }
    }

    //This method will ALWAYS be executed, one time only, BEFORE the test cases execution starts
    //It is recommended to put in here all the setup required BEFORE starting executing the test cases
    private static void SuiteSetup()
    {
    }

    //This method will ALWAYS be executed, one time only, AFTER all the test cases execution ended
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
            Console.WriteLine(@"Executing: [" + testCaseName + @"]" + @"...");
            executed++;

            try
            {
              //TestCases.TestSetup();
              f();
              //TestCases.TestCleanup();

              passed++;
            }
            catch (Exception ex)
            {
              Console.WriteLine(@"Error: " + ex.Message);
              Console.WriteLine(@"Details: " + ex.StackTrace);
            }

            break; //exit foreach, as test case name might match multiple filters
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
