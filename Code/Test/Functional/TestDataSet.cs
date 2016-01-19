using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using CUBRID.Data.CUBRIDClient;

namespace Test.Functional
{
  public partial class TestCases
  {
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
  }
}