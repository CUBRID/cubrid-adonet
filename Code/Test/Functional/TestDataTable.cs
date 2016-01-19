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
  }
}
