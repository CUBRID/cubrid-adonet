using System;
using System.Data;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;

namespace Test.Functional
{
  public partial class TestCases
  {
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

        Debug.Assert(dt.Rows[1][0].ToString() == "event");
        Debug.Assert(dt.Rows[1][1].ToString() == "code");
        Debug.Assert(dt.Rows[1][2].ToString() == "game");
        Debug.Assert(dt.Rows[1][3].ToString() == "event_code");
        Debug.Assert(dt.Rows[1][4].ToString() == "1");
        Debug.Assert(dt.Rows[1][5].ToString() == "1");
        Debug.Assert(dt.Rows[1][6].ToString() == "1");
        Debug.Assert(dt.Rows[1][7].ToString() == "fk_game_event_code");
        Debug.Assert(dt.Rows[1][8].ToString() == "pk_event_code");

        Debug.Assert(dt.Rows[0][0].ToString() == "athlete");
        Debug.Assert(dt.Rows[0][1].ToString() == "code");
        Debug.Assert(dt.Rows[0][2].ToString() == "game");
        Debug.Assert(dt.Rows[0][3].ToString() == "athlete_code");
        Debug.Assert(dt.Rows[0][4].ToString() == "1");
        Debug.Assert(dt.Rows[0][5].ToString() == "1");
        Debug.Assert(dt.Rows[0][6].ToString() == "1");
        Debug.Assert(dt.Rows[0][7].ToString() == "fk_game_athlete_code");
        Debug.Assert(dt.Rows[0][8].ToString() == "pk_athlete_code");
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
        Debug.Assert(dt.Rows.Count == 0);
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

        Debug.Assert(tableName == "record");
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

        Debug.Assert(queryPlan == "Join graph segments (f indicates final):\nseg[0]: [0]\nseg[1]: code[0] (f)\nseg[2]: name[0] (f)\nseg[3]: gender[0] (f)\nseg[4]: nation_code[0] (f)\nseg[5]: event[0] (f)\nJoin graph nodes:\nnode[0]: athlete athlete(6677/32) (loc 0)\n\nQuery plan:\n\niscan\n    class: athlete node[0]\n    index: pk_athlete_code  (desc_index)\n    sort:  1 desc\n    cost:  97 card 6677\n\nQuery stmt:\n\nselect athlete.code, athlete.[name], athlete.gender, athlete.nation_code, athlete.event from athlete athlete order by 1 desc \n\n/* ---> skip ORDER BY */\n");
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

        Debug.Assert(rw.Rows[0][0].ToString() == "ABSOLUTE");
        Debug.Assert(rw.Rows[rw.Rows.Count - 1][0].ToString() == "ZONE");
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
  }
}
