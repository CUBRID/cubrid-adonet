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
    /// Test Encodings support
    /// </summary>
    ///
    public static void TestParameterRun()
    {
        Test_SetDataTypesFromValue();
        Test_SetCUBRIDDataTypeFromDbType();
        Test_InitParameter();
        Test_CommandBuilder();
    }
    private static void Test_SetDataTypesFromValue()
    {
        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?p";

        Boolean b = true;
        param.Value = b;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SHORT);

        SByte sb = new SByte();
        param.Value = sb;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SHORT);

        Byte by = new Byte();
        param.Value = by;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SHORT);

        Int16 i16 = 0;
        param.Value = i16;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SHORT);

        UInt16 ui16 = 0;
        param.Value = ui16;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SHORT);

        UInt32 ui32 = 0;
        param.Value = ui32;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_INT);

        Int64 i64 = 0;
        param.Value = i64;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_BIGINT);

        UInt64 ui64 = 0;
        param.Value = ui64;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_BIGINT);

        param.Value = DateTime.Now;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_DATETIME);

        string str = "cubrid";
        param.Value = str;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_STRING);

        Single sin = 0;
        param.Value = sin;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_FLOAT);

        Double dou = 0;
        param.Value = dou;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_DOUBLE);

        Decimal dec = 0;
        param.Value = dec;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_NUMERIC);

        DBNull n=null;
        param.Value = n;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_NULL);
    }
    private static void Test_SetCUBRIDDataTypeFromDbType()
    {
        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?p";

        DbType db = DbType.Int16;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SHORT);

        db = DbType.Int32;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_INT);

        db = DbType.Int64;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_BIGINT);

        db = DbType.Single;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_FLOAT);

        db = DbType.Double;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_DOUBLE);

        db = DbType.Decimal;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_NUMERIC);

        db = DbType.Date;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_DATE);

        db = DbType.Time;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_TIME);

        db = DbType.DateTime;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_TIMESTAMP);

        db = DbType.Boolean;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_BIT);

        db = DbType.Currency;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_MONETARY);

        db = DbType.StringFixedLength;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_CHAR);

        db = DbType.AnsiString;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_CLOB);

        db = DbType.Byte;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_VARBIT);

        db = DbType.Object;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_OBJECT);

        db = DbType.VarNumeric;
        param.DbType = db;
        Debug.Assert(param.CUBRIDDataType == CUBRIDDataType.CCI_U_TYPE_SET);
    }
    private static void Test_InitParameter()
    {
        Object v = new Object();
        DbType type = DbType.Byte;
        ParameterDirection dir = ParameterDirection.Input;
        DataRowVersion ver = DataRowVersion.Default;
        CUBRIDDataType cdt = CUBRIDDataType.CCI_U_TYPE_BIGINT;
        string sourceColumn = "driver";
        int size = 10;
        bool isNullable = false;
        byte precision =1;
        byte scale = 0;

        CUBRIDParameter p1 = new CUBRIDParameter(v);
        Debug.Assert(p1.Value == v);

        CUBRIDParameter p2 = new CUBRIDParameter(cdt);
        Debug.Assert(p2.CUBRIDDataType == cdt);

        CUBRIDParameter p3 = new CUBRIDParameter("cubrid", type, dir, "col", ver, v);
        Debug.Assert(p3.ParameterName == "cubrid");

        CUBRIDParameter p4 = new CUBRIDParameter("cubrid", cdt, size);
        Debug.Assert(p4.Size == size);

        CUBRIDParameter p5 = new CUBRIDParameter("cubrid", cdt, size, sourceColumn);
        Debug.Assert(p5.SourceColumn == sourceColumn);

        CUBRIDParameter p6 = new CUBRIDParameter("cubrid", cdt, size, dir, isNullable,
            precision, scale, sourceColumn, ver, v);
        Debug.Assert(p6.SourceColumn == sourceColumn);

        CUBRIDParameter p7 = new CUBRIDParameter("cubrid", cdt,dir,"col",ver,v);
        Debug.Assert(p7.CUBRIDDataType == cdt);

        CUBRIDParameter p8 = p7.Clone();
        Debug.Assert(p8.ToString() == p7.ToString());
    }
    private static void Test_CommandBuilder()
    {
        using (CUBRIDConnection conn = new CUBRIDConnection())
        {
            conn.ConnectionString = TestCases.connString;
            conn.Open();

            String sql = "select * from nation order by `code` asc";
            CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn);

            CUBRIDCommandBuilder cmdBuilder = new CUBRIDCommandBuilder(da);
            string sql_format = string.Format("select {0},{1} from {2}",
                cmdBuilder.QuoteIdentifier("name"),
                cmdBuilder.QuoteIdentifier("age"),
                cmdBuilder.QuoteIdentifier("user"));

            Console.WriteLine(sql_format);
        }
    }
  }
}