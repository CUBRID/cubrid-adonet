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
        CUBRIDCommand cmd2 = cmd.Clone();

        try
        {
            cmd.Cancel();
        }
        catch (Exception e)
        {
            string r = "System.NotSupportedException: Specified method is not supported";
            Debug.Assert(e.Message.Substring(0,r.Length) == r);
        }

        Debug.Assert(cmd.CommandType == cmd2.CommandType);
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
        Debug.Assert(da.UpdateCommand.CommandText == "UPDATE `public.nation` SET `code` = ?, `name` = ?, `continent` = ?, `capital` = ? WHERE ((`code` = ?) AND (`name` = ?) AND ((? = 1 AND `continent` IS NULL) OR (`continent` = ?)) AND ((? = 1 AND `capital` IS NULL) OR (`capital` = ?)))");
        da.InsertCommand = cmdBuilder.GetInsertCommand();
        Debug.Assert(da.InsertCommand.CommandText == "INSERT INTO `public.nation` (`code`, `name`, `continent`, `capital`) VALUES (?, ?, ?, ?)");
        da.DeleteCommand = cmdBuilder.GetDeleteCommand();
        Debug.Assert(da.DeleteCommand.CommandText == "DELETE FROM `public.nation` WHERE ((`code` = ?) AND (`name` = ?) AND ((? = 1 AND `continent` IS NULL) OR (`continent` = ?)) AND ((? = 1 AND `capital` IS NULL) OR (`capital` = ?)))");
      }
    }

    /// <summary>
    /// Test large SQL statement
    /// </summary>
    public static void Test_Big_Data()
    {
      using (CUBRIDConnection conn = new CUBRIDConnection())
      {
        conn.ConnectionString = TestCases.connString;
        conn.Open();

        string _create = "create table TBL_RAW_POWER(PDMU_ID int, CHANNEL_NUM int, REG_DATE datetime, AMPERE int," +
                        "ACTIVE_POWER int, POWER_ACT int, APPARENT_POWER int, REACTIVE_POWER int, POWER_REA int," +
                        "SYSTEM_STATUS int, FREQUENCY int, POWER_FACTOR int, POWER_STATUS int, VOLTAGE int);";

        try
        {
          TestCases.ExecuteSQL("drop table if exists TBL_RAW_POWER;", conn);
        }
        catch { }

        /* create new table */
        using (CUBRIDCommand cmd = new CUBRIDCommand(_create, conn))
        {
          cmd.ExecuteNonQuery();
        }

        const string _insert = "INSERT INTO TBL_RAW_POWER(PDMU_ID, CHANNEL_NUM, REG_DATE, AMPERE, ACTIVE_POWER, " +
                        "POWER_ACT, APPARENT_POWER, REACTIVE_POWER, POWER_REA, SYSTEM_STATUS, FREQUENCY, " +
                        "POWER_FACTOR, POWER_STATUS, VOLTAGE ) VALUES " +
                        " (637, 12, '2013-01-18 13:34:19', 1316, 2268, 40729, 2804, 972, 40729, 1000, 596, 94, 0, 1011), " +
                        "(637, 14, '2013-01-18 13:34:19', 456, 942, 15605, 964, 294, 15605, 1000, 597, 95, 0, 1011), " +
                        "(637, 15, '2013-01-18 13:34:19', 4316, 2268, 15151, 2804, 972, 15151, 1000, 596, 94, 0, 1011), " +
                        "(637, 16, '2013-01-18 13:34:19', 1316, 2268, 15279, 2804, 972, 15279, 1000, 596, 94, 0, 1011), " +
                        "(637, 17, '2013-01-18 13:34:19', 4316, 2268, 15347, 2804, 972, 15347, 1000, 596, 94, 0, 1011), " +
                        "(637, 13, '2013-01-18 13:34:19', 456, 942, 15408, 964, 294, 15408, 1000, 597, 95, 0, 1011), " +
                        "(637, 31, '2013-01-18 13:34:19', 456, 942, 15282, 964, 294, 15282, 1000, 597, 95, 0, 1011), " +
                        "(637, 32, '2013-01-18 13:34:19', 1600, 3318, 15480, 3475, 979, 15480, 1000, 597, 95, 0, 1011), " +
                        "(637, 33, '2013-01-18 13:34:19', 1600, 3318, 15132, 3475, 979, 15132, 1000, 597, 95, 0, 1011), " +
                        "(637, 34, '2013-01-18 13:34:19', 4316, 2268, 15363, 2804, 972, 15363, 1000, 596, 94, 0, 1011), " +
                        "(637, 35, '2013-01-18 13:34:19', 0, 0, 15464, 0, 0, 15464, 1000, 597, 95, 0, 1011), " +
                        "(637, 36, '2013-01-18 13:34:19', 1600, 3318, 15415, 3475, 979, 15415, 1000, 597, 95, 0, 1011), " +
                        "(637, 37, '2013-01-18 13:34:19', 1316, 2268, 15251, 2804, 972, 15251, 1000, 596, 94, 0, 1011), " +
                        "(637, 38, '2013-01-18 13:34:19', 4316, 2268, 15299, 2804, 972, 15299, 1000, 596, 94, 0, 1011), " +
                        "(637, 39, '2013-01-18 13:34:19', 1600, 3318, 15384, 3475, 979, 15384, 1000, 597, 95, 0, 1011), " +
                        "(637, 40, '2013-01-18 13:34:19', 1316, 2268, 15305, 2804, 972, 15305, 1000, 596, 94, 0, 1011), " +
                        "(637, 41, '2013-01-18 13:34:19', 456, 942, 15431, 964, 294, 15431, 1000, 597, 95, 0, 1011), " +
                        "(637, 42, '2013-01-18 13:34:19', 1600, 3318, 15341, 3475, 979, 15341, 1000, 597, 95, 0, 1011), " +
                        "(637, 43, '2013-01-18 13:34:19', 1600, 3318, 15202, 3475, 979, 15202, 1000, 597, 95, 0, 1011), " +
                        "(637, 44, '2013-01-18 13:34:19', 1316, 2268, 15170, 2804, 972, 15170, 1000, 596, 94, 0, 1011), " +
                        "(637, 45, '2013-01-18 13:34:19', 456, 942, 15020, 964, 294, 15020, 1000, 597, 95, 0, 1011), " +
                        "(637, 46, '2013-01-18 13:34:19', 1316, 2268, 15268, 2804, 972, 15268, 1000, 596, 94, 0, 1011), " +
                        "(637, 47, '2013-01-18 13:34:19', 456, 942, 15253, 964, 294, 15253, 1000, 597, 95, 0, 1011), " +
                        "(637, 48, '2013-01-18 13:34:19', 1600, 3318, 15258, 3475, 979, 15258, 1000, 597, 95, 0, 1011), " +
                        "(637, 49, '2013-01-18 13:34:19', 1600, 3318, 15159, 3475, 979, 15159, 1000, 597, 95, 0, 1011), " +
                        "(637, 50, '2013-01-18 13:34:19', 1600, 3318, 15178, 3475, 979, 15178, 1000, 597, 95, 0, 1011), " +
                        "(637, 70, '2013-01-18 13:34:19', 1316, 2268, 7522, 2804, 972, 7522, 1000, 596, 94, 0, 1011), " +
                        "(637, 71, '2013-01-18 13:34:19', 1600, 3318, 7532, 3475, 979, 7532, 1000, 597, 95, 0, 1011), " +
                        "(637, 72, '2013-01-18 13:34:19', 456, 942, 7483, 964, 294, 7483, 1000, 597, 95, 0, 1011), " +
                        "(637, 18, '2013-01-18 13:34:19', 1600, 3318, 96721, 3475, 979, 96721, 1000, 597, 95, 0, 1011), " +
                        "(637, 19, '2013-01-18 13:34:19', 456, 942, 96110, 964, 294, 96110, 1000, 597, 95, 0, 1011), " +
                        "(637, 20, '2013-01-18 13:34:19', 0, 0, 56793, 0, 0, 56793, 1000, 597, 95, 0, 1011), " +
                        "(637, 1, '2013-01-18 13:34:19', 456, 942, 15250, 964, 294, 15250, 1000, 597, 95, 0, 1011), " +
                        "(637, 2, '2013-01-18 13:34:19', 456, 942, 15374, 964, 294, 15374, 1000, 597, 95, 0, 1011), " +
                        "(637, 30, '2013-01-18 13:34:19', 4316, 2268, 7595, 2804, 972, 7595, 1000, 596, 94, 0, 1011), " +
                        "(637, 28, '2013-01-18 13:34:19', 1600, 3318, 7713, 3475, 979, 7713, 1000, 597, 95, 0, 1011), " +
                        "(637, 29, '2013-01-18 13:34:19', 456, 942, 7487, 964, 294, 7487, 1000, 597, 95, 0, 1011)";

        /* insert multi rows values */
        using (CUBRIDCommand cmd = new CUBRIDCommand(_insert, conn))
        {
          cmd.ExecuteNonQuery();
        }


        /* verify count */
        string sql = "select count(*) from TBL_RAW_POWER";
        using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
        {
          using (DbDataReader reader = cmd.ExecuteReader())
          {
            reader.Read();
            Debug.Assert(reader.GetInt32(0) == 37);
          }
        }

        TestCases.ExecuteSQL("drop TBL_RAW_POWER;", conn);
      }
    }
  }
}