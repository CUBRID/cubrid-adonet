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
  }
}