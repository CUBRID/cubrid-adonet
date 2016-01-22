using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using CUBRID.Data.CUBRIDClient;

namespace CUBRID.Data.Test.Functional
{
  public partial class TestCases
  {
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

        Blob.SetBytes(1, bytes);

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
          bytes2 = bImage.GetBytes(1, (int)bImage.BlobLength);

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

        Blob.SetBytes(1, bytes1);

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
          bytes = bImage.GetBytes(1, (int)bImage.BlobLength);

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
        Clob.SetString(1, str);

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
          string str2 = cImage.GetString(1, (int)cImage.ClobLength);

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
        Clob1.SetString(1, str1);

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
          string str = cImage.GetString(1, (int)cImage.ClobLength);

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

        Blob1.SetBytes(1, bytes1);

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
          bytes = bImage.GetBytes(1, (int)bImage.BlobLength);

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

        Blob1.SetBytes(1, bytes1);

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
          bytes = bImage.GetBytes(1, (int)bImage.BlobLength);

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
        Clob1.SetString(1, str1);

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
          string str = cImage.GetString(1, (int)cImage.ClobLength);

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
        Clob1.SetString(1, str1);

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
          string str = cImage.GetString(1, (int)cImage.ClobLength);

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

        Blob1.SetBytes(1, bytes1);

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

        Blob.SetBytes(1, bytes);
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
          bytes2 = bImage.GetBytes(1, (int)bImage.BlobLength);

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

          Clob1.SetString(1, "test string to be inserted");

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

          Clob.SetString(1, str);
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
            string str2 = cImage.GetString(1, (int)cImage.ClobLength);

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
          b = new BinaryReader(File.Open("../../CUBRID.ico", FileMode.Open));
          int length = (int)b.BaseStream.Length;
          bytes = b.ReadBytes(length);

          Blob.SetBytes(1, bytes);
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
            bytes2 = bImage.GetBytes(1, (int)bImage.BlobLength);

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

        StreamReader r = new StreamReader("../../BSD License.txt");
        string writestring = r.ReadToEnd();
        r.Close();

        Clob.SetString(1, writestring);

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
            string str2 = cImage.GetString(1, (int)cImage.ClobLength);

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

          Blob.SetBytes(1, bytes);

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

          Blob1.SetBytes(1, bytes1);

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

          Blob.SetBytes(1, bytes);
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
            bytes2 = bImage.GetBytes(1, (int)bImage.BlobLength);

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

          Blob.SetBytes(1, bytes1);

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
            bytes = bImage.GetBytes(1, (int)bImage.BlobLength);

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
  }
}
