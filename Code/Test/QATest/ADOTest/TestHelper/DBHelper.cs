using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using CUBRID.Data.CUBRIDClient;

namespace ADOTest.TestHelper
{
    public static class DBHelper
    {
        public static string connString = ConfigurationManager.ConnectionStrings["CUBRID"].ConnectionString;
        public static string dbName=ConfigurationManager.AppSettings["DBName"];
        public static string dbVersion=ConfigurationManager.AppSettings["DBVersion"];
        public static string serverName = ConfigurationManager.AppSettings["ServerName"];
        public static string port = ConfigurationManager.AppSettings["Port"];

        //public static void ExecuteSQL(string sql)
        //{
        //    using (CUBRIDCommand cmd = new CUBRIDCommand(sql, connectionStr))
        //    {
        //        cmd.ExecuteNonQuery();
        //    }
        //}

        public static void ExecuteSQL(string sql, CUBRIDConnection conn)
        {
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        public static Int64 GetTableRowsCount(string tableName, CUBRIDConnection conn)
        {
            Int64 count = -1;
            string sql = "select count(*) from `" + tableName + "`";

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                count = (Int64)cmd.ExecuteScalar();
            }

            return count;
        }

        public static void CreateTestTable(CUBRIDConnection conn)
        {
            ExecuteSQL("drop table if exists t", conn);
            ExecuteSQL("create table t(a int, b char(10), c string, d float, e double, f date)", conn);
        }

        public static Int64 GetTablesCount(string tableName, CUBRIDConnection conn)
        {
            Int64 count = 0;
            string sql = "select count(*) from db_class where class_name = '" + tableName + "'";

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                count = (Int64)cmd.ExecuteScalar();
            }

            return count;
        }

        public static object GetSingleValue(string sql, CUBRIDConnection conn)
        {
            object ret = null;

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                ret = cmd.ExecuteScalar();
            }

            return ret;
        }
    }
}
