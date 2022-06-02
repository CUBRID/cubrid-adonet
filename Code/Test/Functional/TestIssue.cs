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
        public static void TestIssueRun()
        {
            Test_apis_669();
            Test_apis_514();
        }
        private static void Test_apis_514()
        {
            string sql = "select * from public.nation order by code asc";
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader(CommandBehavior.CloseConnection);

            Console.WriteLine(reader.IsClosed);
            Console.WriteLine(conn.State);

            reader.Close();

            Console.WriteLine(reader.IsClosed);
            Console.WriteLine(conn.State);
        }
        private static void Test_apis_669()
        {
            String sql = "select s_name from public.code where f_name = 'Woman';select * from public.code;";

            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);

            CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader();

            while (reader.Read())
            {
                Console.WriteLine(reader.GetString(0));
            };

            while (reader.NextResult())
            {
                Console.WriteLine("=============================");

                while (reader.Read())
                {
                    Console.WriteLine(reader.GetString(0));
                    //    Console.WriteLine(reader.GetString(1));
                };
            }
        } 
    }
}