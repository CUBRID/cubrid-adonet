using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using CUBRID.Data.CUBRIDClient;
using ADOTest.TestHelper;

namespace ADOTest
{
    /// <summary>
    ///This is a test class for CUBRIDDataAdapter
    ///</summary>
    [TestClass]
    public class CUBRIDDataAdapterTest
    {
        /// <summary>
        ///A test for CUBRIDDataAdapter Constructor
        ///</summary>
        [TestMethod]
        public void CUBRIDDataAdapter_ConstructorNoParam_Test()
        {

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (id int, name varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values (1, 'Nancy')", conn);
                DBHelper.ExecuteSQL("insert into t values (2, 'Peter')", conn);

                string selectCommandText = "select * from t";

                CUBRIDDataAdapter adapter = new CUBRIDDataAdapter();
                DataSet ds = new DataSet();
                adapter.SelectCommand = new CUBRIDCommand(selectCommandText, conn);

                adapter.Fill(ds);
                //Update data
                DataTable dt = ds.Tables[0];

                Assert.AreEqual(1, (int)dt.Rows[0]["id"]);
                Assert.AreEqual("Nancy", dt.Rows[0]["name"].ToString());

                Assert.AreEqual(2, (int)dt.Rows[1]["id"]);
                Assert.AreEqual("Peter", dt.Rows[1]["name"].ToString());

                //revert test db
                DBHelper.ExecuteSQL("drop table if exists t", conn);

            }
        }
        
        /// <summary>
        ///A test for CUBRIDDataAdapter Constructor
        ///</summary>
        [TestMethod]
        public void CUBRIDDataAdapter_ConstructorWithCUBRIDCommand_Test()
        {
           
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (id int, name varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values (1, 'Nancy')", conn);
                DBHelper.ExecuteSQL("insert into t values (2, 'Peter')", conn);

                string selectCommandText = "select * from t";

                using (CUBRIDCommand cmd = new CUBRIDCommand(selectCommandText, conn))
                {
                    CUBRIDDataAdapter adapter = new CUBRIDDataAdapter(cmd);
                    DataSet ds = new DataSet();
                    adapter.Fill(ds);

                    //Update data
                    DataTable dt = ds.Tables[0];

                    Assert.AreEqual(1, (int)dt.Rows[0]["id"]);
                    Assert.AreEqual("Nancy", dt.Rows[0]["name"].ToString());

                    Assert.AreEqual(2, (int)dt.Rows[1]["id"]);
                    Assert.AreEqual("Peter", dt.Rows[1]["name"].ToString());

                    //revert test db
                    DBHelper.ExecuteSQL("drop table if exists t", conn);
                }
            }
        }

        /// <summary>
        ///A test for CUBRIDDataAdapter Constructor
        ///</summary>
        [TestMethod]
        public void CUBRIDDataAdapter_ConstructorWithSqlAndConn_Test()
        {

            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);
                DBHelper.ExecuteSQL("create table t (id int, name varchar(100))", conn);
                DBHelper.ExecuteSQL("insert into t values (1, 'Nancy')", conn);
                DBHelper.ExecuteSQL("insert into t values (2, 'Peter')", conn);

                string selectCommandText = "select * from t";
                CUBRIDDataAdapter adapter = new CUBRIDDataAdapter(selectCommandText, conn);
                DataTable dt = new DataTable("student");
                adapter.Fill(dt);

                //verify data

                Assert.AreEqual(1, (int)dt.Rows[0]["id"]);
                Assert.AreEqual("Nancy", dt.Rows[0]["name"].ToString());

                Assert.AreEqual(2, (int)dt.Rows[1]["id"]);
                Assert.AreEqual("Peter", dt.Rows[1]["name"].ToString());

                //revert test db
                DBHelper.ExecuteSQL("drop table if exists t", conn);
            }
        }

        /// <summary>
        ///A test for CUBRIDDataAdapter Constructor
        ///</summary>
        [TestMethod]
        public void CUBRIDDataAdapter_ConstructorWithSqlAndConnString_Test()
        {

            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            conn.Open();

            DBHelper.ExecuteSQL("drop table if exists t", conn);
            DBHelper.ExecuteSQL("create table t (id int, name varchar(100))", conn);
            DBHelper.ExecuteSQL("insert into t values (1, 'Nancy')", conn);
            DBHelper.ExecuteSQL("insert into t values (2, 'Peter')", conn);
            conn.Close();

            string selectCommandText = "select * from t";
            CUBRIDDataAdapter adapter = new CUBRIDDataAdapter(selectCommandText, DBHelper.connString);
            DataTable dt = new DataTable("student");
            adapter.Fill(dt);

            //verify data

            Assert.AreEqual(1, (int)dt.Rows[0]["id"]);
            Assert.AreEqual("Nancy", dt.Rows[0]["name"].ToString());

            Assert.AreEqual(2, (int)dt.Rows[1]["id"]);
            Assert.AreEqual("Peter", dt.Rows[1]["name"].ToString());

            //revert test db
            conn.Open();
            DBHelper.ExecuteSQL("drop table if exists t", conn);
        }

        /// <summary>
        ///A test for SelectCommand UpdateCommand InsertCommand DeleteCommand
        ///</summary>
        [TestMethod]
        public void CUBRIDDataAdapter_Command_Test()
        {
            CUBRIDConnection conn = new CUBRIDConnection();
            conn.ConnectionString = DBHelper.connString;
            conn.Open();

            DBHelper.ExecuteSQL("drop table if exists t", conn);
            DBHelper.ExecuteSQL("create table t (id int, name varchar(100))", conn);
            DBHelper.ExecuteSQL("insert into t values (1, 'Nancy')", conn);
            DBHelper.ExecuteSQL("insert into t values (2, 'Peter')", conn);
           
            CUBRIDDataAdapter adapter = new CUBRIDDataAdapter();

            //SelectCommand
            string sql = "select * from t";
            CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
            adapter.SelectCommand = cmd;

            DataTable dt = new DataTable("student");
            adapter.Fill(dt);

            //verify data
            Assert.AreEqual(1, (int)dt.Rows[0]["id"]);
            Assert.AreEqual("Nancy", dt.Rows[0]["name"].ToString());
            Assert.AreEqual(2, (int)dt.Rows[1]["id"]);
            Assert.AreEqual("Peter", dt.Rows[1]["name"].ToString());
            Assert.AreEqual(sql, adapter.SelectCommand.CommandText);

            //UpdateCommand      
            sql = "update t set name='Mandy' where id=1";
            cmd = new CUBRIDCommand(sql, conn); 
            adapter.UpdateCommand=cmd;
            adapter.UpdateCommand.ExecuteNonQuery();
            dt.AcceptChanges();
            adapter.Update(dt);

            Console.WriteLine(dt.Rows[0]["name"]);

            //dt.AcceptChanges();
            //Assert.AreEqual(1, (int)dt.Rows[0]["id"]);
            //Assert.AreEqual("Mandy", dt.Rows[0]["name"].ToString());
            //Assert.AreEqual(2, (int)dt.Rows[1]["id"]);
            //Assert.AreEqual("Peter", dt.Rows[1]["name"].ToString());
            //Assert.AreEqual(sql, adapter.UpdateCommand.CommandText);

            //DeleteCommand
            sql = "delete from t where name='Mandy'";
            cmd = new CUBRIDCommand(sql, conn);
            adapter.DeleteCommand=cmd;
            adapter.DeleteCommand.ExecuteNonQuery();
            dt.Rows[0].Delete();

            //TODO: Verifcation: might need to 'SqlCommandBuilder'?
            dt.AcceptChanges();
            adapter.Update(dt);            
            Assert.AreEqual(1, dt.Rows.Count);
            Assert.AreEqual(2, (int)dt.Rows[0]["id"]);
            Assert.AreEqual("Peter", dt.Rows[0]["name"].ToString());
            Assert.AreEqual(sql, adapter.DeleteCommand.CommandText);
            // --
            //InsertCommand


            //revert test db
            DBHelper.ExecuteSQL("drop table if exists t", conn);

            conn.Close();
        }
    }
}
