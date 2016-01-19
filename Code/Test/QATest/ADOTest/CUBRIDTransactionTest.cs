using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using CUBRID.Data.CUBRIDClient;
using ADOTest.TestHelper;

namespace ADOTest
{
    /// <summary>
    /// This is a test class for CUBRIDTransaction
    /// </summary>
    [TestClass]
    public class CUBRIDTransactionTest : BaseTest
    {
        /// <summary>
        /// Test Constructor
        /// </summary>
        [TestMethod]
        public void CUBRIDTransaction_Constructor_Test()
        {
            /*
            LogTestStep("Test the Constructor");
            CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString);
                       
            int timeout = 20;            
            conn.Open();
            conn.SetLockTimeout(timeout);
            CUBRIDTransaction transaction = new CUBRIDTransaction(conn, CUBRIDIsolationLevel.TRAN_SERIALIZABLE);
            //CUBRIDTransaction transaction = new CUBRIDTransaction(conn, CUBRIDIsolationLevel.TRAN_DEFAULT_ISOLATION);
    
            DBHelper.ExecuteSQL("drop table if exists t", conn);
            DBHelper.ExecuteSQL("create table t(id integer, name varchar(50))", conn);
            DBHelper.ExecuteSQL("insert into t values (1, 'peter')", conn);

            CUBRIDConnection conn2 = null;
            try
            {
                Thread thread2 = new Thread(delegate()
                {
                    conn2 = new CUBRIDConnection();
                    conn2.ConnectionString = DBHelper.connString;
                    conn2.Open();

                    conn2.BeginTransaction();
                    DBHelper.ExecuteSQL("select * from t", conn2);
                });
                
                thread2.Start();
                Thread.Sleep(1000);
                conn.BeginTransaction(); 
                DBHelper.ExecuteSQL("update t set name = 'Mandy' where id = 1", conn);
                thread2.Join();
            }
            catch (Exception ex)
            {
                Log(ex.Message);               
            }
            finally
            {               
                DBHelper.ExecuteSQL("drop table if exists t", conn);
                conn.Close();
                conn2.Close();
            }

            LogTestResult();*/
        }

        /// <summary>
        ///Test Commit, Rollback
        ///</summary>
        [TestMethod]
        public void CUBRIDTransaction_Commit_Rollback_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection())
            {
                conn.ConnectionString = DBHelper.connString;
                conn.Open();

                DBHelper.ExecuteSQL("drop table if exists t", conn);

                LogTestStep("Begin a transaction, then rollback");
                CUBRIDTransaction transaction = conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);
                int tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                transaction.Rollback();
                Log("Verify the table does not exist");
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(0, tablesCount);
                LogStepPass();

                LogTestStep("Begin a transaction, then commit");
                transaction = conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                transaction.Commit();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);
                LogStepPass();               
                
                //revert the test db                
                DBHelper.ExecuteSQL("drop table t", conn);
                LogTestResult();
            }
        }

        /// <summary>
        ///Test Commit, Rollback Exception
        ///</summary>
        [TestMethod]
        public void CUBRIDTransaction_Commit_Rollback_Negtive1_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString))
            {
                conn.Open();
                DBHelper.ExecuteSQL("drop table if exists t", conn);

                LogTestStep("Begin a transaction, commit twice");                
                CUBRIDTransaction transaction = conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);

                transaction.Commit();
                int tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                try
                {
                    transaction.Commit();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Transaction has already been committed or is not pending!", ex.Message);
                    tablesCount = DBHelper.GetTablesCount("t", conn); 
                    Assert.AreEqual(1, tablesCount);
                    LogStepPass();
                }

                LogTestStep("Begin a transaction, rollback twice");
                CUBRIDTransaction transaction2 = conn.BeginTransaction();
                DBHelper.ExecuteSQL("drop table if exists t", conn);

                transaction2.Rollback();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(1, tablesCount);

                try
                {
                    transaction.Rollback();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Transaction has already been committed or is not pending!", ex.Message);
                    tablesCount = DBHelper.GetTablesCount("t", conn);
                    Assert.AreEqual(1, tablesCount);
                    LogStepPass();
                }

                LogTestStep("Begin a transaction, commit, then rollback");
                CUBRIDTransaction transaction3 = conn.BeginTransaction();
                DBHelper.ExecuteSQL("drop table if exists t", conn);
                transaction3.Commit();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(0, tablesCount);

                try
                {
                    transaction3.Rollback();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Transaction has already been committed or is not pending!", ex.Message);
                    tablesCount = DBHelper.GetTablesCount("t", conn);
                    Assert.AreEqual(0, tablesCount);
                    LogStepPass();
                }

                LogTestStep("Begin a transaction, rollback, then commit");
                CUBRIDTransaction transaction4 = conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);
                transaction4.Rollback();
                tablesCount = DBHelper.GetTablesCount("t", conn);
                Assert.AreEqual(0, tablesCount);

                try
                {
                    transaction4.Commit();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Transaction has already been committed or is not pending!", ex.Message);
                    tablesCount = DBHelper.GetTablesCount("t", conn);
                    Assert.AreEqual(0, tablesCount);
                    LogStepPass();
                }

                LogTestResult();
            }
        }

        /// <summary>
        ///Test Commit, Rollback Exception
        ///</summary>
        [TestMethod]
        public void CUBRIDTransaction_Commit_Rollback_Negtive2_Test()
        {
            using (CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString))
            {
                conn.Open();
                DBHelper.ExecuteSQL("drop table if exists t", conn);

                LogTestStep("Begin a transaction, commit after close the connection");
                CUBRIDTransaction transaction = conn.BeginTransaction();
                DBHelper.ExecuteSQL("create table t(idx integer)", conn);

                try
                {
                    conn.Close();
                    transaction.Commit();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Connection must be valid and open to commit transaction!", ex.Message);
                    LogStepPass();
                }

                conn.Open();
                LogTestStep("Begin a transaction, rollback after close the connection");
                CUBRIDTransaction transaction2 = conn.BeginTransaction();
                DBHelper.ExecuteSQL("drop table if exists t", conn);

                try
                {
                    conn.Close();
                    transaction2.Rollback();
                    LogStepFail();
                }
                catch (Exception ex)
                {
                    Assert.AreEqual("Connection must be valid and open to rollback transaction!", ex.Message);
                    LogStepPass();
                }     
                LogTestResult();
            }
        }

        /// <summary>
        /// Test CUBRIDTransaction.CUBRIDConnection
        /// </summary>
        [TestMethod]
        public void CUBRIDTransaction_CUBRIDConnection_Test()
        {
            LogTestStep("Test CUBRIDTransaction.CUBRIDConnection");
            CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString);
            conn.Open();

            CUBRIDTransaction transaction = conn.BeginTransaction();
            transaction.Connection.Close();

            try
            {
                DBHelper.ExecuteSQL("drop table if exists t", conn);
                LogStepFail();
            }
            catch(Exception ex)
            {
                Assert.AreEqual("The connection is not open!", ex.Message);
                LogStepPass();
            }
            
            LogTestResult();
        }

        /// <summary>
        /// Test CUBRIDTransaction.IsolationLevel
        /// </summary>
        [TestMethod]
        public void CUBRIDTransaction_CUBRIDIsolationLevel_Test()
        {
            LogTestStep("Test CUBRIDTransaction.CUBRIDConnection");
            CUBRIDConnection conn = new CUBRIDConnection(DBHelper.connString);
            conn.Open();

            CUBRIDTransaction transaction = conn.BeginTransaction(CUBRIDIsolationLevel.TRAN_COMMIT_CLASS_COMMIT_INSTANCE);
            Console.WriteLine(transaction.CUBRIDIsolationLevel);
            
            Assert.AreEqual(CUBRIDIsolationLevel.TRAN_COMMIT_CLASS_COMMIT_INSTANCE, transaction.CUBRIDIsolationLevel);
            LogStepPass();


            LogTestResult();
        }

    }


}

