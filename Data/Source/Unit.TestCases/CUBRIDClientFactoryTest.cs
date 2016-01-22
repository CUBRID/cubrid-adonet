using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data.Common;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDClientFactoryTest and is intended
    ///to contain all CUBRIDClientFactoryTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDClientFactoryTest
    {


        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion


        /// <summary>
        ///A test for CreateCommand
        ///</summary>
        [TestMethod()]
        public void CreateCommandTest()
        {
            CUBRIDClientFactory target = new CUBRIDClientFactory();
            DbCommand actual = target.CreateCommand();
            Assert.IsTrue((actual as CUBRIDCommand) != null);
        }

        /// <summary>
        ///A test for CreateCommandBuilder
        ///</summary>
        [TestMethod()]
        public void CreateCommandBuilderTest()
        {
            CUBRIDClientFactory target = new CUBRIDClientFactory(); 
            DbCommandBuilder actual = target.CreateCommandBuilder();
            Assert.IsTrue((actual as CUBRIDCommandBuilder) != null);
        }

        /// <summary>
        ///A test for CreateConnection
        ///</summary>
        [TestMethod()]
        public void CreateConnectionTest()
        {
            CUBRIDClientFactory target = new CUBRIDClientFactory();
            DbConnection actual = target.CreateConnection();
            Assert.IsTrue((actual as CUBRIDConnection) != null);
        }

        /// <summary>
        ///A test for CreateConnectionStringBuilder
        ///</summary>
        [TestMethod()]
        public void CreateConnectionStringBuilderTest()
        {
            CUBRIDClientFactory target = new CUBRIDClientFactory();
            DbConnectionStringBuilder actual = target.CreateConnectionStringBuilder();
            Assert.IsTrue((actual as CUBRIDConnectionStringBuilder) != null);
        }

        /// <summary>
        ///A test for CreateDataAdapter
        ///</summary>
        [TestMethod()]
        public void CreateDataAdapterTest()
        {
            CUBRIDClientFactory target = new CUBRIDClientFactory(); 
            DbDataAdapter actual = target.CreateDataAdapter();
            Assert.IsTrue((actual as CUBRIDDataAdapter) != null);
        }

        /// <summary>
        ///A test for CreateParameter
        ///</summary>
        [TestMethod()]
        public void CreateParameterTest()
        {
            CUBRIDClientFactory target = new CUBRIDClientFactory();
            DbParameter actual = target.CreateParameter();
            Assert.IsTrue((actual as CUBRIDParameter) != null);
        }
    }
}
