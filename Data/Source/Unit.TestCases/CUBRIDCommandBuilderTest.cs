using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDCommandBuilderTest and is intended
    ///to contain all CUBRIDCommandBuilderTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDCommandBuilderTest
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
        ///A test for UnquoteIdentifier
        ///</summary>
        [TestMethod()]
        public void UnquoteIdentifierTest()
        {
            using (CUBRIDCommandBuilder target = new CUBRIDCommandBuilder())
            {
                string quotedIdentifier = "`select * from nation`";
                string expected = "select * from nation";
                string actual;
                actual = target.UnquoteIdentifier(quotedIdentifier);
                Assert.AreEqual(expected, actual);

                try
                {
                    actual = target.UnquoteIdentifier(null);
                }
                catch (Exception ex)
                {
                    Assert.IsTrue((ex as ArgumentNullException) != null);
                }

                actual = target.UnquoteIdentifier(expected);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        ///A test for QuoteIdentifier
        ///</summary>
        [TestMethod()]
        public void QuoteIdentifierTest()
        {
            using (CUBRIDCommandBuilder target = new CUBRIDCommandBuilder())
            {
                string unquotedIdentifier = "select * from nation";
                string expected = "`select * from nation`";
                string actual;
                actual = target.QuoteIdentifier(unquotedIdentifier);
                Assert.AreEqual(expected, actual);

                try
                {
                    actual = target.QuoteIdentifier(null);
                }
                catch (Exception ex)
                {
                    Assert.IsTrue((ex as ArgumentNullException) != null);
                }

                actual = target.QuoteIdentifier(expected);
                Assert.AreEqual(expected, actual);
            }
        }
    }
}
