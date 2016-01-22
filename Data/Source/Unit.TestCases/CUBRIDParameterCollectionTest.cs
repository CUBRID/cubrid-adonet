using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDParameterCollectionTest and is intended
    ///to contain all CUBRIDParameterCollectionTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDParameterCollectionTest
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
        ///A test for Add
        ///</summary>
        [TestMethod()]
        public void AddTest()
        {
            CUBRIDParameterCollection target = new CUBRIDParameterCollection();
            
            target.Add();
            target[0].ParameterName = "?p1";
            target[0].Value = 1;
            target[0].CUBRIDDataType = CUBRIDDataType.CCI_U_TYPE_INT;

            CUBRIDParameter p3 = target.Add("?p3", CUBRIDDataType.CCI_U_TYPE_NULL);
            p3.Value = null;

            CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
            p2.Value = 'A';

            try
            {
                target.Insert(1, "?p2");
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Only CUBRIDParameter objects are valid!");
            }

            CUBRIDParameter errParam = new CUBRIDParameter();
            try
            {
                target.Insert(1, errParam);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Parameters must be named!");
            }

            errParam.ParameterName = "err";
            try
            {
                target.Insert(1, errParam);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Parameter name must start with '?'!");
            }

            target.Insert(1, p2);

            try
            {
                target.Insert(2, p3);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Parameter already added to the collection!");
            }

            target.Remove(p3);
            target.RemoveAt(0);
            target.RemoveAt("?p2");
        }

        /// <summary>
        ///A test for Add
        ///</summary>
        [TestMethod()]
        public void AddTest2()
        {
            CUBRIDParameterCollection target = new CUBRIDParameterCollection();

            CUBRIDParameter p3 = new CUBRIDParameter("?p3", CUBRIDDataType.CCI_U_TYPE_NULL);
            p3.Value = null;
            target.Add();
            target[0] = p3;

            CUBRIDParameter p2 = new CUBRIDParameter("?p2", CUBRIDDataType.CCI_U_TYPE_CHAR);
            p2.Value = 'A';

            Assert.AreEqual(target.Contains("?p1"), false);

            CUBRIDParameter p4 = new CUBRIDParameter("?p4", CUBRIDDataType.CCI_U_TYPE_CHAR, 1);
            p4.Value = 'A';

            try
            {
                target.Contains(1);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Argument must be of type CUBRIDParameter!");
            }

            try
            {
                target.Add("?p2");
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Only CUBRIDParameter objects are valid!");
            }

            CUBRIDParameter errParam = new CUBRIDParameter();
            try
            {
                target.Add(errParam);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Parameters must be named!");
            }

            errParam.ParameterName = "err";
            try
            {
                target.Add(errParam);
            }
            catch (Exception ex)
            {
                Assert.AreEqual(ex.Message, "Parameter name must start with '?'!");
            }
        }
    }
}
