using CUBRID.Data.CUBRIDClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Unit.TestCases
{
    
    
    /// <summary>
    ///This is a test class for CUBRIDMetaDataTest and is intended
    ///to contain all CUBRIDMetaDataTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CUBRIDMetaDataTest
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
        ///A test for IsNumericType
        ///</summary>
        [TestMethod()]
        public void IsNumericTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("int"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("integer"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("numeric"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("decimal"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("real"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("double"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("float"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("serial"));
            Assert.IsTrue(CUBRIDMetaData.IsNumericType("smallint"));

            Assert.IsFalse(CUBRIDMetaData.IsNumericType("blob"));
        }

        /// <summary>
        ///A test for IsBitType
        ///</summary>
        [TestMethod()]
        public void IsBitTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.IsBitType("bit"));
            Assert.IsTrue(CUBRIDMetaData.IsBitType("bit varying"));
            Assert.IsTrue(CUBRIDMetaData.IsBitType("varbit"));

            Assert.IsFalse(CUBRIDMetaData.IsBitType("clob"));
        }

        /// <summary>
        ///A test for IsCollectionType
        ///</summary>
        [TestMethod()]
        public void IsCollectionTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.IsCollectionType("set"));
            Assert.IsTrue(CUBRIDMetaData.IsCollectionType("multiset"));
            Assert.IsTrue(CUBRIDMetaData.IsCollectionType("list"));
            Assert.IsTrue(CUBRIDMetaData.IsCollectionType("sequence"));

            Assert.IsFalse(CUBRIDMetaData.IsCollectionType("blob"));
        }

        /// <summary>
        ///A test for IsDateTimeType
        ///</summary>
        [TestMethod()]
        public void IsDateTimeTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.IsDateTimeType("date"));
            Assert.IsTrue(CUBRIDMetaData.IsDateTimeType("time"));
            Assert.IsTrue(CUBRIDMetaData.IsDateTimeType("datetime"));
            Assert.IsTrue(CUBRIDMetaData.IsDateTimeType("timestamp"));

            Assert.IsFalse(CUBRIDMetaData.IsDateTimeType("int"));
        }

        /// <summary>
        ///A test for IsLOBType
        ///</summary>
        [TestMethod()]
        public void IsLOBTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.IsLOBType("blob"));
            Assert.IsTrue(CUBRIDMetaData.IsLOBType("clob"));

            Assert.IsFalse(CUBRIDMetaData.IsLOBType("int"));
        }

        /// <summary>
        ///A test for IsTextType
        ///</summary>
        [TestMethod()]
        public void IsTextTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.IsTextType("varchar"));
            Assert.IsTrue(CUBRIDMetaData.IsTextType("char"));
            Assert.IsTrue(CUBRIDMetaData.IsTextType("string"));
            Assert.IsTrue(CUBRIDMetaData.IsTextType("nchar"));
            Assert.IsTrue(CUBRIDMetaData.IsTextType("nvarchar"));

            Assert.IsFalse(CUBRIDMetaData.IsTextType("int"));
        }

        /// <summary>
        ///A test for NameToType
        ///</summary>
        [TestMethod()]
        public void NameToTypeTest()
        {
            Assert.IsTrue(CUBRIDMetaData.NameToType("STRING") == CUBRIDDataType.CCI_U_TYPE_STRING);
            Assert.IsTrue(CUBRIDMetaData.NameToType("CHAR") == CUBRIDDataType.CCI_U_TYPE_CHAR);
            Assert.IsTrue(CUBRIDMetaData.NameToType("VARCHAR") == CUBRIDDataType.CCI_U_TYPE_VARNCHAR);
            Assert.IsTrue(CUBRIDMetaData.NameToType("DATE") == CUBRIDDataType.CCI_U_TYPE_DATE);
            Assert.IsTrue(CUBRIDMetaData.NameToType("DATETIME") == CUBRIDDataType.CCI_U_TYPE_DATETIME);
            Assert.IsTrue(CUBRIDMetaData.NameToType("TIME") == CUBRIDDataType.CCI_U_TYPE_TIME);
            Assert.IsTrue(CUBRIDMetaData.NameToType("TIMESTAMP") == CUBRIDDataType.CCI_U_TYPE_TIMESTAMP);
            Assert.IsTrue(CUBRIDMetaData.NameToType("NUMERIC") == CUBRIDDataType.CCI_U_TYPE_NUMERIC);
            Assert.IsTrue(CUBRIDMetaData.NameToType("DECIMAL") == CUBRIDDataType.CCI_U_TYPE_NUMERIC);
            Assert.IsTrue(CUBRIDMetaData.NameToType("SET") == CUBRIDDataType.CCI_U_TYPE_SET);
            Assert.IsTrue(CUBRIDMetaData.NameToType("MULTISET") == CUBRIDDataType.CCI_U_TYPE_MULTISET);
            Assert.IsTrue(CUBRIDMetaData.NameToType("SEQUENCE") == CUBRIDDataType.CCI_U_TYPE_SEQUENCE);
            Assert.IsTrue(CUBRIDMetaData.NameToType("SHORT") == CUBRIDDataType.CCI_U_TYPE_SHORT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("BIT") == CUBRIDDataType.CCI_U_TYPE_BIT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("VARBIT") == CUBRIDDataType.CCI_U_TYPE_VARBIT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("INT") == CUBRIDDataType.CCI_U_TYPE_INT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("INTEGER") == CUBRIDDataType.CCI_U_TYPE_INT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("BIGINT") == CUBRIDDataType.CCI_U_TYPE_BIGINT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("FLOAT") == CUBRIDDataType.CCI_U_TYPE_FLOAT);
            Assert.IsTrue(CUBRIDMetaData.NameToType("DOUBLE") == CUBRIDDataType.CCI_U_TYPE_DOUBLE);
            Assert.IsTrue(CUBRIDMetaData.NameToType("BLOB") == CUBRIDDataType.CCI_U_TYPE_BLOB);
            Assert.IsTrue(CUBRIDMetaData.NameToType("CLOB") == CUBRIDDataType.CCI_U_TYPE_CLOB);
            Assert.IsTrue(CUBRIDMetaData.NameToType("MONETARY") == CUBRIDDataType.CCI_U_TYPE_MONETARY);
            Assert.IsTrue(CUBRIDMetaData.NameToType("NCHAR") == CUBRIDDataType.CCI_U_TYPE_NCHAR);
            Assert.IsTrue(CUBRIDMetaData.NameToType("VARNCHAR") == CUBRIDDataType.CCI_U_TYPE_VARNCHAR);
            Assert.IsTrue(CUBRIDMetaData.NameToType("OBJECT") == CUBRIDDataType.CCI_U_TYPE_OBJECT);

            Assert.IsTrue(CUBRIDMetaData.NameToType("huodake") == CUBRIDDataType.CCI_U_TYPE_UNKNOWN);
        }

        /// <summary>
        ///A test for SupportsScale
        ///</summary>
        [TestMethod()]
        public void SupportsScaleTest()
        {
            Assert.IsTrue(CUBRIDMetaData.SupportsScale("numeric"));
            Assert.IsTrue(CUBRIDMetaData.SupportsScale("decimal"));
            Assert.IsTrue(CUBRIDMetaData.SupportsScale("float"));
            Assert.IsTrue(CUBRIDMetaData.SupportsScale("real"));

            Assert.IsFalse(CUBRIDMetaData.SupportsScale("int"));
        }
    }
}
