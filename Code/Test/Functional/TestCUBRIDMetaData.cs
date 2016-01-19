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
    public static void TestCUBRIDMetaDataRun()
    {
        TestIsNumericType();
        TestIsBitType();
        TestIsCollectionType();
        TestIsDateTimeType();
        TestIsLOBType();
        TestSupportsScale();
        TestNameToType();
    }
    private static void TestIsNumericType()
    {
        bool result = CUBRIDMetaData.IsNumericType("int");
        Debug.Assert(result == true);

        result = CUBRIDMetaData.IsNumericType("string");
        Debug.Assert(result == false);
    }

    private static void TestIsBitType()
    {
        bool result = CUBRIDMetaData.IsBitType("int");
        Debug.Assert(result == false);

        result = CUBRIDMetaData.IsBitType("string");
        Debug.Assert(result == false);
    }

    private static void TestIsCollectionType()
    {
        bool result = CUBRIDMetaData.IsCollectionType("set");
        Debug.Assert(result == true);

        result = CUBRIDMetaData.IsCollectionType("string");
        Debug.Assert(result == false);
    }

    private static void TestIsDateTimeType()
    {
        bool result = CUBRIDMetaData.IsDateTimeType("date");
        Debug.Assert(result == true);

        result = CUBRIDMetaData.IsDateTimeType("string");
        Debug.Assert(result == false);
    }

    private static void TestIsLOBType()
    {
        bool result = CUBRIDMetaData.IsLOBType("blob");
        Debug.Assert(result == true);

        result = CUBRIDMetaData.IsLOBType("string");
        Debug.Assert(result == false);
    }

    private static void TestSupportsScale()
    {
        bool result = CUBRIDMetaData.SupportsScale("numeric");
        Debug.Assert(result == true);

        result = CUBRIDMetaData.SupportsScale("string");
        Debug.Assert(result == false);
    }

    private static void TestNameToType()
    {
        CUBRIDDataType type = CUBRIDMetaData.NameToType("STRING");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_STRING == type);

        type = CUBRIDMetaData.NameToType("CHAR");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_CHAR ==type);

        type = CUBRIDMetaData.NameToType("VARCHAR");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_VARNCHAR == type);

        type = CUBRIDMetaData.NameToType("DATE");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_DATE == type);

        type = CUBRIDMetaData.NameToType("DATETIME");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_DATETIME == type);

        type = CUBRIDMetaData.NameToType("TIME");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_TIME == type);

        type = CUBRIDMetaData.NameToType("TIMESTAMP");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_TIMESTAMP == type);

        type = CUBRIDMetaData.NameToType("NUMERIC");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_NUMERIC == type);

        type = CUBRIDMetaData.NameToType("DECIMAL");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_NUMERIC == type);

        type = CUBRIDMetaData.NameToType("SET");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_SET == type);

        type = CUBRIDMetaData.NameToType("MULTISET");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_MULTISET == type);

        type = CUBRIDMetaData.NameToType("SEQUENCE");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_SEQUENCE == type);

        type = CUBRIDMetaData.NameToType("SHORT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_SHORT == type);

        type = CUBRIDMetaData.NameToType("BIT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_BIT == type);

        type = CUBRIDMetaData.NameToType("VARBIT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_VARBIT == type);

        type = CUBRIDMetaData.NameToType("INT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_INT == type);

        type = CUBRIDMetaData.NameToType("BIGINT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_BIGINT == type);

        type = CUBRIDMetaData.NameToType("FLOAT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_FLOAT == type);

        type = CUBRIDMetaData.NameToType("DOUBLE");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_DOUBLE == type);

        type = CUBRIDMetaData.NameToType("BLOB");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_BLOB == type);

        type = CUBRIDMetaData.NameToType("CLOB");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_CLOB == type);

        type = CUBRIDMetaData.NameToType("MONETARY");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_MONETARY == type);

        type = CUBRIDMetaData.NameToType("NCHAR");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_NCHAR == type);

        type = CUBRIDMetaData.NameToType("VARNCHAR");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_VARNCHAR == type);

        type = CUBRIDMetaData.NameToType("ENUM");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_ENUM == type);

        type = CUBRIDMetaData.NameToType("OBJECT");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_OBJECT == type);

        type = CUBRIDMetaData.NameToType("UNKNOWN");
        Debug.Assert(CUBRIDDataType.CCI_U_TYPE_UNKNOWN == type);
    }
  }
}