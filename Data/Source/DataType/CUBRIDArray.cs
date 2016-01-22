using System;
using System.Collections.Generic;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
	internal class CUBRIDArray
	{
		private CUBRIDDataType baseType;
		private int length;
		private Object[] internalArray;

		public CUBRIDArray(CUBRIDDataType type, int arrayLength)
		{
			baseType = type;

			length = arrayLength;
			if (length < 0)
				return;

			switch (type)
			{
				case CUBRIDDataType.CCI_U_TYPE_BIT:
				case CUBRIDDataType.CCI_U_TYPE_VARBIT:
					internalArray = (Object[])(new byte[length][]);
					break;
				case CUBRIDDataType.CCI_U_TYPE_SHORT:
					(new Int16[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_INT:
					(new int[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_BIGINT:
					(new long[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_FLOAT:
					(new float[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_DOUBLE:
				case CUBRIDDataType.CCI_U_TYPE_MONETARY:
					(new Double[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_NUMERIC:
					(new Decimal[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_DATE:
					(new DateTime[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_TIME:
					(new DateTime[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
				case CUBRIDDataType.CCI_U_TYPE_DATETIME:
					(new DateTime[length]).CopyTo(internalArray, length);
					break;
				case CUBRIDDataType.CCI_U_TYPE_CHAR:
				case CUBRIDDataType.CCI_U_TYPE_NCHAR:
				case CUBRIDDataType.CCI_U_TYPE_STRING:
				case CUBRIDDataType.CCI_U_TYPE_VARNCHAR:
					internalArray = (Object[])(new String[length]);
					break;
				case CUBRIDDataType.CCI_U_TYPE_OBJECT:
					internalArray = (Object[])(new CUBRIDOid[length]);
					break;
				case CUBRIDDataType.CCI_U_TYPE_BLOB:
					//TODO
					//internalArray = (Object[]) (new CUBRIDBlob[length]);
					break;
				case CUBRIDDataType.CCI_U_TYPE_CLOB:
					//TODO
					//internalArray = (Object[])(new CUBRIDClob[length]);
					break;
				default:
					baseType = CUBRIDDataType.CCI_U_TYPE_NULL;
					internalArray = new Object[length];
					break;
			}
		}

		public CUBRIDArray(Object values)
		{
			if ((values is Object[]) && (((Object[])values).Length == 0))
				baseType = CUBRIDDataType.CCI_U_TYPE_OBJECT;
			else
				baseType = Utils.GetObjArrBaseDBtype(values);

			if (baseType == CUBRIDDataType.CCI_U_TYPE_NULL)
				throw new ArgumentException();

			internalArray = (Object[])((Object[])values).Clone();
			length = ((Object[])values).Length;
		}

		Object getArray()
		{
			return internalArray;
		}

		Object getArrayClone()
		{
			if (internalArray == null)
				return null;

			Object[] obj = (Object[])internalArray.Clone();

			return obj;
		}

		int getBaseType()
		{
			return (int)baseType;
		}

		int getLength()
		{
			return length;
		}

		void setElement(int index, Object data)
		{
			internalArray[index] = data;
		}

	}
}
