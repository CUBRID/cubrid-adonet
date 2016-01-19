/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

using System;
using System.Collections.Generic;
using System.Data.Common;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   The exception that is thrown when CUBRID returns an error.
  /// </summary>
  [Serializable]
  public sealed class CUBRIDException : DbException
  {
    #region CUBRIDErrorCode enum

    /// <summary>
    ///   CUBRID error codes.
    /// </summary>
    public enum CUBRIDErrorCode
    {
#pragma warning disable 1591
      ER_NO_ERROR = 0,
      ER_NOT_OBJECT = 1,
      ER_DBMS = 2,
      ER_COMMUNICATION = 3,
      ER_NO_MORE_DATA = 4,
      ER_TYPE_CONVERSION = 5,
      ER_BIND_INDEX = 6,
      ER_NOT_BIND = 7,
      ER_WAS_NULL = 8,
      ER_COLUMN_INDEX = 9,
      ER_TRUNCATE = 10,
      ER_SCHEMA_TYPE = 11,
      ER_FILE = 12,
      ER_CONNECTION = 13,
      ER_ISO_TYPE = 14,
      ER_ILLEGAL_REQUEST = 15,
      ER_INVALID_ARGUMENT = 16,
      ER_IS_CLOSED = 17,
      ER_ILLEGAL_FLAG = 18,
      ER_ILLEGAL_DATA_SIZE = 19,
      ER_NO_MORE_RESULT = 20,
      ER_OID_IS_NOT_INCLUDED = 21,
      ER_CMD_IS_NOT_INSERT = 22,
      ER_UNKNOWN = 23,

      /* CAS Error Codes, cas_error.h */

      CAS_ER_DBMS = -1000,
      CAS_ER_INTERNAL = -1001,
      CAS_ER_NO_MORE_MEMORY = -1002,
      CAS_ER_COMMUNICATION = -1003,
      CAS_ER_ARGS = -1004,
      CAS_ER_TRAN_TYPE = -1005,
      CAS_ER_SRV_HANDLE = -1006,
      CAS_ER_NUM_BIND = -1007,
      CAS_ER_UNKNOWN_U_TYPE = -1008,
      CAS_ER_DB_VALUE = -1009,
      CAS_ER_TYPE_CONVERSION = -1010,
      CAS_ER_PARAM_NAME = -1011,
      CAS_ER_NO_MORE_DATA = -1012,
      CAS_ER_OBJECT = -1013,
      CAS_ER_OPEN_FILE = -1014,
      CAS_ER_SCHEMA_TYPE = -1015,
      CAS_ER_VERSION = -1016,
      CAS_ER_FREE_SERVER = -1017,
      CAS_ER_NOT_AUTHORIZED_CLIENT = -1018,
      CAS_ER_QUERY_CANCEL = -1019,
      CAS_ER_NOT_COLLECTION = -1020,
      CAS_ER_COLLECTION_DOMAIN = -1021,
      CAS_ER_NO_MORE_RESULT_SET = -1022,
      CAS_ER_INVALID_CALL_STMT = -1023,
      CAS_ER_STMT_POOLING = -1024,
      CAS_ER_DBSERVER_DISCONNECTED = -1025,
      CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED = -1026,
      CAS_ER_HOLDABLE_NOT_ALLOWED = -1027,
      CAS_ER_NOT_IMPLEMENTED = -1100,

      CAS_ER_IS = -1200,
#pragma warning restore 1591
    }

    #endregion

    private const int errorCodeGeneric = -1;

    private readonly int errorCode;
    private readonly string errorMessage;
    private readonly Dictionary<CUBRIDErrorCode, string> errorMessages = new Dictionary<CUBRIDErrorCode, string>();

    internal CUBRIDException()
    {
      Initialize();
      errorCode = (int)CUBRIDErrorCode.ER_NO_ERROR;
      errorMessage = null;
    }

    internal CUBRIDException(string message)
    {
      Initialize();
      errorMessage = message;
      if (errorMessages.ContainsValue(message))
      {
        var key = GetKey(message);
        if (key != null)
          errorCode = (int)key;
      }
      else
      {
        errorCode = errorCodeGeneric;
      }
    }

    internal CUBRIDException(int errorCode)
    {
      Initialize();
      this.errorCode = errorCode;
      errorMessage = errorMessages.ContainsKey((CUBRIDErrorCode)errorCode) ? errorMessages[(CUBRIDErrorCode)errorCode] : null;
    }

    internal CUBRIDException(CUBRIDErrorCode errorCode)
    {
      Initialize();
      this.errorCode = (int)errorCode;
      errorMessage = errorMessages.ContainsKey(errorCode) ? errorMessages[errorCode] : null;
    }

    internal CUBRIDException(int errorCode, string message)
    {
      Initialize();
      this.errorCode = errorCode;
      errorMessage = message;
    }

    internal CUBRIDException(CUBRIDErrorCode errorCode, string message)
    {
      Initialize();
      this.errorCode = (int)errorCode;
      errorMessage = message;
    }

    /// <summary>
    ///   Get CUBRID error code.
    /// </summary>
    public override int ErrorCode
    {
      get { return errorCode; }
    }

    /// <summary>
    ///   Get CUBRID error message.
    /// </summary>
    public override string Message
    {
      get { return errorMessage; }
    }

    private void Initialize()
    {
      errorMessages.Add(CUBRIDErrorCode.ER_UNKNOWN, "Error");
      errorMessages.Add(CUBRIDErrorCode.ER_NO_ERROR, "No Error");

      errorMessages.Add(CUBRIDErrorCode.ER_DBMS, "Server error");
      errorMessages.Add(CUBRIDErrorCode.ER_COMMUNICATION, "Cannot communicate with the broker");
      errorMessages.Add(CUBRIDErrorCode.ER_NO_MORE_DATA, "Invalid dataReader position");
      errorMessages.Add(CUBRIDErrorCode.ER_TYPE_CONVERSION, "DataType conversion error");
      errorMessages.Add(CUBRIDErrorCode.ER_BIND_INDEX, "Missing or invalid position of the bind variable provided");
      errorMessages.Add(CUBRIDErrorCode.ER_NOT_BIND,
                        "Attempt to execute the query when not all the parameters are binded");
      errorMessages.Add(CUBRIDErrorCode.ER_WAS_NULL, "Internal Error: NULL value");
      errorMessages.Add(CUBRIDErrorCode.ER_COLUMN_INDEX, "Column index is out of range");
      errorMessages.Add(CUBRIDErrorCode.ER_TRUNCATE, "Data is truncated because receive buffer is too small");
      errorMessages.Add(CUBRIDErrorCode.ER_SCHEMA_TYPE, "Internal error: Illegal schema paramCUBRIDDataType");
      errorMessages.Add(CUBRIDErrorCode.ER_FILE, "File access failed");
      errorMessages.Add(CUBRIDErrorCode.ER_CONNECTION, "Cannot connect to a broker");
      errorMessages.Add(CUBRIDErrorCode.ER_ISO_TYPE, "Unknown transaction isolation level");
      errorMessages.Add(CUBRIDErrorCode.ER_ILLEGAL_REQUEST, "Internal error: The requested information is not available");
      errorMessages.Add(CUBRIDErrorCode.ER_INVALID_ARGUMENT, "The argument is invalid");
      errorMessages.Add(CUBRIDErrorCode.ER_IS_CLOSED, "Connection or Statement might be closed");
      errorMessages.Add(CUBRIDErrorCode.ER_ILLEGAL_FLAG, "Internal error: Invalid argument");
      errorMessages.Add(CUBRIDErrorCode.ER_ILLEGAL_DATA_SIZE,
                        "Cannot communicate with the broker or received invalid packet");
      errorMessages.Add(CUBRIDErrorCode.ER_NOT_OBJECT, "Index's Column is Not Object");
      errorMessages.Add(CUBRIDErrorCode.ER_NO_MORE_RESULT, "No More Result");
      errorMessages.Add(CUBRIDErrorCode.ER_OID_IS_NOT_INCLUDED, "This ResultSet do not include the OID");
      errorMessages.Add(CUBRIDErrorCode.ER_CMD_IS_NOT_INSERT, "Command is not insert");

      errorMessages.Add(CUBRIDErrorCode.CAS_ER_DBMS, "Database connection error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_INTERNAL, "General server error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NO_MORE_MEMORY, "Memory allocation error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_COMMUNICATION, "Communication error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_ARGS, "Invalid argument");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_TRAN_TYPE, "Unknown transaction paramCUBRIDDataType");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_SRV_HANDLE, "Internal server error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NUM_BIND, "Parameter binding error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_UNKNOWN_U_TYPE, "Parameter binding error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_DB_VALUE, "Cannot make DB_VALUE");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_TYPE_CONVERSION, "DataType conversion error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_PARAM_NAME, "Invalid database parameter name");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NO_MORE_DATA, "No more data");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_OBJECT, "Object is not valid");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_OPEN_FILE, "File open error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_SCHEMA_TYPE, "Invalid schema paramCUBRIDDataType");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_VERSION, "Version mismatch");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_FREE_SERVER, "Cannot process the request. Try again later");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NOT_AUTHORIZED_CLIENT, "Authorization error");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_QUERY_CANCEL, "Cannot cancel the query");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NOT_COLLECTION,
                        "The attribute domain must be the set paramCUBRIDDataType");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_COLLECTION_DOMAIN,
                        "The domain of a set must be the same data paramCUBRIDDataType");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NO_MORE_RESULT_SET, "No More Result");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_INVALID_CALL_STMT, "Illegal CALL statement");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_STMT_POOLING, "Statement Pooling");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_DBSERVER_DISCONNECTED, "DB Server disconnected");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED,
                        "Cannot prepare more than MAX_PREPARED_STMT_COUNT statements");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_HOLDABLE_NOT_ALLOWED,
                        "Holdable results may not be updatable or sensitive");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_NOT_IMPLEMENTED, "Attempt to use a not supported service");
      errorMessages.Add(CUBRIDErrorCode.CAS_ER_IS, "Authentication failure");
    }

    private int? GetKey(string value)
    {
      foreach (var pair in errorMessages)
      {
        if (pair.Value == value)
        {
          return (int)pair.Key;
        }
      }
      return null;
    }
  }
}