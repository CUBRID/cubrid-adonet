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
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES, LOSS OF USE, DATA, 
 * OR PROFITS, OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CAS Function codes
  /// </summary>
  public enum CASFunctionCode
  {
    /* cas_protocol.h */

#pragma warning disable 1591
    CAS_FC_END_TRAN = 1,
    CAS_FC_PREPARE = 2,
    CAS_FC_EXECUTE = 3,
    CAS_FC_GET_DB_PARAMETER = 4,
    CAS_FC_SET_DB_PARAMETER = 5,
    CAS_FC_CLOSE_REQ_HANDLE = 6,
    CAS_FC_CURSOR = 7,
    CAS_FC_FETCH = 8,
    CAS_FC_SCHEMA_INFO = 9,
    CAS_FC_OID_GET = 10,
    CAS_FC_OID_PUT = 11,
    CAS_FC_DEPRECATED1 = 12,
    CAS_FC_DEPRECATED2 = 13,
    CAS_FC_DEPRECATED3 = 14,
    CAS_FC_GET_DB_VERSION = 15,
    CAS_FC_GET_CLASS_NUM_OBJS = 16,
    CAS_FC_OID_CMD = 17,
    CAS_FC_COLLECTION = 18,
    CAS_FC_NEXT_RESULT = 19,
    CAS_FC_EXECUTE_BATCH = 20,
    CAS_FC_EXECUTE_ARRAY = 21,
    CAS_FC_CURSOR_UPDATE = 22,
    CAS_FC_GET_ATTR_TYPE_STR = 23,
    CAS_FC_GET_QUERY_INFO = 24,
    CAS_FC_DEPRECATED4 = 25,
    CAS_FC_SAVEPOINT = 26,
    CAS_FC_PARAMETER_INFO = 27,
    CAS_FC_XA_PREPARE = 28,
    CAS_FC_XA_RECOVER = 29,
    CAS_FC_XA_END_TRAN = 30,
    CAS_FC_CON_CLOSE = 31,
    CAS_FC_CHECK_CAS = 32,
    CAS_FC_MAKE_OUT_RS = 33,
    CAS_FC_GET_GENERATED_KEYS = 34,
    CAS_FC_LOB_NEW = 35,
    CAS_FC_LOB_WRITE = 36,
    CAS_FC_LOB_READ = 37,
    CAS_FC_END_SESSION = 38,
    CAS_FC_GET_ROW_COUNT = 39,
    CAS_FC_GET_LAST_INSERT_ID = 40,
    CAS_FC_CURSOR_CLOSE = 41,
#pragma warning restore 1591
  }

  /// <summary>
  ///   CUBRID Statement types
  /// </summary>
  public enum CUBRIDStatementType
  {
    /* cas_protocol.h */
    /* cas_cci.h */

#pragma warning disable 1591
    CUBRID_STMT_ALTER_CLASS,
    CUBRID_STMT_ALTER_SERIAL,
    CUBRID_STMT_COMMIT_WORK,
    CUBRID_STMT_REGISTER_DATABASE,
    CUBRID_STMT_CREATE_CLASS,
    CUBRID_STMT_CREATE_INDEX,
    CUBRID_STMT_CREATE_TRIGGER,
    CUBRID_STMT_CREATE_SERIAL,
    CUBRID_STMT_DROP_DATABASE,
    CUBRID_STMT_DROP_CLASS,
    CUBRID_STMT_DROP_INDEX,
    CUBRID_STMT_DROP_LABEL,
    CUBRID_STMT_DROP_TRIGGER,
    CUBRID_STMT_DROP_SERIAL,
    CUBRID_STMT_EVALUATE,
    CUBRID_STMT_RENAME_CLASS,
    CUBRID_STMT_ROLLBACK_WORK,
    CUBRID_STMT_GRANT,
    CUBRID_STMT_REVOKE,
    CUBRID_STMT_STATISTICS,
    CUBRID_STMT_INSERT,
    CUBRID_STMT_SELECT,
    CUBRID_STMT_UPDATE,
    CUBRID_STMT_DELETE,
    CUBRID_STMT_CALL,
    CUBRID_STMT_GET_ISO_LVL,
    CUBRID_STMT_GET_TIMEOUT,
    CUBRID_STMT_GET_OPT_LVL,
    CUBRID_STMT_SET_OPT_LVL,
    CUBRID_STMT_SCOPE,
    CUBRID_STMT_GET_TRIGGER,
    CUBRID_STMT_SET_TRIGGER,
    CUBRID_STMT_SAVEPOINT,
    CUBRID_STMT_PREPARE,
    CUBRID_STMT_ATTACH,
    CUBRID_STMT_USE,
    CUBRID_STMT_REMOVE_TRIGGER,
    CUBRID_STMT_RENAME_TRIGGER,
    CUBRID_STMT_ON_LDB,
    CUBRID_STMT_GET_LDB,
    CUBRID_STMT_SET_LDB,

    CUBRID_STMT_GET_STATS,
    CUBRID_STMT_CREATE_USER,
    CUBRID_STMT_DROP_USER,
    CUBRID_STMT_ALTER_USER,


    CUBRID_STMT_CALL_SP = 0x7e,
    CUBRID_STMT_UNKNOWN = 0x7f
#pragma warning restore 1591
  }

  /// <summary>
  ///   CUBRID Transaction isolation levels
  /// </summary>
  public enum CUBRIDIsolationLevel
  {
    /* cubrid_api.h */
#pragma warning disable 1591
    TRAN_UNKNOWN_ISOLATION = 0x00,
    TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE = 0x01,
    TRAN_COMMIT_CLASS_COMMIT_INSTANCE = 0x02,
    TRAN_REP_CLASS_UNCOMMIT_INSTANCE = 0x03,
    TRAN_REP_CLASS_COMMIT_INSTANCE = 0x04,
    TRAN_REP_CLASS_REP_INSTANCE = 0x05,
    TRAN_SERIALIZABLE = 0x06,
    TRAN_DEFAULT_ISOLATION = TRAN_REP_CLASS_UNCOMMIT_INSTANCE,
#pragma warning restore 1591
  }

  /// <summary>
  ///   CUBRID data types
  /// </summary>
  public enum CUBRIDDataType
  {
    /* cas_cci.h */
#pragma warning disable 1591
    CCI_U_TYPE_FIRST = 0,
    CCI_U_TYPE_UNKNOWN = 0,
    CCI_U_TYPE_NULL = 0,

    CCI_U_TYPE_CHAR = 1,
    CCI_U_TYPE_STRING = 2,
    CCI_U_TYPE_NCHAR = 3,
    CCI_U_TYPE_VARNCHAR = 4,
    CCI_U_TYPE_BIT = 5,
    CCI_U_TYPE_VARBIT = 6,
    CCI_U_TYPE_NUMERIC = 7,
    CCI_U_TYPE_INT = 8,
    CCI_U_TYPE_SHORT = 9,
    CCI_U_TYPE_MONETARY = 10,
    CCI_U_TYPE_FLOAT = 11,
    CCI_U_TYPE_DOUBLE = 12,
    CCI_U_TYPE_DATE = 13,
    CCI_U_TYPE_TIME = 14,
    CCI_U_TYPE_TIMESTAMP = 15,
    CCI_U_TYPE_SET = 16,
    CCI_U_TYPE_MULTISET = 17,
    CCI_U_TYPE_SEQUENCE = 18,
    CCI_U_TYPE_OBJECT = 19,
    CCI_U_TYPE_RESULTSET = 20,
    CCI_U_TYPE_BIGINT = 21,
    CCI_U_TYPE_DATETIME = 22,
    CCI_U_TYPE_BLOB = 23,
    CCI_U_TYPE_CLOB = 24,
    CCI_U_TYPE_ENUM = 25,

    CCI_U_TYPE_LAST = CCI_U_TYPE_ENUM
#pragma warning restore 1591
  }

  internal enum CUBRIDSchemaType
  {
    /* cas_cci.h */

    CCI_SCH_CLASS = 1,
    CCI_SCH_VCLASS = 2,
    CCI_SCH_QUERY_SPEC = 3,
    CCI_SCH_ATTRIBUTE = 4,
    CCI_SCH_CLASS_ATTRIBUTE = 5,
    CCI_SCH_METHOD = 6,
    CCI_SCH_CLASS_METHOD = 7,
    CCI_SCH_METHOD_FILE = 8,
    CCI_SCH_SUPERCLASS = 9,
    CCI_SCH_SUBCLASS = 10,
    CCI_SCH_CONSTRAIT = 11,
    CCI_SCH_TRIGGER = 12,
    CCI_SCH_CLASS_PRIVILEGE = 13,
    CCI_SCH_ATTR_PRIVILEGE = 14,
    CCI_SCH_DIRECT_SUPER_CLASS = 15,
    CCI_SCH_PRIMARY_KEY = 16,
    CCI_SCH_IMPORTED_KEYS = 17,
    CCI_SCH_EXPORTED_KEYS = 18,
    CCI_SCH_CROSS_REFERENCE = 19,
  }

  internal enum CCIPrepareOption
  {
    /* cas_cci.h */

    CCI_PREPARE_NORMAL = 0x00,
    CCI_PREPARE_INCLUDE_OID = 0x01,
    CCI_PREPARE_UPDATABLE = 0x02,
    CCI_PREPARE_QUERY_INFO = 0x04,
    CCI_PREPARE_HOLDABLE = 0x08,
    CCI_PREPARE_CALL = 0x40,
  }

  internal enum OidCommand
  {
    /* UConnection.java */

    DROP_BY_OID = 1,
    IS_INSTANCE = 2,
    GET_READ_LOCK_BY_OID = 3,
    GET_WRITE_LOCK_BY_OID = 4,
    GET_CLASS_NAME_BY_OID = 5
  }

  internal enum CCITransactionType
  {
    /* cas_cci.h */

    CCI_TRAN_COMMIT = 1,
    CCI_TRAN_ROLLBACK = 2
  }

  internal enum CCIDbParam
  {
    /* cas_cci.h */

    CCI_PARAM_ISOLATION_LEVEL = 1,
    CCI_PARAM_LOCK_TIMEOUT = 2,
    CCI_PARAM_MAX_STRING_LENGTH = 3,
    CCI_PARAM_AUTO_COMMIT = 4,
  }

  internal enum CUBRIDCollectionCommand
  {
    /* UConnection.java */

    GET_COLLECTION_VALUE = 1,
    GET_SIZE_OF_COLLECTION = 2,
    DROP_ELEMENT_IN_SET = 3,
    ADD_ELEMENT_TO_SET = 4,
    DROP_ELEMENT_IN_SEQUENCE = 5,
    INSERT_ELEMENT_INTO_SEQUENCE = 6,
    PUT_ELEMENT_ON_SEQUENCE = 7
  }

  internal enum ConnectionStatus
  {
    /* broker_shm.h */

    CON_STATUS_OUT_TRAN = 0,
    CON_STATUS_IN_TRAN = 1,
    CON_STATUS_CLOSE = 2,
    CON_STATUS_CLOSE_AND_CONNECT = 3
  }

  internal enum CCICursorPosition
  {
    /* cas_cci.h */

    CCI_CURSOR_FIRST = 0,
    CCI_CURSOR_CURRENT = 1,
    CCI_CURSOR_LAST = 2
  }

  internal enum StmtType
  {
    NORMAL = 0,
    GET_BY_OID = 1,
    GET_SCHEMA_INFO = 2,
    GET_AUTOINCREMENT_KEYS = 3
  }

  internal enum QueryExecutionMode
  {
    /* cas_cci.h */

    SYNC_EXEC = 0,
    ASYNC_EXEC = 1
  }

  internal enum CCIExecutionOption
  {
    /* cas_cci.h */

    CCI_EXEC_NORMAL = 0x00,
    CCI_EXEC_ASYNC = 0x01,
    CCI_EXEC_QUERY_ALL = 0x02,
    CCI_EXEC_QUERY_INFO = 0x04,
    CCI_EXEC_ONLY_QUERY_PLAN = 0x08,
    CCI_EXEC_THREAD = 0x10,
    CCI_EXEC_HOLDABLE = 0x20,
  }

  /// <summary>
  ///   Is column Auto incrementable?
  /// </summary>
  internal enum IsAutoIncrementable
  {
    No = 0,
    Yes = 1
  }

  /// <summary>
  ///   Is column Fixed Length?
  /// </summary>
  internal enum IsFixedLength
  {
    No = 0,
    Yes = 1
  }

  /// <summary>
  ///   Is column Fixed Precision Scale?
  /// </summary>
  internal enum IsFixedPrecisionScale
  {
    No = 0,
    Yes = 1
  }

  /// <summary>
  ///   Is column Long?
  /// </summary>
  internal enum IsLong
  {
    No = 0,
    Yes = 1
  }

  /// <summary>
  ///   Is column Nullable?
  /// </summary>
  internal enum IsNullable
  {
    No = 0,
    Yes = 1
  }

  /// <summary>
  ///   Is true?
  ///   (to be used when a int type is needed instead of a boolean)
  /// </summary>
  public enum IsTrue
  {
    /// <summary>
    /// No
    /// </summary>
    No = 0,
    /// <summary>
    /// Yes
    /// </summary>
    Yes = 1
  }
}