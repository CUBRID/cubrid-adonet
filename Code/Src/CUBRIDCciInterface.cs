using System;
using System.Text;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.InteropServices;

namespace CUBRID.Data.CUBRIDClient
{
    enum T_CCI_PREPARE_FLAG
    {
        CCI_PREPARE_INCLUDE_OID = 0x01,
        CCI_PREPARE_UPDATABLE = 0x02,
        CCI_PREPARE_CALL = 0x40,
    }
    enum T_CCI_ERROR_CODE
    {
        CCI_ER_NO_ERROR = 0,
        CCI_ER_DBMS = -20001,
        CCI_ER_CON_HANDLE = -20002,
        CCI_ER_NO_MORE_MEMORY = -20003,
        CCI_ER_COMMUNICATION = -20004,
        CCI_ER_NO_MORE_DATA = -20005,
        CCI_ER_TRAN_TYPE = -20006,
        CCI_ER_STRING_PARAM = -20007,
        CCI_ER_TYPE_CONVERSION = -20008,
        CCI_ER_BIND_INDEX = -20009,
        CCI_ER_ATYPE = -20010,
        CCI_ER_NOT_BIND = -20011,
        CCI_ER_PARAM_NAME = -20012,
        CCI_ER_COLUMN_INDEX = -20013,
        CCI_ER_SCHEMA_TYPE = -20014,
        CCI_ER_FILE = -20015,
        CCI_ER_CONNECT = -20016,

        CCI_ER_ALLOC_CON_HANDLE = -20017,
        CCI_ER_REQ_HANDLE = -20018,
        CCI_ER_INVALID_CURSOR_POS = -20019,
        CCI_ER_OBJECT = -20020,
        CCI_ER_CAS = -20021,
        CCI_ER_HOSTNAME = -20022,
        CCI_ER_OID_CMD = -20023,

        CCI_ER_BIND_ARRAY_SIZE = -20024,
        CCI_ER_ISOLATION_LEVEL = -20025,

        CCI_ER_SET_INDEX = -20026,
        CCI_ER_DELETED_TUPLE = -20027,

        CCI_ER_SAVEPOINT_CMD = -20028,
        CCI_ER_THREAD_RUNNING = -20029,
        CCI_ER_INVALID_URL = -20030,
        CCI_ER_INVALID_LOB_READ_POS = -20031,
        CCI_ER_INVALID_LOB_HANDLE = -20032,

        CCI_ER_NO_PROPERTY = -20033,

        CCI_ER_PROPERTY_TYPE = -20034,
        CCI_ER_INVALID_PROPERTY_VALUE = CCI_ER_PROPERTY_TYPE,

        CCI_ER_INVALID_DATASOURCE = -20035,
        CCI_ER_DATASOURCE_TIMEOUT = -20036,
        CCI_ER_DATASOURCE_TIMEDWAIT = -20037,

        CCI_ER_LOGIN_TIMEOUT = -20038,
        CCI_ER_QUERY_TIMEOUT = -20039,

        CCI_ER_RESULT_SET_CLOSED = -20040,

        CCI_ER_INVALID_HOLDABILITY = -20041,
        CCI_ER_NOT_UPDATABLE = -20042,

        CCI_ER_INVALID_ARGS = -20043,
        CCI_ER_USED_CONNECTION = -20044,

        CCI_ER_NO_SHARD_AVAILABLE = -20045,
        CCI_ER_INVALID_SHARD = -20046,

        CCI_ER_NOT_IMPLEMENTED = -20099,
        CCI_ER_END = -20100
    }

    enum T_CCI_SCH_TYPE
    {
        CCI_SCH_FIRST = 1,
        CCI_SCH_CLASS = 1,
        CCI_SCH_VCLASS,
        CCI_SCH_QUERY_SPEC,
        CCI_SCH_ATTRIBUTE,
        CCI_SCH_CLASS_ATTRIBUTE,
        CCI_SCH_METHOD,
        CCI_SCH_CLASS_METHOD,
        CCI_SCH_METHOD_FILE,
        CCI_SCH_SUPERCLASS,
        CCI_SCH_SUBCLASS,
        CCI_SCH_CONSTRAINT,
        CCI_SCH_TRIGGER,
        CCI_SCH_CLASS_PRIVILEGE,
        CCI_SCH_ATTR_PRIVILEGE,
        CCI_SCH_DIRECT_SUPER_CLASS,
        CCI_SCH_PRIMARY_KEY,
        CCI_SCH_IMPORTED_KEYS,
        CCI_SCH_EXPORTED_KEYS,
        CCI_SCH_CROSS_REFERENCE,
        CCI_SCH_LAST = CCI_SCH_CROSS_REFERENCE
    }

    enum T_CCI_A_TYPE
    {
        CCI_A_TYPE_FIRST = 1,
        CCI_A_TYPE_STR = 1,
        CCI_A_TYPE_INT,
        CCI_A_TYPE_FLOAT,
        CCI_A_TYPE_DOUBLE,
        CCI_A_TYPE_BIT,
        CCI_A_TYPE_DATE,
        CCI_A_TYPE_SET,
        CCI_A_TYPE_BIGINT,
        CCI_A_TYPE_BLOB,
        CCI_A_TYPE_CLOB,
        CCI_A_TYPE_REQ_HANDLE,
        CCI_A_TYPE_LAST = CCI_A_TYPE_REQ_HANDLE,

        CCI_A_TYTP_LAST = CCI_A_TYPE_LAST	/* typo but backward compatibility */
    }

    enum CCI_AUTOCOMMIT_MODE
    {
        CCI_AUTOCOMMIT_FALSE = 0,
        CCI_AUTOCOMMIT_TRUE
    }

    enum CCI_TRAN_MODE
    {
        CCI_TRAN_COMMIT = 1,
        CCI_TRAN_ROLLBACK =2
    }

    enum T_CCI_DB_PARAM
    {
        CCI_PARAM_FIRST = 1,
        CCI_PARAM_ISOLATION_LEVEL = 1,
        CCI_PARAM_LOCK_TIMEOUT = 2,
        CCI_PARAM_MAX_STRING_LENGTH = 3,
        CCI_PARAM_AUTO_COMMIT = 4,
        CCI_PARAM_LAST = CCI_PARAM_AUTO_COMMIT,

        /* below parameters are used internally */
        CCI_PARAM_NO_BACKSLASH_ESCAPES = 5
    }

    internal struct T_CCI_COL_INFO
    {
        public char ext_type;
        public char is_non_null;
        public short scale;
        public int precision;
        public IntPtr col_name;
        public IntPtr real_attr;
        public IntPtr class_name;
        public IntPtr default_value;
        public char is_auto_increment;
        public char is_unique_key;
        public char is_primary_key;
        public char is_foreign_key;
        public char is_reverse_index;
        public char is_reverse_unique;
        public char is_shared;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    internal struct T_CCI_ERROR
    {
        public int err_code;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 1024)]
        public string err_msg;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    internal struct T_CCI_BIT
    {
        public int size;
        public IntPtr buf;
    } 

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    internal struct T_CCI_QUERY_RESULT
    {
        public int result_count;
        public int stmt_type;
        public int err_no;
        public string err_msg;
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst=32)]
        public string oid;
    }

    internal static class CciInterface
    {
        const int MAX_TBALE_COLUMN_NAME_LENGTH = 254;
        const string dll_name = @"cascci.dll";
        [DllImport(dll_name, EntryPoint = "cci_get_db_version", CharSet = CharSet.Ansi)]
        public static extern int cci_get_db_version(int con_handle, StringBuilder out_buf, int capacity);
        public static string cci_get_db_version(CUBRIDConnection conn_handle, int capacity)
        {
            StringBuilder out_buf = new StringBuilder(capacity);
            int ret = cci_get_db_version(conn_handle.Conection, out_buf, capacity);
            if (ret < 0)
            {
                throw new CUBRIDException(ret);
            }
            return out_buf.ToString();
        }

        [DllImport(dll_name, EntryPoint = "cci_col_size", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_col_size(int mapped_conn_id, string oid_str, string col_attr, ref int col_size, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_col_set_add", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_col_set_add(int mapped_conn_id, string oid_str, string col_attr,
            string value, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_col_set_drop", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int  cci_col_set_drop(int mapped_conn_id, string oid_str, string col_attr, string value, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_col_seq_put", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_col_seq_put(int mapped_conn_id, string oid_str, string col_attr, 
            int index, string value, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_col_seq_insert", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_col_seq_insert(int mapped_conn_id, string oid_str, string col_attr,
		    int index, string value, ref T_CCI_ERROR  err_buf);

        [DllImport(dll_name, EntryPoint = "cci_col_seq_drop", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_col_seq_drop(int mapped_conn_id, string oid_str, string col_attr,
            int index, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_connect_with_url_ex", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_connect_with_url_ex(string url, string db_user, string db_password, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_oid_get_class_name", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_oid_get_class_name(int mapped_conn_id, string oid_str, byte[] out_buf,
            int out_buf_size, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_set_holdability", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_set_holdability(int mapped_conn_id, int holdable);

        [DllImport(dll_name, EntryPoint = "cci_set_charset", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_set_charset(int mapped_conn_id, string charset);

        [DllImport(dll_name, EntryPoint = "cci_disconnect", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_disconnect(int mapped_conn_id, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_close_req_handle", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_close_req_handle(int req_id);

        [DllImport(dll_name, EntryPoint = "cci_prepare", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_prepare(int conn_handle, byte[] sql_stmt, char flag, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_execute", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_execute(int req_handle, char flag, int max_col_size, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_get_result_info", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern IntPtr cci_get_result_info_internal(int req_handle, ref int stmt_type, ref int col_num);

        public static ColumnMetaData[] cci_get_result_info(CUBRIDConnection conn, int req_handle)
        {
            int stmt_type = 0;
            int col_num = 0;
            int n;
            byte[] name = new byte[MAX_TBALE_COLUMN_NAME_LENGTH];

            IntPtr pt = cci_get_result_info_internal(req_handle, ref stmt_type, ref col_num);
            ColumnMetaData[] item = new ColumnMetaData[col_num];

            for (int i = 0; i < col_num; i++)
            {
                ColumnMetaData data = new ColumnMetaData();
                try
                {
                    T_CCI_COL_INFO tmp =
                        (T_CCI_COL_INFO)Marshal.PtrToStructure((IntPtr)((UInt32)pt +
                        i * Marshal.SizeOf(typeof(T_CCI_COL_INFO))), typeof(T_CCI_COL_INFO));
                    data.Type = (CUBRIDDataType)tmp.ext_type;

                    data.IsAutoIncrement = int_to_bool(tmp.is_auto_increment - 0);
                    data.IsForeignKey = int_to_bool(tmp.is_foreign_key - 0);
                    data.IsNullable = int_to_bool(tmp.is_non_null - 0) == true ? false : true;
                    data.IsPrimaryKey = int_to_bool(tmp.is_primary_key - 0);
                    data.IsReverseIndex = int_to_bool(tmp.is_reverse_index - 0);
                    data.IsReverseUnique = int_to_bool(tmp.is_reverse_unique - 0);
                    data.IsShared = int_to_bool(tmp.is_shared - 0);
                    data.IsUniqueKey = int_to_bool(tmp.is_unique_key - 0);
                    data.Precision = tmp.precision;
                    data.Scale = tmp.scale;
                    data.Type = (CUBRIDDataType)tmp.ext_type;

                    Marshal.Copy(tmp.col_name, name, 0, MAX_TBALE_COLUMN_NAME_LENGTH);
                    for (n = 0; n < MAX_TBALE_COLUMN_NAME_LENGTH; n++) if (name[n] == 0) break;
                    if (conn.GetEncoding().Equals(Encoding.UTF8))
                        data.Name = Encoding.UTF8.GetString(name, 0, n);
                    else
                        data.Name = Encoding.Unicode.GetString(name, 0, n);

                    Marshal.Copy(tmp.class_name, name, 0, MAX_TBALE_COLUMN_NAME_LENGTH);
                    for (n = 0; n < MAX_TBALE_COLUMN_NAME_LENGTH; n++) if (name[n] == 0) break;
                    if (conn.GetEncoding().Equals(Encoding.UTF8))
                    {
                        data.RealName = Encoding.UTF8.GetString(name, 0, n);
                        data.Table = Encoding.UTF8.GetString(name, 0, n);
                    }
                    else
                    {
                        data.RealName = Encoding.Unicode.GetString(name, 0, n);
                        data.Table = Encoding.Unicode.GetString(name, 0, n);
                    }
                }
                catch
                {
                    //Not throw exception, just set default value
                }
                finally
                {
                    item[i] = data;
                }          
            }
            return item;
        }

        [DllImport(dll_name, EntryPoint = "cci_cursor", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_cursor(int mapped_stmt_id, int offset, CCICursorPosition origin, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_fetch", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_fetch(int mapped_stmt_id, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_get_value", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_get_value(CUBRIDConnection con_handle, int col_no, int type, ref IntPtr value);

        [DllImport(dll_name, EntryPoint = "cci_get_data", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_get_data(int req_handle, int col_no, int type, ref IntPtr value, ref int indicator);

        [DllImport(dll_name, EntryPoint = "cci_get_data", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_get_data(int req_handle, int col_no, int type, ref T_CCI_BIT value, ref int indicator);

        [DllImport(dll_name, EntryPoint = "cci_set_make", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_set_make(ref IntPtr set, CUBRIDDataType u_type, int size, string[] value, int[] indicator);

        [DllImport(dll_name, EntryPoint = "cci_set_free", CallingConvention = CallingConvention.Cdecl)]
        public static extern void cci_set_free (IntPtr set);

        [DllImport(dll_name, EntryPoint = "cci_blob_new", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_blob_new(int con_h_id, ref IntPtr blob, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_blob_size", CallingConvention = CallingConvention.Cdecl)]
        public static extern UInt64 cci_blob_size(IntPtr blob);

        [DllImport(dll_name, EntryPoint = "cci_blob_write", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_blob_write(int con_h_id, IntPtr blob, UInt64 start_pos, int length, byte[] buf, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_blob_read", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_blob_read(int con_h_id, IntPtr blob, UInt64 start_pos, int length, byte[] buf, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_blob_free", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_blob_free(IntPtr blob);

        [DllImport(dll_name, EntryPoint = "cci_clob_new", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_clob_new(int con_h_id, ref IntPtr clob, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_clob_size", CallingConvention = CallingConvention.Cdecl)]
        public static extern UInt64 cci_clob_size(IntPtr clob);

        [DllImport(dll_name, EntryPoint = "cci_clob_write", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_clob_write(int con_h_id, IntPtr clob, UInt64 start_pos, int length, string buf, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_clob_read", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_clob_read(int con_h_id, IntPtr clob, UInt64 start_pos, int length, byte[] buf, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_clob_free", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_clob_free(IntPtr clob);

        [DllImport(dll_name, EntryPoint = "cci_set_autocommit", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_set_autocommit(int mapped_conn_id, CCI_AUTOCOMMIT_MODE mode);
        public static int cci_set_autocommit(CUBRIDConnection conn_handle, CCI_AUTOCOMMIT_MODE mode)
        {
            return cci_set_autocommit(conn_handle.Conection, mode);
        }

        [DllImport(dll_name, EntryPoint = "cci_get_autocommit", CallingConvention = CallingConvention.Cdecl)]
        public static extern CCI_AUTOCOMMIT_MODE cci_get_autocommit(int mapped_conn_id);
        public static CCI_AUTOCOMMIT_MODE cci_get_autocommit(CUBRIDConnection conn_handle)
        {
            return cci_get_autocommit(conn_handle.Conection);
        }
        [DllImport(dll_name, EntryPoint = "cci_get_db_parameter", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_get_db_parameter(int con_handle, T_CCI_DB_PARAM param_name,
                 ref int value, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_set_db_parameter", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_set_db_parameter(int con_handle, T_CCI_DB_PARAM param_name,
                         ref int value, ref T_CCI_ERROR err_buf);
        public static int cci_set_db_parameter(int con_handle, T_CCI_DB_PARAM param_name,
                 int value, ref T_CCI_ERROR err_buf)
        {

            return cci_set_db_parameter(con_handle, param_name, ref value, ref err_buf);
        }

        [DllImport(dll_name, EntryPoint = "cci_end_tran", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_end_tran(int con_handle, char type, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_bind_param", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int bind_param(int mapped_stmt_id, int index, T_CCI_A_TYPE a_type, byte[] value, CUBRIDDataType u_type, char flag);

        [DllImport(dll_name, EntryPoint = "cci_bind_param", CharSet = CharSet.Ansi, CallingConvention = CallingConvention.Cdecl)]
        public static extern int bind_param(int mapped_stmt_id, int index, T_CCI_A_TYPE a_type, IntPtr value, CUBRIDDataType u_type, char flag);

        [DllImport(dll_name, EntryPoint = "cci_register_out_param", CharSet = CharSet.Ansi, CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_register_out_param(int mapped_stmt_id, int index, T_CCI_A_TYPE a_type);

        [DllImport(dll_name, EntryPoint = "cci_execute_batch", CharSet = CharSet.Ansi, CallingConvention=CallingConvention.Cdecl)]
        public static extern int cci_execute_batch_internal(int conn_handle, int num_sql_stmt, string[] sql_stmt, ref IntPtr query_result, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_query_result_free", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_query_result_free(IntPtr query_result, int num_query);

        [DllImport(dll_name, EntryPoint = "cci_next_result", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_next_result(int req_handle, ref T_CCI_ERROR err_buf);

        [DllImport(dll_name, EntryPoint = "cci_get_query_plan", CallingConvention = CallingConvention.Cdecl)]
        public static extern int get_query_plan(int req_handle, ref IntPtr out_buf_p);
        public static string cci_get_query_plan(int req_handle, ref IntPtr out_buf_p)
        {
            int res = get_query_plan(req_handle, ref out_buf_p);
            if (res < 0)
            {
                throw new CUBRIDException(res);
            }
            return Marshal.PtrToStringAnsi(out_buf_p);
        }

        [DllImport(dll_name, EntryPoint = "cci_schema_info", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_schema_info(int conn_handle, T_CCI_SCH_TYPE type, string class_name, string attr_name, char flag, ref T_CCI_ERROR err_buf);
        public static int cci_schema_info(CUBRIDConnection con, T_CCI_SCH_TYPE type, string class_name, string attr_name, char flag, ref T_CCI_ERROR err_buf)
        {
            return cci_schema_info(con.Conection, type, class_name, attr_name, flag, ref err_buf);
        }

        [DllImport(dll_name, EntryPoint = "cci_query_info_free", CallingConvention = CallingConvention.Cdecl)]
        public static extern int cci_query_info_free(IntPtr out_buf);

        public static int cci_execute_batch(int conn_handle, string[] sql_stmt, ref T_CCI_QUERY_RESULT[] query_result, ref T_CCI_ERROR err_buf)
        {
            IntPtr qr_ptr = IntPtr.Zero;
            int n_executed = cci_execute_batch_internal(conn_handle, sql_stmt.Length, sql_stmt, ref qr_ptr, ref err_buf);
            if (n_executed < 0)
            {
                query_result = null;
            }
            else 
            {
                query_result = new T_CCI_QUERY_RESULT[n_executed];
                for (int i = 0; i < n_executed; i++) {                    
                    T_CCI_QUERY_RESULT tmp = (T_CCI_QUERY_RESULT) Marshal.PtrToStructure((IntPtr)((UInt32)qr_ptr + i * Marshal.SizeOf(typeof(T_CCI_QUERY_RESULT))), typeof(T_CCI_QUERY_RESULT));
                    query_result[i] = tmp;
                }
                cci_query_result_free(qr_ptr, n_executed);
            }
            return n_executed;
        }

        public static int cci_prepare(CUBRIDConnection conn_handle, string sql_stmt, ref T_CCI_ERROR err_buf)
        {
            int ret = cci_set_holdability(conn_handle.Conection,0);
            if (ret < 0)
            {
                return ret;
            }

            byte[] sql = conn_handle.GetEncoding().GetBytes(sql_stmt);

            string trimed_stmt = new string(sql_stmt.ToCharArray()
                                .Where(c => !Char.IsWhiteSpace(c))
                                .ToArray());

            if ((trimed_stmt.Substring(0, 6).ToLower() == "?=call") ||
                (trimed_stmt.Substring(0, 4).ToLower() == "call"))
            {
                return cci_prepare(
                    conn_handle.Conection, sql,
                    (char)T_CCI_PREPARE_FLAG.CCI_PREPARE_CALL,
                    ref err_buf);
            }
            else
            {
                return cci_prepare(
                conn_handle.Conection, sql,
                (char)(T_CCI_PREPARE_FLAG.CCI_PREPARE_INCLUDE_OID |
                T_CCI_PREPARE_FLAG.CCI_PREPARE_UPDATABLE),
                ref err_buf);
            }
        }

        public static int cci_get_value(CUBRIDConnection con_handle, int col_no, CUBRIDDataType type, ref object val)
        {
            int req_handle = con_handle.Conection;
            IntPtr value = IntPtr.Zero;
            int indicator = 0, res = 0;
            switch (type)
            {
                case CUBRIDDataType.CCI_U_TYPE_BLOB:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_BLOB, ref value, ref indicator);
                    CUBRIDBlob blob = new CUBRIDBlob(value, con_handle);
                    val = blob;
                    break;
                case CUBRIDDataType.CCI_U_TYPE_CLOB:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_CLOB, ref value, ref indicator);
                    CUBRIDClob clob = new CUBRIDClob(value, con_handle);
                    val = clob;
                    break;
                case CUBRIDDataType.CCI_U_TYPE_INT:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    if (Marshal.PtrToStringAnsi(value) == null)
                    {
                        val = null;
                    }
                    else
                    {
                        val = Convert.ToInt32(Marshal.PtrToStringAnsi(value));
                    }
                    break;
                case CUBRIDDataType.CCI_U_TYPE_BIGINT:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    val = Convert.ToInt64(Marshal.PtrToStringAnsi(value));
                    break;
                case CUBRIDDataType.CCI_U_TYPE_OBJECT:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    string oid = Marshal.PtrToStringAnsi(value);
                    val = new CUBRIDOid(oid);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_BIT:
                    T_CCI_BIT bit = new T_CCI_BIT();
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_BIT, ref bit, ref indicator);
                    byte[] data = new byte[bit.size];
                    for (int i = 0; i < bit.size; i++) data[i] = Marshal.ReadByte(bit.buf, i);
                    val = new byte[bit.size];
                    Array.Copy(data, (byte[])val, bit.size);
                    break;
                default:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    if (value != IntPtr.Zero)
                    {
                        if (con_handle.GetEncoding().Equals(Encoding.UTF8))
                        {
                            Byte[] v = Encoding.Unicode.GetBytes(Marshal.PtrToStringUni(value));
                            int count = 0;
                            while (count < v.Length && v[count] != 0)
                            {
                                count++;
                            }

                            if ((CUBRIDDataType)type == CUBRIDDataType.CCI_U_TYPE_VARBIT)
                            {
                                val = Enumerable.Range(0, count)
                                                .Where(x => x % 2 == 0)
                                                .Select(x => Convert.ToByte(Marshal.PtrToStringAnsi(value).Substring(x, 2), 16))
                                                .ToArray();
                            }
                            else val = Encoding.Unicode.GetString(Encoding.Convert(Encoding.UTF8, Encoding.Unicode, v, 0, count));
                        }
                        else
                        {
                            val = Marshal.PtrToStringAnsi(value);
                        }
                    }
                    else
                    {
                        val = null; //String.Empty;
                    }
                    break;
            }
            return res;
        }

        public static int cci_get_data(ResultTuple rt, int req_handle, int col_no, int type, CUBRIDConnection conn)
        {
            IntPtr value = IntPtr.Zero;
            int indicator = 0, res=0;
            switch ((CUBRIDDataType)type)
            {
                case CUBRIDDataType.CCI_U_TYPE_BLOB:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_BLOB, ref value, ref indicator);
                    CUBRIDBlob blob = new CUBRIDBlob(value,conn);
                    rt[col_no - 1] = blob;
                    break;
                case CUBRIDDataType.CCI_U_TYPE_CLOB:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_CLOB, ref value, ref indicator);
                    CUBRIDClob clob = new CUBRIDClob(value,conn);
                    rt[col_no - 1] = clob;
                    break;
                case CUBRIDDataType.CCI_U_TYPE_INT:             
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    if (Marshal.PtrToStringAnsi(value) == null)
                    {
                        rt[col_no - 1] = null;
                    }
                    else
                    {
                        rt[col_no - 1] = Convert.ToInt32(Marshal.PtrToStringAnsi(value));
                    }
                    if (is_collection_type((CUBRIDDataType)type))
                    {
                        rt.toArray(col_no - 1);
                    }
                    break;
                case CUBRIDDataType.CCI_U_TYPE_BIGINT:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    rt[col_no - 1] = Convert.ToInt64(Marshal.PtrToStringAnsi(value));
                    if (is_collection_type((CUBRIDDataType)type))
                    {
                        rt.toArray(col_no - 1);
                    }
                    break;
                case CUBRIDDataType.CCI_U_TYPE_OBJECT:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    string oid = Marshal.PtrToStringAnsi(value);
                    rt[col_no - 1] = new CUBRIDOid(oid);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_BIT:
                    T_CCI_BIT bit = new T_CCI_BIT();
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_BIT, ref bit, ref indicator);
                    byte[] data = new byte[bit.size];
                    for (int i = 0; i < bit.size; i++) data[i] = Marshal.ReadByte(bit.buf, i);
                    rt[col_no - 1] = new byte[bit.size];
                    Array.Copy(data,(byte[])rt[col_no - 1], bit.size);
                    break;
                default:
                    res = cci_get_data(req_handle, col_no, (int)T_CCI_A_TYPE.CCI_A_TYPE_STR, ref value, ref indicator);
                    if (value != IntPtr.Zero)
                    {
                        if (conn.GetEncoding().Equals(Encoding.UTF8))
                        {
                            Byte[] v = Encoding.Unicode.GetBytes(Marshal.PtrToStringUni(value));
                            int count = 0;
                            while (count < v.Length && v[count] != 0)
                            {
                                count++;
                            }

                            if ((CUBRIDDataType)type == CUBRIDDataType.CCI_U_TYPE_VARBIT)
                            {
                                rt[col_no - 1] = Enumerable.Range(0, count)
                                                .Where(x => x % 2 == 0)
                                                .Select(x => Convert.ToByte(Marshal.PtrToStringAnsi(value).Substring(x, 2), 16))
                                                .ToArray();
                            }
                            else rt[col_no - 1] = Encoding.Unicode.GetString(Encoding.Convert(Encoding.UTF8, Encoding.Unicode, v, 0, count));
                        }
                        else
                        {
                            rt[col_no - 1] = Marshal.PtrToStringAnsi(value);
                        }
                    }
                    else {
                        rt[col_no - 1] = null; // String.Empty;
                    }
                    if (is_collection_type((CUBRIDDataType)type))
                    {
                        rt.toArray(col_no - 1);
                    }
                  break;
            }
            return res;
        }

        public static int cci_bind_param(CUBRIDConnection conn, int handle, int index, T_CCI_A_TYPE a_type, CUBRIDParameter param, CUBRIDDataType u_type, char flag)
        {
            int ret = 0;
            IntPtr p = IntPtr.Zero;
            switch (param.CUBRIDDataType)
            {
                case CUBRIDDataType.CCI_U_TYPE_DATE:
                case CUBRIDDataType.CCI_U_TYPE_TIME:
                case CUBRIDDataType.CCI_U_TYPE_DATETIME:
                case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
                    string date  = param.Value.ToString();
                    if (param.Value.GetType()== typeof(DateTime))
                    {
                        DateTime d = Convert.ToDateTime(param.Value);
                        date = string.Format("{0:u}", d);
                        date = date.Remove(date.Length - 1);
                    }
                    p = Marshal.StringToCoTaskMemAnsi(date);
                    ret = bind_param(handle, index, a_type, p, u_type, flag);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_SET:
                case CUBRIDDataType.CCI_U_TYPE_MULTISET:
                case CUBRIDDataType.CCI_U_TYPE_SEQUENCE:
                    IntPtr set = IntPtr.Zero;
                    string[] value = data_format((object[])param.Value, param.InnerCUBRIDDataType);
                    int[] indicator = new int[value.Length];
                    for (int i = 0; i < value.Length; i++)
                    {
                        if (value[i] != null)
                        {
                            indicator[i] = 0;
                        }
                        else
                        {
                            indicator[i] = 1;
                        }
                    }
                    //CUBRIDDataType.CCI_U_TYPE_STRING or param.InnerCUBRIDDataType
                    ret = cci_set_make(ref set, CUBRIDDataType.CCI_U_TYPE_STRING, value.Length, value, indicator);
                    if (ret < 0)
                    {
                        return ret;
                    }

                    ret = bind_param(handle, index, T_CCI_A_TYPE.CCI_A_TYPE_SET, set, param.CUBRIDDataType, flag);
                    cci_set_free(set);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_BLOB:
                    CUBRIDBlob blob = (CUBRIDBlob)param.Value;
                    bind_param(handle, index, T_CCI_A_TYPE.CCI_A_TYPE_BLOB, blob.GetPackedLobHandle(), param.CUBRIDDataType, flag);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_CLOB:
                    CUBRIDClob clob = (CUBRIDClob)param.Value;
                    bind_param(handle, index, T_CCI_A_TYPE.CCI_A_TYPE_CLOB, clob.GetPackedLobHandle(), param.CUBRIDDataType, flag);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_BIT:
                case CUBRIDDataType.CCI_U_TYPE_VARBIT:
                    T_CCI_BIT bit = new T_CCI_BIT();
                    bit.size = ((byte[])param.Value).Length;
                    bit.buf = Marshal.AllocHGlobal(bit.size);
                    Marshal.Copy((byte[])param.Value, 0, bit.buf, bit.size);
                    p = Marshal.AllocHGlobal(Marshal.SizeOf(bit));
                    Marshal.StructureToPtr(bit, p, false);
                    ret = bind_param(handle, index, T_CCI_A_TYPE.CCI_A_TYPE_BIT, p, param.CUBRIDDataType, flag);
                    Marshal.FreeHGlobal(p);
                    break;
                case CUBRIDDataType.CCI_U_TYPE_NULL:
                    ret = bind_param(handle, index, a_type, IntPtr.Zero, u_type, flag);
                    break;
                default:
                    byte[] bind_value; // = param.Value.ToString();
                    if (conn.GetEncoding() != null)
                    {
                        bind_value = conn.GetEncoding().GetBytes(param.Value.ToString());
                    }
                    else
                    {
                        bind_value = param.GetParameterEncoding().GetBytes(param.Value.ToString());
                    }
                    ret = bind_param(handle, index, a_type, bind_value, u_type, flag);
                    break;
            }
            return ret;                  
        }

        private static string[] data_format(object[] value,CUBRIDDataType type)
        {
            switch (type)
            {
                case CUBRIDDataType.CCI_U_TYPE_TIME:
                case CUBRIDDataType.CCI_U_TYPE_TIMESTAMP:
                case CUBRIDDataType.CCI_U_TYPE_DATE:
                case CUBRIDDataType.CCI_U_TYPE_DATETIME:
                    string[] date = (string[])value;
                    for (int i = 0; i < date.Length; i++)
                    {
                        if (value[i] != null)
                        {
                            DateTime d = Convert.ToDateTime(date[i]);
                            date[i] = string.Format("{0:u}", d);
                            date[i] = date[i].Remove(date[i].Length - 1);
                        }
                    }
                    break;
                case CUBRIDDataType.CCI_U_TYPE_OBJECT:
                    string[] ob = new string[value.Length];
                    for (int i = 0; i < value.Length; i++)
                    {
                        if (value[i] != null)
                        {
                            ob[i] = value[i].ToString();
                        }
                    }
                    return ob;
            } 
            return (string[])value;
        }

        private static bool int_to_bool(object obj)
        {
            if (Convert.ToInt32(obj) == 1)
                return true;
            else
                return false;
        }

        private static bool is_collection_type(CUBRIDDataType type)
        {
            if (type == CUBRIDDataType.CCI_U_TYPE_SET ||
                type == CUBRIDDataType.CCI_U_TYPE_MULTISET ||
                type == CUBRIDDataType.CCI_U_TYPE_SEQUENCE)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}