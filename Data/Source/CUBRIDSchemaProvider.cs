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
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace CUBRID.Data.CUBRIDClient
{
	/// <summary>
	/// CUBRID helper class which provides support for database schema
	/// </summary>
	public class CUBRIDSchemaProvider
	{
		/// <summary>
		/// The CUBRID connection 
		/// </summary>
		protected CUBRIDConnection conn;

		internal static string MetaCollectionName = "MetaDataCollections";

		/// <summary>
		/// Initializes a new instance of the <see cref="CUBRIDSchemaProvider"/> class.
		/// </summary>
		/// <param name="connection">The connection.</param>
		public CUBRIDSchemaProvider(CUBRIDConnection connection)
		{
			this.conn = connection;
		}

		/// <summary>
		/// Gets the list of databases.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetDatabases(string[] filters)
		{
			if (filters != null)
			{
				if (filters.Length > 1)
					throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));
			}

			string catalog = "%";
			if (filters != null)
			{
				catalog = filters[0];
			}

			string sql = "SELECT LIST_DBS()";

            using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
            {
                DataTable dt = new DataTable();
                da.Fill(dt);

                using (DataTable table = new DataTable("Databases"))
                {
                    table.Columns.Add("CATALOG_NAME", typeof(string));
                    table.Columns.Add("SCHEMA_NAME", typeof(string));

                    foreach (DataRow row in dt.Rows) //just one row is returned always
                    {
                        //CUBRID returns the list of databases as one single row/one column
                        String[] dbs = row[0].ToString().Split(' ');

                        foreach (String dbname in dbs)
                        {
                            string sqlDb = String.Format("SELECT COUNT('{0}') FROM db_root WHERE '{1}' LIKE '{2}'", dbname, dbname, catalog);
                            using (CUBRIDCommand cmd = new CUBRIDCommand(sqlDb, conn))
                            {
                                int count = (int)cmd.ExecuteScalar();

                                if (count > 0)
                                {
                                    DataRow newRow = table.NewRow();

                                    newRow[0] = dbname;
                                    newRow[1] = dbname;

                                    table.Rows.Add(newRow);
                                }
                            }
                        }
                    }

                    return table;
                }
            }
		}

		/// <summary>
		/// Gets the tables.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetTables(string[] filters)
		{
			if (filters == null)
				throw new ArgumentNullException(Utils.GetStr(MsgId.NoFiltersSpecified));

			if (filters.Length != 1)
				throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));

            using (DataTable dt = new DataTable("Tables"))
            {
                dt.Columns.Add("TABLE_CATALOG", typeof(string));
                dt.Columns.Add("TABLE_SCHEMA", typeof(string));
                dt.Columns.Add("TABLE_NAME", typeof(string));
                this.FindTables(dt, filters);

                return dt;
            }
		}

		/// <summary>
		/// Gets the views.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetViews(string[] filters)
		{
			if (filters != null)
			{
				if (filters.Length > 1)
					throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));
			}

            using (DataTable dt = new DataTable("Views"))
            {
                dt.Columns.Add("VIEW_CATALOG", typeof(string));
                dt.Columns.Add("VIEW_SCHEMA", typeof(string));
                dt.Columns.Add("VIEW_NAME", typeof(string));

                this.FindViews(dt, filters);

                return dt;
            }
		}

		protected void QuoteDefaultValues(DataTable dt)
		{
			if (dt == null)
				return;

			if (!dt.Columns.Contains("COLUMN_DEFAULT"))
				return;

			foreach (DataRow row in dt.Rows)
			{
				object defaultValue = row["COLUMN_DEFAULT"];
				if (CUBRIDMetaData.IsTextType(row["DATA_TYPE"].ToString()))
				{
					row["COLUMN_DEFAULT"] = String.Format("'{0}'", defaultValue);
				}
			}
		}

		/// <summary>
		/// Gets the table columns.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetColumns(string[] filters)
		{
			if (filters == null)
				throw new ArgumentNullException(Utils.GetStr(MsgId.NoFiltersSpecified));

			if (filters.Length > 2)
				throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));

            using (DataTable dt = new DataTable("Columns"))
            {
                dt.Columns.Add("TABLE_CATALOG", typeof(string));
                dt.Columns.Add("TABLE_SCHEMA", typeof(string));
                dt.Columns.Add("TABLE_NAME", typeof(string));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("ORDINAL_POSITION", typeof(uint));
                dt.Columns.Add("COLUMN_DEFAULT", typeof(string));
                dt.Columns.Add("IS_NULLABLE", typeof(bool));
                dt.Columns.Add("DATA_TYPE", typeof(string));
                dt.Columns.Add("NUMERIC_PRECISION", typeof(uint));
                dt.Columns.Add("NUMERIC_SCALE", typeof(uint));
                dt.Columns.Add("CHARACTER_SET", typeof(byte));

                string tableName = filters[0];

                string columnName = "%";
                if (filters.Length == 2)
                {
                    columnName = filters[1];
                }

                DataTable tables = GetTables(new String[] { tableName });
                foreach (DataRow row in tables.Rows)
                {
                    LoadTableColumns(dt, row["TABLE_SCHEMA"].ToString(), row["TABLE_NAME"].ToString(), columnName);
                }
                QuoteDefaultValues(dt);

                return dt;
            }
		}

		private void LoadTableColumns(DataTable dt, string schema, string tableName, string columnRestriction)
		{
			string sql = String.Format("select attr_name, default_value, is_nullable, `data_type`, prec, scale, code_set from db_attribute where class_name like '{0}' and attr_name like '{1}' order by def_order asc", tableName, columnRestriction);
            using (CUBRIDCommand cmd = new CUBRIDCommand(sql, conn))
            {
                int pos = 1;
                using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string colName = reader.GetString(0);
                        DataRow row = dt.NewRow();

                        row["TABLE_CATALOG"] = conn.Database;
                        row["TABLE_SCHEMA"] = conn.Database;
                        row["TABLE_NAME"] = tableName;
                        row["COLUMN_NAME"] = colName;
                        row["ORDINAL_POSITION"] = pos++;

                        row["COLUMN_DEFAULT"] = reader.GetString(1);
                        row["IS_NULLABLE"] = reader.GetString(2).Equals("YES") ? true : false;
                        row["DATA_TYPE"] = reader.GetString(3);
                        row["NUMERIC_PRECISION"] = reader.GetInt(4);
                        row["NUMERIC_SCALE"] = reader.GetInt(5);
                        row["CHARACTER_SET"] = reader.GetInt(6);

                        dt.Rows.Add(row);
                    }
                }
            }
		}

		/// <summary>
		/// Gets the indexes.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetIndexes(string[] filters)
		{
			if (filters == null)
				throw new ArgumentNullException(Utils.GetStr(MsgId.NoFiltersSpecified));

			if (filters.Length > 3)
				throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));

            using (DataTable dt = new DataTable("Indexes"))
            {
                dt.Columns.Add("INDEX_CATALOG", typeof(string));
                dt.Columns.Add("INDEX_SCHEMA", typeof(string));
                dt.Columns.Add("INDEX_NAME", typeof(string));
                dt.Columns.Add("TABLE_NAME", typeof(string));
                dt.Columns.Add("UNIQUE", typeof(bool));
                dt.Columns.Add("REVERSE", typeof(bool));
                dt.Columns.Add("PRIMARY", typeof(bool));
                dt.Columns.Add("FOREIGN_KEY", typeof(bool));
                dt.Columns.Add("DIRECTION", typeof(string));

                string tableName = filters[0];
                string columnName = "%";
                string indexName = "%";

                if (filters.Length == 2)
                {
                    columnName = filters[1];
                }

                if (filters.Length == 3)
                {
                    columnName = filters[1];
                    indexName = filters[2];
                }

                DataTable tables = GetTables(new String[] { tableName });

                string raw_sql = "select b.index_name, b.class_name, a.is_unique, a.is_reverse, a.is_primary_key, a.is_foreign_key, b.asc_desc";
                raw_sql += " from db_index a,db_index_key b";
                raw_sql += " where a.index_name=b.index_name";
                raw_sql += " and a.class_name like '{0}'";
                raw_sql += " and b.key_attr_name like '{1}'";
                raw_sql += " and a.index_name like '{2}'";
                raw_sql += " order by b.key_order";
                foreach (DataRow table in tables.Rows)
                {
                    string sql = String.Format(raw_sql, tableName, columnName, indexName);
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                    {
                        DataTable indexes = new DataTable();
                        da.Fill(indexes);
                        foreach (DataRow index in indexes.Rows)
                        {
                            DataRow row = dt.NewRow();

                            row["INDEX_CATALOG"] = conn.Database;
                            row["INDEX_SCHEMA"] = conn.Database;
                            row["INDEX_NAME"] = index[0];
                            row["TABLE_NAME"] = index[1];
                            row["UNIQUE"] = index[2].ToString().Equals("YES") ? true : false;
                            row["REVERSE"] = index[3].ToString().Equals("YES") ? true : false;
                            row["PRIMARY"] = index[4].ToString().Equals("YES") ? true : false;
                            row["FOREIGN_KEY"] = index[5].ToString().Equals("YES") ? true : false;
                            row["DIRECTION"] = index[6].ToString();

                            dt.Rows.Add(row);
                        }
                    }
                }

                return dt;
            }
		}

		/// <summary>
		/// Gets the index columns.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetIndexColumns(string[] filters)
		{
			if (filters != null)
			{
				if (filters.Length > 2)
					throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));
			}

            using (DataTable dt = new DataTable("IndexColumns"))
            {
                dt.Columns.Add("INDEX_CATALOG", typeof(string));
                dt.Columns.Add("INDEX_SCHEMA", typeof(string));
                dt.Columns.Add("INDEX_NAME", typeof(string));
                dt.Columns.Add("TABLE_NAME", typeof(string));
                dt.Columns.Add("COLUMN_NAME", typeof(string));
                dt.Columns.Add("ORDINAL_POSITION", typeof(int));
                dt.Columns.Add("DIRECTION", typeof(string));

                string tableName = "%";
                string indexName = "%";

                if (filters.Length == 1)
                {
                    tableName = filters[0];
                }

                if (filters.Length == 2)
                {
                    tableName = filters[0];
                    indexName = filters[1];
                }

                DataTable tables = GetTables(new String[] { tableName });

                string raw_sql = "select b.index_name, b.class_name, b.key_attr_name, b.key_order, b.asc_desc";
                raw_sql += " from db_index a, db_index_key b";
                raw_sql += " where a.class_name=b.class_name and a.index_name=b.index_name";
                raw_sql += " and a.class_name like '{0}'";
                raw_sql += " and a.index_name like '{1}'";
                raw_sql += " order by b.key_order asc";
                foreach (DataRow table in tables.Rows)
                {
                    string sql = String.Format(raw_sql, tableName, indexName);
                    using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sql, conn))
                    {
                        DataTable indexes = new DataTable();
                        da.Fill(indexes);
                        foreach (DataRow index in indexes.Rows)
                        {
                            DataRow row = dt.NewRow();

                            row["INDEX_CATALOG"] = conn.Database;
                            row["INDEX_SCHEMA"] = conn.Database;
                            row["INDEX_NAME"] = index[0].ToString();
                            row["TABLE_NAME"] = index[1].ToString();
                            row["COLUMN_NAME"] = index[2].ToString();
                            row["ORDINAL_POSITION"] = (int)index[3];
                            row["DIRECTION"] = index[4].ToString();

                            dt.Rows.Add(row);
                        }
                    }
                }

                return dt;
            }
		}

		/// <summary>
		/// Gets the exported keys.
		/// </summary>
		/// <param name="filters">The filters: table name</param>
		/// <returns></returns>
		public DataTable GetExportedKeys(string[] filters)
		{
			if (filters == null)
				throw new ArgumentNullException(Utils.GetStr(MsgId.NoFiltersSpecified));

			if (filters.Length > 1)
				throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));

			//TODO Implement as in CUBRIDDatabaseMetaData.getExportedKeys(...)

			return null;
		}

		/// <summary>
		/// Gets the cross reference keys.
		/// </summary>
		/// <param name="filters">The filters: primary table name, foreign table name</param>
		/// <returns></returns>
		public DataTable GetCrossReferenceKeys(string[] filters)
		{
			if (filters == null)
				throw new ArgumentNullException(Utils.GetStr(MsgId.NoFiltersSpecified));

			if (filters.Length > 2)
				throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));

			//TODO Implement as in CUBRIDDatabaseMetaData.getCrossReference(...)

			return null;
		}

		/// <summary>
		/// Gets the foreign keys.
		/// </summary>
		/// <param name="filters">The filters: table name, column name</param>
		/// <returns></returns>
		public DataTable GetForeignKeys(string[] filters)
		{
			if (filters == null)
				throw new ArgumentNullException(Utils.GetStr(MsgId.NoFiltersSpecified));

			if (filters.Length > 2)
				throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));

            using (DataTable dt = new DataTable("ForeignKeys"))
            {
                dt.Columns.Add("PKTABLE_NAME", typeof(string));
                dt.Columns.Add("PKCOLUMN_NAME", typeof(string));
                dt.Columns.Add("FKTABLE_NAME", typeof(string));
                dt.Columns.Add("FKCOLUMN_NAME", typeof(string));
                dt.Columns.Add("KEY_SEQ", typeof(short));
                dt.Columns.Add("UPDATE_ACTION", typeof(short));
                dt.Columns.Add("DELETE_ACTION", typeof(short));
                dt.Columns.Add("FK_NAME", typeof(string));
                dt.Columns.Add("PK_NAME", typeof(string));

                string tableName = filters[0];
                string keyName = "";
                if (filters.Length > 1)
                {
                    keyName = filters[1];
                }

                DataTable tables = GetTables(new String[] { tableName });
                foreach (DataRow table in tables.Rows)
                {
                    GetForeignKeys(dt, table[2].ToString(), keyName);
                }

                return dt;
            }
		}

		#region Foreign Key routines

		/// <summary>
		/// GetForeignKeys retrieves the foreign keys on the given tableName.
		/// </summary>
		private void GetForeignKeys(DataTable dt, String table, string keyName)
		{
			/*
			outBuffer.newRequest(out, UFunctionCode.GET_SCHEMA_INFO);
			outBuffer.addInt(type);
			outBuffer.addStringWithNull(arg1);
			outBuffer.addStringWithNull(arg2);
			outBuffer.addByte(flag);				
			*/
			conn.stream.ClearBuffer();
			conn.stream.WriteCommand(CASFunctionCode.CAS_FC_SCHEMA_INFO);
			conn.stream.WriteIntArg((int)CUBRIDSchemaType.CCI_SCH_IMPORTED_KEYS);
            conn.stream.WriteStringArg(table, conn.GetEncoding());
			if (keyName == "")
			{
				conn.stream.WriteNullArg();
			}
			else
			{
                conn.stream.WriteStringArg(keyName, conn.GetEncoding());
			}
			conn.stream.WriteByteArg((byte)3); //flag

			conn.stream.Send();

			conn.stream.ClearBuffer();
			int res = conn.stream.Receive(); //is serverHandler
			if (res < 0)
			{
				return;
			}

			/*
			serverHandler = inBuffer.getResCode();
			totalTupleNumber = inBuffer.readInt();
			columnCount = inBuffer.readInt();
			readColumnInfo(inBuffer);
			*/
			int rowsCount = conn.Stream.ReadInt();
			int columnsCount = conn.Stream.ReadInt();
			ColumnMetaData[] columnInfos = conn.Stream.ReadColumnInfo(columnsCount, false);

			int handle = res;

			//us.fetch();
			conn.stream.RequestFetch(handle);

			//Read tuples count
			int tuplesCount = conn.stream.ReadInt(); //it is equal to te number of foreign keys defined in the table

			for (int i = 0; i < tuplesCount; i++)
			{
				ResultTuple rt = new ResultTuple(columnsCount);

				conn.stream.ReadSchemaProviderResultTuple(rt, columnInfos, conn);

				///* 3. PKTABLE_NAME (String) */
				//value[2] = us.getString(0);
				///* 4. PKCOLUMN_NAME (String) */
				//value[3] = us.getString(1);
				///* 7. FKTABLE_NAME (String) */
				//value[6] = us.getString(2);
				///* 8. FKCOLUMN_NAME (String) */
				//value[7] = us.getString(3);
				///* 9. KEY_SEQ (short) */
				//value[8] = us.getShort(4);
				///* 10. UPDATE_RULE (short) */
				//value[9] = convertForeignKeyAction(us.getShort(5));
				///* 11. DELETE_RULE (short) */
				//value[10] = convertForeignKeyAction(us.getShort(6));
				///* 12. FK_NAME (String) */
				//value[11] = us.getString(7);
				///* 13. PK_NAME (String) */
				//value[12] = us.getString(8);

				DataRow row = dt.NewRow();

				row["PKTABLE_NAME"] = (string)rt[0];
				row["PKCOLUMN_NAME"] = (string)rt[1];
				row["FKTABLE_NAME"] = (string)rt[2];
				row["FKCOLUMN_NAME"] = (string)rt[3];
				row["KEY_SEQ"] = (short)rt[4];
				row["UPDATE_ACTION"] = (short)rt[5];
				row["DELETE_ACTION"] = (short)rt[6];
				row["FK_NAME"] = (string)rt[7];
				row["PK_NAME"] = (string)rt[8];

				dt.Rows.Add(row);
			}
		}

		#endregion

		/// <summary>
		/// Gets the users.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetUsers(string[] filters)
		{
			string pattern = "%";
			if (filters != null)
			{
				pattern = filters[0];
			}

			String sb = String.Format("SELECT `name` from db_user where `name` LIKE '{0}'", pattern);
            using (CUBRIDDataAdapter da = new CUBRIDDataAdapter(sb.ToString(), conn))
            {
                DataTable dt = new DataTable();
                da.Fill(dt);
                dt.TableName = "USERS";
                dt.Columns[0].ColumnName = "USERNAME";

                return dt;
            }
		}

		/// <summary>
		/// Gets the procedures.
		/// </summary>
		/// <param name="filters">The filters.</param>
		/// <returns></returns>
		public DataTable GetProcedures(string[] filters)
		{
			if (filters != null)
			{
				if (filters.Length > 1)
					throw new ArgumentException(Utils.GetStr(MsgId.IncorrectNumberOfFilters));
			}

            using (DataTable dt = new DataTable("Procedures"))
            {
                dt.Columns.Add(new DataColumn("PROCEDURE_NAME", typeof(string)));
                dt.Columns.Add(new DataColumn("PROCEDURE_TYPE", typeof(string)));
                dt.Columns.Add(new DataColumn("RETURN_TYPE", typeof(string)));
                dt.Columns.Add(new DataColumn("ARGUMENTS_COUNT", typeof(int)));
                dt.Columns.Add(new DataColumn("LANGUAGE", typeof(string)));
                dt.Columns.Add(new DataColumn("TARGET", typeof(string)));
                dt.Columns.Add(new DataColumn("OWNER", typeof(string)));

                string procedureName = "%";
                if (filters != null)
                {
                    procedureName = filters[0];
                }

                string sql = String.Format("select * from db_stored_procedure where sp_name like '{0}'", procedureName);

                using (CUBRIDCommand cmd = new CUBRIDCommand(sql.ToString(), conn))
                {
                    using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            DataRow row = dt.NewRow();

                            row["PROCEDURE_NAME"] = reader.GetString(0);
                            row["PROCEDURE_TYPE"] = reader.GetString(1);
                            row["RETURN_TYPE"] = reader.GetString(2);
                            row["ARGUMENTS_COUNT"] = reader.GetInt(3);
                            row["LANGUAGE"] = reader.GetString(4);
                            row["TARGET"] = reader.GetString(5);
                            row["OWNER"] = reader.GetString(6);

                            dt.Rows.Add(row);
                        }
                    }
                }

                return dt;
            }
		}

		/// <summary>
		/// Gets the data types.
		/// </summary>
		/// <returns></returns>
		public static DataTable GetDataTypes()
		{
            using (DataTable dt = new DataTable("DataTypes"))
            {
                dt.Columns.Add(new DataColumn("TypeName", typeof(string)));
                dt.Columns.Add(new DataColumn("ProviderDataType", typeof(int)));
                dt.Columns.Add(new DataColumn("DbType", typeof(Type)));
                dt.Columns.Add(new DataColumn("Size", typeof(long)));
                dt.Columns.Add(new DataColumn("IsLong", typeof(bool)));
                dt.Columns.Add(new DataColumn("IsFixedLength", typeof(bool)));
                dt.Columns.Add(new DataColumn("IsFixedPrecisionScale", typeof(bool)));
                dt.Columns.Add(new DataColumn("IsNullable", typeof(bool)));
                dt.Columns.Add(new DataColumn("IsAutoIncrementable", typeof(bool)));

                SetDataTypeInfo(dt, "BIGINT", CUBRIDDataType.CCI_U_TYPE_BIGINT, typeof(Int32), ToBool(IsAutoIncrementable.Yes), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.Yes), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "BIT", CUBRIDDataType.CCI_U_TYPE_BIT, typeof(UInt64), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "BLOB", CUBRIDDataType.CCI_U_TYPE_BLOB, typeof(BitArray), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "CHAR", CUBRIDDataType.CCI_U_TYPE_CHAR, typeof(char), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "CLOB", CUBRIDDataType.CCI_U_TYPE_CLOB, typeof(BitArray), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "DATE", CUBRIDDataType.CCI_U_TYPE_DATE, typeof(DateTime), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "DATETIME", CUBRIDDataType.CCI_U_TYPE_DATETIME, typeof(DateTime), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "DOUBLE", CUBRIDDataType.CCI_U_TYPE_DOUBLE, typeof(Double), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.No), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "FLOAT", CUBRIDDataType.CCI_U_TYPE_FLOAT, typeof(float), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.No), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "INT", CUBRIDDataType.CCI_U_TYPE_INT, typeof(int), ToBool(IsAutoIncrementable.Yes), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "MONETARY", CUBRIDDataType.CCI_U_TYPE_MONETARY, typeof(Decimal), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.No), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "NCHAR", CUBRIDDataType.CCI_U_TYPE_NCHAR, typeof(Char), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "NUMERIC", CUBRIDDataType.CCI_U_TYPE_NUMERIC, typeof(Decimal), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.No), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "OBJECT", CUBRIDDataType.CCI_U_TYPE_OBJECT, typeof(Object), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "SET", CUBRIDDataType.CCI_U_TYPE_SET, typeof(int), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.No), ToBool(IsFixedPrecisionScale.No), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "SHORT", CUBRIDDataType.CCI_U_TYPE_SHORT, typeof(short), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "STRING", CUBRIDDataType.CCI_U_TYPE_STRING, typeof(String), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "TIME", CUBRIDDataType.CCI_U_TYPE_TIME, typeof(DateTime), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));
                SetDataTypeInfo(dt, "TIMESTAMP", CUBRIDDataType.CCI_U_TYPE_TIMESTAMP, typeof(UInt64), ToBool(IsAutoIncrementable.No), ToBool(IsFixedLength.Yes), ToBool(IsFixedPrecisionScale.Yes), ToBool(IsLong.No), ToBool(IsNullable.Yes));

                return dt;
            }
		}

		private static bool ToBool(object val)
		{
			bool ret = (int)val == 0 ? false : true;
			return ret;
		}

		private static void SetDataTypeInfo(DataTable dsTable, string typeName, CUBRIDDataType cubridDataType, Type dataType,
			bool isAutoincrementable, bool isFixedLength, bool isFixedPrecisionScale, bool isLong, bool isNullable)
		{
			DataRow row = dsTable.NewRow();

			row["TypeName"] = typeName;
			row["ProviderDataType"] = cubridDataType;
			row["DbType"] = dataType;
			row["IsAutoIncrementable"] = isAutoincrementable;
			row["IsFixedLength"] = isFixedLength;
			row["IsFixedPrecisionScale"] = isFixedPrecisionScale;
			row["IsLong"] = isLong;
			row["IsNullable"] = isNullable;

			dsTable.Rows.Add(row);
		}

		/// <summary>
		/// Gets the reserved words.
		/// </summary>
		/// <returns></returns>
		public static DataTable GetReservedWords()
		{
            using (DataTable dt = new DataTable("ReservedWords"))
            {
                dt.Columns.Add(new DataColumn(DbMetaDataColumnNames.ReservedWord, typeof(string)));

                //string line = "ADD, ADD_MONTHS, AFTER, ALIAS, ASYNC, ATTACH, ATTRIBUTE, BEFORE, "
                //        + "BOOLEAN, BREADTH, CALL, CHANGE, CLASS, CLASSES, CLUSTER, COMPLETION, "
                //        + "CYCLE, DATA, DATA_TYPE___, DEPTH, DICTIONARY, DIFFERENCE, EACH, ELSEIF, "
                //        + "EQUALS, EVALUATE, EXCLUDE, FILE, FUNCTION, GENERAL, IF, IGNORE, INCREMENT, "
                //        + "INDEX, INHERIT, INOUT, INTERSECTION, LAST_DAY, LDB, LEAVE, LESS, LIMIT, "
                //        + "LIST, LOOP, LPAD, LTRIM, MAXVALUE, METHOD, MINVALUE, MODIFY, MONETARY, "
                //        + "MONTHS_BETWEEN, MULTISET, MULTISET_OF, NA, NOCYCLE, NOMAXVALUE, NOMINVALUE, "
                //        + "NONE, OBJECT, OFF, OID, OLD, OPERATION, OPERATORS, OPTIMIZATION, OTHERS, "
                //        + "OUT, PARAMETERS, PENDANT, PREORDER, PRIVATE, PROXY, PROTECTED, QUERY, "
                //        + "RECURSIVE, REF, REFERENCING, REGISTER, RENAME, REPLACE, RESIGNAL, RETURN, "
                //        + "RETURNS, ROLE, ROUTINE, ROW, RPAD, RTRIM, SAVEPOINT, SCOPE___, SEARCH, "
                //        + "SENSITIVE, SEQUENCE, SEQUENCE_OF, SERIAL, SERIALIZABLE, SETEQ, SETNEQ, "
                //        + "SET_OF, SHARED, SHORT, SIGNAL, SIMILAR, SQLEXCEPTION, SQLWARNING, START, "
                //        + "TATISTICS, STDDEV, STRING, STRUCTURE, SUBCLASS, SUBSET, SUBSETEQ, "
                //        + "SUPERCLASS, SUPERSET, SUPERSETEQ, SYS_DATE, SYS_TIME, SYS_TIMESTAMP, "
                //        + "SYS_USER, TEST, THERE, TO_CHAR, TO_DATE, TO_NUMBER, TO_TIME, TO_TIMESTAMP, "
                //        + "TRIGGER, TYPE, UNDER, USE, UTIME, VARIABLE, VARIANCE, VCLASS, VIRTUAL, "
                //        + "VISIBLE, WAIT, WHILE, WITHOUT, SYS_DATETIME, TO_DATETIME";

                string line = "ABSOLUTE, ACTION, ADD, ADD_MONTHS, AFTER, ALIAS, ALL, ALLOCATE, "
                            + "ALTER, AND, ANY, ARE, AS, ASC, ASSERTION, ASYNC, AT, ATTACH, "
                            + "ATTRIBUTE, AVG, BEFORE, BETWEEN, BIGINT, BIT, BIT_LENGTH, BLOB, "
                            + "BOOLEAN, BOTH, BREADTH, BY, CALL, CASCADE, CASCADED, CASE, "
                            + "CAST, CATALOG, CHANGE, CHAR, CHARACTER, CHECK, CLASS, CLASSES, "
                            + "CLOB, CLOSE, CLUSTER, COALESCE, COLLATE, COLLATION, COLUMN, "
                            + "COMMIT, COMPLETION, CONNECT, CONNECT_BY_ISCYCLE, CONNECT_BY_ISLEAF, "
                            + "CONNECT_BY_ROOT, CONNECTION, CONSTRAINT, CONSTRAINTS, CONTINUE, "
                            + "CONVERT, CORRESPONDING, COUNT, CREATE, CROSS, CURRENT, CURRENT_DATE, "
                            + "CURRENT_DATETIME, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER, "
                            + "CURSOR, CYCLE, DATA, DATA_TYPE, DATABASE, DATE, DATETIME, DAY, "
                            + "DAY_HOUR, DAY_MILLISECOND, DAY_MINUTE, DAY_SECOND, DEALLOCATE, "
                            + "DEC, DECIMAL, DECLARE, DEFAULT, DEFERRABLE, DEFERRED, DELETE, "
                            + "DEPTH, DESC, DESCRIBE, DESCRIPTOR, DIAGNOSTICS, DICTIONARY, "
                            + "DIFFERENCE, DISCONNECT, DISTINCT, DISTINCTROW, DIV, DO, "
                            + "DOMAIN, DOUBLE, DUPLICATE, DROP, EACH, ELSE, ELSEIF, END, "
                            + "EQUALS, ESCAPE, EVALUATE, EXCEPT, EXCEPTION, EXCLUDE, EXEC, "
                            + "EXECUTE, EXISTS, EXTERNAL, EXTRACT, FALSE, FETCH, FILE, FIRST, "
                            + "FLOAT, FOR, FOREIGN, FOUND, FROM, FULL, FUNCTION, GENERAL, GET, "
                            + "GLOBAL, GO, GOTO, GRANT, GROUP, HAVING, HOUR, HOUR_MILLISECOND, "
                            + "HOUR_MINUTE, HOUR_SECOND, IDENTITY, IF, IGNORE, IMMEDIATE, IN, "
                            + "INDEX, INDICATOR, INHERIT, INITIALLY, INNER, INOUT, INPUT, "
                            + "INSERT, INT, INTEGER, INTERSECT, INTERSECTION, INTERVAL, INTO, "
                            + "IS, ISOLATION, JOIN, KEY, LANGUAGE, LAST, LDB, LEADING, LEAVE, "
                            + "LEFT, LESS, LEVEL, LIKE, LIMIT, LIST, LOCAL, LOCAL_TRANSACTION_ID, "
                            + "LOCALTIME, LOCALTIMESTAMP, LOOP, LOWER, MATCH, MAX, METHOD, "
                            + "MILLISECOND, MIN, MINUTE, MINUTE_MILLISECOND, MINUTE_SECOND, "
                            + "MOD, MODIFY, MODULE, MONETARY, MONTH, MULTISET, MULTISET_OF, "
                            + "NA, NAMES, NATIONAL, NATURAL, NCHAR, NEXT, NO, NONE, NOT, NULL, "
                            + "NULLIF, NUMERIC, OBJECT, OCTET_LENGTH, OF, OFF, OID, ON, ONLY, "
                            + "OPEN, OPERATION, OPERATORS, OPTIMIZATION, OPTION, OR, ORDER, OTHERS, "
                            + "OUT, OUTER, OUTPUT, OVERLAPS, PARAMETERS, PARTIAL, PENDANT, POSITION, "
                            + "PRECISION, PREORDER, PREPARE, PRESERVE, PRIMARY, PRIOR, PRIVATE, "
                            + "PRIVILEGES, PROCEDURE, PROTECTED, PROXY, QUERY, READ, REAL, RECURSIVE, "
                            + "REF, REFERENCES, REFERENCING, REGISTER, RELATIVE, RENAME, REPLACE, "
                            + "RESIGNAL, RESTRICT, RETURN, RETURNS, REVOKE, RIGHT, ROLE, ROLLBACK, "
                            + "ROLLUP, ROUTINE, ROW, ROWNUM, ROWS, SAVEPOINT, SCHEMA, SCOPE, SCROLL, "
                            + "SEARCH, SECOND, SECOND_MILLISECOND, SECTION, SELECT, SENSITIVE, "
                            + "SEQUENCE, SEQUENCE_OF, SERIALIZABLE, SESSION, SESSION_USER, SET, "
                            + "SET_OF, SETEQ, SHARED, SIBLINGS, SIGNAL, SIMILAR, SIZE, SMALLINT, "
                            + "SOME, SQL, SQLCODE, SQLERROR, SQLEXCEPTION, SQLSTATE, SQLWARNING, "
                            + "STATISTICS, STRING, STRUCTURE, SUBCLASS, SUBSET, SUBSETEQ, "
                            + "SUBSTRING, SUM, SUPERCLASS, SUPERSET, SUPERSETEQ, SYS_CONNECT_BY_PATH, "
                            + "SYS_DATE, SYS_DATETIME, SYS_TIME, SYS_TIMESTAMP, SYS_USER, SYSDATE, "
                            + "SYSDATETIME, SYSTEM_USER, SYSTIME, TABLE, TEMPORARY, TEST, THEN, "
                            + "THERE, TIME, TIMESTAMP, TIMEZONE_HOUR, TIMEZONE_MINUTE, TO, TRAILING, "
                            + "TRANSACTION, TRANSLATE, TRANSLATION, TRIGGER, TRIM, TRUE, TRUNCATE, "
                            + "TYPE, UNDER, UNION, UNIQUE, UNKNOWN, UPDATE, UPPER, USAGE, USE, "
                            + "USER, USING, UTIME, VALUE, VALUES, VARCHAR, VARIABLE, VARYING, VCLASS, "
                            + "VIEW, VIRTUAL, VISIBLE, WAIT, WHEN, WHENEVER, WHERE, WHILE, WITH, "
                            + "WITHOUT, WORK, WRITE, XOR, YEAR, YEAR_MONTH, ZONE";


                string[] keywords = line.Split(new char[] { ' ' });
                foreach (string s in keywords)
                {
                    string keyword = s.Replace(",", String.Empty);
                    if (String.IsNullOrEmpty(keyword))
                        continue;

                    DataRow row = dt.NewRow();
                    row[0] = keyword;
                    dt.Rows.Add(row);
                }

                return dt;
            }
		}

		/// <summary>
		/// Gets the Numeric-type functions.
		/// </summary>
		/// <returns></returns>
		public static String[] GetNumericFunctions()
		{
			string str = "AVG, COUNT, MAX, MIN, STDDEV, SUM, VARIANCE";
			string[] list = str.Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);

			return list;
		}

		/// <summary>
		/// Gets the String-type functions.
		/// </summary>
		/// <returns></returns>
		public static String[] GetStringFunctions()
		{
			string str = "BIT_LENGTH, CHAR_LENGTH, LOWER, LTRIM, OCTET_LENGTH, POSITION, REPLACE, "
					+ "RPAD, RTRIM, SUBSTRING, TRANSLATE, TRIM, TO_CHAR, TO_DATE, TO_NUMBER, "
					+ "TO_TIME, TO_TIMESTAMP, TO_DATETIME, UPPER";
			string[] list = str.Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);

			return list;
		}

		/// <summary>
		/// Fills the table.
		/// </summary>
		/// <param name="dt">The DataTable</param>
		/// <param name="data">The data</param>
		protected static void FillTable(DataTable dt, object[][] data)
		{
			foreach (object[] dataItem in data)
			{
				DataRow row = dt.NewRow();
				for (int i = 0; i < dataItem.Length; i++)
				{
					row[i] = dataItem[i];
				}
				dt.Rows.Add(row);
			}
		}

		private void FindTables(DataTable schemaTable, string[] filters)
		{
			StringBuilder sql = new StringBuilder();
			StringBuilder where = new StringBuilder();
			String selectTables = "select class_name from db_class where is_system_class='NO' and class_type='CLASS'";

			sql.AppendFormat(CultureInfo.InvariantCulture, selectTables, filters[0]);
			if (filters != null)
			{
				string table_name_pattern = filters[0];
				where.AppendFormat(CultureInfo.InvariantCulture, " and class_name LIKE '{0}'", table_name_pattern);
				sql.Append(where.ToString());
			}

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql.ToString(), conn))
            {
                using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        DataRow row = schemaTable.NewRow();
                        row["TABLE_CATALOG"] = conn.Database;
                        row["TABLE_SCHEMA"] = conn.Database;
                        row["TABLE_NAME"] = reader.GetString(0);
                        schemaTable.Rows.Add(row);
                    }
                }
            }
		}

		private void FindViews(DataTable schemaTable, string[] filters)
		{
			StringBuilder sql = new StringBuilder();
			StringBuilder where = new StringBuilder();
			String selectTables = "select class_name from db_class where is_system_class='NO' and class_type='VCLASS'";
			sql.AppendFormat(CultureInfo.InvariantCulture, selectTables);
			if (filters != null)
			{
				string view_name_pattern = filters[0];
				where.AppendFormat(CultureInfo.InvariantCulture, " and class_name LIKE '{0}'", view_name_pattern);
				sql.Append(where.ToString());
			}

            using (CUBRIDCommand cmd = new CUBRIDCommand(sql.ToString(), conn))
            {
                using (CUBRIDDataReader reader = (CUBRIDDataReader)cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        DataRow row = schemaTable.NewRow();

                        row["TABLE_CATALOG"] = conn.Database;
                        row["TABLE_SCHEMA"] = conn.Database;
                        row["TABLE_NAME"] = reader.GetString(0);

                        schemaTable.Rows.Add(row);
                    }
                }
            }
		}

		public DataTable GetSchema(string collection, string[] filters)
		{
			switch (collection.ToUpper())
			{
				// common collections
				case "RESERVED_WORDS":
					return GetReservedWords();

				// collections specific to our provider
				case "USERS":
					return GetUsers(filters); //name
				case "DATABASES":
					return GetDatabases(filters);
				case "PROCEDURES":
					return GetProcedures(filters);
				case "TABLES":
					return GetTables(filters);
				case "VIEWS":
					return GetViews(filters);
				case "COLUMNS":
					return GetColumns(filters);
				case "INDEXES":
					return GetIndexes(filters);
				case "INDEX_COLUMNS":
					return GetIndexColumns(filters);
				case "FOREIGN_KEYS":
					return GetForeignKeys(filters);
				default:
					return null;
			}
		}

		private static string GetString(CUBRIDDataReader reader, int index)
		{
			if (reader.IsDBNull(index))
				return null;

			return reader.GetString(index);
		}

		internal string[] CleanFilters(string[] filters)
		{
			string[] cleaned_filters = null;

			if (filters != null)
			{
				cleaned_filters = (string[])filters.Clone();

				for (int x = 0; x < cleaned_filters.Length; x++)
				{
					string s = cleaned_filters[x];
					if (s == null)
						continue;

					cleaned_filters[x] = s.Trim('`');
				}
			}

			return cleaned_filters;
		}
	}
}
