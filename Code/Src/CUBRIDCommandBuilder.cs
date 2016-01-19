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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID implementation of the <see cref="T:System.Data.Common.DbCommandBuilder" /> class.
  /// </summary>
  [ToolboxItem(false)]
  [DesignerCategory("Code")]
  public sealed class CUBRIDCommandBuilder : DbCommandBuilder
  {
    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDCommandBuilder" /> class.
    /// </summary>
    public CUBRIDCommandBuilder()
    {
      QuotePrefix = QuoteSuffix = "`";
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDCommandBuilder" /> class.
    /// </summary>
    /// <param name="dataAdapter"> The data dataAdapter. </param>
    public CUBRIDCommandBuilder(CUBRIDDataAdapter dataAdapter)
      : this()
    {
      DataAdapter = dataAdapter;
    }

    /// <summary>
    ///   Gets or sets a <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDDataAdapter" /> object for which statements are automatically generated.
    /// </summary>
    /// <returns> A <see cref="T:CUBRID.Data.CUBRIDClient.CUBRIDDataAdapter" /> object. </returns>
    public new CUBRIDDataAdapter DataAdapter
    {
      get { return (CUBRIDDataAdapter)base.DataAdapter; }
      set { base.DataAdapter = value; }
    }

    /// <summary>
    ///   Allows the provider implementation of the <see cref="T:System.Data.Common.DbCommandBuilder" /> class to handle additional parameter properties.
    /// </summary>
    /// <param name="parameter"> A <see cref="T:System.Data.Common.DbParameter" /> to which the additional modifications are applied. </param>
    /// <param name="row"> The <see cref="T:System.Data.DataRow" /> from the schema table provided by <see
    ///    cref="M:System.Data.Common.DbDataReader.GetSchemaTable" /> . </param>
    /// <param name="statementType"> The type of command being generated; INSERT, UPDATE or DELETE. </param>
    /// <param name="whereClause"> true if the parameter is part of the update or delete WHERE clause, false if it is part of the insert or update values. </param>
    protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType,
                                               bool whereClause)
    {
      ((CUBRIDParameter)parameter).CUBRIDDataType = (CUBRIDDataType)row["ProviderType"];
    }

    /// <summary>
    ///   Gets the delete command.
    /// </summary>
    /// <returns> </returns>
    public new CUBRIDCommand GetDeleteCommand()
    {
      CUBRIDCommand cmd = (CUBRIDCommand)base.GetDeleteCommand();
      char[] delimitators = { ',', ')', ' ', '=' };
      int from = cmd.CommandText.IndexOf("?", StringComparison.Ordinal);
      int to = cmd.CommandText.IndexOfAny(delimitators, from);

      while (from != -1)
      {
        cmd.CommandText = cmd.CommandText.Remove(from, to - from);
        cmd.CommandText = cmd.CommandText.Insert(from, "?");
        from = cmd.CommandText.IndexOf("?", from + 1, StringComparison.Ordinal);
        if (from != -1)
          to = cmd.CommandText.IndexOfAny(delimitators, from);
      }

      cmd.CommandText = cmd.CommandText.Replace(" = NULL", " IS NULL");

      return cmd;
    }

    /// <summary>
    ///   Gets the update command.
    /// </summary>
    /// <returns> </returns>
    public new CUBRIDCommand GetUpdateCommand()
    {
      CUBRIDCommand cmd = (CUBRIDCommand)base.GetUpdateCommand();
      char[] delimitators = { ',', ')', ' ' };
      int from = cmd.CommandText.IndexOf("?", StringComparison.Ordinal);
      int to = cmd.CommandText.IndexOfAny(delimitators, from);

      while (from != -1)
      {
        cmd.CommandText = cmd.CommandText.Remove(from, to - from);
        cmd.CommandText = cmd.CommandText.Insert(from, "?");
        from = cmd.CommandText.IndexOf("?", from + 1, StringComparison.Ordinal);
        if (from != -1)
          to = cmd.CommandText.IndexOfAny(delimitators, from);
      }

      cmd.CommandText = cmd.CommandText.Replace(" = NULL", " IS NULL");

      return cmd;
    }

    /// <summary>
    ///   Gets the insert command.
    /// </summary>
    /// <returns> </returns>
    public new CUBRIDCommand GetInsertCommand()
    {
      CUBRIDCommand cmd = (CUBRIDCommand)GetInsertCommand(false);
      char[] delimitators = { ',', ')' };
      int from = cmd.CommandText.IndexOf("?", StringComparison.Ordinal);
      int to = cmd.CommandText.IndexOfAny(delimitators, from);

      while (from != -1)
      {
        cmd.CommandText = cmd.CommandText.Remove(from, to - from);
        cmd.CommandText = cmd.CommandText.Insert(from, "?");
        from = cmd.CommandText.IndexOf("?", from + 1, StringComparison.Ordinal);
        if (from != -1)
          to = cmd.CommandText.IndexOfAny(delimitators, from);
      }

      cmd.CommandText = cmd.CommandText.Replace(" = NULL", " IS NULL");

      return cmd;
    }

    /// <summary>
    ///   Gets the name of the parameter.
    /// </summary>
    /// <param name="unformattedParameterName"> Unformatted name of the parameter. </param>
    /// <returns> </returns>
    protected override string GetParameterName(string unformattedParameterName)
    {
      StringBuilder sb = new StringBuilder(unformattedParameterName);

      sb.Replace(" ", "");
      sb.Replace("/", "_per_");
      sb.Replace("-", "_");
      sb.Replace(")", "_cb_");
      sb.Replace("(", "_ob_");
      sb.Replace("%", "_pct_");
      sb.Replace("<", "_lt_");
      sb.Replace(">", "_gt_");
      sb.Replace(".", "_pt_");

      if (unformattedParameterName.StartsWith("?"))
      {
        return String.Format("{0}", sb);
      }

      return String.Format("?{0}", sb);
    }

    /// <summary>
    ///   Returns the name of the specified parameter in the format of ?p#. Use when building a custom command builder.
    /// </summary>
    /// <param name="parameterOrdinal"> The number to be included as part of the parameter's name.. </param>
    /// <returns> The name of the parameter with the specified number appended as part of the parameter name. </returns>
    protected override string GetParameterName(int parameterOrdinal)
    {
      return String.Format("?p{0}", parameterOrdinal.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>
    ///   Returns the placeholder for the parameter in the associated SQL statement.
    /// </summary>
    /// <param name="parameterOrdinal"> The number to be included as part of the parameter's name. </param>
    /// <returns> The name of the parameter with the specified number appended. </returns>
    protected override string GetParameterPlaceholder(int parameterOrdinal)
    {
      return GetParameterName(parameterOrdinal);
    }

    /// <summary>
    ///   Given an unquoted identifier in the correct catalog case, returns the correct quoted form of that identifier, including properly escaping any embedded quotes in the identifier.
    /// </summary>
    /// <param name="unquotedIdentifier"> The original unquoted identifier. </param>
    /// <returns> The quoted version of the identifier. Embedded quotes within the identifier are properly escaped. </returns>
    /// <PermissionSet>
    ///   <IPermission
    ///     class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
    ///     version="1" PathDiscovery="*AllFiles*" />
    /// </PermissionSet>
    public override string QuoteIdentifier(string unquotedIdentifier)
    {
      if (unquotedIdentifier == null)
        throw new ArgumentNullException();

      if (unquotedIdentifier.StartsWith(QuotePrefix) && unquotedIdentifier.EndsWith(QuoteSuffix))
        return unquotedIdentifier;

      string quotedIdentifier = unquotedIdentifier.Replace(QuotePrefix, QuotePrefix + QuotePrefix);

      return String.Format("{0}{1}{2}", QuotePrefix, quotedIdentifier, QuoteSuffix);
    }

    /// <summary>
    ///   Given a quoted identifier, returns the correct unquoted form of that identifier, including properly un-escaping any embedded quotes in the identifier.
    /// </summary>
    /// <param name="quotedIdentifier"> The identifier that will have its embedded quotes removed. </param>
    /// <returns> The unquoted identifier, with embedded quotes properly un-escaped. </returns>
    /// <PermissionSet>
    ///   <IPermission
    ///     class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
    ///     version="1" PathDiscovery="*AllFiles*" />
    /// </PermissionSet>
    public override string UnquoteIdentifier(string quotedIdentifier)
    {
      string unquotedIdentifier = quotedIdentifier;

      if (quotedIdentifier == null)
        throw new ArgumentNullException();

      if (!quotedIdentifier.StartsWith(QuotePrefix) || !quotedIdentifier.EndsWith(QuoteSuffix))
        return quotedIdentifier;

      if (quotedIdentifier.StartsWith(QuotePrefix))
        unquotedIdentifier = quotedIdentifier.Substring(1);

      if (unquotedIdentifier.EndsWith(QuoteSuffix))
        unquotedIdentifier = unquotedIdentifier.Substring(0, unquotedIdentifier.Length - 1);

      unquotedIdentifier = unquotedIdentifier.Replace(QuotePrefix + QuotePrefix, QuotePrefix);

      return unquotedIdentifier;
    }

    /// <summary>
    ///   Registers the <see cref="T:System.Data.Common.DbCommandBuilder" /> to handle the <see
    ///    cref="E:System.Data.OleDb.OleDbDataAdapter.RowUpdating" /> event for a <see cref="T:System.Data.Common.DbDataAdapter" />.
    /// </summary>
    /// <param name="dataAdapter"> The <see cref="T:System.Data.Common.DbDataAdapter" /> to be used for the update. </param>
    protected override void SetRowUpdatingHandler(DbDataAdapter dataAdapter)
    {
      CUBRIDDataAdapter adapter = (dataAdapter as CUBRIDDataAdapter);
      if (adapter != null)
      {
        if (dataAdapter != base.DataAdapter)
          adapter.RowUpdating += RowUpdating;
        else
          adapter.RowUpdating -= RowUpdating;
      }
    }

    private void RowUpdating(object sender, CUBRIDRowUpdatingEventArgs args)
    {
      RowUpdatingHandler(args);
    }
  }
}