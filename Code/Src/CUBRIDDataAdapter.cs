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

using System.Data;
using System.Data.Common;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   CUBRID DataAdapter implementation
  /// </summary>
  public sealed class CUBRIDDataAdapter : DbDataAdapter
  {
    private int updateBatchSize = 1; //Disables batch updating

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDDataAdapter" /> class.
    /// </summary>
    public CUBRIDDataAdapter()
    {
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDDataAdapter" /> class.
    /// </summary>
    /// <param name="selectCommand"> The select command. </param>
    public CUBRIDDataAdapter(CUBRIDCommand selectCommand)
      : this()
    {
      SelectCommand = selectCommand;
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDDataAdapter" /> class.
    /// </summary>
    /// <param name="selectCommandText"> The select command text. </param>
    /// <param name="connection"> The connection. </param>
    public CUBRIDDataAdapter(string selectCommandText, CUBRIDConnection connection)
      : this()
    {
      SelectCommand = new CUBRIDCommand(selectCommandText, connection);
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="CUBRIDDataAdapter" /> class.
    /// </summary>
    /// <param name="selectCommandText"> The select command text. </param>
    /// <param name="selectConnString"> The select connection string. </param>
    public CUBRIDDataAdapter(string selectCommandText, string selectConnString)
      : this()
    {
      SelectCommand = new CUBRIDCommand(selectCommandText, new CUBRIDConnection(selectConnString));
    }

    /// <summary>
    ///   Gets or sets a command for deleting records from the data set.
    /// </summary>
    /// <returns> An <see cref="T:System.Data.IDbCommand" /> used during <see
    ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to delete records in the data source for deleted rows in the data set. </returns>
    public new CUBRIDCommand DeleteCommand
    {
      get { return (CUBRIDCommand)base.DeleteCommand; }
      set { base.DeleteCommand = value; }
    }

    /// <summary>
    ///   Gets or sets a command used to insert new records into the data source.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.IDbCommand" /> used during <see
    ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to insert records in the data source for new rows in the data set. </returns>
    public new CUBRIDCommand InsertCommand
    {
      get { return (CUBRIDCommand)base.InsertCommand; }
      set { base.InsertCommand = value; }
    }

    /// <summary>
    ///   Gets or sets a command used to select records in the data source.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.IDbCommand" /> that is used during <see
    ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to select records from data source for placement in the data set. </returns>
    public new CUBRIDCommand SelectCommand
    {
      get { return (CUBRIDCommand)base.SelectCommand; }
      set { base.SelectCommand = value; }
    }

    /// <summary>
    ///   Gets or sets a command used to update records in the data source.
    /// </summary>
    /// <returns> A <see cref="T:System.Data.IDbCommand" /> used during <see
    ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> to update records in the data source for modified rows in the data set. </returns>
    public new CUBRIDCommand UpdateCommand
    {
      get { return (CUBRIDCommand)base.UpdateCommand; }
      set { base.UpdateCommand = value; }
    }

    /// <summary>
    ///   Gets or sets a value that enables or disables batch processing support, and specifies the number of commands that can be executed in a batch.
    /// </summary>
    /// <returns> The number of rows to process per batch. Value is Effect 0 There is no limit on the batch size. 1 Disables batch updating. &gt; 1 Changes are sent using batches of <see
    ///    cref="P:System.Data.Common.DbDataAdapter.UpdateBatchSize" /> operations at a time. When setting this to a value other than 1 ,all the commands associated with the <see
    ///    cref="T:System.Data.Common.DbDataAdapter" /> must have their <see cref="P:System.Data.IDbCommand.UpdatedRowSource" /> property set to None or OutputParameters. An exception will be thrown otherwise. </returns>
    /// <PermissionSet>
    ///   <IPermission
    ///     class="System.Security.Permissions.FileIOPermission, mscorlib, Version=2.0.3600.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
    ///     version="1" PathDiscovery="*AllFiles*" />
    /// </PermissionSet>
    public override int UpdateBatchSize
    {
      get { return updateBatchSize; }
      set { updateBatchSize = value; }
    }

    /// <summary>
    ///   Occurs during Update before a command is executed.
    /// </summary>
    public event CUBRIDRowUpdatingEventHandler RowUpdating;

    /// <summary>
    ///   Occurs during Update after a command is executed.
    /// </summary>
    public event CUBRIDRowUpdatedEventHandler RowUpdated;

    /// <summary>
    ///   Initializes a new instance of the <see cref="T:System.Data.Common.RowUpdatedEventArgs" /> class.
    /// </summary>
    /// <param name="dataRow"> The <see cref="T:System.Data.DataRow" /> used to update the data source. </param>
    /// <param name="command"> The <see cref="T:System.Data.IDbCommand" /> executed during the <see
    ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> . </param>
    /// <param name="statementType"> Whether the command is an UPDATE, INSERT, DELETE, or SELECT statement. </param>
    /// <param name="tableMapping"> A <see cref="T:System.Data.Common.DataTableMapping" /> object. </param>
    /// <returns> A new instance of the <see cref="T:System.Data.Common.RowUpdatedEventArgs" /> class. </returns>
    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command,
                                                                 StatementType statementType,
                                                                 DataTableMapping tableMapping)
    {
      return new CUBRIDRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);
    }

    /// <summary>
    ///   Initializes a new instance of the <see cref="T:System.Data.Common.RowUpdatingEventArgs" /> class.
    /// </summary>
    /// <param name="dataRow"> The <see cref="T:System.Data.DataRow" /> that updates the data source. </param>
    /// <param name="command"> The <see cref="T:System.Data.IDbCommand" /> to execute during the <see
    ///    cref="M:System.Data.IDataAdapter.Update(System.Data.DataSet)" /> . </param>
    /// <param name="statementType"> Whether the command is an UPDATE, INSERT, DELETE, or SELECT statement. </param>
    /// <param name="tableMapping"> A <see cref="T:System.Data.Common.DataTableMapping" /> object. </param>
    /// <returns> A new instance of the <see cref="T:System.Data.Common.RowUpdatingEventArgs" /> class. </returns>
    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command,
                                                                   StatementType statementType,
                                                                   DataTableMapping tableMapping)
    {
      return new CUBRIDRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);
    }

    /// <summary>
    ///   Raises the RowUpdating event of a .NET Framework data provider.
    /// </summary>
    /// <param name="value"> An <see cref="T:System.Data.Common.RowUpdatingEventArgs" /> that contains the event data. </param>
    protected override void OnRowUpdating(RowUpdatingEventArgs value)
    {
      if (RowUpdating != null)
        RowUpdating(this, (value as CUBRIDRowUpdatingEventArgs));
    }

    /// <summary>
    ///   Raises the RowUpdated event of a .NET Framework data provider.
    /// </summary>
    /// <param name="value"> A <see cref="T:System.Data.Common.RowUpdatedEventArgs" /> that contains the event data. </param>
    protected override void OnRowUpdated(RowUpdatedEventArgs value)
    {
      if (RowUpdated != null)
        RowUpdated(this, (value as CUBRIDRowUpdatedEventArgs));
    }
  }

  /// <summary>
  ///   CUBRIDRowUpdatingEventHandler
  /// </summary>
  /// <param name="sender"> The sender. </param>
  /// <param name="e"> The <see cref="CUBRID.Data.CUBRIDClient.CUBRIDRowUpdatingEventArgs" /> instance containing the event data. </param>
  public delegate void CUBRIDRowUpdatingEventHandler(object sender, CUBRIDRowUpdatingEventArgs e);

  /// <summary>
  ///   CUBRIDRowUpdatedEventHandler
  /// </summary>
  /// <param name="sender"> The sender. </param>
  /// <param name="e"> The <see cref="CUBRID.Data.CUBRIDClient.CUBRIDRowUpdatedEventArgs" /> instance containing the event data. </param>
  public delegate void CUBRIDRowUpdatedEventHandler(object sender, CUBRIDRowUpdatedEventArgs e);

  /// <summary>
  ///   CUBRID RowUpdatingEventArgs implementation.
  /// </summary>
  public sealed class CUBRIDRowUpdatingEventArgs : RowUpdatingEventArgs
  {
    /// <summary>
    ///   Initializes a new instance of the CUBRIDRowUpdatingEventArgs class.
    /// </summary>
    /// <paramList name="row">The
    ///   <see cref="DataRow" />
    ///   to
    ///   <see cref="DbDataAdapter.Update(DataSet)" />
    ///   .</paramList>
    /// <paramList name="command">The
    ///   <see cref="IDbCommand" />
    ///   to execute during
    ///   <see cref="DbDataAdapter.Update(DataSet)" />
    ///   .</paramList>
    /// <paramList name="statementType">One of the
    ///   <see cref="StatementType" />
    ///   values that specifies the type of query executed.</paramList>
    /// <paramList name="tableMapping">The
    ///   <see cref="DataTableMapping" />
    ///   sent through an
    ///   <see cref="DbDataAdapter.Update(DataSet)" />
    ///   .</paramList>
    public CUBRIDRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType,
                                      DataTableMapping tableMapping)
      : base(row, command, statementType, tableMapping)
    {
    }

    /// <summary>
    ///   Gets or sets the CUBRIDCommand to execute when performing the Update.
    /// </summary>
    public new CUBRIDCommand Command
    {
      get { return (CUBRIDCommand)base.Command; }
      set { base.Command = value; }
    }
  }

  /// <summary>
  ///   CUBRID RowUpdatedEventArgs implementation.
  /// </summary>
  public sealed class CUBRIDRowUpdatedEventArgs : RowUpdatedEventArgs
  {
    /// <summary>
    ///   Initializes a new instance of the CUBRIDRowUpdatedEventArgs class.
    /// </summary>
    /// <paramList name="row">The
    ///   <see cref="DataRow" />
    ///   sent through an
    ///   <see cref="DbDataAdapter.Update(DataSet)" />
    ///   .</paramList>
    /// <paramList name="command">The
    ///   <see cref="IDbCommand" />
    ///   executed when
    ///   <see cref="DbDataAdapter.Update(DataSet)" />
    ///   is called.</paramList>
    /// <paramList name="statementType">One of the
    ///   <see cref="StatementType" />
    ///   values that specifies the type of query executed.</paramList>
    /// <paramList name="tableMapping">The
    ///   <see cref="DataTableMapping" />
    ///   sent through an
    ///   <see cref="DbDataAdapter.Update(DataSet)" />
    ///   .</paramList>
    public CUBRIDRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType,
                                     DataTableMapping tableMapping)
      : base(row, command, statementType, tableMapping)
    {
    }

    /// <summary>
    ///   Gets or sets the CUBRIDCommand executed when Update is called.
    /// </summary>
    public new CUBRIDCommand Command
    {
      get { return (CUBRIDCommand)base.Command; }
    }
  }
}