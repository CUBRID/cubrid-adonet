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
using System.Text;

namespace CUBRID.Data.CUBRIDClient
{
  /// <summary>
  ///   Class used to store column metadata.
  /// </summary>
  public class ColumnMetaData
  {
    private string defaultValue;
    private bool isAutoIncrement;
    private bool isForeignKey;
    private bool isNullable;
    private bool isPrimaryKey;
    private bool isReverseIndex;
    private bool isReverseUnique;
    private bool isShared;
    private bool isUniqueKey;
    private string name;
    private int precision;
    private string realName;
    private short scale;
    private string tableName;
    private CUBRIDDataType type;

    internal ColumnMetaData()
    {
      type = CUBRIDDataType.CCI_U_TYPE_UNKNOWN;
      scale = -1;
      precision = -1;
      realName = null;
      tableName = null;
      name = null;
      isNullable = false;

      defaultValue = null;
      isAutoIncrement = false;
      isUniqueKey = false;
      isPrimaryKey = false;
      isForeignKey = false;
      isReverseIndex = false;
      isReverseUnique = false;
      isShared = false;
    }
    /*
    internal ColumnMetaData(CUBRIDDataType cType, short cScale, int cPrecision, string cName)
    {
      type = cType;

      //InferCollectionElementsType does not seem to work properly, using ConfirmType instead
      //InferCollectionElementsType(type);
      ConfirmType(type);

      scale = cScale;
      precision = cPrecision;
      realName = cName;
      tableName = null;
      name = null;
      isNullable = false;

      defaultValue = null;
      isAutoIncrement = false;
      isUniqueKey = false;
      isPrimaryKey = false;
      isForeignKey = false;
      isReverseIndex = false;
      isReverseUnique = false;
      isShared = false;
    }
    */
    /// <summary>
    ///   Gets or sets the type.
    /// </summary>
    /// <value> The type. </value>
    public CUBRIDDataType Type
    {
      get { return type; }
      set { InferCollectionElementsType(value); }
    }

    /// <summary>
    ///   Gets or sets the scale.
    /// </summary>
    /// <value> The scale. </value>
    public short Scale
    {
      get { return scale; }
      set { scale = value; }
    }

    /// <summary>
    ///   Gets or sets the precision.
    /// </summary>
    /// <value> The precision. </value>
    public int Precision
    {
      get { return precision; }
      set { precision = value; }
    }

    /// <summary>
    ///   Gets or sets the name of the real.
    /// </summary>
    /// <value> The name of the real. </value>
    public string RealName
    {
      get { return realName; }
      set { realName = value; }
    }

    /// <summary>
    ///   Gets or sets the table.
    /// </summary>
    /// <value> The table. </value>
    public string Table
    {
      get { return tableName; }
      set { tableName = value; }
    }

    /// <summary>
    ///   Gets or sets the name.
    /// </summary>
    /// <value> The name. </value>
    public string Name
    {
      get { return name; }
      set { name = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is nullable.
    /// </summary>
    /// <value> <c>true</c> if this instance is nullable; otherwise, <c>false</c> . </value>
    public bool IsNullable
    {
      get { return isNullable; }
      set { isNullable = value; }
    }

    /// <summary>
    ///   Gets or sets the default value.
    /// </summary>
    /// <value> The default value. </value>
    public string DefaultValue
    {
      get { return defaultValue; }
      set { defaultValue = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is auto increment.
    /// </summary>
    /// <value> <c>true</c> if this instance is auto increment; otherwise, <c>false</c> . </value>
    public bool IsAutoIncrement
    {
      get { return isAutoIncrement; }
      set { isAutoIncrement = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is unique key.
    /// </summary>
    /// <value> <c>true</c> if this instance is unique key; otherwise, <c>false</c> . </value>
    public bool IsUniqueKey
    {
      get { return isUniqueKey; }
      set { isUniqueKey = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is primary key.
    /// </summary>
    /// <value> <c>true</c> if this instance is primary key; otherwise, <c>false</c> . </value>
    public bool IsPrimaryKey
    {
      get { return isPrimaryKey; }
      set { isPrimaryKey = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is foreign key.
    /// </summary>
    /// <value> <c>true</c> if this instance is foreign key; otherwise, <c>false</c> . </value>
    public bool IsForeignKey
    {
      get { return isForeignKey; }
      set { isForeignKey = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is reverse index.
    /// </summary>
    /// <value> <c>true</c> if this instance is reverse index; otherwise, <c>false</c> . </value>
    public bool IsReverseIndex
    {
      get { return isReverseIndex; }
      set { isReverseIndex = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is reverse unique.
    /// </summary>
    /// <value> <c>true</c> if this instance is reverse unique; otherwise, <c>false</c> . </value>
    public bool IsReverseUnique
    {
      get { return isReverseUnique; }
      set { isReverseUnique = value; }
    }

    /// <summary>
    ///   Gets or sets a value indicating whether this column is shared.
    /// </summary>
    /// <value> <c>true</c> if this instance is shared; otherwise, <c>false</c> . </value>
    public bool IsShared
    {
      get { return isShared; }
      set { isShared = value; }
    }

    internal void InferCollectionElementsType(CUBRIDDataType dataType)
    {
      int collectionType = (Convert.ToByte(dataType)) & (0x60);

      switch (collectionType)
      {
        case 0x00:
          type = dataType;
          break;
        case 0x20:
          type = CUBRIDDataType.CCI_U_TYPE_SET;
          break;
        case 0x40:
          type = CUBRIDDataType.CCI_U_TYPE_MULTISET;
          break;
        case 0x60:
          type = CUBRIDDataType.CCI_U_TYPE_SEQUENCE;
          break;
        default:
          type = CUBRIDDataType.CCI_U_TYPE_NULL;
          break;
      }
    }

    internal byte[] ConfirmType(CUBRIDDataType originalType)
    {
      byte[] typeInfo = new byte[2];
      int collectionTypeOrNot = Convert.ToByte(originalType) & 0140;
      switch (collectionTypeOrNot)
      {
        case 0:
          typeInfo[0] = Convert.ToByte(originalType);
          typeInfo[1] = 0;

          return typeInfo;
        case 040:
          typeInfo[0] = (byte) CUBRIDDataType.CCI_U_TYPE_SET;
          typeInfo[1] = (byte) (Convert.ToByte(originalType) & 037);

          return typeInfo;
        case 0100:
          typeInfo[0] = (byte) CUBRIDDataType.CCI_U_TYPE_MULTISET;
          typeInfo[1] = (byte) (Convert.ToByte(originalType) & 037);

          return typeInfo;
        case 0140:
          typeInfo[0] = (byte) CUBRIDDataType.CCI_U_TYPE_SEQUENCE;
          typeInfo[1] = (byte) (Convert.ToByte(originalType) & 037);

          return typeInfo;
        default:
          typeInfo[0] = (byte) CUBRIDDataType.CCI_U_TYPE_NULL;
          typeInfo[1] = 0;
          break;
      }

      return typeInfo;
    }

    /// <summary>
    ///   Returns a <see cref="System.String" /> that represents this instance.
    /// </summary>
    /// <returns> A <see cref="System.String" /> that represents this instance. </returns>
    public override string ToString()
    {
      StringBuilder builder = new StringBuilder();

      builder.Append("ColumnMetaData: column::CUBRIDDataType = " + type);
      builder.Append(", column::Scale = " + scale);
      builder.Append(", column::Precision = " + precision);
      builder.Append(", column::RealName = " + name);
      builder.Append(", column::RealName = " + realName);
      builder.Append(", column::tableName = " + tableName);
      builder.Append(", column::isNullable = " + isNullable);
      builder.Append(", column::defaultValue = " + defaultValue);
      builder.Append(", column::isAutoIncrement = " + isAutoIncrement);
      builder.Append(", column::isUniqueKey = " + isUniqueKey);
      builder.Append(", column::isPrimaryKey = " + isPrimaryKey);
      builder.Append(", column::isForeignKey = " + isForeignKey);
      builder.Append(", column::isReverseIndex = " + isReverseIndex);
      builder.Append(", column::isReverseUnique = " + isReverseUnique);
      builder.Append(", column::isShared = " + isShared);
      builder.Append(Environment.NewLine);

      return builder.ToString();
    }
  }
}