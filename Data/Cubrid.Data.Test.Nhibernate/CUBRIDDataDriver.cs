using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NHibernate.Driver;
using NHibernate.Engine;

namespace Cubrid.Data.Test.Nhibernate
{
    public class CUBRIDDataDriver : ReflectionBasedDriver, ISqlParameterFormatter
    {
        public CUBRIDDataDriver()
            : base(
            "CUBRID.Data",
            "CUBRID.Data.CUBRIDClient.CUBRIDConnection",
            "CUBRID.Data.CUBRIDClient.CUBRIDCommand")
        {
        }

        public override bool UseNamedPrefixInSql
        {
            get { return true; }
        }

        public override bool UseNamedPrefixInParameter
        {
            get { return true; }
        }

        public override string NamedPrefix
        {
            get { return "?"; }
        }

        string ISqlParameterFormatter.GetParameterName(int index)
        {
            return "?";
        }

        public override bool SupportsMultipleOpenReaders
        {
            get { return false; }
        }
    }
}
