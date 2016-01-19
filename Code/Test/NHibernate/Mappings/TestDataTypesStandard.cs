using System;

namespace CUBRID.Data.TestNHibernate
{
  public class TestDataTypesStandard
  {
    virtual public int c_integer { get; set; }
    virtual public short c_smallint { get; set; }
    virtual public long c_bigint { get; set; }
    virtual public decimal c_numeric { get; set; }
    virtual public float c_float { get; set; }
    virtual public decimal c_decimal { get; set; }
    virtual public double c_double { get; set; }
    virtual public string c_char { get; set; }
    virtual public string c_varchar { get; set; }
    virtual public DateTime c_time { get; set; }
    virtual public DateTime c_date { get; set; }
    virtual public DateTime c_timestamp { get; set; }
    virtual public DateTime c_datetime { get; set; }
    virtual public int c_monetary { get; set; }
    virtual public String c_string { get; set; }
    virtual public byte c_bit { get; set; }
    virtual public byte c_varbit { get; set; }
  }
}
