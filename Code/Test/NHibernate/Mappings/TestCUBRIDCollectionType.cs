namespace CUBRID.Data.TestNHibernate
{
  class TestCUBRIDCollectionType
  {
    virtual public int id { get; set; }
    virtual public object[] seq { get; set; }
    virtual public object[] set { get; set; }
    virtual public object[] multiset { get; set; }
  }
}
