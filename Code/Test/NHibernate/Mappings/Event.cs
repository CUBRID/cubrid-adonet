using System.Collections.Generic;

namespace CUBRID.Data.TestNHibernate
{
  public class Event
  {
    virtual public int code { get; set; }
    virtual public string sports { get; set; }
    virtual public string name { get; set; }
    virtual public string gender { get; set; }
    virtual public int players { get; set; }
    virtual public IList<AthleteManyToMany> Athletes { get; set; }
  }
}
