using System.Collections.Generic;

namespace CUBRID.Data.TestNHibernate
{
  public class AthleteManyToMany
  {
    virtual public int code { get; set; }
    virtual public string name { get; set; }
    virtual public string gender { get; set; }
    virtual public string nation_code { get; set; }
    virtual public string athlete_event { get; set; }
    virtual public IList<Event> Events { get; set; }
  }
}
