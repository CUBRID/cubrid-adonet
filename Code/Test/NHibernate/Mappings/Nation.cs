using System.Collections.Generic;

namespace CUBRID.Data.TestNHibernate
{
  public class Nation
  {
    virtual public string code { get; set; }
    virtual public string name { get; set; }
    virtual public string continent { get; set; }
    virtual public string capital { get; set; }
    public virtual IList<AthleteOneToMany> Athletes { get; set; }
  }
}
