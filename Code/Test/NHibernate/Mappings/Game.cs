using System;

namespace CUBRID.Data.TestNHibernate
{
  public class Game
  {
    virtual public int host_year { get; set; }
    virtual public string medal { get; set; }
    virtual public DateTime game_date { get; set; }
    virtual public Event Event { get; set; }
    virtual public AthleteOneToOne Athlete { get; set; }
    virtual public Stadium Stadium { get; set; }
    virtual public Nation Nation { get; set; }
  }
}
