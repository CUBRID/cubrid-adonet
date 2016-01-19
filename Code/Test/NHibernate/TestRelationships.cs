using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Linq;

namespace CUBRID.Data.TestNHibernate
{
  public partial class TestCases
  {
    public static void Test_OneToManySelect()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      ISessionFactory sessionFactory = cfg.BuildSessionFactory();

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        nations.MoveNext();
        nations.MoveNext();

        List<object> nationValues = GetTableValues("nation", 214, new string[] {"code", "name", "capital", "continent"});
        List<object> athleteValues = GetTableValues("athlete", 5223, new string[] { "nation_code", "gender", "name", "event" });
        Debug.Assert(nations.Current.code == (string)nationValues[0]);
        Debug.Assert(nations.Current.name == (string)nationValues[1]);
        Debug.Assert(nations.Current.capital == (string)nationValues[2]);
        Debug.Assert(nations.Current.continent == (string)nationValues[3]);
        Debug.Assert(nations.Current.Athletes.Count == 1);
        Debug.Assert(nations.Current.Athletes[0].nation_code == (string)athleteValues[0]);
        Debug.Assert(nations.Current.Athletes[0].gender == (string)athleteValues[1]);
        Debug.Assert(nations.Current.Athletes[0].name == (string)athleteValues[2]);
        Debug.Assert(nations.Current.Athletes[0].athlete_event == (string)athleteValues[3]);
      }
    }

    public static void Test_OneToManyInsert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      Nation nation = new Nation
                        {
                          code = "ROM",
                          name = "Romania",
                          capital = "Bucharest",
                          continent = "Europe",
                          Athletes = new List<AthleteOneToMany>()
                        };
      AthleteOneToMany athleteOneToMany = new AthleteOneToMany
                                            {
                                              name = "Lucian Bute",
                                              gender = "M",
                                              nation_code = "ROM",
                                              athlete_event = "Boxing",
                                            };

      ISessionFactory sessionFactory = cfg.BuildSessionFactory();
      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          nation.Athletes.Add(athleteOneToMany);
          session.Save(nation);
          tx.Commit();
        }
      }
      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        while (nations.MoveNext())
        {
          if (nations.Current.code == "ROM")
            break;
        }
        List<object> nationValues = GetTableValues("nation", 216, new string[] { "code", "name", "capital", "continent" });
        List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "nation_code", "gender", "name", "event" });
        Debug.Assert(nations.Current.code == (string)nationValues[0]);
        Debug.Assert(nations.Current.name == (string)nationValues[1]);
        Debug.Assert(nations.Current.capital == (string)nationValues[2]);
        Debug.Assert(nations.Current.continent == (string)nationValues[3]);
        Debug.Assert(nations.Current.Athletes.Count == 1);
        Debug.Assert(nations.Current.Athletes[0].nation_code == (string)athleteValues[0]);
        Debug.Assert(nations.Current.Athletes[0].gender == (string)athleteValues[1]);
        Debug.Assert(nations.Current.Athletes[0].name == (string)athleteValues[2]);
        Debug.Assert(nations.Current.Athletes[0].athlete_event == (string)athleteValues[3]);
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        //Delete a inserted information
        using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Delete(nation);
          trans.Commit();
        }
      }
    }

    public static void Test_OneToManyUpdate()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      Nation nation = new Nation
                        {
                          code = "ROM",
                          name = "Romania",
                          capital = "Budapest",
                          continent = "Europe",
                          Athletes = new List<AthleteOneToMany>()
                        };
      AthleteOneToMany athleteOneToMany = new AthleteOneToMany
                                            {
                                              name = "Lucian Bute",
                                              gender = "M",
                                              nation_code = "ROM",
                                              athlete_event = "Boxing",
                                            };
      nation.Athletes.Add(athleteOneToMany);

      ISessionFactory sessionFactory = cfg.BuildSessionFactory();
      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Save(nation);
          tx.Commit();
        }
      }
      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        while (nations.MoveNext())
        {
          if (nations.Current.code == "ROM")
            break;
        }
        List<object> nationValues = GetTableValues("nation", 216, new string[] { "code", "name", "capital", "continent" });
        List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "nation_code", "gender", "name", "event" });
        Debug.Assert(nations.Current.code == (string)nationValues[0]);
        Debug.Assert(nations.Current.name == (string)nationValues[1]);
        Debug.Assert(nations.Current.capital == (string)nationValues[2]);
        Debug.Assert(nations.Current.continent == (string)nationValues[3]);
        Debug.Assert(nations.Current.Athletes.Count == 1);
        Debug.Assert(nations.Current.Athletes[0].nation_code == (string)athleteValues[0]);
        Debug.Assert(nations.Current.Athletes[0].gender == (string)athleteValues[1]);
        Debug.Assert(nations.Current.Athletes[0].name == (string)athleteValues[2]);
        Debug.Assert(nations.Current.Athletes[0].athlete_event == (string)athleteValues[3]);
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          nation.capital = "Bucharest";
          nation.Athletes[0].name = "Leonard Doroftei";
          session.Update(nation);
          tx.Commit();
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        while (nations.MoveNext())
        {
          if (nations.Current.code == "ROM")
            break;
        }
        List<object> nationValues = GetTableValues("nation", 216, new string[] { "code", "name", "capital", "continent" });
        List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "nation_code", "gender", "name", "event" });
        Debug.Assert(nations.Current.code == (string)nationValues[0]);
        Debug.Assert(nations.Current.name == (string)nationValues[1]);
        Debug.Assert(nations.Current.capital == (string)nationValues[2]);
        Debug.Assert(nations.Current.continent == (string)nationValues[3]);
        Debug.Assert(nations.Current.Athletes.Count == 1);
        Debug.Assert(nations.Current.Athletes[0].nation_code == (string)athleteValues[0]);
        Debug.Assert(nations.Current.Athletes[0].gender == (string)athleteValues[1]);
        Debug.Assert(nations.Current.Athletes[0].name == (string)athleteValues[2]);
        Debug.Assert(nations.Current.Athletes[0].athlete_event == (string)athleteValues[3]);
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        //Delete a inserted information
        using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Delete(nation);
          trans.Commit();
        }
      }
    }

    public static void Test_OneToManyDelete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      ISessionFactory sessionFactory = cfg.BuildSessionFactory();
      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        int count = 0;
        while (nations.MoveNext())
        {
          count++;
        }
        Debug.Assert(count == 215);
      }

      Nation nation = new Nation
                        {
                          code = "ROM",
                          name = "Romania",
                          capital = "Bucharest",
                          continent = "Europe",
                          Athletes = new List<AthleteOneToMany>()
                        };
      AthleteOneToMany athleteOneToMany = new AthleteOneToMany
                                            {
                                              name = "Lucian Bute",
                                              gender = "M",
                                              nation_code = "ROM",
                                              athlete_event = "Boxing",
                                            };
      nation.Athletes.Add(athleteOneToMany);
      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Save(nation);
          tx.Commit();
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        int count = 0;
        while (nations.MoveNext())
        {
          count++;
        }
        Debug.Assert(count == 216);
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        //Delete a inserted information
        using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Delete(nation);
          trans.Commit();
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Nation> nations = session.Query<Nation>().Fetch(b => b.Athletes).GetEnumerator();
        int count = 0;
        while (nations.MoveNext())
        {
          count++;
        }
        Debug.Assert(count == 215);
      }
    }

    public static void Test_ManyToManySelect()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      using(CUBRIDConnection con = TestCases.GetDemodbConnection())
      {
        TestCases.ExecuteSQL("drop table if exists AthleteEvent", con);
        TestCases.ExecuteSQL("create table AthleteEvent (event_code int, athlete_code int, primary key(event_code,athlete_code))", con);
        TestCases.ExecuteSQL("insert into AthleteEvent values(20038, 10011)", con);
        TestCases.ExecuteSQL("insert into AthleteEvent values(20038, 14313)", con);

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (ISession session = sessionFactory.OpenSession())
        {
          IEnumerator<AthleteManyToMany> athletes = session.Query<AthleteManyToMany>().Fetch(b => b.Events).GetEnumerator();
          while (athletes.MoveNext())
          {
            if (athletes.Current.Events.Count != 0)
            {
              List<object> eventValues = GetTableValues("event", 37, new string[] { "code", "name", "sports", "gender", "players" });
              List<object> athleteValues = GetTableValues("athlete", 989, new string[] { "code", "name", "gender", "nation_code", "event" });
              Debug.Assert(athletes.Current.code == (int)athleteValues[0]);
              Debug.Assert(athletes.Current.name == (string)athleteValues[1]);
              Debug.Assert(athletes.Current.gender == (string)athleteValues[2]);
              Debug.Assert(athletes.Current.nation_code == (string)athleteValues[3]);
              Debug.Assert(athletes.Current.athlete_event == (string)athleteValues[4]);
              Debug.Assert(athletes.Current.Events.Count == 1);
              Debug.Assert(athletes.Current.Events[0].code == (int)eventValues[0]);
              Debug.Assert(athletes.Current.Events[0].name == (string)eventValues[1]);
              Debug.Assert(athletes.Current.Events[0].sports == (string)eventValues[2]);
              Debug.Assert(athletes.Current.Events[0].gender == (string)eventValues[3]);
              Debug.Assert(athletes.Current.Events[0].players == (int)eventValues[4]);
              Debug.Assert(athletes.Current.Events[0].Athletes.Count == 2);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].code == (int)athleteValues[0]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].name == (string)athleteValues[1]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].gender == (string)athleteValues[2]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].nation_code == (string)athleteValues[3]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].athlete_event == (string)athleteValues[4]);
              break;
            }
          }
        }
        //Clean database schema
        TestCases.ExecuteSQL("drop table AthleteEvent", con);
      }
    }

    public static void Test_ManyToManyInsert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      using (CUBRIDConnection con = TestCases.GetDemodbConnection())
      {
        TestCases.ExecuteSQL("drop table if exists AthleteEvent", con);
        TestCases.ExecuteSQL("create table AthleteEvent (event_code int, athlete_code int, primary key(event_code,athlete_code))", con);
        TestCases.ExecuteSQL("insert into Event(code) values(20422)", con);

        AthleteManyToMany athleteManyToMany = new AthleteManyToMany
                                                {
                                                  name = "Lucian Bute",
                                                  gender = "M",
                                                  nation_code = "ROM",
                                                  athlete_event = "Boxing",
                                                  Events = new List<Event>()
                                                };
        athleteManyToMany.Events.Add(new Event { code = 20422, sports = "Boxing", name = "70 Kg", gender = "M", players = 2 });

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(athleteManyToMany);
            tx.Commit();
          }
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          IEnumerator<AthleteManyToMany> athletes = session.Query<AthleteManyToMany>().Fetch(b => b.Events).GetEnumerator();
          while (athletes.MoveNext())
          {
            if (athletes.Current.name == "Lucian Bute")
            {
              List<object> eventValues = GetTableValues("event", 423, new string[] { "code", "name", "sports", "gender", "players" });
              List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "code", "name", "gender", "nation_code", "event" });
              Debug.Assert(athletes.Current.code == (int)athleteValues[0]);
              Debug.Assert(athletes.Current.name == (string)athleteValues[1]);
              Debug.Assert(athletes.Current.gender == (string)athleteValues[2]);
              Debug.Assert(athletes.Current.nation_code == (string)athleteValues[3]);
              Debug.Assert(athletes.Current.athlete_event == (string)athleteValues[4]);
              Debug.Assert(athletes.Current.Events.Count == 1);
              Debug.Assert(athletes.Current.Events[0].code == (int)eventValues[0]);
              Debug.Assert(athletes.Current.Events[0].name == (string)eventValues[1]);
              Debug.Assert(athletes.Current.Events[0].sports == (string)eventValues[2]);
              Debug.Assert(athletes.Current.Events[0].gender == (string)eventValues[3]);
              Debug.Assert(athletes.Current.Events[0].players == (int)eventValues[4]);
              Debug.Assert(athletes.Current.Events[0].Athletes.Count == 1);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].name == (string)athleteValues[1]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].gender == (string)athleteValues[2]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].nation_code == (string)athleteValues[3]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].athlete_event == (string)athleteValues[4]);
              break;
            }
          }
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(athleteManyToMany);
            tx.Commit();
          }
        }

        TestCases.ExecuteSQL("drop table AthleteEvent", con);
      }
    }

    public static void Test_ManyToManyUpdate()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      using (CUBRIDConnection con = TestCases.GetDemodbConnection())
      {
        TestCases.ExecuteSQL("drop table if exists AthleteEvent", con);
        TestCases.ExecuteSQL("create table AthleteEvent (event_code int, athlete_code int, primary key(event_code,athlete_code))", con);
        TestCases.ExecuteSQL("insert into Event(code) values(20422)", con);

        AthleteManyToMany athleteManyToMany = new AthleteManyToMany
                                                {
                                                  name = "Lucian Bute",
                                                  gender = "M",
                                                  nation_code = "ROM",
                                                  athlete_event = "Boxing",
                                                  Events = new List<Event>()
                                                };
        athleteManyToMany.Events.Add(new Event { code = 20422, sports = "Boxing", name = "70 Kg", gender = "M", players = 2 });

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(athleteManyToMany);
            tx.Commit();
          }
        }

        athleteManyToMany.name = "Leonard Doroftei";
        athleteManyToMany.Events[0].name = "65 Kg";
        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Update(athleteManyToMany);
            tx.Commit();
          }
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          IEnumerator<AthleteManyToMany> athletes = session.Query<AthleteManyToMany>().Fetch(b => b.Events).GetEnumerator();
          while (athletes.MoveNext())
          {
            if (athletes.Current.name == "Leonard Doroftei")
            {
              List<object> eventValues = GetTableValues("event", 423, new string[] { "code", "name", "sports", "gender", "players" });
              List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "code", "name", "gender", "nation_code", "event" });
              Debug.Assert(athletes.Current.code == (int)athleteValues[0]);
              Debug.Assert(athletes.Current.name == (string)athleteValues[1]);
              Debug.Assert(athletes.Current.gender == (string)athleteValues[2]);
              Debug.Assert(athletes.Current.nation_code == (string)athleteValues[3]);
              Debug.Assert(athletes.Current.athlete_event == (string)athleteValues[4]);
              Debug.Assert(athletes.Current.Events.Count == 1);
              Debug.Assert(athletes.Current.Events[0].code == (int)eventValues[0]);
              Debug.Assert(athletes.Current.Events[0].name == (string)eventValues[1]);
              Debug.Assert(athletes.Current.Events[0].sports == (string)eventValues[2]);
              Debug.Assert(athletes.Current.Events[0].gender == (string)eventValues[3]);
              Debug.Assert(athletes.Current.Events[0].players == (int)eventValues[4]);
              Debug.Assert(athletes.Current.Events[0].Athletes.Count == 1);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].name == (string)athleteValues[1]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].gender == (string)athleteValues[2]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].nation_code == (string)athleteValues[3]);
              Debug.Assert(athletes.Current.Events[0].Athletes[0].athlete_event == (string)athleteValues[4]);
              break;
            }
          }
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(athleteManyToMany);
            tx.Commit();
          }
        }

        TestCases.ExecuteSQL("drop table AthleteEvent", con);
      }
    }

    public static void Test_ManyToManyDelete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      using (CUBRIDConnection con = TestCases.GetDemodbConnection())
      {
        TestCases.ExecuteSQL("drop table if exists AthleteEvent", con);
        TestCases.ExecuteSQL(
          "create table AthleteEvent (event_code int, athlete_code int, primary key(event_code,athlete_code))", con);
        TestCases.ExecuteSQL("insert into Event(code) values(20422)", con);

        AthleteManyToMany athleteManyToMany = new AthleteManyToMany()
        {
          name = "Lucian Bute",
          gender = "M",
          nation_code = "ROM",
          athlete_event = "Boxing",
          Events = new List<Event>()
        };
        athleteManyToMany.Events.Add(new Event() { code = 20422, sports = "Boxing", name = "70 Kg", gender = "M", players = 2 });

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (ISession session = sessionFactory.OpenSession())
        {
          IEnumerator<AthleteManyToMany> athletes = session.Query<AthleteManyToMany>().Fetch(b => b.Events).GetEnumerator();
          int count = 0;
          while (athletes.MoveNext())
          {
            count++;
          }
          Debug.Assert(count == 6677);
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(athleteManyToMany);
            tx.Commit();
          }
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          IEnumerator<AthleteManyToMany> athletes = session.Query<AthleteManyToMany>().Fetch(b => b.Events).GetEnumerator();
          int count = 0;
          while (athletes.MoveNext())
          {
            count++;
          }
          Debug.Assert(count == 6678);
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(athleteManyToMany);
            tx.Commit();
          }
        }

        using (ISession session = sessionFactory.OpenSession())
        {
          IEnumerator<AthleteManyToMany> athletes = session.Query<AthleteManyToMany>().Fetch(b => b.Events).GetEnumerator();
          int count = 0;
          while (athletes.MoveNext())
          {
            count++;
          }
          Debug.Assert(count == 6677);
        }

        TestCases.ExecuteSQL("drop table AthleteEvent", con);
      }
    }

    public static void Test_OneToOneSelect()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      ISessionFactory sessionFactory = cfg.BuildSessionFactory();

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        games.MoveNext();
        List<object> gameValues = GetTableValues("game", 1, new string[] { "host_year", "medal", "game_date" });
        List<object> athleteValues = GetTableValues("athlete", 4660, new string[] { "code", "name", "gender", "nation_code", "event" });
        List<object> nationValues = GetTableValues("nation", 75, new string[] { "code", "name", "capital"});
        List<object> stadiumValues = GetTableValues("stadium", 102, new string[] { "code", "nation_code", "name", "area", "seats", "address"});
        List<object> eventValues = GetTableValues("event", 54, new string[] { "code", "name", "sports", "gender", "players" });
        Debug.Assert(games.Current.host_year == (int)gameValues[0]);
        Debug.Assert(games.Current.medal == (string)gameValues[1]);
        Debug.Assert(games.Current.game_date == (DateTime)gameValues[2]);
        Debug.Assert(games.Current.Nation.code == (string)nationValues[0]);
        Debug.Assert(games.Current.Nation.name == (string)nationValues[1]);
        Debug.Assert(games.Current.Nation.capital == (string)nationValues[2]);
        Debug.Assert(games.Current.Athlete.name == (string)athleteValues[1]);
        Debug.Assert(games.Current.Athlete.gender == (string)athleteValues[2]);
        Debug.Assert(games.Current.Stadium.code == (int)stadiumValues[0]);
        Debug.Assert(games.Current.Stadium.nation_code == (string)stadiumValues[1]);
        Debug.Assert(games.Current.Stadium.name == (string)stadiumValues[2]);
        Debug.Assert(games.Current.Stadium.area == (decimal)stadiumValues[3]);
        Debug.Assert(games.Current.Stadium.seats == (int)stadiumValues[4]);
        Debug.Assert(games.Current.Stadium.address == (string)stadiumValues[5]);
        Debug.Assert(games.Current.Event.code == (int)eventValues[0]);
        Debug.Assert(games.Current.Event.name == (string)eventValues[1]);
        Debug.Assert(games.Current.Event.sports == (string)eventValues[2]);
        Debug.Assert(games.Current.Event.gender == (string)eventValues[3]);
        Debug.Assert(games.Current.Event.players == (int)eventValues[4]);
      }
    }

    public static void Test_OneToOneInsert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      Nation nation = new Nation
                        {
                          code = "ROM",
                          name = "Romania",
                          capital = "Bucharest",
                          continent = "Europe",
                          Athletes = new List<AthleteOneToMany>()
                        };

      Stadium stadium = new Stadium
                          {
                            code = 30141,
                            nation_code = "ROM",
                            name = "National Arena",
                            area = (decimal)120400.00,
                            seats = 55000,
                            address = "Bucharest, Romania"
                          };

      Event game_event = new Event
                           {
                             code = 20422,
                             sports = "Football",
                             name = "UEFA Europa League",
                             gender = "M",
                             players = 11
                           };

      AthleteOneToOne athlete = new AthleteOneToOne
                                  {
                                    name = "Raul Rusescu",
                                    gender = "M",
                                    nation_code = "ROM",
                                    athlete_event = "Football",
                                  };

      Game game = new Game
                    {
                      host_year = 2012,
                      medal = "G",
                      game_date = new DateTime(2012, 5, 8),
                      Athlete = athlete,
                      Event = game_event,
                      Stadium = stadium,
                      Nation = nation
                    };

      ISessionFactory sessionFactory = cfg.BuildSessionFactory();
      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Save(game);
          tx.Commit();
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        while (games.MoveNext())
        {
          if (games.Current.host_year == 2012)
          {
            List<object> gameValues = GetTableValues("game", 8654, new string[] { "host_year", "medal", "game_date" });
            List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "code", "name", "gender", "nation_code", "event" });
            List<object> nationValues = GetTableValues("nation", 216, new string[] { "code", "name", "capital" });
            List<object> stadiumValues = GetTableValues("stadium", 142, new string[] { "code", "nation_code", "name", "area", "seats", "address" });
            List<object> eventValues = GetTableValues("event", 423, new string[] { "code", "name", "sports", "gender", "players" });
            Debug.Assert(games.Current.host_year == (int)gameValues[0]);
            Debug.Assert(games.Current.medal == (string)gameValues[1]);
            Debug.Assert(games.Current.game_date == (DateTime)gameValues[2]);
            Debug.Assert(games.Current.Nation.code == (string)nationValues[0]);
            Debug.Assert(games.Current.Nation.name == (string)nationValues[1]);
            Debug.Assert(games.Current.Nation.capital == (string)nationValues[2]);
            Debug.Assert(games.Current.Athlete.name == (string)athleteValues[1]);
            Debug.Assert(games.Current.Athlete.gender == (string)athleteValues[2]);
            Debug.Assert(games.Current.Stadium.code == (int)stadiumValues[0]);
            Debug.Assert(games.Current.Stadium.nation_code == (string)stadiumValues[1]);
            Debug.Assert(games.Current.Stadium.name == (string)stadiumValues[2]);
            Debug.Assert(games.Current.Stadium.area == (decimal)stadiumValues[3]);
            Debug.Assert(games.Current.Stadium.seats == (int)stadiumValues[4]);
            Debug.Assert(games.Current.Stadium.address == (string)stadiumValues[5]);
            Debug.Assert(games.Current.Event.code == (int)eventValues[0]);
            Debug.Assert(games.Current.Event.name == (string)eventValues[1]);
            Debug.Assert(games.Current.Event.sports == (string)eventValues[2]);
            Debug.Assert(games.Current.Event.gender == (string)eventValues[3]);
            Debug.Assert(games.Current.Event.players == (int)eventValues[4]);
          }
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Delete(game);
          tx.Commit();
        }
      }
    }

    public static void Test_OneToOneUpdate()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      Nation nation = new Nation
                        {
                          code = "ROM",
                          name = "Romania",
                          capital = "Bucharest",
                          continent = "Europe",
                          Athletes = new List<AthleteOneToMany>()
                        };

      Stadium stadium = new Stadium
                          {
                            code = 30141,
                            nation_code = "ROM",
                            name = "National Arena",
                            area = (decimal)120400.00,
                            seats = 55000,
                            address = "Bucharest, Romania"
                          };

      Event game_event = new Event
                           {
                             code = 20422,
                             sports = "Football",
                             name = "UEFA Europa League",
                             gender = "M",
                             players = 11
                           };

      AthleteOneToOne athlete = new AthleteOneToOne
                                  {
                                    name = "Raul Rusescu",
                                    gender = "M",
                                    nation_code = "ROM",
                                    athlete_event = "Football",
                                  };

      Game game = new Game
                    {
                      host_year = 2012,
                      medal = "G",
                      game_date = new DateTime(2012, 5, 8),
                      Athlete = athlete,
                      Event = game_event,
                      Stadium = stadium,
                      Nation = nation
                    };

      ISessionFactory sessionFactory = cfg.BuildSessionFactory();
      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Save(game);
          tx.Commit();
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        while (games.MoveNext())
        {
          if (games.Current.host_year == 2012)
          {
            List<object> gameValues = GetTableValues("game", 8654, new string[] { "host_year", "medal", "game_date" });
            List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "code", "name", "gender", "nation_code", "event" });
            List<object> nationValues = GetTableValues("nation", 216, new string[] { "code", "name", "capital" });
            List<object> stadiumValues = GetTableValues("stadium", 142, new string[] { "code", "nation_code", "name", "area", "seats", "address" });
            List<object> eventValues = GetTableValues("event", 423, new string[] { "code", "name", "sports", "gender", "players" });
            Debug.Assert(games.Current.host_year == (int)gameValues[0]);
            Debug.Assert(games.Current.medal == (string)gameValues[1]);
            Debug.Assert(games.Current.game_date == (DateTime)gameValues[2]);
            Debug.Assert(games.Current.Nation.code == (string)nationValues[0]);
            Debug.Assert(games.Current.Nation.name == (string)nationValues[1]);
            Debug.Assert(games.Current.Nation.capital == (string)nationValues[2]);
            Debug.Assert(games.Current.Athlete.name == (string)athleteValues[1]);
            Debug.Assert(games.Current.Athlete.gender == (string)athleteValues[2]);
            Debug.Assert(games.Current.Stadium.code == (int)stadiumValues[0]);
            Debug.Assert(games.Current.Stadium.nation_code == (string)stadiumValues[1]);
            Debug.Assert(games.Current.Stadium.name == (string)stadiumValues[2]);
            Debug.Assert(games.Current.Stadium.area == (decimal)stadiumValues[3]);
            Debug.Assert(games.Current.Stadium.seats == (int)stadiumValues[4]);
            Debug.Assert(games.Current.Stadium.address == (string)stadiumValues[5]);
            Debug.Assert(games.Current.Event.code == (int)eventValues[0]);
            Debug.Assert(games.Current.Event.name == (string)eventValues[1]);
            Debug.Assert(games.Current.Event.sports == (string)eventValues[2]);
            Debug.Assert(games.Current.Event.gender == (string)eventValues[3]);
            Debug.Assert(games.Current.Event.players == (int)eventValues[4]);
          }
        }
      }

      game.game_date = new DateTime(2013, 4, 19);
      game.Athlete.name = "Ciprian Tatarusanu";
      game.Nation.capital = "Bucuresti";
      game.Stadium.name = "Bucuresti Arena";
      game.Event.name = "UEFA Europa League Final";

      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Update(game);
          tx.Commit();
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        while (games.MoveNext())
        {
          if (games.Current.host_year == 2012)
          {
            List<object> gameValues = GetTableValues("game", 8654, new string[] { "host_year", "medal", "game_date" });
            List<object> athleteValues = GetTableValues("athlete", 6678, new string[] { "code", "name", "gender", "nation_code", "event" });
            List<object> nationValues = GetTableValues("nation", 216, new string[] { "code", "name", "capital" });
            List<object> stadiumValues = GetTableValues("stadium", 142, new string[] { "code", "nation_code", "name", "area", "seats", "address" });
            List<object> eventValues = GetTableValues("event", 423, new string[] { "code", "name", "sports", "gender", "players" });
            Debug.Assert(games.Current.host_year == (int)gameValues[0]);
            Debug.Assert(games.Current.medal == (string)gameValues[1]);
            Debug.Assert(games.Current.game_date == (DateTime)gameValues[2]);
            Debug.Assert(games.Current.Nation.code == (string)nationValues[0]);
            Debug.Assert(games.Current.Nation.name == (string)nationValues[1]);
            Debug.Assert(games.Current.Nation.capital == (string)nationValues[2]);
            Debug.Assert(games.Current.Athlete.name == (string)athleteValues[1]);
            Debug.Assert(games.Current.Athlete.gender == (string)athleteValues[2]);
            Debug.Assert(games.Current.Stadium.code == (int)stadiumValues[0]);
            Debug.Assert(games.Current.Stadium.nation_code == (string)stadiumValues[1]);
            Debug.Assert(games.Current.Stadium.name == (string)stadiumValues[2]);
            Debug.Assert(games.Current.Stadium.area == (decimal)stadiumValues[3]);
            Debug.Assert(games.Current.Stadium.seats == (int)stadiumValues[4]);
            Debug.Assert(games.Current.Stadium.address == (string)stadiumValues[5]);
            Debug.Assert(games.Current.Event.code == (int)eventValues[0]);
            Debug.Assert(games.Current.Event.name == (string)eventValues[1]);
            Debug.Assert(games.Current.Event.sports == (string)eventValues[2]);
            Debug.Assert(games.Current.Event.gender == (string)eventValues[3]);
            Debug.Assert(games.Current.Event.players == (int)eventValues[4]);
          }
        }
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Delete(game);
          tx.Commit();
        }
      }
    }

    public static void Test_OneToOneDelete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);

      Nation nation = new Nation
                        {
                          code = "ROM",
                          name = "Romania",
                          capital = "Bucharest",
                          continent = "Europe",
                          Athletes = new List<AthleteOneToMany>()
                        };

      Stadium stadium = new Stadium
                          {
                            code = 30141,
                            nation_code = "ROM",
                            name = "National Arena",
                            area = (decimal)120400.00,
                            seats = 55000,
                            address = "Bucharest, Romania"
                          };

      Event game_event = new Event
                           {
                             code = 20422,
                             sports = "Football",
                             name = "UEFA Europa League",
                             gender = "M",
                             players = 11
                           };

      AthleteOneToOne athlete = new AthleteOneToOne
                                  {
                                    name = "Raul Rusescu",
                                    gender = "M",
                                    nation_code = "ROM",
                                    athlete_event = "Football",
                                  };

      Game game = new Game
                    {
                      host_year = 2012,
                      medal = "G",
                      game_date = new DateTime(2012, 5, 8),
                      Athlete = athlete,
                      Event = game_event,
                      Stadium = stadium,
                      Nation = nation
                    };

      int count = 0;
      ISessionFactory sessionFactory = cfg.BuildSessionFactory();
      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        while (games.MoveNext())
        {
          count++;
        }
        Debug.Assert(count == 8653);
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Save(game);
          tx.Commit();
        }
      }

      count = 0;
      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        while (games.MoveNext())
        {
          count++;
        }
        Debug.Assert(count == 8654);
      }

      using (ISession session = sessionFactory.OpenSession())
      {
        using (ITransaction tx = session.BeginTransaction(IsolationLevel.ReadUncommitted))
        {
          session.Delete(game);
          tx.Commit();
        }
      }

      count = 0;
      using (ISession session = sessionFactory.OpenSession())
      {
        IEnumerator<Game> games = session.Query<Game>().GetEnumerator();
        while (games.MoveNext())
        {
          count++;
        }
        Debug.Assert(count == 8653);
      }
    }
  }
}