using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using NHibernate;
using NHibernate.Cfg;
using CUBRID.Data.CUBRIDClient;
using CUBRID.Data.Driver;
using System.Diagnostics;

namespace Cubrid.Data.Test.Nhibernate
{

    public class athlete
    {
        virtual public int code { get; set; }
        virtual public string Name { get; set; }
        virtual public string Gender { get; set; }
        virtual public string NationCode { get; set; }
        virtual public string Event { get; set; }

        virtual public void Print()
        {
            Console.WriteLine(
                string.Format("|{0}|{1}|{2}|{3}|",
            AlignCentre(Name, 30),
            AlignCentre(Gender, 3),
            AlignCentre(NationCode, 5),
            AlignCentre(Event, 15)));
        }

        static string AlignCentre(string text, int width)
        {
            if (string.IsNullOrEmpty(text))
            {
                return new string(' ', width);
            }
            else
            {
                return text.PadRight(width - (width - text.Length) / 2).PadLeft(width);
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            athlete John = new athlete { code = 17000, Name = "John Terry", Gender = "M", NationCode = "GBR", Event = "Football" };

            John.Print();
            
            //lets insert John into the database
            try
            {
                using (ISession session = OpenSession())
                {
                    using (ITransaction transaction = session.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                    {

                        session.Save(John);

                        transaction.Commit();
                        Console.WriteLine("Saved John to database\n");
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e); }

            //let's read the last 10 athletes in the database
            using (ISession session = OpenSession())
            {
                IQuery query = session.CreateQuery("FROM athlete WHERE rownum < 11 order by code desc");
                IList<athlete> athlete = query.List<athlete>();
                Console.Out.WriteLine("athlete.Count = " + athlete.Count + "\n");
                athlete.ToList().ForEach(p => p.Print());
                athlete a = athlete.ToArray()[0];
                Debug.Assert(a.Name.Equals("John Terry"));
                Debug.Assert(a.Gender.Equals("M"));
                Debug.Assert(a.NationCode.Equals("GBR"));
                Debug.Assert(a.Event.Equals("Football"));
            }

            //let's update our athlete in the database
            using (ISession session = OpenSession())
            {
                using (ITransaction transaction = session.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                {
                    IQuery query = session.CreateQuery("FROM athlete WHERE name = 'John Terry'");
                    athlete athlete = query.List<athlete>()[0];
                    athlete.Name = "Wayne Rooney";
                    transaction.Commit();
                }
                Console.WriteLine("Updated John. His name is now Wayne.\n");
            }

            //let's see
            //let's read the last 10 athletes in the database (again)
            using (ISession session = OpenSession())
            {
                IQuery query = session.CreateQuery("FROM athlete WHERE rownum < 11 order by code desc");
                IList<athlete> athlete = query.List<athlete>();
                Console.Out.WriteLine("athlete.Count = " + athlete.Count + "\n");
                athlete.ToList().ForEach(p => p.Print());
                athlete a = athlete.ToArray()[0];
                Debug.Assert(a.Name.Equals("Wayne Rooney"));
                Debug.Assert(a.Gender.Equals("M"));
                Debug.Assert(a.NationCode.Equals("GBR"));
                Debug.Assert(a.Event.Equals("Football"));
            }

            //let's delete our athlete from the database
            using (ISession session = OpenSession())
            {
                using (ITransaction transaction = session.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted))
                {
                    IQuery query = session.CreateQuery("FROM athlete WHERE Name = 'Wayne Rooney'");
                    athlete athlete = query.List<athlete>()[0];
                    session.Delete(athlete);
                    transaction.Commit();
                }
                Console.WriteLine("Deleted Wayne Rooney from Database.\n");
            }

            //let's see
            //let's read the last 10 athletes in the database (again)
            using (ISession session = OpenSession())
            {
                IQuery query = session.CreateQuery("FROM athlete WHERE rownum < 11 order by code desc");
                IList<athlete> athlete = query.List<athlete>();
                Console.Out.WriteLine("athlete.Count = " + athlete.Count + "\n");
                athlete.ToList().ForEach(p => p.Print());
                athlete a = athlete.ToArray()[0];
                Debug.Assert(a.Name.Equals("Zulianello Clavdio F"));
                Debug.Assert(a.Gender.Equals("M"));
                Debug.Assert(a.NationCode.Equals("ARG"));
                Debug.Assert(a.Event.Equals("Volleyball"));
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }
        static ISessionFactory SessionFactory;

        static ISession OpenSession()
        {
            if (SessionFactory == null) //not threadsafe
            { 
                //SessionFactories are expensive, create only once
                Configuration configuration = new Configuration();
                configuration.AddAssembly(Assembly.GetCallingAssembly());
                SessionFactory = configuration.BuildSessionFactory();
            }
            return SessionFactory.OpenSession();
        }
    }
}
