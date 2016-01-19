using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;
using NHibernate;
using NHibernate.Cfg;

namespace CUBRID.Data.TestNHibernate
{
  public partial class TestCases
  {
    public static void Test_DataTypesStandard_Insert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
        TestCases.ExecuteSQL("create table TestDataTypesStandard (" +
          "c_integer integer," +
          "c_smallint smallint," +
          "c_bigint bigint," +
          "c_numeric numeric(10,2)," +
          "c_float float," +
          "c_decimal decimal(19,5)," +
          "c_double double," +
          "c_char char(1)," +
          "c_varchar varchar(30)," +
          "c_time time," +
          "c_date date," +
          "c_timestamp timestamp," +
          "c_datetime datetime," +
          "c_monetary monetary," +
          "c_string string," +
          "c_bit BIT(8)," +
          "c_varbit bit varying(8)," +
          "primary key (c_integer))", conn);

        TestDataTypesStandard test = new TestDataTypesStandard
          {
            c_bigint = 1,
            c_char = "a",
            c_date = new DateTime(2012, 06, 19),
            c_datetime = new DateTime(2012, 06, 19, 12, 05, 14),
            c_decimal = (decimal)0.5,
            c_double = 1.5,
            c_float = 1.5f,
            c_integer = 14,
            c_monetary = 50,
            c_numeric = (decimal)20.12,
            c_smallint = 1,
            c_string = "qwerty",
            c_time = new DateTime(2012, 06, 19, 12, 05, 14),
            c_timestamp = new DateTime(2012, 06, 19, 12, 05, 14),
            c_varchar = "qwerty",
            c_bit = 1,
            c_varbit = 1,
          };

        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }

          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestDataTypesStandard");
          IList<TestDataTypesStandard> testQuery = query.List<TestDataTypesStandard>();
          Debug.Assert(testQuery.Count == 1);
          Debug.Assert(testQuery[0].c_integer == 14);
          Debug.Assert(testQuery[0].c_bigint == 1);
          Debug.Assert(testQuery[0].c_char == "a");
          Debug.Assert(testQuery[0].c_date == new DateTime(2012, 06, 19));
          Debug.Assert(testQuery[0].c_datetime == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_decimal == (decimal)0.5);
          Debug.Assert(testQuery[0].c_double.Equals(1.5));
          Debug.Assert(testQuery[0].c_float.Equals(1.5f));
          Debug.Assert(testQuery[0].c_monetary == 50);
          Debug.Assert(testQuery[0].c_numeric == (decimal)20.12);
          Debug.Assert(testQuery[0].c_smallint == 1);
          Debug.Assert(testQuery[0].c_string == "qwerty");
          Debug.Assert(testQuery[0].c_time == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_timestamp == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_varchar == "qwerty");
          Debug.Assert(testQuery[0].c_bit == (byte)1);
          Debug.Assert(testQuery[0].c_varbit == (byte)1);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
      }
    }

    public static void Test_DataTypesStandard_Select()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
        TestCases.ExecuteSQL("create table TestDataTypesStandard (" +
          "c_integer integer," +
          "c_smallint smallint," +
          "c_bigint bigint," +
          "c_numeric numeric(10,2)," +
          "c_float float," +
          "c_decimal decimal(19,5)," +
          "c_double double," +
          "c_char char(1)," +
          "c_varchar varchar(30)," +
          "c_time time," +
          "c_date date," +
          "c_timestamp timestamp," +
          "c_datetime datetime," +
          "c_monetary monetary," +
          "c_string string," +
          "c_bit BIT(8)," +
          "c_varbit bit varying(8)," +
          "primary key (c_integer))", conn);

        TestDataTypesStandard test = new TestDataTypesStandard
        {
          c_bigint = 1,
          c_char = "a",
          c_date = new DateTime(2012, 06, 19),
          c_datetime = new DateTime(2012, 06, 19, 12, 05, 14),
          c_decimal = (decimal)0.5,
          c_double = 1.5,
          c_float = 1.5f,
          c_integer = 14,
          c_monetary = 50,
          c_numeric = (decimal)20.12,
          c_smallint = 1,
          c_string = "qwerty",
          c_time = new DateTime(2012, 06, 19, 12, 05, 14),
          c_timestamp = new DateTime(2012, 06, 19, 12, 05, 14),
          c_varchar = "qwerty",
          c_bit = 1,
          c_varbit = 1,
        };

        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }

          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestDataTypesStandard");
          IList<TestDataTypesStandard> testQuery = query.List<TestDataTypesStandard>();
          Debug.Assert(testQuery.Count == 1);
          Debug.Assert(testQuery[0].c_integer == 14);
          Debug.Assert(testQuery[0].c_bigint == 1);
          Debug.Assert(testQuery[0].c_char == "a");
          Debug.Assert(testQuery[0].c_date == new DateTime(2012, 06, 19));
          Debug.Assert(testQuery[0].c_datetime == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_decimal == (decimal)0.5);
          Debug.Assert(testQuery[0].c_double.Equals(1.5));
          Debug.Assert(testQuery[0].c_float.Equals(1.5f));
          Debug.Assert(testQuery[0].c_monetary == 50);
          Debug.Assert(testQuery[0].c_numeric == (decimal)20.12);
          Debug.Assert(testQuery[0].c_smallint == 1);
          Debug.Assert(testQuery[0].c_string == "qwerty");
          Debug.Assert(testQuery[0].c_time == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_timestamp == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_varchar == "qwerty");
          Debug.Assert(testQuery[0].c_bit == (byte)1);
          Debug.Assert(testQuery[0].c_varbit == (byte)1);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
      }
    }

    public static void Test_DataTypesStandard_Update()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
        TestCases.ExecuteSQL("create table TestDataTypesStandard (" +
          "c_integer integer," +
          "c_smallint smallint," +
          "c_bigint bigint," +
          "c_numeric numeric(10,2)," +
          "c_float float," +
          "c_decimal decimal(19,5)," +
          "c_double double," +
          "c_char char(1)," +
          "c_varchar varchar(30)," +
          "c_time time," +
          "c_date date," +
          "c_timestamp timestamp," +
          "c_datetime datetime," +
          "c_monetary monetary," +
          "c_string string," +
          "c_bit BIT(8)," +
          "c_varbit bit varying(8)," +
          "primary key (c_integer))", conn);

        TestDataTypesStandard test = new TestDataTypesStandard
        {
          c_bigint = 1,
          c_char = "a",
          c_date = new DateTime(2012, 06, 19),
          c_datetime = new DateTime(2012, 06, 19, 12, 05, 14),
          c_decimal = (decimal)0.5,
          c_double = 1.5,
          c_float = 1.5f,
          c_integer = 14,
          c_monetary = 50,
          c_numeric = (decimal)20.12,
          c_smallint = 1,
          c_string = "qwerty",
          c_time = new DateTime(2012, 06, 19, 12, 05, 14),
          c_timestamp = new DateTime(2012, 06, 19, 12, 05, 14),
          c_varchar = "qwerty",
          c_bit = 1,
          c_varbit = 1,
        };

        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }

          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestDataTypesStandard");
          IList<TestDataTypesStandard> testQuery = query.List<TestDataTypesStandard>();
          Debug.Assert(testQuery.Count == 1);
          Debug.Assert(testQuery[0].c_integer == 14);
          Debug.Assert(testQuery[0].c_bigint == 1);
          Debug.Assert(testQuery[0].c_char == "a");
          Debug.Assert(testQuery[0].c_date == new DateTime(2012, 06, 19));
          Debug.Assert(testQuery[0].c_datetime == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_decimal == (decimal)0.5);
          Debug.Assert(testQuery[0].c_double.Equals(1.5));
          Debug.Assert(testQuery[0].c_float.Equals(1.5f));
          Debug.Assert(testQuery[0].c_monetary == 50);
          Debug.Assert(testQuery[0].c_numeric == (decimal)20.12);
          Debug.Assert(testQuery[0].c_smallint == 1);
          Debug.Assert(testQuery[0].c_string == "qwerty");
          Debug.Assert(testQuery[0].c_time == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_timestamp == new DateTime(2012, 06, 19, 12, 05, 14));
          Debug.Assert(testQuery[0].c_varchar == "qwerty");
          Debug.Assert(testQuery[0].c_bit == (byte)1);
          Debug.Assert(testQuery[0].c_varbit == (byte)1);

          TestDataTypesStandard pGet = session.Get<TestDataTypesStandard>(test.c_integer);
          pGet.c_bigint = 2;
          pGet.c_char = "b";
          pGet.c_date = new DateTime(2013, 07, 20);
          pGet.c_datetime = new DateTime(2013, 07, 20, 12, 0, 0);
          pGet.c_decimal = (decimal)1.5;
          pGet.c_double = 2.5;
          pGet.c_float = 2.5f;
          pGet.c_monetary = 100;
          pGet.c_numeric = (decimal)25.12;
          pGet.c_smallint = 2;
          pGet.c_string = "updated";
          pGet.c_time = new DateTime(2012, 06, 19, 12, 0, 0);
          pGet.c_timestamp = new DateTime(2013, 07, 20, 12, 0, 0);
          pGet.c_varchar = "updated";
          pGet.c_bit = 0;
          pGet.c_varbit = 0;
          //Update
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Update(pGet);
            trans.Commit();
          }

          //Retrieve the updated information
          IQuery queryUpdated = session.CreateQuery("FROM TestDataTypesStandard");
          IList<TestDataTypesStandard> testQueryUpdated = queryUpdated.List<TestDataTypesStandard>();
          Debug.Assert(testQueryUpdated.Count == 1);
          Debug.Assert(testQueryUpdated[0].c_integer == 14);
          Debug.Assert(testQueryUpdated[0].c_bigint == 2);
          Debug.Assert(testQueryUpdated[0].c_char == "b");
          Debug.Assert(testQueryUpdated[0].c_date == new DateTime(2013, 07, 20));
          Debug.Assert(testQueryUpdated[0].c_datetime == new DateTime(2013, 07, 20, 12, 0, 0));
          Debug.Assert(testQueryUpdated[0].c_decimal == (decimal)1.5);
          Debug.Assert(testQueryUpdated[0].c_double.Equals(2.5));
          Debug.Assert(testQueryUpdated[0].c_float.Equals(2.5f));
          Debug.Assert(testQueryUpdated[0].c_monetary == 100);
          Debug.Assert(testQueryUpdated[0].c_numeric == (decimal)25.12);
          Debug.Assert(testQueryUpdated[0].c_smallint == 2);
          Debug.Assert(testQueryUpdated[0].c_string == "updated");
          Debug.Assert(testQueryUpdated[0].c_time == new DateTime(2012, 06, 19, 12, 0, 0));
          Debug.Assert(testQueryUpdated[0].c_timestamp == new DateTime(2013, 07, 20, 12, 0, 0));
          Debug.Assert(testQueryUpdated[0].c_varchar == "updated");
          Debug.Assert(testQuery[0].c_bit == (byte)0);
          Debug.Assert(testQuery[0].c_varbit == (byte)0);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
      }
    }

    public static void Test_DataTypesStandard_Delete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
        TestCases.ExecuteSQL("create table TestDataTypesStandard (" +
          "c_integer integer," +
          "c_smallint smallint," +
          "c_bigint bigint," +
          "c_numeric numeric(10,2)," +
          "c_float float," +
          "c_decimal decimal(19,5)," +
          "c_double double," +
          "c_char char(1)," +
          "c_varchar varchar(30)," +
          "c_time time," +
          "c_date date," +
          "c_timestamp timestamp," +
          "c_datetime datetime," +
          "c_monetary monetary," +
          "c_string string," +
          "c_bit BIT(8)," +
          "c_varbit bit varying(8)," +
          "primary key (c_integer))", conn);

        TestDataTypesStandard test = new TestDataTypesStandard
        {
          c_bigint = 1,
          c_char = "a",
          c_date = new DateTime(2012, 06, 19),
          c_datetime = new DateTime(2012, 06, 19, 12, 05, 14),
          c_decimal = (decimal)0.5,
          c_double = 1.5,
          c_float = 1.5f,
          c_integer = 14,
          c_monetary = 50,
          c_numeric = (decimal)20.12,
          c_smallint = 1,
          c_string = "qwerty",
          c_time = new DateTime(2012, 06, 19, 12, 05, 14),
          c_timestamp = new DateTime(2012, 06, 19, 12, 05, 14),
          c_varchar = "qwerty",
          c_bit = 1,
          c_varbit = 1,
        };

        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }

          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestDataTypesStandard");
          IList<TestDataTypesStandard> testQuery = query.List<TestDataTypesStandard>();
          Debug.Assert(testQuery.Count == 1);

          //Delete the inserted information
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(test);
            trans.Commit();
          }

          IQuery queryAfterDelete = session.CreateQuery("FROM TestDataTypesStandard");
          IList<TestDataTypesStandard> testQueryAfterDelete = queryAfterDelete.List<TestDataTypesStandard>();
          Debug.Assert(testQueryAfterDelete.Count == 0);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestDataTypesStandard", conn);
      }
    }
  }
}