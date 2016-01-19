using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using CUBRID.Data.CUBRIDClient;
using NHibernate;
using NHibernate.Cfg;
using System.IO;

namespace CUBRID.Data.TestNHibernate
{
  public partial class TestCases
  {
    public static void Test_CUBRIDBlob_Insert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDBlob(c_integer int not null auto_increment," +
                             "c_blob BLOB," +
                              "primary key (c_integer))", conn);

        TestCUBRIDBlobType test = new TestCUBRIDBlobType
        {
          c_blob = new CUBRIDBlob(conn)
        };

        BinaryReader origianlFileReader = new BinaryReader(File.Open("../../CUBRID.ico", FileMode.Open));
        byte[] bytesOriginalData = origianlFileReader.ReadBytes((int)origianlFileReader.BaseStream.Length);
        origianlFileReader.Close();
        test.c_blob.SetBytes(1, bytesOriginalData);
        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }
        }

        const string sql2 = "SELECT c_blob from TestCUBRIDBlob";
        CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn);
        DbDataReader reader = cmd2.ExecuteReader();
        while (reader.Read())
        {
          CUBRIDBlob bImage = (CUBRIDBlob)reader[0];
          byte[] bytesRetrievedData = bImage.GetBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytesOriginalData.Length == bytesRetrievedData.Length);
          Debug.Assert(bytesOriginalData[0] == bytesRetrievedData[0]);
          Debug.Assert(bytesOriginalData[bytesOriginalData.Length - 1] == bytesRetrievedData[bytesRetrievedData.Length - 1]);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
      }
    }

    public static void Test_CUBRIDBlob_Select()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDBlob(c_integer int not null auto_increment," +
                             "c_blob BLOB," +
                              "primary key (c_integer))", conn);

        const string sql = "insert into TestCUBRIDBlob values(1, ?)";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        CUBRIDBlob Blob = new CUBRIDBlob(conn);

        BinaryReader originalFileReader = new BinaryReader(File.Open("../../CUBRID.ico", FileMode.Open));
        byte[] bytesOriginalData = originalFileReader.ReadBytes((int)originalFileReader.BaseStream.Length);
        originalFileReader.Close();
        Blob.SetBytes(1, bytesOriginalData);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?p";
        param.Value = Blob;
        cmd.Parameters.Add(param);
        cmd.Parameters[0].DbType = DbType.Binary;
        cmd.ExecuteNonQuery();
        cmd.Close();

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestCUBRIDBlobType");
          IList<TestCUBRIDBlobType> testQuery = query.List<TestCUBRIDBlobType>();
          Debug.Assert(testQuery[0].c_integer == 1);
          CUBRIDBlob bImage = testQuery[0].c_blob;
          byte[] bytesRetrievedData = bImage.GetBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytesOriginalData.Length == bytesRetrievedData.Length);
          Debug.Assert(bytesOriginalData[0] == bytesRetrievedData[0]);
          Debug.Assert(bytesOriginalData[bytesOriginalData.Length - 1] == bytesRetrievedData[bytesRetrievedData.Length - 1]);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
      }
    }

    public static void Test_CUBRIDBlob_Update()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDBlob(c_integer int not null auto_increment," +
                             "c_blob BLOB," +
                              "primary key (c_integer))", conn);

        TestCUBRIDBlobType test = new TestCUBRIDBlobType
        {
          c_blob = new CUBRIDBlob(conn)
        };

        BinaryReader originalFileReader = new BinaryReader(File.Open("../../CUBRIDOld.ico", FileMode.Open));
        byte[] bytesOriginalData = originalFileReader.ReadBytes((int)originalFileReader.BaseStream.Length);
        originalFileReader.Close();
        test.c_blob.SetBytes(1, bytesOriginalData);
        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }

          TestCUBRIDBlobType pGet = session.Get<TestCUBRIDBlobType>(test.c_integer);
          pGet.c_blob = new CUBRIDBlob(conn);

          BinaryReader updateFileReader = new BinaryReader(File.Open("../../CUBRID.ico", FileMode.Open));
          byte[] bytesUpdatedData = updateFileReader.ReadBytes((int)updateFileReader.BaseStream.Length);
          updateFileReader.Close();
          test.c_blob.SetBytes(1, bytesUpdatedData);
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Update(pGet);
            trans.Commit();
          }

          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestCUBRIDBlobType");
          IList<TestCUBRIDBlobType> testQuery = query.List<TestCUBRIDBlobType>();
          Debug.Assert(testQuery[0].c_integer == 1);
          CUBRIDBlob bImage = testQuery[0].c_blob;
          byte[] bytesRetrievedData = bImage.GetBytes(1, (int)bImage.BlobLength);

          Debug.Assert(bytesUpdatedData.Length == bytesRetrievedData.Length);
          Debug.Assert(bytesUpdatedData[0] == bytesRetrievedData[0]);
          Debug.Assert(bytesUpdatedData[bytesUpdatedData.Length - 1] == bytesRetrievedData[bytesRetrievedData.Length - 1]);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
      }
    }

    public static void Test_CUBRIDBlob_Delete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDBlob(c_integer int not null auto_increment," +
                             "c_blob BLOB," +
                              "primary key (c_integer))", conn);

        TestCUBRIDBlobType test = new TestCUBRIDBlobType
        {
          c_blob = new CUBRIDBlob(conn)
        };

        BinaryReader originalFileReader = new BinaryReader(File.Open("../../CUBRID.ico", FileMode.Open));
        byte[] bytesOriginalData = originalFileReader.ReadBytes((int)originalFileReader.BaseStream.Length);
        originalFileReader.Close();
        test.c_blob.SetBytes(1, bytesOriginalData);
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
          IQuery query = session.CreateQuery("FROM TestCUBRIDBlobType");
          IList<TestCUBRIDBlobType> testQuery = query.List<TestCUBRIDBlobType>();
          Debug.Assert(testQuery.Count == 1);

          //Delete the inserted information
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(test);
            trans.Commit();
          }

          IQuery queryAfterDelete = session.CreateQuery("FROM TestCUBRIDBlobType");
          IList<TestCUBRIDBlobType> testQueryAfterDelete = queryAfterDelete.List<TestCUBRIDBlobType>();
          Debug.Assert(testQueryAfterDelete.Count == 0);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
      }
    }

    public static void Test_CUBRIDClob_Insert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDBlobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDClob(c_integer int not null auto_increment," +
                             "c_clob CLOB," +
                              "primary key (c_integer))", conn);


        TestCUBRIDClobType test = new TestCUBRIDClobType
        {
          c_clob = new CUBRIDClob(conn)
        };

        StreamReader originalFileReader = new StreamReader("../../BSD License.txt");
        string clobStringToInsert = originalFileReader.ReadToEnd();
        originalFileReader.Close();
        test.c_clob.SetString(1, clobStringToInsert);
        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }
        }

        const string sql2 = "SELECT c_clob from TestCUBRIDClob";
        CUBRIDCommand cmd2 = new CUBRIDCommand(sql2, conn);
        DbDataReader reader = cmd2.ExecuteReader();
        while (reader.Read())
        {
          CUBRIDClob cString = (CUBRIDClob)reader[0];
          string clobStringInserted = cString.GetString(1, (int)cString.ClobLength);

          Debug.Assert(clobStringToInsert.Length == clobStringInserted.Length);
          Debug.Assert(clobStringToInsert == clobStringInserted);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
      }
    }

    public static void Test_CUBRIDClob_Select()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDClobType).Assembly);
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDClob(c_integer int not null auto_increment," +
                             "c_clob CLOB," +
                              "primary key (c_integer))", conn);

        const string sql = "insert into TestCUBRIDClob values(1, ?)";
        CUBRIDCommand cmd = new CUBRIDCommand(sql, conn);
        CUBRIDClob Clob = new CUBRIDClob(conn);

        StreamReader originalFileReader = new StreamReader("../../BSD License.txt");
        string clobStringToInsert = originalFileReader.ReadToEnd();
        originalFileReader.Close();
        Clob.SetString(1, clobStringToInsert);

        CUBRIDParameter param = new CUBRIDParameter();
        param.ParameterName = "?p";
        param.Value = Clob;
        cmd.Parameters.Add(param);
        cmd.Parameters[0].DbType = DbType.AnsiString;
        cmd.ExecuteNonQuery();
        cmd.Close();

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestCUBRIDClobType");
          IList<TestCUBRIDClobType> testQuery = query.List<TestCUBRIDClobType>();
          Debug.Assert(testQuery[0].c_integer == 1);
          CUBRIDClob bImage = testQuery[0].c_clob;
          string clobInserted = bImage.GetString(1, (int)testQuery[0].c_clob.ClobLength);

          Debug.Assert(clobStringToInsert.Length == clobInserted.Length);
          Debug.Assert(clobStringToInsert == clobInserted);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
      }
    }

    public static void Test_CUBRIDClob_Update()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDClobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDClob(c_integer int not null auto_increment," +
                             "c_clob CLOB," +
                              "primary key (c_integer))", conn);

        TestCUBRIDClobType test = new TestCUBRIDClobType
        {
          c_clob = new CUBRIDClob(conn)
        };

        StreamReader originalFileReader = new StreamReader("../../BSD License.txt");
        string clobStringToInsert = originalFileReader.ReadToEnd();
        originalFileReader.Close();
        test.c_clob.SetString(1, clobStringToInsert);
        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(test);
            trans.Commit();
          }

          TestCUBRIDClobType pGet = session.Get<TestCUBRIDClobType>(test.c_integer);
          pGet.c_clob = new CUBRIDClob(conn);

          StreamReader updateFileReader = new StreamReader("../../BSD License Updated.txt");
          string clobStringToUpdate = updateFileReader.ReadToEnd();
          updateFileReader.Close();
          test.c_clob.SetString(1, clobStringToUpdate);
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Update(pGet);
            trans.Commit();
          }

          //Retrieve the inserted information
          IQuery query = session.CreateQuery("FROM TestCUBRIDClobType");
          IList<TestCUBRIDClobType> testQuery = query.List<TestCUBRIDClobType>();
          Debug.Assert(testQuery[0].c_integer == 1);

          string clobStringUpdated = testQuery[0].c_clob.GetString(1, (int)testQuery[0].c_clob.ClobLength);
          Debug.Assert(clobStringToUpdate.Length == clobStringUpdated.Length);
          Debug.Assert(clobStringToUpdate == clobStringUpdated);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDBlob", conn);
      }
    }

    public static void Test_CUBRIDClob_Delete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDClobType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDClob(c_integer int not null auto_increment," +
                             "c_clob CLOB," +
                              "primary key (c_integer))", conn);

        TestCUBRIDClobType test = new TestCUBRIDClobType
        {
          c_clob = new CUBRIDClob(conn)
        };

        StreamReader updateFileReader = new StreamReader("../../BSD License.txt");
        string clobStringToInsert = updateFileReader.ReadToEnd();
        updateFileReader.Close();
        test.c_clob.SetString(1, clobStringToInsert);
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
          IQuery query = session.CreateQuery("FROM TestCUBRIDClobType");
          IList<TestCUBRIDClobType> testQuery = query.List<TestCUBRIDClobType>();
          Debug.Assert(testQuery.Count == 1);

          //Delete the inserted information
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(test);
            trans.Commit();
          }

          IQuery queryAfterDelete = session.CreateQuery("FROM TestCUBRIDClobType");
          IList<TestCUBRIDClobType> testQueryAfterDelete = queryAfterDelete.List<TestCUBRIDClobType>();
          Debug.Assert(testQueryAfterDelete.Count == 0);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDClob", conn);
      }
    }

    public static void Test_CUBRIDCollections_Select()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDCollectionType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDCollectionType(id int not null auto_increment," +
                             "sequence_column sequence(int), " +
                             "set_column set(int), " +
                             "multiset_column multiset(string)," +
                              "primary key (id))", conn);
        //Insert some data in the sequence column
        TestCases.ExecuteSQL("INSERT INTO TestCUBRIDCollectionType(sequence_column, set_column, multiset_column)" +
           "VALUES({0, 1, 2, 3}, { 4, 5, 6, 7}, { 'CUBRID', 'qwerty' })", conn);

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          //Retrieve the inserted information
          IQuery query = session.CreateQuery(" from TestCUBRIDCollectionType");
          IList<TestCUBRIDCollectionType> testQuery = query.List<TestCUBRIDCollectionType>();

          int[] expectedSequence = { 0, 1, 2, 3 };
          int[] expectedSet = { 4, 5, 6, 7 };
          string[] expectedMultiset = { "CUBRID", "qwerty" };
          Debug.Assert((int)testQuery[0].id == 1);
          Debug.Assert((int)testQuery[0].seq[0] == expectedSequence[0]);
          Debug.Assert((int)testQuery[0].seq[1] == expectedSequence[1]);
          Debug.Assert((int)testQuery[0].seq[2] == expectedSequence[2]);
          Debug.Assert((int)testQuery[0].seq[3] == expectedSequence[3]);
          Debug.Assert((int)testQuery[0].set[0] == expectedSet[0]);
          Debug.Assert((int)testQuery[0].set[1] == expectedSet[1]);
          Debug.Assert((int)testQuery[0].set[2] == expectedSet[2]);
          Debug.Assert((int)testQuery[0].set[3] == expectedSet[3]);
          Debug.Assert((string)testQuery[0].multiset[0] == expectedMultiset[0]);
          Debug.Assert((string)testQuery[0].multiset[1] == expectedMultiset[1]);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
      }
    }

    public static void Test_CUBRIDCollections_Insert()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDCollectionType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDCollectionType(id int not null auto_increment," +
                             "sequence_column sequence(int), " +
                             "set_column set(int), " +
                             "multiset_column multiset(string)," +
                              "primary key (id))", conn);

        object[] insertSequence = { 0, 1, 2, 3 };
        object[] insertSet = { 4, 5, 6, 7 };
        object[] insertMultiset = { "CUBRID", "qwerty" };
        TestCUBRIDCollectionType testCollections = new TestCUBRIDCollectionType()
        {
          seq = insertSequence,
          set = insertSet,
          multiset = insertMultiset
        };

        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(testCollections);
            trans.Commit();
          }
        }

        //Check the the insert performed correctly
        using (var session = sessionFactory.OpenSession())
        {
          //Retrieve the inserted information
          IQuery query = session.CreateQuery(" from TestCUBRIDCollectionType");
          IList<TestCUBRIDCollectionType> testQuery = query.List<TestCUBRIDCollectionType>();

          int[] expectedSequence = { 0, 1, 2, 3 };
          int[] expectedSet = { 4, 5, 6, 7 };
          string[] expectedMultiset = { "CUBRID", "qwerty" };
          Debug.Assert((int)testQuery[0].id == 1);
          Debug.Assert((int)testQuery[0].seq[0] == expectedSequence[0]);
          Debug.Assert((int)testQuery[0].seq[1] == expectedSequence[1]);
          Debug.Assert((int)testQuery[0].seq[2] == expectedSequence[2]);
          Debug.Assert((int)testQuery[0].seq[3] == expectedSequence[3]);
          Debug.Assert((int)testQuery[0].set[0] == expectedSet[0]);
          Debug.Assert((int)testQuery[0].set[1] == expectedSet[1]);
          Debug.Assert((int)testQuery[0].set[2] == expectedSet[2]);
          Debug.Assert((int)testQuery[0].set[3] == expectedSet[3]);
          Debug.Assert((string)testQuery[0].multiset[0] == expectedMultiset[0]);
          Debug.Assert((string)testQuery[0].multiset[1] == expectedMultiset[1]);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
      }
    }

    public static void Test_CUBRIDCollections_Update()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDCollectionType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDCollectionType(id int not null auto_increment," +
                             "sequence_column sequence(int), " +
                             "set_column set(int), " +
                             "multiset_column multiset(string)," +
                              "primary key (id))", conn);

        object[] insertSequence = { 0, 1, 2, 3 };
        object[] insertSet = { 4, 5, 6, 7 };
        object[] insertMultiset = { "CUBRID", "qwerty" };
        TestCUBRIDCollectionType testCollections = new TestCUBRIDCollectionType()
        {
          seq = insertSequence,
          set = insertSet,
          multiset = insertMultiset
        };

        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(testCollections);
            trans.Commit();
          }
        }

        object[] updateSequence = { 10, 11, 12, 13 };
        object[] updateSet = { 14, 15, 16, 17 };
        object[] updateMultiset = {  "ADO.NET", "NHibernate" };
        testCollections.seq = updateSequence;
        testCollections.set = updateSet;
        testCollections.multiset = updateMultiset;

        //Update
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Update(testCollections);
            trans.Commit();
          }
        }

        //Check the the insert performed correctly
        using (var session = sessionFactory.OpenSession())
        {
          //Retrieve the inserted information
          IQuery query = session.CreateQuery(" from TestCUBRIDCollectionType");
          IList<TestCUBRIDCollectionType> testQuery = query.List<TestCUBRIDCollectionType>();

          int[] expectedSequence = { 10, 11, 12, 13 };
          int[] expectedSet = { 14, 15, 16, 17 };
          string[] expectedMultiset = { "ADO.NET", "NHibernate" };
          Debug.Assert((int)testQuery[0].id == 1);
          Debug.Assert((int)testQuery[0].seq[0] == expectedSequence[0]);
          Debug.Assert((int)testQuery[0].seq[1] == expectedSequence[1]);
          Debug.Assert((int)testQuery[0].seq[2] == expectedSequence[2]);
          Debug.Assert((int)testQuery[0].seq[3] == expectedSequence[3]);
          Debug.Assert((int)testQuery[0].set[0] == expectedSet[0]);
          Debug.Assert((int)testQuery[0].set[1] == expectedSet[1]);
          Debug.Assert((int)testQuery[0].set[2] == expectedSet[2]);
          Debug.Assert((int)testQuery[0].set[3] == expectedSet[3]);
          Debug.Assert((string)testQuery[0].multiset[0] == expectedMultiset[0]);
          Debug.Assert((string)testQuery[0].multiset[1] == expectedMultiset[1]);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
      }
    }

    public static void Test_CUBRIDCollections_Delete()
    {
      Configuration cfg = (new Configuration()).Configure().AddAssembly(typeof(TestCUBRIDCollectionType).Assembly);
      //Create the database schema
      using (CUBRIDConnection conn = new CUBRIDConnection(cfg.GetProperty(NHibernate.Cfg.Environment.ConnectionString)))
      {
        conn.Open();
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
        TestCases.ExecuteSQL("create table TestCUBRIDCollectionType(id int not null auto_increment," +
                             "sequence_column sequence(int), " +
                             "set_column set(int), " +
                             "multiset_column multiset(string)," +
                              "primary key (id))", conn);

        object[] insertSequence = { 0, 1, 2, 3 };
        object[] insertSet = { 4, 5, 6, 7 };
        object[] insertMultiset = { "CUBRID", "qwerty" };
        TestCUBRIDCollectionType testCollections = new TestCUBRIDCollectionType()
        {
          seq = insertSequence,
          set = insertSet,
          multiset = insertMultiset
        };

        //Insert
        ISessionFactory sessionFactory = cfg.BuildSessionFactory();
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Save(testCollections);
            trans.Commit();
          }
        }

        //Check the the insert performed correctly
        using (var session = sessionFactory.OpenSession())
        {
          //Retrieve the inserted information
          IQuery queryAfterInsert = session.CreateQuery(" from TestCUBRIDCollectionType");
          IList<TestCUBRIDCollectionType> testQueryAfterInsert = queryAfterInsert.List<TestCUBRIDCollectionType>();

          Debug.Assert(testQueryAfterInsert.Count == 1);
        }

        //Delete the inserted information
        using (var session = sessionFactory.OpenSession())
        {
          using (var trans = session.BeginTransaction(IsolationLevel.ReadUncommitted))
          {
            session.Delete(testCollections);
            trans.Commit();
          }
        }

        using (var session = sessionFactory.OpenSession())
        {
          IQuery queryAfterDelete = session.CreateQuery("FROM TestCUBRIDCollectionType");
          IList<TestCUBRIDClobType> testQueryAfterDelete = queryAfterDelete.List<TestCUBRIDClobType>();
          Debug.Assert(testQueryAfterDelete.Count == 0);
        }

        //Clean the database schema
        TestCases.ExecuteSQL("drop table if exists TestCUBRIDCollectionType", conn);
      }
    }
  }
}
