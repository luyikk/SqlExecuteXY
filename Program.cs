//using System;
//using System.Collections.Generic;
//using System.Text;
//using System.Data.SqlClient;
//using System.Threading.Tasks;
//using System.Data;

//namespace SqlXY
//{
//    class Program
//    {
//        static void Main(string[] args)
//        {





//            GetSqlPoolHandler.LogOut += new LogOutHandle(GetSqlPoolHandler_LogOut);

//            //using (SqlExecuteXY obj = new SqlExecuteXY())
//            //{

//            //}


//            //List<T_MNumber> nm = null;


//            //System.Diagnostics.Stopwatch np = new System.Diagnostics.Stopwatch();
//            //np.Start();
//            //using (ODBCExecuteXY sqlobj = new ODBCExecuteXY("ODBCConStr1"))
//            //{
//            //    nm = sqlobj.SqlExcuteSelectObject<T_MNumber>("SELECT * FROM T_MNumber");
//            //}
//            //np.Stop();

//            //Console.WriteLine(np.ElapsedTicks);

//            //int pn = 0;

//            //Parallel.ForEach<T_MNumber>(nm, new Action<T_MNumber>((p) =>
//            //{
//            //    using (SqlExecuteXY obj = new SqlExecuteXY())
//            //    {
//            //        try
//            //        {
//            //            int i = obj.SqlExcuteUpdateOrInsertOrDeleteObject<T_MNumber>("INSERT INTO T_MNumber(DateTime ,SoNumber,Name,WebSite)VALUES(@DateTime,@SoNumber,@Name,@WebSite)", p);

//            //            if (i == 1)
//            //                pn++;

//            //        }
//            //        catch (SqlException e)
//            //        {
//            //            Console.WriteLine(e.Message);
//            //        }
//            //    }

//            //}));


//            //Parallel.ForEach<T_MNumber>(nm, new Action<T_MNumber>((p) =>
//            //    {
//            //using (NpgsqlExecuteXY obj = new NpgsqlExecuteXY("NpgsqlConStr1"))
//            //{

//            //obj.BeginTrans();



//            //try
//            //{
//            //    int i = obj.SqlExcuteUpdateOrInsertOrDeleteObject<T_MNumber>("INSERT INTO T_MNumber(DateTime ,SoNumber,Name,WebSite)VALUES(@DateTime,@SoNumber,@Name,@WebSite)", p);

//            //    //if (i == 1)
//            //    //    pn++;

//            //    obj.CommitTrans();

//            //}
//            //catch (Npgsql.Data.NpgsqlClient.NpgsqlException e)
//            //{
//            //    Console.WriteLine(e.Message);
//            //    obj.RollbackTrans();
//            //}
//            //}

//            //}));




//            //np.Reset();
//            //np.Start();
//            //using (NpgsqlExecuteXY sqlobj = new NpgsqlExecuteXY("NpgsqlConStr1"))
//            //{
//            //    nm = sqlobj.SqlExcuteSelectObject<T_MNumber>("SELECT * FROM T_MNumber");

//            //}
//            //np.Stop();
//            //Console.WriteLine(np.ElapsedTicks);




//            //   Console.WriteLine(nm.Count);



//            //byte[] data = new byte[] { 1, 2, 3, 4, 5, 6 };
//            //using (NpgsqlExecuteXY obj2 = new NpgsqlExecuteXY("NpgsqlConStr2"))
//            //{
//            //    using (NpgsqlExecuteXY obj = new NpgsqlExecuteXY("NpgsqlConStr1"))
//            //    {
//            //        obj.BeginTrans();

//            //        try
//            //        {
//            //            Npgsql.Data.NpgsqlClient.NpgsqlParameter pdata = new Npgsql.Data.NpgsqlClient.NpgsqlParameter("@Data", Npgsql.Data.NpgsqlClient.NpgsqlDbType.Blob, data.Length);
//            //            pdata.Value = data;

//            //            int i = obj.SqlExecuteNonQuery("INSERT INTO Table1(Name,Data)VALUE(@Name,@Data)",
//            //                 new Npgsql.Data.NpgsqlClient.NpgsqlParameter("@Name", "图片666"),
//            //                 pdata);


//            //            obj2.BeginTrans();

//            //            int e = obj2.SqlExecuteNonQuery("INSERT INTO table1(Name,vale)VALUE('123',1775)");

//            //            i = 0;

//            //            if (i == 0 || e == 0)
//            //            {
//            //                obj2.RollbackTrans();
//            //                obj.CommitTrans();
//            //            }
//            //            else
//            //            {
//            //                obj2.CommitTrans();
//            //                obj.CommitTrans();
//            //            }

//            //            Console.WriteLine("影响" + i);
//            //            Console.ReadLine();
//            //        }
//            //        catch
//            //        {
//            //            obj.RollbackTrans();
//            //        }

//            //    }


//            //}


//            //using (SqlXY.NpgSqlExecuteXY obj = new SqlXY.NpgSqlExecuteXY())
//            //{
//            //    obj.BeginTrans();

//            //    obj.SqlExecuteNonQuery("UPDATE accounts SET password=1235566 WHERE account=247501031");

//            //    obj.CommitTrans();

//            //}
//            using (SqlXY.MySqlExecuteXY obj = new SqlXY.MySqlExecuteXY())
//            {
//                const string query = "select * from ItemBase";

//                for (int i = 0; i < 100; i++)
//                {
//                    obj.SqlExcuteSelectObject<ItemBase>(query);
//                }
              

//            }

//            Console.WriteLine("Close");
//            Console.ReadLine();
//        }


//        static void GetSqlPoolHandler_LogOut(string log, object sender)
//        {
//            Console.WriteLine(log);
//        }


//    }

//    //public class UserList
//    //{
//    //    public int Id { get; set; }
//    //    public string UserName { get; set; }
//    //    public string PassWord { get; set; }
//    //    public uint Money { get; set; }
//    //    public uint Static { get; set; }
//    //    public DateTime FullTime { get; set; }
//    //}

//    //public class T_MNumber
//    //{
//    //    public int Id { get; set; }
//    //    public DateTime DateTime { get; set; }
//    //    public string SoNumber { get; set; }
//    //    public string Name { get; set; }
//    //    public string WebSite { get; set; }

//    //}

//    public class ItemBase
//    {
//        public int Id { get; set; }

//        public string Name { get; set; }
//    }


//}
