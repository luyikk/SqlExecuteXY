//by luyikk 2010.5.9
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Configuration;

namespace SqlXY
{
    public class OleDbExecuteXY:ListDeserializerBase,IDisposable
    {
        #region 静态
        /// <summary>
        /// CONN连接对象池
        /// </summary>
        public static Dictionary<string, ObjectPool<System.Data.OleDb.OleDbConnection>> DBConnPool { get; private set; }
        public static ObjectPool<OleDbCommand> DBCommandPool { get; private set; }




        static OleDbExecuteXY()
        {
            DBConnPool = GetSqlPoolHandler.GetOleDbConnInstance();         
            DBCommandPool = GetSqlPoolHandler.GetOleDbCommandInstance();
            
        }

        #endregion

        private object LockThis=new object();

        /// <summary>
        /// 数据库连接器
        /// </summary>
        public OleDbConnection DBConn { get; protected set; }

        /// <summary>
        /// 命令
        /// </summary>
        public OleDbCommand Command { get; protected set; }

        private OleDbTransaction trans { get; set; }

        public string Key { get; private set; }
       

        public OleDbExecuteXY()
        {

            DBConn = OleDbExecuteXY.DBConnPool["DefautConnectionString"].GetObject();
            if (DBConn == null)
            {
                throw new Exception("Sql Connection obj is NULL,Please Look LogOut ERROR Msg!!"); 
            }

            this.Key = "DefautConnectionString";
            Command = OleDbExecuteXY.DBCommandPool.GetObject();
            Command.Connection = DBConn;

        }


        public OleDbExecuteXY(string key)
        {
            
            DBConn = OleDbExecuteXY.DBConnPool[key].GetObject();

            this.Key = key;

            if (DBConn == null)
            {
                throw new Exception(string.Format("Sql Connection obj is NULL,Please Look LogOut ERROR Msg!! For KEY{0}", key)); 
            }

            Command = OleDbExecuteXY.DBCommandPool.GetObject();
            Command.Connection = DBConn;

        }



        /// <summary>
        /// 打开数据库连接
        /// </summary>
        public void Open()
        {
            if (this.DBConn.State != ConnectionState.Open)
            {
                this.DBConn.Open();
            }
        }

        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public void Close()
        {
            if (this.DBConn.State == ConnectionState.Open)
            {
                this.DBConn.Close();
            }            
        }
              

        /// <summary>
        /// 释放
        /// </summary>
        public void Dispose()
        {
            Dispose(false);
        }

        public void Dispose(bool isDispose)
        {
            if (TransStats == 1)
                RollbackTrans();

            if (isDispose)
            {
                DBConn.Close();
                DBConn.Dispose();
                Command.Dispose();
                DBConn = null;
                Command = null;

            }
            else
            {
               

                if (DBConn != null)
                {
                    OleDbExecuteXY.DBConnPool[this.Key].ReleaseObject(DBConn);
                }
                if (Command != null)
                    OleDbExecuteXY.DBCommandPool.ReleaseObject(Command);

                DBConn = null;
                Command = null;
            }
        }


        #region 事务处理

        public byte TransStats { get; set; }

        /// <summary>
        /// 开始一个事务
        /// </summary>
        public void BeginTrans()
        {
            if (this.DBConn.State == ConnectionState.Closed)
            {
                this.DBConn.Open();
            }

            this.trans = this.DBConn.BeginTransaction();
            this.Command.Transaction = this.trans;
            TransStats = 1;
        }

        /// <summary>
        /// 提交一个事务
        /// </summary>
        public void CommitTrans()
        {
            this.trans.Commit();
            TransStats = 3;
        }

        /// <summary>
        /// 终止回滚一个事务
        /// </summary>
        public void RollbackTrans()
        {
            this.trans.Rollback();
            TransStats = 2;
        }

        #endregion

        



        /// <summary>
        /// 运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>行数</returns>
        public int SqlExecuteNonQuery(string sql)
        {
            return SqlExecuteNonQuery(sql, null);
        }

        /// <summary>
        /// 运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public int SqlExecuteNonQuery(string sql, bool bolIsProcedure)
        {
            return SqlExecuteNonQuery(sql,bolIsProcedure,null);
        }


        /// <summary>
        /// 运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public int SqlExecuteNonQuery(string sql, params OleDbParameter[] parem)
        {
            
            return SqlExecuteNonQuery(sql, false,parem);
        }

        /// <summary>
        ///  运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns>行数</returns>
        public int SqlExecuteNonQuery(string sql, bool bolIsProcedure,params OleDbParameter[] parem)
        {

            lock (LockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if(parem!=null)
                     Command.Parameters.AddRange(parem);
                if (bolIsProcedure)
                    Command.CommandType = CommandType.StoredProcedure;
                else
                    Command.CommandType = CommandType.Text;
                return Command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// 运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public OleDbDataReader SqlExecuteReader(string sql, CommandBehavior commandBehavior = CommandBehavior.SingleResult)
        {
            return SqlExecuteReader(sql, commandBehavior,null);
        }

        /// <summary>
        /// 运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public OleDbDataReader SqlExecuteReader(string sql, bool bolIsProcedure, CommandBehavior commandBehavior = CommandBehavior.SingleResult)
        {
            return SqlExecuteReader(sql,bolIsProcedure, commandBehavior,null);            
        }

        /// <summary>
        /// 运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public OleDbDataReader SqlExecuteReader(string sql, CommandBehavior commandBehavior,params OleDbParameter[] parem)
        {
            return SqlExecuteReader(sql, false, commandBehavior,parem);
        }

        /// <summary>
        /// 运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public OleDbDataReader SqlExecuteReader(string sql, bool bolIsProcedure, CommandBehavior commandBehavior, params OleDbParameter[] parem)
        {
            lock (LockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                if (bolIsProcedure)
                    Command.CommandType = CommandType.StoredProcedure;
                else
                    Command.CommandType = CommandType.Text;
                return Command.ExecuteReader();
            }
        }

        /// <summary>
        /// 查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql)
        {
            return SqlExecuteScalar(sql, false,null);
        }
        /// <summary>
        /// 查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, bool bolIsProcedure)
        {
            return SqlExecuteScalar(sql, bolIsProcedure,null);
        }
        /// <summary>
        /// 查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql,params OleDbParameter[] parem)
        {
            return SqlExecuteScalar(sql,false,parem);
        }
        /// <summary>
        /// 查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql,bool bolIsProcedure,params OleDbParameter[] parem)
        {
            lock (LockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                if (bolIsProcedure)
                    Command.CommandType = CommandType.StoredProcedure;
                else
                    Command.CommandType = CommandType.Text;
                return Command.ExecuteScalar();
            }
        }

        /// <summary>
        /// 查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql)
        {
            return SqlExcuteDataSet(sql,null);
        }

        /// <summary>
        /// 查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, params OleDbParameter[] parem)
        {
            return SqlExcuteDataSet(sql,false,parem);
        }

        /// <summary>
        /// 查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql,bool bolIsProcedure,params  OleDbParameter[] parem)
        {
            return SqlExcuteDataSet(sql, "this",bolIsProcedure, parem);
        }


        /// <summary>
        /// 查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="tablename">DATASET TABLE name</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, string tablename, bool bolIsProcedure, params OleDbParameter[] parem)
        {
            lock (LockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                if (bolIsProcedure)
                    Command.CommandType = CommandType.StoredProcedure;
                else
                    Command.CommandType = CommandType.Text;

                OleDbDataAdapter adapter = new OleDbDataAdapter(Command);

                DataSet dataset = new DataSet();

                adapter.Fill(dataset, tablename);

                adapter.Dispose();

                return dataset;

            }
        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false);
        }

        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, string tablename) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, tablename, false);
        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>        
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, bolIsProcedure, "", null);
        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <param name="bolIsProcedure"></param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, string tablename, bool bolIsProcedure) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, bolIsProcedure, tablename, null);
        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>      
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, params OleDbParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, null, parem);

        }

        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <param name="parem"></param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, string tablename, params OleDbParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, tablename, parem);

        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>        
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, params OleDbParameter[] parem) where T : new()
        {
            T b;
            return SqlExcuteSelectObject<T>(sql, bolIsProcedure, out b, null, parem);

        }

        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="tableName">表名</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>        
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, string tableName, params OleDbParameter[] parem) where T : new()
        {
            T b;
            return SqlExcuteSelectObject<T>(sql, bolIsProcedure, out b, tableName, parem);

        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj, params OleDbParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, out obj, null, parem);
        }

        /// <summary>
        /// 返回第一个结果
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="First">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns>结果数量</returns>
        public int SqlExcuteSelectFirst<T>(string sql, out T First, params OleDbParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, out First, null, parem).Count;

        }

        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <param name="tableName">表名</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj, string tableName, params OleDbParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, out obj, tableName, parem);
        }


        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>       
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, out obj, null);
        }

        /// <summary>
        /// 更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <param name="obj">填充对象</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, out T obj, string tablename,  params OleDbParameter[] parem) where T : new()
        {

            lock (LockThis)
            {
                obj = new T();

                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                if (bolIsProcedure)
                    Command.CommandType = CommandType.StoredProcedure;
                else
                    Command.CommandType = CommandType.Text;


                var ObjList = base.Deserializer<T>(Command);


                if (ObjList.Count > 0)
                    obj = ObjList[0];

                return ObjList;
            }
            
        }





    }
}
