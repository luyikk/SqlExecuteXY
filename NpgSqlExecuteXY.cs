//by luyikk 2010.5.9

using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;

namespace SqlXY
{
    public class NpgSqlExecuteXY : ListDeserializerBase, IDisposable
    {
        private readonly object _lockThis = new object();


        #region 静态

        /// <summary>
        ///     CONN连接对象池
        /// </summary>
        public static Dictionary<string, ObjectPool<NpgsqlConnection>> DbConnPool { get; }

        public static ObjectPool<NpgsqlCommand> DbCommandPool { get; set; }


        static NpgSqlExecuteXY()
        {
            DbConnPool = GetSqlPoolHandler.GetNpgsqlConnInstance();
            DbCommandPool = GetSqlPoolHandler.GetNpgsqlCommandInstance();
        }

        #endregion

        #region 事务处理

        public byte TransStats { get; set; }

        /// <summary>
        ///     开始一个事务
        /// </summary>
        public void BeginTrans()
        {
            if (DbConn.State == ConnectionState.Closed)
                DbConn.Open();

            Trans = DbConn.BeginTransaction();
            Command.Transaction = Trans;

            TransStats = 1;
        }

        /// <summary>
        ///     提交一个事务
        /// </summary>
        public void CommitTrans()
        {
            Trans.Commit();
            TransStats = 3;
        }

        /// <summary>
        ///     终止回滚一个事务
        /// </summary>
        public void RollbackTrans()
        {
            Trans.Rollback();
            TransStats = 2;
        }

        #endregion

        public NpgSqlExecuteXY()
        {
            DbConn = DbConnPool["DefautConnectionString"].GetObject();
            if (DbConn == null)
                throw new Exception("Sql Connection obj is NULL,Please Look LogOut ERROR Msg!!");

            Key = "DefautConnectionString";
            Command = DbCommandPool.GetObject();
            Command.Connection = DbConn;
        }


        public NpgSqlExecuteXY(string key)
        {
            DbConn = DbConnPool[key].GetObject();

            Key = key;

            if (DbConn == null)
                throw new Exception(
                    string.Format("Sql Connection obj is NULL,Please Look LogOut ERROR Msg!! FOR KEY:{0}", key));

            Command = DbCommandPool.GetObject();
            Command.Connection = DbConn;
        }

        /// <summary>
        ///     数据库连接器
        /// </summary>
        public NpgsqlConnection DbConn { get; protected set; }

        /// <summary>
        ///     命令
        /// </summary>
        public NpgsqlCommand Command { get; protected set; }

        private NpgsqlTransaction Trans { get; set; }

        public string Key { get; }

        /// <summary>
        ///     释放
        /// </summary>
        public void Dispose()
        {
            Dispose(false);
        }


        /// <summary>
        ///     打开数据库连接
        /// </summary>
        public void Open()
        {
            if (DbConn.State != ConnectionState.Open)
                DbConn.Open();
        }

        /// <summary>
        ///     关闭数据库连接
        /// </summary>
        public void Close()
        {
            if (DbConn.State == ConnectionState.Open)
                DbConn.Close();
        }

        public void Dispose(bool isDispose)
        {
            if (TransStats == 1)
                RollbackTrans();


            if (isDispose)
            {
                DbConn.Close();
                DbConn.Dispose();
                Command.Dispose();
                DbConn = null;
                Command = null;
            }
            else
            {
                if (DbConn != null)
                    DbConnPool[Key].ReleaseObject(DbConn);
                if (Command != null)
                    DbCommandPool.ReleaseObject(Command);

                DbConn = null;
                Command = null;
            }
        }


        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns>行数</returns>
        public int SqlExecuteNonQuery(string sql)
        {
            return SqlExecuteNonQuery(sql, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public int SqlExecuteNonQuery(string sql, bool bolIsProcedure)
        {
            return SqlExecuteNonQuery(sql, bolIsProcedure, null);
        }


        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public int SqlExecuteNonQuery(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExecuteNonQuery(sql, false, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回行数
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns>行数</returns>
        public int SqlExecuteNonQuery(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                return Command.ExecuteNonQuery();
            }
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql,
            CommandBehavior commandBehavior = CommandBehavior.SingleResult)
        {
            return SqlExecuteReader(sql, commandBehavior, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <param name="commandBehavior"></param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql, bool bolIsProcedure,
            CommandBehavior commandBehavior = CommandBehavior.SingleResult)
        {
            return SqlExecuteReader(sql, bolIsProcedure, commandBehavior, null);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql, CommandBehavior commandBehavior,
            params NpgsqlParameter[] parem)
        {
            return SqlExecuteReader(sql, false, commandBehavior, parem);
        }

        /// <summary>
        ///     运行一条SQL语句并返回READER
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="commandBehavior"></param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public NpgsqlDataReader SqlExecuteReader(string sql, bool bolIsProcedure, CommandBehavior commandBehavior,
            params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                return Command.ExecuteReader();
            }
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql)
        {
            return SqlExecuteScalar(sql, false, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, bool bolIsProcedure)
        {
            return SqlExecuteScalar(sql, bolIsProcedure, null);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExecuteScalar(sql, false, parem);
        }

        /// <summary>
        ///     查询返回记过中的第一行第一列,忽略其他行
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否存储过程</param>
        /// <returns></returns>
        public object SqlExecuteScalar(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;
                return Command.ExecuteScalar();
            }
        }

        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql)
        {
            return SqlExcuteDataSet(sql, null);
        }

        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, params NpgsqlParameter[] parem)
        {
            return SqlExcuteDataSet(sql, false, parem);
        }

        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
        {
            return SqlExcuteDataSet(sql, "this", bolIsProcedure, parem);
        }


        /// <summary>
        ///     查询并返回DATASET
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="tablename">DATASET TABLE name</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <returns></returns>
        public DataSet SqlExcuteDataSet(string sql, string tablename, bool bolIsProcedure,
            params NpgsqlParameter[] parem)
        {
            lock (_lockThis)
            {
                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;

                var adapter = new NpgsqlDataAdapter(Command);

                var dataset = new DataSet();

                adapter.Fill(dataset, tablename);

                adapter.Dispose();

                return dataset;
            }
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
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
        ///     更具对象读取表数据填充对象,并返回此类的集合
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
        ///     更具对象读取表数据填充对象,并返回此类的集合
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
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, params NpgsqlParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, null, parem);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="tablename"></param>
        /// <param name="parem"></param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, string tablename, params NpgsqlParameter[] parem)
            where T : new()
        {
            return SqlExcuteSelectObject<T>(sql, false, tablename, parem);
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, params NpgsqlParameter[] parem)
            where T : new()
        {
            T b;
            return SqlExcuteSelectObject(sql, bolIsProcedure, out b, null, parem);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="tableName">表名</param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, string tableName,
            params NpgsqlParameter[] parem) where T : new()
        {
            T b;
            return SqlExcuteSelectObject(sql, bolIsProcedure, out b, tableName, parem);
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj, params NpgsqlParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject(sql, false, out obj, null, parem);
        }

        /// <summary>
        ///     返回第一个结果
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="first">返回第一个对象</param>
        /// <param name="parem">参数</param>
        /// <returns>结果数量</returns>
        public int SqlExcuteSelectFirst<T>(string sql, out T first, params NpgsqlParameter[] parem) where T : new()
        {
            return SqlExcuteSelectObject(sql, false, out first, null, parem).Count;
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <param name="tableName">表名</param>
        /// <param name="parem">参数</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj, string tableName, params NpgsqlParameter[] parem)
            where T : new()
        {
            return SqlExcuteSelectObject(sql, false, out obj, tableName, parem);
        }


        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="obj">返回第一个对象</param>
        /// <returns></returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, out T obj) where T : new()
        {
            return SqlExcuteSelectObject(sql, false, out obj, null);
        }

        /// <summary>
        ///     更具对象读取表数据填充对象,并返回此类的集合
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">SQL语句</param>
        /// <param name="tablename"></param>
        /// <param name="parem">参数</param>
        /// <param name="bolIsProcedure">是否为存储过程</param>
        /// <param name="obj">填充对象</param>
        /// <returns>对象集合</returns>
        public List<T> SqlExcuteSelectObject<T>(string sql, bool bolIsProcedure, out T obj, string tablename,
            params NpgsqlParameter[] parem) where T : new()
        {
            lock (_lockThis)
            {
                obj = new T();

                Command.CommandText = sql;
                Command.Parameters.Clear();
                if (parem != null)
                    Command.Parameters.AddRange(parem);
                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;


                var objList = Deserializer<T>(Command);


                if (objList.Count > 0)
                    obj = objList[0];

                return objList;
            }
        }


        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public int SqlExcuteUpdateOrInsertOrDeleteObject<T>(string sql, T obj)
        {
            return SqlExcuteUpdateOrInsertOrDeleteObject(sql, false, obj);
        }

        /// <summary>
        ///     更新一个对象数据到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="sql">语句</param>
        /// <param name="bolIsProcedure">是否是存储过程</param>
        /// <param name="obj">数据对象</param>
        /// <returns>更新数量</returns>
        public int SqlExcuteUpdateOrInsertOrDeleteObject<T>(string sql, bool bolIsProcedure, T obj)
        {
            lock (_lockThis)
            {
                var objType = obj.GetType();

                var propertyArray = TypeOfCacheManager.GetInstance().GetTypeProperty(objType).Values;

                Command.CommandText = sql;
                Command.Parameters.Clear();

                Command.CommandType = bolIsProcedure ? CommandType.StoredProcedure : CommandType.Text;

                foreach (var props in propertyArray)
                {
                    var values = props.GetValue(obj, null);

                    if (values != null)
                        Command.Parameters.AddWithValue("@" + props.Name, values);
                }

                return Command.ExecuteNonQuery();
            }
        }


    }
}