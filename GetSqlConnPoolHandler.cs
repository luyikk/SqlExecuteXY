//by luyikk 2010.5.9
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;
using Npgsql;

namespace SqlXY
{

    public delegate void LogOutHandle(string log, object sender);
    public delegate string DeCodeConnStringHandle(string connstr);

    public static class GetSqlPoolHandler
    {

        static Dictionary<string, ObjectPool<System.Data.SqlClient.SqlConnection>> DBConnPool;
        static ObjectPool<System.Data.SqlClient.SqlCommand> DBCommandPool;


        static Dictionary<string, ObjectPool<System.Data.Odbc.OdbcConnection>> ODBCConnPool;
        static ObjectPool<System.Data.Odbc.OdbcCommand> ODBCCommandPool;


        static Dictionary<string, ObjectPool<System.Data.OleDb.OleDbConnection>> OleDbConnPool;
        static ObjectPool<System.Data.OleDb.OleDbCommand> OleDbCommandPool;


        static Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>> MySqlConnPool;
        static ObjectPool<MySql.Data.MySqlClient.MySqlCommand> MySqlCommandPool;


        static Dictionary<string, ObjectPool<NpgsqlConnection>> NpgsqlConnPool;
        static ObjectPool<NpgsqlCommand> NpgsqlCommandPool;

        static object Lock2 = new object();
        static object Lock1 = new object();


        static object Lock4 = new object();
        static object Lock3 = new object();

        static object Lock6 = new object();
        static object Lock5 = new object();

        static object Lock8 = new object();
        static object Lock7 = new object();

        static object Lock10 = new object();
        static object Lock9 = new object();

        static GetSqlPoolHandler()
        {

        }


        #region SQLSERVER
        /// <summary>
        /// 返回Command对象
        /// </summary>
        /// <returns></returns>
        public static ObjectPool<System.Data.SqlClient.SqlCommand> GetSqlCommandInstance()
        {

            lock (Lock2)
            {
                try
                {
                    if (DBCommandPool == null)
                    {
                        DBCommandPool = new ObjectPool<SqlCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                        DBCommandPool.ReleaseObjectRunTime = new ObjectPool<SqlCommand>.ObjectRunTimeHandle((command, pool) =>
                        {
                            command.CommandText = "";
                            command.Connection = null;
                            command.CommandType = CommandType.Text;
                            command.Parameters.Clear();
                            return command;
                        });

                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }

            }

            return DBCommandPool;
        }

        /// <summary>
        /// 返回Conn对象池
        /// </summary>
        /// <returns></returns>
        public static Dictionary<string, ObjectPool<System.Data.SqlClient.SqlConnection>> GetSqlConnInstance()
        {
            lock (Lock1)
            {
                try
                {
                    if (DBConnPool == null)
                    {
                        DBConnPool = new Dictionary<string, ObjectPool<SqlConnection>>();

                        foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                        {

                            if (ar.ProviderName.Equals("System.Data.SqlClient", StringComparison.CurrentCulture))
                            {

                                string name = ar.Name;
                                string connectionstring = ar.ConnectionString;

                                if (name.IndexOf(":") > 0)
                                {
                                    string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                                    if (sp.Length == 2)
                                    {
                                        name = sp[0];

                                        if (sp[1].ToUpper() == "ENCODE")
                                        {
                                            connectionstring = DeCodeConn(connectionstring);
                                        }
                                    }

                                }


                                ObjectPool<SqlConnection> temp = new ObjectPool<SqlConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                                temp.TheConstructor = typeof(SqlConnection).GetConstructor(new Type[] { typeof(string) });

                                temp.param = new object[] { connectionstring };

                                temp.GetObjectRunTime = new ObjectPool<SqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    try
                                    {
                                        conn.Open();
                                        return conn;
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutMananger(e.Message, e);
                                        return null;
                                    }
                                });

                                temp.ReleaseObjectRunTime = new ObjectPool<SqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    conn.Close();

                                    return conn;
                                });


                                DBConnPool.Add(name, temp);
                            }
                        }
                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }
            }

            return DBConnPool;
        }

        #endregion

        #region ODBC
        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public static Dictionary<string, ObjectPool<System.Data.Odbc.OdbcConnection>> GetOdbcConnInstance()
        {
            lock (Lock3)
            {
                try
                {
                    if (ODBCConnPool == null)
                    {
                        ODBCConnPool = new Dictionary<string, ObjectPool<System.Data.Odbc.OdbcConnection>>();

                        foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                        {
                            if (ar.ProviderName.Equals("System.Data.Odbc", StringComparison.CurrentCulture))
                            {
                                string name = ar.Name;
                                string connectionstring = ar.ConnectionString;

                                if (name.IndexOf(":") > 0)
                                {
                                    string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                                    if (sp.Length == 2)
                                    {
                                        name = sp[0];

                                        if (sp[1].ToUpper() == "ENCODE")
                                        {
                                            connectionstring = DeCodeConn(connectionstring);
                                        }
                                    }

                                }

                                ObjectPool<System.Data.Odbc.OdbcConnection> temp = new ObjectPool<System.Data.Odbc.OdbcConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                                temp.TheConstructor = typeof(System.Data.Odbc.OdbcConnection).GetConstructor(new Type[] { typeof(string) });

                                temp.param = new object[] { connectionstring };

                                temp.GetObjectRunTime = new ObjectPool<System.Data.Odbc.OdbcConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    try
                                    {
                                        conn.Open();
                                        return conn;
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutMananger(e.Message, e);
                                        return null;
                                    }
                                });

                                temp.ReleaseObjectRunTime = new ObjectPool<System.Data.Odbc.OdbcConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    conn.Close();

                                    return conn;
                                });


                                ODBCConnPool.Add(name, temp);
                            }
                        }
                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }
            }


            return ODBCConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public static ObjectPool<System.Data.Odbc.OdbcCommand> GetOdbcCommandInstance()
        {

            lock (Lock4)
            {
                try
                {
                    if (ODBCCommandPool == null)
                    {
                        ODBCCommandPool = new ObjectPool<System.Data.Odbc.OdbcCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                        ODBCCommandPool.ReleaseObjectRunTime = new ObjectPool<System.Data.Odbc.OdbcCommand>.ObjectRunTimeHandle((command, pool) =>
                        {
                            command.CommandText = "";
                            command.Connection = null;
                            command.CommandType = CommandType.Text;
                            command.Parameters.Clear();
                            return command;
                        });

                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }

            }

            return ODBCCommandPool;
        }
        #endregion

        #region OleDb
        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public static Dictionary<string, ObjectPool<System.Data.OleDb.OleDbConnection>> GetOleDbConnInstance()
        {
            lock (Lock5)
            {
                try
                {
                    if (OleDbConnPool == null)
                    {
                        OleDbConnPool = new Dictionary<string, ObjectPool<System.Data.OleDb.OleDbConnection>>();

                        foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                        {
                            if (ar.ProviderName.Equals("System.Data.OleDb", StringComparison.CurrentCulture))
                            {
                                string name = ar.Name;
                                string connectionstring = ar.ConnectionString;

                                if (name.IndexOf(":") > 0)
                                {
                                    string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                                    if (sp.Length == 2)
                                    {
                                        name = sp[0];

                                        if (sp[1].ToUpper() == "ENCODE")
                                        {
                                            connectionstring = DeCodeConn(connectionstring);
                                        }
                                    }

                                }

                                ObjectPool<System.Data.OleDb.OleDbConnection> temp = new ObjectPool<System.Data.OleDb.OleDbConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                                temp.TheConstructor = typeof(System.Data.OleDb.OleDbConnection).GetConstructor(new Type[] { typeof(string) });

                                temp.param = new object[] { connectionstring };

                                temp.GetObjectRunTime = new ObjectPool<System.Data.OleDb.OleDbConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    try
                                    {
                                        conn.Open();
                                        return conn;
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutMananger(e.Message, e);
                                        return null;
                                    }
                                });

                                temp.ReleaseObjectRunTime = new ObjectPool<System.Data.OleDb.OleDbConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    conn.Close();

                                    return conn;
                                });


                                OleDbConnPool.Add(name, temp);
                            }
                        }
                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }
            }


            return OleDbConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public static ObjectPool<System.Data.OleDb.OleDbCommand> GetOleDbCommandInstance()
        {

            lock (Lock6)
            {
                try
                {
                    if (OleDbCommandPool == null)
                    {
                        OleDbCommandPool = new ObjectPool<System.Data.OleDb.OleDbCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                        OleDbCommandPool.ReleaseObjectRunTime = new ObjectPool<System.Data.OleDb.OleDbCommand>.ObjectRunTimeHandle((command, pool) =>
                        {
                            command.CommandText = "";
                            command.Connection = null;
                            command.CommandType = CommandType.Text;
                            command.Parameters.Clear();
                            return command;
                        });

                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }

            }

            return OleDbCommandPool;
        }
        #endregion

        #region MySQL
        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public static Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>> GetMySqlConnInstance()
        {
            lock (Lock7)
            {
                try
                {
                    if (MySqlConnPool == null)
                    {
                        MySqlConnPool = new Dictionary<string, ObjectPool<MySql.Data.MySqlClient.MySqlConnection>>();

                        foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                        {
                            if (ar.ProviderName.Equals("MySql.Data.MySqlClient", StringComparison.CurrentCulture))
                            {
                                string name = ar.Name;
                                string connectionstring = ar.ConnectionString;

                                if (name.IndexOf(":") > 0)
                                {
                                    string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                                    if (sp.Length == 2)
                                    {
                                        name = sp[0];

                                        if (sp[1].ToUpper() == "ENCODE")
                                        {
                                            connectionstring = DeCodeConn(connectionstring);
                                        }
                                    }

                                }

                                ObjectPool<MySql.Data.MySqlClient.MySqlConnection> temp = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                                temp.TheConstructor = typeof(MySql.Data.MySqlClient.MySqlConnection).GetConstructor(new Type[] { typeof(string) });

                                temp.param = new object[] { connectionstring };

                                temp.GetObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    try
                                    {
                                        conn.Open();
                                        return conn;
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutMananger(e.Message, e);
                                        return null;
                                    }
                                });

                                temp.ReleaseObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    conn.Close();

                                    return conn;
                                });


                                MySqlConnPool.Add(name, temp);
                            }
                        }
                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }
            }


            return MySqlConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public static ObjectPool<MySql.Data.MySqlClient.MySqlCommand> GetMySqlCommandInstance()
        {
            
            lock (Lock8)
            {
                try
                {
                    if (MySqlCommandPool == null)
                    {
                        MySqlCommandPool = new ObjectPool<MySql.Data.MySqlClient.MySqlCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                        MySqlCommandPool.ReleaseObjectRunTime = new ObjectPool<MySql.Data.MySqlClient.MySqlCommand>.ObjectRunTimeHandle((command, pool) =>
                        {
                            command.CommandText = "";
                            command.Connection = null;
                            command.CommandType = CommandType.Text;
                            command.Parameters.Clear();
                            return command;
                        });

                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }

            }

            return MySqlCommandPool;
        }

        #endregion

        #region NPGSQL
        /// <summary>
        /// 返回ODBCConn
        /// </summary>
        /// <returns></returns>
        public static Dictionary<string, ObjectPool<NpgsqlConnection>> GetNpgsqlConnInstance()
        {
            lock (Lock7)
            {
                try
                {
                    if (NpgsqlConnPool == null)
                    {
                        NpgsqlConnPool = new Dictionary<string, ObjectPool<NpgsqlConnection>>();


                        foreach (ConnectionStringSettings ar in ConfigurationManager.ConnectionStrings)
                        {
                            if (ar.ProviderName.Equals("NpgSqlClient", StringComparison.CurrentCulture))
                            {
                                string name = ar.Name;
                                string connectionstring = ar.ConnectionString;

                                if (name.IndexOf(":") > 0)
                                {
                                    string[] sp = name.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

                                    if (sp.Length == 2)
                                    {
                                        name = sp[0];

                                        if (sp[1].ToUpper() == "ENCODE")
                                        {
                                            connectionstring = DeCodeConn(connectionstring);
                                        }
                                    }

                                }

                                ObjectPool<NpgsqlConnection> temp = new ObjectPool<NpgsqlConnection>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]));

                                temp.TheConstructor = typeof(NpgsqlConnection).GetConstructor(new Type[] { typeof(string) });

                                temp.param = new object[] { connectionstring };

                                temp.GetObjectRunTime = new ObjectPool<NpgsqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    try
                                    {
                                        conn.Open();
                                        return conn;
                                    }
                                    catch (Exception e)
                                    {
                                        LogOutMananger(e.Message, e);
                                        return null;
                                    }
                                });

                                temp.ReleaseObjectRunTime = new ObjectPool<NpgsqlConnection>.ObjectRunTimeHandle((conn, pool) =>
                                {
                                    conn.Close();

                                    return conn;
                                });


                                NpgsqlConnPool.Add(name, temp);
                            }
                        }
                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置："+er.Message, NpgsqlConnPool);
                }
            }


            return NpgsqlConnPool;
        }


        /// <summary>
        /// 返回ODBCCommand对象
        /// </summary>
        /// <returns></returns>
        public static ObjectPool<NpgsqlCommand> GetNpgsqlCommandInstance()
        {

            lock (Lock8)
            {
                try
                {
                    if (NpgsqlCommandPool == null)
                    {
                        NpgsqlCommandPool = new ObjectPool<NpgsqlCommand>(int.Parse(ConfigurationManager.AppSettings["MaxCount"]) * (ConfigurationManager.ConnectionStrings.Count - 1));
                        NpgsqlCommandPool.ReleaseObjectRunTime = new ObjectPool<NpgsqlCommand>.ObjectRunTimeHandle((command, pool) =>
                        {
                            command.CommandText = "";
                            command.Connection = null;
                            command.CommandType = CommandType.Text;
                            command.Parameters.Clear();
                            return command;
                        });

                    }
                }
                catch (Exception er)
                {
                    LogOutMananger("初始化Config失败，请检查Config配置：" + er.Message, NpgsqlConnPool);
                }
            }


            return NpgsqlCommandPool;
        }

        #endregion

        #region 日记输出

        /// <summary>
        /// 日记错误输出事件
        /// </summary>
        public static event LogOutHandle LogOut;


        public static event DeCodeConnStringHandle DeCodeConnStr;


        private static string DeCodeConn(string connStr)
        {
            if (DeCodeConnStr != null)
                return DeCodeConnStr(connStr);
            else
                return connStr;
        }

        /// <summary>
        /// 输出日记
        /// </summary>
        /// <param name="log"></param>
        /// <param name="sender"></param>
        private static void LogOutMananger(string log, object sender)
        {
            if (LogOut != null)
            {
                LogOut(log, sender);
            }
            else
            {
                throw new Exception(log);
            }

        }

        #endregion
    }
}
