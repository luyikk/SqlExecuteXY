using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
#if USEConcurrent
using System.Collections.Concurrent;

#endif

namespace SqlXY
{
    public class TypeOfCacheManager
    {
        #region 全局静态唯一对象

        private static readonly object Lockthis = new object();

        private static TypeOfCacheManager _my;

        public static TypeOfCacheManager GetInstance()
        {
            lock (Lockthis)
            {
                if (_my == null)
                    _my = new TypeOfCacheManager();
            }

            return _my;
        }

        private TypeOfCacheManager()
        {
        }

        #endregion

#if USEConcurrent
        public ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>> PropertyTable =
            new ConcurrentDictionary<Type, Dictionary<string, PropertyInfo>>();
#else
        object lockobj = new object();
        public  Dictionary<Type, Dictionary<string, System.Reflection.PropertyInfo>> PropertyTable = new Dictionary<Type, Dictionary<string, System.Reflection.PropertyInfo>>();
#endif

#if USEConcurrent
        public Dictionary<string, PropertyInfo> GetTypeProperty(Type type)
        {
            if (PropertyTable.ContainsKey(type))
                return PropertyTable[type];
            var propertyArray = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            var tmp = new Dictionary<string, PropertyInfo>(propertyArray.Length);

            foreach (var item in propertyArray)
                tmp[item.Name] = item;

            SpinWait.SpinUntil(() => PropertyTable.TryAdd(type, tmp));
            return tmp;
        }

#else
        public Dictionary<string, System.Reflection.PropertyInfo> GetTypeProperty(Type type)
        {
            lock (lockobj)
            {
                if (PropertyTable.ContainsKey(type))
                {
                    return PropertyTable[type];
                }
                else
                {
                    System.Reflection.PropertyInfo[] propertyArray = type.GetProperties(BindingFlags.Instance|BindingFlags.Public);

                    Dictionary<string, System.Reflection.PropertyInfo> tmp = new Dictionary<string, System.Reflection.PropertyInfo>(propertyArray.Length);

                    foreach (var item in propertyArray)
                    {
                        tmp[item.Name] = item;
                    }


                    PropertyTable.Add(type, tmp);
                    return tmp;
                }

            }
        }

#endif
    }
}