using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

#if USEConcurrent
using System.Collections.Concurrent;
#endif

namespace SqlXY
{
    public  class TypeOfCacheManager
    {

        #region 全局静态唯一对象
        static object lockthis = new object();

        static TypeOfCacheManager _My;

        public static TypeOfCacheManager GetInstance()
        {
            lock (lockthis)
            {


                if (_My == null)
                    _My = new TypeOfCacheManager();
            }

            return _My;
        }
        private TypeOfCacheManager()
        {

        }

        #endregion




#if USEConcurrent
        public ConcurrentDictionary<Type, Dictionary<string, System.Reflection.PropertyInfo>> PropertyTable = new ConcurrentDictionary<Type, Dictionary<string, System.Reflection.PropertyInfo>>();
#else
        object lockobj = new object();
        public  Dictionary<Type, Dictionary<string, System.Reflection.PropertyInfo>> PropertyTable = new Dictionary<Type, Dictionary<string, System.Reflection.PropertyInfo>>();
#endif

#if USEConcurrent
        public Dictionary<string, System.Reflection.PropertyInfo> GetTypeProperty(Type type)
        {

            if (PropertyTable.ContainsKey(type))
            {
                return PropertyTable[type];
            }
            else
            {
                System.Reflection.PropertyInfo[] propertyArray = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

                Dictionary<string, System.Reflection.PropertyInfo> tmp = new Dictionary<string, System.Reflection.PropertyInfo>(propertyArray.Length);

                foreach (var item in propertyArray)
                {
                    tmp[item.Name] = item;
                }

                System.Threading.SpinWait.SpinUntil(() => PropertyTable.TryAdd(type, tmp));
                return tmp;
            }

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
