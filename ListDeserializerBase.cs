using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace SqlXY
{
    public abstract class ListDeserializerBase
    {
        protected List<T> Deserializer<T>(IDbCommand Command) where T : new()
        {
            List<T> ObjList = new List<T>();

            Type objType = typeof(T);

            using (var dataRead = Command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
            {

#if NET45
              var func= DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);

               ObjList = func(dataRead);

#else
                List<string> names = new List<string>(dataRead.FieldCount);

                for (int i = 0; i < dataRead.FieldCount; i++)
                {
                    names.Add(dataRead.GetName(i));
                }

                Dictionary<string, System.Reflection.PropertyInfo> tmpdiy = TypeOfCacheManager.GetInstance().GetTypeProperty(objType);


                while (dataRead.Read())
                {
                    T temp = new T();

                    foreach (var fieldname in names)
                    {
                        System.Reflection.PropertyInfo prope;
                        if (tmpdiy.TryGetValue(fieldname, out prope))
                        {
                            var data = dataRead[fieldname];

                            if (!(data is DBNull))
                            {
                                prope.SetValue(temp, data, null);

                            }

                        }
                    }

                    ObjList.Add(temp);

                }

                dataRead.Close();

#endif

            }

            return ObjList;
        }
    }
}
