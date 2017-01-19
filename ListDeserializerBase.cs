using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace SqlXY
{
    public abstract class ListDeserializerBase
    {
        protected List<T> Deserializer<T>(IDbCommand command) where T : new()
        {
            using (var dataRead = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)
            )
            {
#if NET45
              var func= DeserializerManager.GetInstance().GetFuncForType<T>(dataRead);

              return func(dataRead);

#else

                var objList = new List<T>();

                var objType = typeof(T);

                var names = new List<string>(dataRead.FieldCount);

                for (var i = 0; i < dataRead.FieldCount; i++)
                    names.Add(dataRead.GetName(i));

                var tmpdiy = TypeOfCacheManager.GetInstance().GetTypeProperty(objType);


                while (dataRead.Read())
                {
                    var temp = new T();

                    foreach (var fieldname in names)
                    {
                        PropertyInfo prope;
                        if (tmpdiy.TryGetValue(fieldname, out prope))
                        {
                            var data = dataRead[fieldname];

                            if (!(data is DBNull))
                                prope.SetValue(temp, data, null);
                        }
                    }

                    objList.Add(temp);
                }

                dataRead.Close();

                return objList;

#endif
            }
        }
    }
}