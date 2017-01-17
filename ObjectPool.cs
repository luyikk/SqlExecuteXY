﻿//#define USEConcurrent//lockStack//USEConcurrent //lockStack or USEConcurrent of ...

//by luyikk 2010.5.9
using System;
using System.Collections.Generic;
using System.Text;

#if USEConcurrent
using System.Collections.Concurrent;
#endif


namespace SqlXY
{

    
    /// <summary>
    /// 泛型的对象池-可输入构造函数,以及参数
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ObjectPool<T> where T : new()
    {

        /// <summary>
        /// 对象处理代理
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="pool"></param>
        /// <returns></returns>
        public delegate T ObjectRunTimeHandle(T obj, ObjectPool<T> pool);

        /// <summary>
        /// 获取对象时所处理的方法
        /// </summary>
        public ObjectRunTimeHandle GetObjectRunTime { get; set; }

        /// <summary>
        /// 回收对象时处理的方法
        /// </summary>
        public ObjectRunTimeHandle ReleaseObjectRunTime { get; set; }

        /// <summary>
        /// 最大对象数量
        /// </summary>
        public int MaxObjectCount { get; set; }

#if USEConcurrent
        /// <summary>
        /// 对象存储Stack
        /// </summary>
        public ConcurrentStack<T> ObjectStack { get; set; }
#else

        /// <summary>
        /// 对象存储Stack
        /// </summary>
        public Stack<T> ObjectStack { get; set; }

#if  lockStack==true
        private object lockStack;
#endif

#endif

        /// <summary>
        /// 构造函数
        /// </summary>
        public System.Reflection.ConstructorInfo TheConstructor { get; set; }

        /// <summary>
        /// 参数
        /// </summary>
        public object[] param { get; set; }

        public ObjectPool(int maxObjectCount)
        {
#if USEConcurrent
            ObjectStack = new ConcurrentStack<T>();
#else
            ObjectStack = new Stack<T>();
#if  lockStack==true
            lockStack = new object();
#endif
#endif
            this.MaxObjectCount = maxObjectCount;
        }

        private T GetT()
        {
            if (TheConstructor != null)
            {
                return (T)TheConstructor.Invoke(param);
            }
            else
            {
                return new T();
            }
        }


        /// <summary>
        /// 对象正常创建次数 i1+i2=i3
        /// </summary>
       public int i1 { get; set; }
        /// <summary>
       /// 对象从堆栈分配次数 i1+i2=i3
        /// </summary>
       public int i2 { get; set; }
        /// <summary>
        /// 对象回收次数 i1+i2=i3
        /// </summary>
       public int i3 { get; set; }

        /// <summary>
        /// 获取对象
        /// </summary>
        /// <returns></returns>
        public T GetObject()
        {
            if (ObjectStack.Count == 0)
            {
                T p = GetT();
                if (GetObjectRunTime != null)
                    p = GetObjectRunTime(p, this);
                i1++;
                return p;
            }
            else
            {
 #if USEConcurrent

                T p;

                if (ObjectStack.TryPop(out p))
                {
                    if (GetObjectRunTime != null)
                        p = GetObjectRunTime(p, this);

                    i2++;

                    return p;
                }
                else
                {
                    p = GetT();

                    if (GetObjectRunTime != null)
                        p = GetObjectRunTime(p, this);

                    return p;
                }

#else
                T p;

#if  lockStack==true
                lock (lockStack)
                {
#endif
                    if (ObjectStack.Count > 0)
                    {
                        i2++;

                        p = ObjectStack.Pop();

                        if (p == null)
                        {
                            p = GetT();
                        }

                        if (GetObjectRunTime != null)
                            p = GetObjectRunTime(p, this);

                        return p;
                        

                    }
                    else
                    {
                        p = GetT();

                        if (GetObjectRunTime != null)
                            p = GetObjectRunTime(p, this);

                        return p;
                    }
#if  lockStack==true
                }
#endif

#endif

            }

        }

        /// <summary>
        /// 获取对象
        /// </summary>
        /// <param name="cout"></param>
        /// <returns></returns>
        public T[] GetObject(int cout)
        {


            if (ObjectStack.Count == 0)
            {
                T[] p = new T[cout];

                for (int i = 0; i < cout; i++)
                {
                    p[i] = GetT();
                    if (GetObjectRunTime != null)
                        p[i] = GetObjectRunTime(p[i], this);
                }

                return p;
            }
            else
            {
#if USEConcurrent

                T[] p = new T[cout];

                int Lpcout = ObjectStack.TryPopRange(p);
                               

                if (Lpcout < cout)
                {
                    int x = cout - Lpcout;

                    T[] xp = new T[x];

                    for (int i = 0; i < x; i++)
                    {
                        xp[i] = GetT();
                    }

                    Array.Copy(xp, 0, p, Lpcout, x);



                }

                if (GetObjectRunTime != null)
                {
                    for (int i = 0; i < p.Length; i++)
                    {
                        p[i] = GetObjectRunTime(p[i], this);

                    }

                }


                return p;

#else
#if  lockStack==true
                lock (lockStack)
                {
#endif

                T[] p = new T[cout];

                    for (int i = 0; i < cout; i++)
                    {
                        if (ObjectStack.Count > 0)
                        {
                            p[i] = ObjectStack.Pop();

                            if (p[i] == null)
                                p[i] = GetT();
                        }
                        else
                        {
                            p[i] = GetT();
                        }


                        if (GetObjectRunTime != null)
                            p[i] = GetObjectRunTime(p[i], this);
                    }

                    return p;
#if  lockStack==true
                }
#endif


#endif
            }

        }


        /// <summary>
        /// 回收对象
        /// </summary>
        /// <param name="obj"></param>
        public void ReleaseObject(T obj)
        {

            if (ReleaseObjectRunTime != null)
            {
                obj = ReleaseObjectRunTime(obj, this);

                if (obj == null) 
                    return;
            }
#if lockStack==true
            lock (lockStack)
            {
#endif

                if (ObjectStack.Count >= MaxObjectCount)
                {
                    if (obj is IDisposable)
                    {
                        ((IDisposable)obj).Dispose();
                    }

                }
                else
                {
                    i3++;
                    ObjectStack.Push(obj);
                }


#if lockStack==true
            }
#endif

        }

        /// <summary>
        /// 回收对象
        /// </summary>
        /// <param name="obj"></param>
        public void ReleaseObject(T[] obj)
        {
            foreach (T p in obj)
            {
                ReleaseObject(p);
            }

        }



    }
}
