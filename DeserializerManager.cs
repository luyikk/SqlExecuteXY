#if NET45

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Reflection;
using System.Reflection.Emit;

namespace SqlXY
{
    public class DeserializerManager
    {
        private static readonly DeserializerManager Instance = new DeserializerManager();

        public static DeserializerManager GetInstance()
        {
            return Instance;
        }

        private DeserializerManager()
        {

        }


        public ConcurrentDictionary<Type, Delegate> FuncDiy { get; set; } = new ConcurrentDictionary<Type, Delegate>();




        public Func<IDataReader, List<T>> GetFuncForType<T>(IDataReader read) where T: new()
        {
             var t= typeof(T);

            if (FuncDiy.ContainsKey(t))
                return (Func<IDataReader, List<T>>)FuncDiy[t];
            else
            {
                var func = GetTypeDeserializerImpl<T>(read);

                FuncDiy.AddOrUpdate(t, func, (a, b) => func);

                return func;
            }
        }


        public static Func<IDataReader, List<T>> GetTypeDeserializerImpl<T>(IDataReader read) where T : new()
        {
            Type type = typeof(T);
            Type returnType = typeof(List<T>);
            Type readType = typeof(IDataReader);
            Type dr = typeof(IDataRecord);


            var dm = new DynamicMethod("Deserialize" + Guid.NewGuid().ToString(), returnType,
                new[] { typeof(IDataReader) }, type, true);
            var il = dm.GetILGenerator();

            var endref = il.DefineLabel();
            var next = il.DefineLabel();
            il.DefineLabel();


            var props = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);


            Label[] proplable = new Label[props.Length];

            for (int i = 0; i < props.Length; i++)
            {
                proplable[i] = il.DefineLabel();
            }


            il.DeclareLocal(returnType); //list<t> return
            il.DeclareLocal(type);
            il.DeclareLocal(typeof(object)); //read?
            il.DeclareLocal(typeof(int));
            il.DeclareLocal(typeof(int));
            il.DeclareLocal(typeof(string));

            il.Emit(OpCodes.Nop);
            il.Emit(OpCodes.Newobj, returnType.GetConstructor(new Type[] { }));
            il.Emit(OpCodes.Stloc_0);

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, dr.GetProperty(nameof(read.FieldCount)).GetMethod);
            il.Emit(OpCodes.Stloc_S, 4);
            il.Emit(OpCodes.Ldloc_S, 4);
            il.Emit(OpCodes.Brfalse, endref);


            ////while (read.Read())
            ////{
            ////    ItemBase tmp = new ItemBase();
            il.MarkLabel(next);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, readType.GetMethod(nameof(read.Read)));
            il.Emit(OpCodes.Brfalse, endref);
            il.Emit(OpCodes.Newobj, type.GetConstructor(new Type[] { }));
            il.Emit(OpCodes.Stloc_1);


            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_3);

            var fori = il.DefineLabel();
            il.MarkLabel(fori);

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldloc_3);
            il.Emit(OpCodes.Callvirt, dr.GetMethod(nameof(read.GetName)));
            il.Emit(OpCodes.Stloc_S, 5);

            int ii = 0;

            var makeNext = il.DefineLabel();

            foreach (var item in props)
            {
                il.Emit(OpCodes.Ldloc_S, 5);
                il.Emit(OpCodes.Ldstr, item.Name);
                il.Emit(OpCodes.Call, typeof(String).GetMethod("op_Equality"));
                il.Emit(OpCodes.Brfalse_S, proplable[ii]);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldloc_3);
                il.Emit(OpCodes.Callvirt, dr.GetMethod("get_Item", new [] { typeof(int) }));
                il.Emit(OpCodes.Stloc_2);
                il.Emit(OpCodes.Ldloc_2);
                il.Emit(OpCodes.Isinst, typeof(DBNull));
                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Cgt_Un);
                il.Emit(OpCodes.Brtrue_S, proplable[ii]);
                il.Emit(OpCodes.Ldloc_2);
                il.Emit(OpCodes.Brfalse_S, proplable[ii]);

                if (item.PropertyType == typeof(string))
                {
                    il.Emit(OpCodes.Ldloc_1);
                    il.Emit(OpCodes.Ldloc_2);
                    il.Emit(OpCodes.Castclass, typeof(string));
                    il.Emit(OpCodes.Callvirt, item.SetMethod);
                }
                else
                {
                    il.Emit(OpCodes.Ldloc_1);
                    il.Emit(OpCodes.Ldloc_2);
                    FlexibleConvertBoxedFromHeadOfStack(il, typeof(object), item.PropertyType, null);
                    il.Emit(OpCodes.Callvirt, item.SetMethod);
                }

                il.Emit(OpCodes.Br, makeNext);


                il.MarkLabel(proplable[ii]);
                ii++;
            }

            il.MarkLabel(makeNext);
            il.Emit(OpCodes.Ldloc_3);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Add);
            il.Emit(OpCodes.Stloc_3);
            il.Emit(OpCodes.Ldloc_3);
            il.Emit(OpCodes.Ldloc_S, 4);
            il.Emit(OpCodes.Clt);
            il.Emit(OpCodes.Brtrue, fori);


            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Callvirt, returnType.GetMethod(nameof(List<T>.Add)));
            il.Emit(OpCodes.Nop);
            il.Emit(OpCodes.Br, next);
            il.MarkLabel(endref);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, readType.GetMethod(nameof(read.Close)));
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            var funcType = System.Linq.Expressions.Expression.GetFuncType(typeof(IDataReader), returnType);
            return (Func<IDataReader, List<T>>)dm.CreateDelegate(funcType);
        }

        private static void FlexibleConvertBoxedFromHeadOfStack(ILGenerator il, Type from, Type to, Type via)
        {
            MethodInfo op;
            if (from == (via ?? to))
            {
                il.Emit(OpCodes.Unbox_Any, to); // stack is now [target][target][typed-value]
            }
            else if ((op = GetOperator(from, to)) != null)
            {
                // this is handy for things like decimal <===> double
                il.Emit(OpCodes.Unbox_Any, from); // stack is now [target][target][data-typed-value]
                il.Emit(OpCodes.Call, op); // stack is now [target][target][typed-value]
            }
            else
            {
                bool handled = false;
                OpCode opCode = default(OpCode);
                switch (TypeExtensions.GetTypeCode(from))
                {
                    case TypeCode.Boolean:
                    case TypeCode.Byte:
                    case TypeCode.SByte:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                    case TypeCode.Single:
                    case TypeCode.Double:
                        handled = true;
                        switch (TypeExtensions.GetTypeCode(via ?? to))
                        {
                            case TypeCode.Byte:
                                opCode = OpCodes.Conv_Ovf_I1_Un; break;
                            case TypeCode.SByte:
                                opCode = OpCodes.Conv_Ovf_I1; break;
                            case TypeCode.UInt16:
                                opCode = OpCodes.Conv_Ovf_I2_Un; break;
                            case TypeCode.Int16:
                                opCode = OpCodes.Conv_Ovf_I2; break;
                            case TypeCode.UInt32:
                                opCode = OpCodes.Conv_Ovf_I4_Un; break;
                            case TypeCode.Boolean: // boolean is basically an int, at least at this level
                            case TypeCode.Int32:
                                opCode = OpCodes.Conv_Ovf_I4; break;
                            case TypeCode.UInt64:
                                opCode = OpCodes.Conv_Ovf_I8_Un; break;
                            case TypeCode.Int64:
                                opCode = OpCodes.Conv_Ovf_I8; break;
                            case TypeCode.Single:
                                opCode = OpCodes.Conv_R4; break;
                            case TypeCode.Double:
                                opCode = OpCodes.Conv_R8; break;
                            default:
                                handled = false;
                                break;
                        }
                        break;
                }
                if (handled)
                {
                    il.Emit(OpCodes.Unbox_Any, from); // stack is now [target][target][col-typed-value]
                    il.Emit(opCode); // stack is now [target][target][typed-value]
                    if (to == typeof(bool))
                    { // compare to zero; I checked "csc" - this is the trick it uses; nice
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                    }
                }
                else
                {
                    il.Emit(OpCodes.Ldtoken, via ?? to); // stack is now [target][target][value][member-type-token]
                    il.EmitCall(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle)), null); // stack is now [target][target][value][member-type]
                    il.EmitCall(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) }), null); // stack is now [target][target][boxed-member-type-value]
                    il.Emit(OpCodes.Unbox_Any, to); // stack is now [target][target][typed-value]
                }
            }
        }


        static MethodInfo GetOperator(Type from, Type to)
        {
            if (to == null) return null;
            MethodInfo[] fromMethods, toMethods;
            return ResolveOperator(fromMethods = from.GetMethods(BindingFlags.Static | BindingFlags.Public), from, to, "op_Implicit")
                ?? ResolveOperator(toMethods = to.GetMethods(BindingFlags.Static | BindingFlags.Public), from, to, "op_Implicit")
                ?? ResolveOperator(fromMethods, from, to, "op_Explicit")
                ?? ResolveOperator(toMethods, from, to, "op_Explicit");
        }

        static MethodInfo ResolveOperator(MethodInfo[] methods, Type from, Type to, string name)
        {
            for (int i = 0; i < methods.Length; i++)
            {
                if (methods[i].Name != name || methods[i].ReturnType != to) continue;
                var args = methods[i].GetParameters();
                if (args.Length != 1 || args[0].ParameterType != from) continue;
                return methods[i];
            }
            return null;
        }

    }


    internal static class TypeExtensions
    {
        public static string Name(this Type type)
        {
#if COREFX
            return type.GetTypeInfo().Name;
#else
            return type.Name;
#endif
        }

        public static bool IsValueType(this Type type)
        {
#if COREFX
            return type.GetTypeInfo().IsValueType;
#else
            return type.IsValueType;
#endif
        }
        public static bool IsEnum(this Type type)
        {
#if COREFX
            return type.GetTypeInfo().IsEnum;
#else
            return type.IsEnum;
#endif
        }
        public static bool IsGenericType(this Type type)
        {
#if COREFX
            return type.GetTypeInfo().IsGenericType;
#else
            return type.IsGenericType;
#endif
        }
        public static bool IsInterface(this Type type)
        {
#if COREFX
            return type.GetTypeInfo().IsInterface;
#else
            return type.IsInterface;
#endif
        }
#if COREFX
        public static IEnumerable<Attribute> GetCustomAttributes(this Type type, bool inherit)
        {
            return type.GetTypeInfo().GetCustomAttributes(inherit);
        }

        public static TypeCode GetTypeCode(Type type)
        {
            if (type == null) return TypeCode.Empty;
            TypeCode result;
            if (typeCodeLookup.TryGetValue(type, out result)) return result;

            if (type.IsEnum())
            {
                type = Enum.GetUnderlyingType(type);
                if (typeCodeLookup.TryGetValue(type, out result)) return result;
            }
            return TypeCode.Object;
        }
        static readonly Dictionary<Type, TypeCode> typeCodeLookup = new Dictionary<Type, TypeCode>
        {
            {typeof(bool), TypeCode.Boolean },
            {typeof(byte), TypeCode.Byte },
            {typeof(char), TypeCode.Char},
            {typeof(DateTime), TypeCode.DateTime},
            {typeof(decimal), TypeCode.Decimal},
            {typeof(double), TypeCode.Double },
            {typeof(short), TypeCode.Int16 },
            {typeof(int), TypeCode.Int32 },
            {typeof(long), TypeCode.Int64 },
            {typeof(object), TypeCode.Object},
            {typeof(sbyte), TypeCode.SByte },
            {typeof(float), TypeCode.Single },
            {typeof(string), TypeCode.String },
            {typeof(ushort), TypeCode.UInt16 },
            {typeof(uint), TypeCode.UInt32 },
            {typeof(ulong), TypeCode.UInt64 },
        };
#else
        public static TypeCode GetTypeCode(Type type)
        {
            return Type.GetTypeCode(type);
        }
#endif
        public static MethodInfo GetPublicInstanceMethod(this Type type, string name, Type[] types)
        {
#if COREFX
            var method = type.GetMethod(name, types);
            return (method != null && method.IsPublic && !method.IsStatic) ? method : null;
#else
            return type.GetMethod(name, BindingFlags.Instance | BindingFlags.Public, null, types, null);
#endif
        }


    }
}
#endif