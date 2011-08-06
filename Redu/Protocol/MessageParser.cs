using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;


namespace Redu.Protocol
{
    public class MessageParser
    {
        private static MessageParser mp = new MessageParser();
        public static MessageParser Inst
        {
            get { return mp; }
        }

        //public void Parse<TMsg, TPkg>(TMsg msg, IList<Action<TPkg>> pkgActions)
        //    where TMsg : IList<byte>, new()
        //    where TPkg : class, IList<IBulk>, new()
        //{
        //    int msgIndex = 0;
        //    //var actsEnum = pkgActions.GetEnumerator();

        //    for (int i = 0; (msgIndex < msg.Count) && (i < pkgActions.Count); i++)
        //    {
        //        //if (actsEnum.MoveNext())
        //        //{
        //            Parse(msg, pkgActions, ref msgIndex, ref i);
        //        //}
        //        //else
        //        //{
        //        //    break;
        //        //}
        //    }
        //}

        //private TPkg Parse<TMsg, TPkg>(TMsg msg, IList<Action<TPkg>> pkgActions, ref int msgIndex, ref int actIndex)
        //    where TMsg : IList<byte>, new()
        //    where TPkg : class, IList<IBulk>, new()
        //{
        //    var act = pkgActions[actIndex];
        //    int hValue = 0;
        //    var type = PkgGetHeader(msg, ref msgIndex, out hValue);
        //    TPkg pkg = new TPkg();

        //    if (type == MarkupType.MultiBulk) //A Redis MultiBulk response.
        //    {
        //        if (hValue == -1)
        //        {
        //            act(null);
        //            return default(TPkg);
        //        }
        //        if (hValue == 0)
        //        {
        //            act(new TPkg());
        //            return default(TPkg);
        //        }

        //        TPkg ret = default(TPkg);

        //        for (int i = 0; i < hValue; i++)
        //        {
        //            ret = Parse(msg, pkgActions, ref msgIndex, ref actIndex);

        //            if (ret != default(TPkg))
        //            {
        //                pkg.Add(ret[0]);
        //            }
        //        }

        //        if (pkg.Count > 0)
        //        {
        //            act(pkg);
        //        }

        //        pkg = default(TPkg);
        //    }
        //    else if (type == MarkupType.Bulk) //A Redis Bulk response.
        //    {
        //        if (hValue == -1)
        //        {
        //            pkg.Add(null);
        //        }
        //        else
        //        {
        //            pkg.Add(new Bulk(GetRange(msg, msgIndex, hValue).ToArray()));
        //            msgIndex += hValue + Markup.BTerm.Length;
        //        }
        //    }
        //    else if (type == MarkupType.Status || type == MarkupType.Error || type == MarkupType.Integer)
        //    {
        //        var start = msgIndex;

        //        Markup.Find(Markup.BTerm, msg, start, msg.Count, ref msgIndex);

        //        pkg.Add(type == MarkupType.Status ? (Bulk)new Status(GetRange(msg, start, msgIndex - start).ToArray()) : type == MarkupType.Error ? (Bulk)new Error(GetRange(msg, start, msgIndex - start).ToArray()) : (Bulk)new Integer(GetRange(msg, start, msgIndex - start).ToArray()));

        //        msgIndex += Markup.BTerm.Length;
        //    }

        //    return pkg;
        //}

        public int Parse<TPkg>(IList<byte> msg, IList<TPkg> lpkg, int pkgCount = 1)
            where TPkg : class, IList<IBulk>, new()
        {
            int index = 0;
            List<byte> msgl = new List<byte>(msg);

            for (int i = 0; (index < msg.Count) && (i < pkgCount); i++)
            {
            //do
            //{
                var ret = Parse<TPkg>(msgl, lpkg, ref index);

                if (ret != default(TPkg))
                {
                    lpkg.Add(ret);
                }
            //} while (index < msg.Count && parseAll);
            }

            return index;
        }

        //public void Parse<TPkg>(IList<byte> msg, IList<TPkg> lpkg)
        //    where TPkg : class, IList<IBulk>, new()
        //{
        //    int index = 0;
        //    List<byte> msgl = new List<byte>(msg);

        //    for (int i = 0; (index < msg.Count) && (i < 1000000); i++)
        //    {
        //        var ret = Parse<TPkg>(msgl, lpkg, ref index);

        //        if (ret != default(TPkg))
        //        {
        //            lpkg.Add(ret);
        //        }
        //    }
        //}

        public void Parse<TPkg>(IList<byte> msg, IList<Action<TPkg>> lapkg)
            where TPkg : class, IList<IBulk>, new()
        {
            var lpkg = new List<TPkg>();

            Parse(msg, lpkg);

            var lapkgEnum = lapkg.GetEnumerator();
            foreach (var pkg in lpkg)
            {
                if (lapkgEnum.MoveNext())
                {
                    lapkgEnum.Current(pkg);
                }
                else
                {
                    break;
                }
            }
        }

        private TPkg Parse<TPkg>(List<byte> msg, IList<TPkg> lpkg, ref int index)
            where TPkg : class, IList<IBulk>, new()
        {
            int hvalue = 0;
            var type = PkgGetHeader(msg, ref index, out hvalue);
            TPkg pkg = new TPkg();

            if (type == MarkupType.MultiBulk) //A Redis MultiBulk response.
            {
                if (hvalue == -1) //This is a null response from Redis, we add null to lpkg and return null telling the calling method not to add the return to the collection.
                {
                    lpkg.Add(null);
                    return null;
                }
                if (hvalue == 0) //This is an empty list response from Redis, we add an empty list to lpkg and return null to signal the caller not to add the return to the collection.
                {
                    lpkg.Add(new TPkg());
                    return null;
                }

                TPkg ret = default(TPkg);

                for (int i = 0; i < hvalue; i++) //Loop for the number of items in the msg.
                {
                    ret = Parse<TPkg>(msg, lpkg, ref index);

                    if (ret != default(TPkg)) //Check here for a null return value, which is used to indicate that the return is coming from a MultiBulk branch.
                    {
                        pkg.Add(ret[0]); //If return isn't null then it contains some data or a null from the Bulk or Status branches, and must be added to lpkg.
                    }

                }

                if (pkg.Count > 0) //If msg only had MultiBulks in it, then the pkg.Count will be 0, and we don't add it to the collection, lpkg.
                {
                    lpkg.Add(pkg);
                }

                pkg = default(TPkg); //MultiBulk branch must return null, so that it is not added to the lpkg if it is a nested multi bulk.
            }
            else if (type == MarkupType.Bulk) //A Redis Bulk response.
            {
                if (hvalue == -1) //This is a null item value from Redis, we had null to the pkg, which will be added to the collection, lpkg, after it is returned to the caller.
                {
                    pkg.Add(null);
                }
                else
                {
                    pkg.Add(new Bulk(msg.GetRange(index, hvalue).ToArray()));
                    index += hvalue + Markup.BTerm.Length;
                }
            }
            else if (type == MarkupType.Status || type == MarkupType.Error || type == MarkupType.Integer)
            {
                var start = index;

                Markup.Find(Markup.BTerm, msg, start, msg.Count, ref index);

                pkg.Add(type == MarkupType.Status ? (Bulk)new Status(msg.GetRange(start, index - start).ToArray()) : type == MarkupType.Error ? (Bulk)new Error(msg.GetRange(start, index - start).ToArray()) : (Bulk)new Integer(msg.GetRange(start, index - start).ToArray()));

                index += Markup.BTerm.Length;
            }

            return pkg;
        }

        private MarkupType PkgGetHeader(List<byte> msg, ref int index, out int value)
        {
            var type = Markup.Find(msg, index, 13, ref index);
            var start = index;

            value = 0;

            if (type == default(MarkupType))
            {
                throw new Exception("Failed to parse msg, no Redis protocol markup found.");
            }
            else if (type == MarkupType.MultiBulk || type == MarkupType.Bulk)
            {
                start += Markup.B[type].Length;
                Markup.Find(Markup.BTerm, msg, start, 13 + Markup.BTerm.Length, ref index);
                value = Convert.ToInt32(msg.GetRange(start, index - start).ToArray().ToString(EncodingType.UTF8));
                index += Markup.BTerm.Length;
            }
            else if (type == MarkupType.Status || type == MarkupType.Error || type == MarkupType.Integer)
            {
                index += Markup.B[type].Length;
                value = 0;
            }

            return type;
        }




        //private MarkupType PkgGetHeader<TMsg>(TMsg msg, ref int index, out int value)
        //    where TMsg : IList<byte>, new()
        //{
        //    var type = Markup.Find(msg, index, 13, ref index);
        //    var start = index;

        //    value = 0;

        //    if (type == default(MarkupType))
        //    {
        //        throw new Exception("Failed to parse msg, no Redis protocol markup found.");
        //    }
        //    else if (type == MarkupType.MultiBulk || type == MarkupType.Bulk)
        //    {
        //        start += Markup.B[type].Length;
        //        Markup.Find(Markup.BTerm, msg, start, 13 + Markup.BTerm.Length, ref index);
        //        value = Convert.ToInt32(GetRange(msg, start, index - start).ToArray().ToString(EncodingType.UTF8));
        //        index += Markup.BTerm.Length;
        //    }
        //    else if (type == MarkupType.Status || type == MarkupType.Error || type == MarkupType.Integer)
        //    {
        //        index += Markup.B[type].Length;
        //        value = 0;
        //    }

        //    return type;
        //}

        //private T GetRange<T>(T data, int index, int count)
        //    where T : IList<byte>, new()
        //{
        //    var rng = new T();
            
        //    for (int i = index; i < index + count; i++)
        //    {
        //        rng.Add(data[i]);
        //    }

        //    return rng;
        //}
    }
}
