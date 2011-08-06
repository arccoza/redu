using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;


namespace Redu.Protocol
{
    public sealed class MessageHandler : IMessageBuilder, IMessageParser
    {
        private static MessageHandler mh = new MessageHandler();

        public static IMessageBuilder Builder
        {
            get { return mh; }
        }

        public static IMessageParser Parser
        {
            get { return mh; }
        }

        public MessageHandler()
        {
        }

        #region Builder.

        public void Build<T>(string instruction, ICollection<string> keys, T msg)
            where T : IList<byte>
        {
            if (keys.Count > 1)
            {
                MsgSetHeader(msg, Markup.BMultiBulk, (keys.Count + 1));
            }

            MsgAddBulk(msg, instruction.ToBytes(EncodingType.UTF8));
            foreach (var item in keys)
            {
                MsgAddBulk(msg, item.ToBytes(EncodingType.UTF8));
            }
        }

        public void Build<T>(T msg, string instruction, params ICollection<string>[] argCollections)
            where T : IList<byte>
        {
            int total = 1;
            int max = 0;
            var enums = new List<IEnumerator<string>>();
            
            foreach (var argCollection in argCollections)
            {
                total += argCollection.Count;
                max = Math.Max(max, argCollection.Count);
                enums.Add(argCollection.GetEnumerator());
            }

            MsgSetHeader(msg, Markup.BMultiBulk, (total));

            MsgAddBulk(msg, instruction.ToBytes(EncodingType.UTF8));

            for (int i = 0; i < max; i++)
            {
                foreach (var en in enums)
                {
                    if (en.MoveNext())
                    {
                        MsgAddBulk(msg, en.Current.ToBytes(EncodingType.UTF8));
                    }
                }
            }
            
        }

        public void Build<T>(IList<T> pkg, out byte[] msg)
            where T : IList<byte>
        {
            List<byte> buff = new List<byte>();

            //if (pkg.Count > 1)
            //{
                MsgSetHeader(buff, Markup.BMultiBulk, pkg.Count);
            //}

            foreach (var ba in pkg)
            {
                MsgAddBulk(buff, ba);
            }

            msg = buff.ToArray();
        }

        public void Build<T>(Func<IList<T>> pkg, out byte[] msg)
            where T : IList<byte>
        {
            Build(pkg(), out msg);
        }

        private void MsgSetHeader<T1, T2>(T1 msg, T2 markup, int value)
            where T1 : IList<byte>
            where T2 : IList<byte>
        {
            //msg.AddRange(markup);
            //msg.AddRange(value.ToString().ToBytes(EncodingType.UTF8));
            //msg.AddRange(Markup.BTerm);

            AddRange(msg, markup);
            AddRange(msg, value.ToString().ToBytes(EncodingType.UTF8));
            AddRange(msg, Markup.BTerm);
        }

        private void MsgAddBulk<T1, T2>(T1 msg, T2 bulk)
            where T1 : IList<byte>
            where T2 : IList<byte>
        {
            MsgSetHeader(msg, Markup.BBulk, bulk.Count);
            AddRange(msg, bulk);
            AddRange(msg, Markup.BTerm);
        }

        private void AddRange<T1, T2>(T1 msg, T2 data)
            where T1 : IList<byte>
            where T2 : IList<byte>
        {
            foreach(var item in data)
            {
                msg.Add(item);
            }
        }

        #endregion

        public void Parse<TPkg>(IList<byte> msg, IList<TPkg> lpkg)
            where TPkg : class, IList<IBulk>, new()
        {
            int index = 0;
            List<byte> msgl = new List<byte>(msg);

            for (int i = 0; (index < msg.Count) && (i < 1000000); i++)
            {
                var ret = Parse<TPkg>(msgl, lpkg, ref index);

                if (ret != default(TPkg))
                {
                    lpkg.Add(ret);
                }
            }
        }

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

                pkg.Add( type == MarkupType.Status ? (Bulk)new Status(msg.GetRange(start, index - start).ToArray()) : type == MarkupType.Error ? (Bulk)new Error(msg.GetRange(start, index - start).ToArray()) : (Bulk)new Integer(msg.GetRange(start, index - start).ToArray()) );

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

    }
}
