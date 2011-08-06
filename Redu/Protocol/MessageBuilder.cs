using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;


namespace Redu.Protocol
{
    public class MessageBuilder : IMessageBuilder
    {
        private static MessageBuilder mb = new MessageBuilder();
        public static MessageBuilder Inst
        {
            get { return mb; }
        }

        public MessageBuilder()
        {
        }

        public void Build<TMsg>(TMsg msg, string instruction, params ICollection<string>[] tableOfArgs)
            where TMsg : IList<byte>
        {
            int total = 1;
            int max = 0;
            var enums = new List<IEnumerator<string>>();

            foreach (var argCollection in tableOfArgs)
            {
                if (argCollection != null)
                {
                    total += argCollection.Count;
                    max = Math.Max(max, argCollection.Count);
                    enums.Add(argCollection.GetEnumerator());
                }
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

        public void Build<TMsg>(TMsg msg, string instruction, params IEnumerable<ICollection<string>>[] tablesOfArgs)
            where TMsg : IList<byte>
        {
            int total = 1;
            var maxColLengths = new List<int>();
            int max = 0;
            var enumTables = new List<List<IEnumerator<string>>>();
            List<IEnumerator<string>> enumCols;

            foreach (var table in tablesOfArgs)
            {
                enumCols = new List<IEnumerator<string>>();

                foreach (var argCollection/*column*/ in table)
                {
                    total += argCollection.Count;
                    max = Math.Max(max, argCollection.Count);
                    enumCols.Add(argCollection.GetEnumerator());
                }

                maxColLengths.Add(max);
                max = 0;
                enumTables.Add(enumCols);
            }

            MsgSetHeader(msg, Markup.BMultiBulk, (total));

            MsgAddBulk(msg, instruction.ToBytes(EncodingType.UTF8));

            for (int i = 0; i < enumTables.Count; i++)
            {
                enumCols = enumTables[i];

                for (int j = 0; j < maxColLengths[i]; j++)
                {
                    foreach (var en/*enum column*/ in enumCols)
                    {
                        if (en.MoveNext())
                        {
                            MsgAddBulk(msg, en.Current.ToBytes(EncodingType.UTF8));
                        }
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
            foreach (var item in data)
            {
                msg.Add(item);
            }
        }
    }
}
