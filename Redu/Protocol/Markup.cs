using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Redu.Util;


namespace Redu.Protocol
{
    public static class Markup
    {
        public static readonly string SMultiBulk = "*";
        public static readonly string SBulk = "$";
        public static readonly string SStatus = "+";
        public static readonly string SError = "-";
        public static readonly string SInteger = ":";
        public static readonly string STerm = "\r\n";

        public static readonly byte[] BMultiBulk = Encoding.UTF8.GetBytes(SMultiBulk);
        public static readonly byte[] BBulk = Encoding.UTF8.GetBytes(SBulk);
        public static readonly byte[] BStatus = Encoding.UTF8.GetBytes(SStatus);
        public static readonly byte[] BError = Encoding.UTF8.GetBytes(SError);
        public static readonly byte[] BInteger = Encoding.UTF8.GetBytes(SInteger);
        public static readonly byte[] BTerm = Encoding.UTF8.GetBytes(STerm);

        //Dictionary matching markup type to byte[] value.
        public static readonly Dictionary<MarkupType, byte[]> B = new Dictionary<MarkupType, byte[]>(){
            { MarkupType.Status, BStatus },
            { MarkupType.Error, BError },
            { MarkupType.Integer, BInteger },
            { MarkupType.MultiBulk, BMultiBulk },
            { MarkupType.Bulk, BBulk },
            { MarkupType.Term, BTerm }
        };

        public static readonly Dictionary<byte[], MarkupType> BInv = new Dictionary<byte[], MarkupType>(){
            { BStatus, MarkupType.Status },
            { BError, MarkupType.Error },
            { BInteger, MarkupType.Integer },
            { BMultiBulk, MarkupType.MultiBulk },
            { BBulk, MarkupType.Bulk },
            { BTerm, MarkupType.Term }
        };

        //Dictionary matching markup type to string value.
        public static readonly Dictionary<MarkupType, string> S = new Dictionary<MarkupType, string>(){
            { MarkupType.Status, SStatus },
            { MarkupType.Error, SError },
            { MarkupType.Integer, SInteger },
            { MarkupType.MultiBulk, SMultiBulk },
            { MarkupType.Bulk, SBulk },
            { MarkupType.Term, STerm }
        };

        public static MarkupType Find(IList<byte> data, int start, int count, ref int index)
        {
            return Find(B.Values, data, start, count, ref index);
        }

        public static bool Find(MarkupType markup, IList<byte> data, int start, int count, ref int index)
        {
            return Find(B[markup], data, start, count, ref index);
        }

        public static bool Find(byte[] markup, IList<byte> data, int start, int count, ref int index)
        {
            var match = true;
            int o = 0;

            for (o = start; o < Math.Min(data.Count, start + count); o++)
            {
                match = true;

                for (int i = 0; i < markup.Length; i++)
                {
                    if (data[o + i] != markup[i])
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    index = o;
                    return true;
                }
            }

            return false;
        }

        public static MarkupType Find(IEnumerable<byte[]> markups, IList<byte> data, int start, int count, ref int index)
        {
            int matchCount = 0;
            bool match = false;

            
            for (int i = start; i < start + count; i++)
            {
                foreach (byte[] markup in markups)
                {
                    matchCount = 0;

                    for (int j = 0; j < markup.Length; j++)
                    {
                        if (data[i + j] == markup[j])
                        {
                            matchCount += 1;
                        }
                        else
                        {
                            match = false;
                            break;
                        }

                        if (matchCount == markup.Length)
                        {
                            match = true;
                            break;
                        }
                    }

                    if (match)
                    {
                        return Markup.BInv[markup];
                    }
                }

                index = i;
            }

            return default(MarkupType);
        }





        /*private static readonly Regex argCountRx = new Regex(string.Format(@"^\{0}(?<ArgCount>\d+){1}", Markup.ArgCount, Markup.Term), RegexOptions.Compiled);
        private static readonly Regex byteCountRx = new Regex(string.Format(@"(?<Head>\{0}(?<ByteCount>[0-9\-]+){1})", Markup.ByteCount, Markup.Term), RegexOptions.Compiled);
        private static readonly Regex statusRx = new Regex(string.Format(@"(?<Line>(?<Head>\{0})(?<Value>[A-Za-z0-9_ ]+){1})", Markup.Status, Markup.Term), RegexOptions.Compiled);
        private static readonly Regex errorRx = new Regex(string.Format(@"(?<Line>(?<Head>\{0})(?<Value>[A-Za-z0-9_ ]+){1})", Markup.Error, Markup.Term), RegexOptions.Compiled);
        private static readonly Regex integerRx = new Regex(string.Format(@"(?<Line>(?<Head>\{0})(?<Value>\d+){1})", Markup.Integer, Markup.Term), RegexOptions.Compiled);
        private static Dictionary<MessageType, Regex> rxs = new Dictionary<MessageType, Regex>() {
            { MessageType.MultiBulk, argCountRx },
            { MessageType.Bulk, byteCountRx },
            { MessageType.Status, statusRx },
            { MessageType.Error, errorRx },
            { MessageType.Integer, integerRx } };

        public static MessageType Match(ref byte[] m, out int index, out int length, out int value)
        {
            var s = m.ToString(EncodingType.UTF8);
            MessageType type = default(MessageType);
            Match match = null;

            index = -1;
            length = -1;
            value = -1;

            foreach (var kv in rxs)
            {
                match = kv.Value.Match(s);

                if (match.Success)
                {
                    type = kv.Key;

                    index = match.Index;
                    length = match.Length;
                    value = Convert.ToInt32(match.Groups["Value"].Value);

                    break;
                }
            }

            return type;
        }*/
    }

}
