using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu
{
    public class SortOrder
    {
        public static SortOrder Asc
        {
            get { return new SortOrder("ASC"); }
        }

        public static SortOrder Desc
        {
            get { return new SortOrder("DESC"); }
        }

        public string Value
        {
            get;
            private set;
        }

        private SortOrder(string order)
        {
            Value = order;
        }
    }

    public enum Order
    {
        //None = 0,
        Desc = 1,
        Asc
    }

    public class Limit
    {
        public uint Offset;
        public uint Count;
    }

    public enum Aggregate
    {
        //None,
        Sum = 1,
        Min,
        Max
    }

    public class RangeMarker
    {
        public string Marker
        {
            get;
            private set;
        }

        private RangeMarker()
        {
        }

        public static RangeMarker Value(double value, bool exclusive = false)
        {
            var rm = new RangeMarker();
            rm.Marker = string.Format("{0}{1}", exclusive ? "(" : "", value);

            return rm;
        }

        public static RangeMarker NegInf()
        {
            var rm = new RangeMarker();
            rm.Marker = "-inf";

            return rm;
        }

        public static RangeMarker PosInf()
        {
            var rm = new RangeMarker();
            rm.Marker = "+inf";

            return rm;
        }

        public override string ToString()
        {
            return Marker;
        }

        public class PubSubMessage
        {
            public string Channel
            {
                get;
                protected set;
            }

            public string Pattern
            {
                get;
                protected set;
            }

            public string Data
            {
                get;
                protected set;
            }

            public PubSubMessage(string channel, string pattern, string data)
            {
                Channel = channel;
                Pattern = pattern;
                Data = data;
            }
        }
    }
}
