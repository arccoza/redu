using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public static class IntExtensions
    {
        public static Int16 Times(this Int16 i, Action action)
        {
            for (Int16 j = 0; j < i; j++)
            {
                action();
            }

            return i;
        }

        public static Int32 Times(this Int32 i, Action action)
        {
            for (Int32 j = 0; j < i; j++)
            {
                action();
            }

            return i;
        }

        public static Int64 Times(this Int64 i, Action action)
        {
            for (Int64 j = 0; j < i; j++)
            {
                action();
            }

            return i;
        }

        public static Int16 Times(this Int16 i, Func<Int16> action)
        {
            for (Int16 j = 0; j < i; j++)
            {
                i = action();
            }

            return i;
        }

        public static Int32 Times(this Int32 i, Func<Int32> action)
        {
            for (Int32 j = 0; j < i; j++)
            {
                i = action();
            }

            return i;
        }

        public static Int64 Times(this Int64 i, Func<Int64> action)
        {
            for (Int64 j = 0; j < i; j++)
            {
                i = action();
            }

            return i;
        }
    }
}
