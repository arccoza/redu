using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public static class FloatExtensions
    {
        public static Single Times(this Single f, Action action)
        {
            for (var j = 0; j < (int)f; j++)
            {
                action();
            }

            return f;
        }

        public static Double Times(this Double f, Action action)
        {
            for (var j = 0; j < (int)f; j++)
            {
                action();
            }

            return f;
        }

        public static Decimal Times(this Decimal f, Action action)
        {
            for (var j = 0; j < (int)f; j++)
            {
                action();
            }

            return f;
        }

        public static Single Times(this Single f, Func<Single> action)
        {
            for (var j = 0; j < (int)f; j++)
            {
                f = action();
            }

            return f;
        }

        public static Double Times(this Double f, Func<Double> action)
        {
            for (var j = 0; j < (int)f; j++)
            {
                f = action();
            }

            return f;
        }

        public static Decimal Times(this Decimal f, Func<Decimal> action)
        {
            for (var j = 0; j < (int)f; j++)
            {
                f = action();
            }

            return f;
        }
    }
}
