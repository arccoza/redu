using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public static class DateTimeExtensions
    {
        public static double ToUnixTimestamp(this DateTime dt)
        {
            return (dt - new DateTime(1970, 1, 1, 0, 0, 0, 0)).TotalSeconds;
        }

        public static DateTime FromUnixTimestamp(this DateTime dt, double unixTimestamp)
        {
            return new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(unixTimestamp);
        }
    }
}
