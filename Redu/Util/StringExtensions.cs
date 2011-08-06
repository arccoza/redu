using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public static class StringExtensions
    {
        public static byte[] ToBytes(this char c, EncodingType type)
        {
            return ToBytes(new string(new char[] { c }), type);
        }

        public static byte[] ToBytes(this string s, EncodingType type)
        {
            byte[] ba = null;

            switch (type)
            {
                case EncodingType.ASCII:
                    ba = Encoding.ASCII.GetBytes(s);
                    break;

                case EncodingType.ASCII64:
                    ba = Convert.FromBase64String(s);
                    break;

                case EncodingType.ASCII85:
                    var a85 = new Util.Ascii85();
                    a85.EnforceMarks = false;
                    ba = a85.Decode(s);
                    break;

                case EncodingType.UTF7:
                    ba = Encoding.UTF7.GetBytes(s);
                    break;

                case EncodingType.UTF8:
                    ba = Encoding.UTF8.GetBytes(s);
                    break;

                case EncodingType.UTF16:
                case EncodingType.Unicode:
                    ba = Encoding.Unicode.GetBytes(s);
                    break;

                case EncodingType.UTF32:
                    ba = Encoding.UTF32.GetBytes(s);
                    break;
            }

            return ba;
        }

        public static int ByteCount(this char c, EncodingType type)
        {
            return ByteCount(new string(new char[] { c }), type);
        }

        public static int ByteCount(this string s, EncodingType type)
        {
            int bc = 0;

            switch (type)
            {
                case EncodingType.ASCII:
                    bc = Encoding.ASCII.GetByteCount(s);
                    break;

                case EncodingType.ASCII64:
                    bc = Convert.FromBase64String(s).Length;
                    break;

                case EncodingType.ASCII85:
                    var a85 = new Util.Ascii85();
                    a85.EnforceMarks = false;
                    bc = a85.Decode(s).Length;
                    break;

                case EncodingType.UTF7:
                    bc = Encoding.UTF7.GetByteCount(s);
                    break;

                case EncodingType.UTF8:
                    bc = Encoding.UTF8.GetByteCount(s);
                    break;

                case EncodingType.UTF16:
                case EncodingType.Unicode:
                    bc = Encoding.Unicode.GetByteCount(s);
                    break;

                case EncodingType.UTF32:
                    bc = Encoding.UTF32.GetByteCount(s);
                    break;
            }

            return bc;
        }
    }
}
