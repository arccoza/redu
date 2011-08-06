using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public static class ByteExtensions
    {
        public static string ToString(this byte b, EncodingType type)
        {
            return ToString(new byte[] { b }, type);
        }

        public static string ToString(this byte[] ba, EncodingType type)
        {
            string s = null;

            switch (type)
            {
                case EncodingType.ASCII:
                    s = Encoding.ASCII.GetString(ba);
                    break;

                case EncodingType.ASCII64:
                    s = Convert.ToBase64String(ba);
                    break;

                case EncodingType.ASCII85:
                    var a85 = new Util.Ascii85();
                    a85.EnforceMarks = false;
                    s = a85.Encode(ba);
                    break;

                case EncodingType.UTF7:
                    s = Encoding.UTF7.GetString(ba);
                    break;

                case EncodingType.UTF8:
                    s = Encoding.UTF8.GetString(ba);
                    break;

                case EncodingType.UTF16:
                case EncodingType.Unicode:
                    s = Encoding.Unicode.GetString(ba);
                    break;

                case EncodingType.UTF32:
                    s = Encoding.UTF32.GetString(ba);
                    break;
            }

            return s;
        }

        public static int CharCount(this byte b, EncodingType type)
        {
            return CharCount(new byte[] { b }, type);
        }

        public static int CharCount(this byte[] ba, EncodingType type)
        {
            int cc = 0;

            switch (type)
            {
                case EncodingType.ASCII:
                    cc = Encoding.ASCII.GetCharCount(ba);
                    break;

                case EncodingType.ASCII64:
                    cc = Convert.ToBase64String(ba).Length;
                    break;

                case EncodingType.ASCII85:
                    var a85 = new Util.Ascii85();
                    a85.EnforceMarks = false;
                    cc = a85.Encode(ba).Length;
                    break;

                case EncodingType.UTF7:
                    cc = Encoding.UTF7.GetCharCount(ba);
                    break;

                case EncodingType.UTF8:
                    cc = Encoding.UTF8.GetCharCount(ba);
                    break;

                case EncodingType.UTF16:
                case EncodingType.Unicode:
                    cc = Encoding.Unicode.GetCharCount(ba);
                    break;

                case EncodingType.UTF32:
                    cc = Encoding.UTF32.GetCharCount(ba);
                    break;
            }

            return cc;
        }
    }

    public enum EncodingType
    {
        ASCII = 1,
        ASCII64,
        ASCII85,
        UTF7,
        UTF8,
        UTF16,
        Unicode,
        UTF32
    }
}
