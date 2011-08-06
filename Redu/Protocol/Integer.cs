using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;


namespace Redu.Protocol
{
    public interface IInteger : IBulk
    {
    }

    public class Integer : Bulk, IInteger
    {
        public Integer(byte[] data)
        {
            bytes = data;
        }

        public Integer(string data)
        {
            bytes = data.ToBytes(EncodingType.UTF8);
        }

        public Integer(int data)
        {
            bytes = data.ToString().ToBytes(EncodingType.UTF8);
        }

        public static implicit operator Integer(byte[] data)
        {
            return new Integer(data);
        }

        public static implicit operator Integer(string data)
        {
            return new Integer(data);
        }

        public static implicit operator Integer(int data)
        {
            return new Integer(data);
        }

        public static implicit operator byte[](Integer obj)
        {
            return obj.ToBytes();
        }

        public static implicit operator string(Integer obj)
        {
            return obj.ToString();
        }

        public static implicit operator int(Integer obj)
        {
            return obj.ToInt32();
        }
    }
}
