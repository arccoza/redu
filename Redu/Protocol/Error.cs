using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;


namespace Redu.Protocol
{
    public interface IError : IBulk
    {
    }

    public class Error : Bulk, IError
    {
        public Error(byte[] data)
        {
            bytes = data;
        }

        public Error(string data)
        {
            bytes = data.ToBytes(EncodingType.UTF8);
        }

        public Error(int data)
        {
            bytes = data.ToString().ToBytes(EncodingType.UTF8);
        }

        public static implicit operator Error(byte[] data)
        {
            return new Error(data);
        }

        public static implicit operator Error(string data)
        {
            return new Error(data);
        }

        public static implicit operator Error(int data)
        {
            return new Error(data);
        }

        public static implicit operator byte[](Error obj)
        {
            return obj.ToBytes();
        }

        public static implicit operator string(Error obj)
        {
            return obj.ToString();
        }

        public static implicit operator int(Error obj)
        {
            return obj.ToInt32();
        }
    }
}
