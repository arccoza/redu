using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;


namespace Redu.Protocol
{
    public interface IStatus : IBulk
    {
    }

    public class Status : Bulk, IStatus
    {
        public Status(byte[] data)
        {
            bytes = data;
        }

        public Status(string data)
        {
            bytes = data.ToBytes(EncodingType.UTF8);
        }

        public Status(int data)
        {
            bytes = data.ToString().ToBytes(EncodingType.UTF8);
        }

        public static implicit operator Status(byte[] data)
        {
            return new Status(data);
        }

        public static implicit operator Status(string data)
        {
            return new Status(data);
        }

        public static implicit operator Status(int data)
        {
            return new Status(data);
        }

        public static implicit operator byte[](Status obj)
        {
            return obj.ToBytes();
        }

        public static implicit operator string(Status obj)
        {
            return obj.ToString();
        }

        public static implicit operator int(Status obj)
        {
            return obj.ToInt32();
        }
    }
}
