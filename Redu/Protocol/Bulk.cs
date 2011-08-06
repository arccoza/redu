using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;

namespace Redu.Protocol
{
    public interface IBulk : IList<byte>
    {
        int Length
        {
            get;
        }

        long LongLength
        {
            get;
        }

        IBulk FromBytes(byte[] data);
        IBulk FromString(string data);
        IBulk FromInt(int data);
        IBulk FromDouble(double data);

        byte[] ToBytes();
        string ToString();
        int ToInt32();
        double ToDouble();
    }

    public class Bulk : IBulk
    {
        protected byte[] bytes;

        public int Length
        {
            get { return bytes.Length; }
        }

        public long LongLength
        {
            get { return bytes.LongLength; }
        }

        public Bulk()
        {
        }

        public Bulk(byte[] data)
        {
            bytes = data;
        }

        public Bulk(string data)
        {
            bytes = data.ToBytes(EncodingType.UTF8);
        }

        public Bulk(int data)
        {
            bytes = data.ToString().ToBytes(EncodingType.UTF8);
        }

        public IBulk FromBytes(byte[] data)
        {
            bytes = data;

            return this;
        }

        public IBulk FromString(string data)
        {
            bytes = data.ToBytes(EncodingType.UTF8);

            return this;
        }

        public IBulk FromInt(int data)
        {
            bytes = data.ToString().ToBytes(EncodingType.UTF8);

            return this;
        }

        public IBulk FromDouble(double data)
        {
            bytes = data.ToString().ToBytes(EncodingType.UTF8);

            return this;
        }

        public byte[] ToBytes()
        {
            return bytes;
        }

        public override string ToString()
        {
            return bytes.ToString(EncodingType.UTF8);
        }

        public int ToInt32()
        {
            return Convert.ToInt32(bytes.ToString(EncodingType.UTF8));
        }

        public double ToDouble()
        {
            return Convert.ToDouble(bytes.ToString(EncodingType.UTF8));
        }

        public static implicit operator Bulk(byte[] data)
        {
            return new Bulk(data);
        }

        public static implicit operator Bulk(string data)
        {
            return new Bulk(data);
        }

        public static implicit operator Bulk(int data)
        {
            return new Bulk(data);
        }

        public static implicit operator byte[](Bulk obj)
        {
            return obj.ToBytes();
        }

        public static implicit operator string(Bulk obj)
        {
            return obj.ToString();
        }

        public static implicit operator int(Bulk obj)
        {
            return obj.ToInt32();
        }


        public int IndexOf(byte item)
        {
            int i;
            bool found = false;
            
            for (i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] == item)
                {
                    found = true;
                    break;
                }
            }

            return found ? i : -1;
        }

        void IList<byte>.Insert(int index, byte item)
        {
            throw new NotImplementedException();
        }

        void IList<byte>.RemoveAt(int index)
        {
            throw new NotImplementedException();
        }

        public byte this[int index]
        {
            get
            {
                return bytes[index];
            }
            set
            {
                bytes[index] = value;
            }
        }

        void ICollection<byte>.Add(byte item)
        {
            throw new NotImplementedException();
        }

        void ICollection<byte>.Clear()
        {
            throw new NotImplementedException();
        }

        bool ICollection<byte>.Contains(byte item)
        {
            return IndexOf(item) < 0 ? false : true;
        }

        public void CopyTo(byte[] array, int arrayIndex)
        {
            Buffer.BlockCopy(bytes, 0, array, arrayIndex, bytes.Length);
        }

        int ICollection<byte>.Count
        {
            get { return bytes.Length; }
        }

        bool ICollection<byte>.IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }

        bool ICollection<byte>.Remove(byte item)
        {
            throw new NotImplementedException();
        }

        IEnumerator<byte> IEnumerable<byte>.GetEnumerator()
        {
            return (bytes as IEnumerable<byte>).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return bytes.GetEnumerator();
        }
    }
}
