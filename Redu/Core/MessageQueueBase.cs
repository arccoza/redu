using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public abstract class MessageQueueBase<TPkg>
        //where TRet : MessageQueue<TRet, TPkg>
        where TPkg : List<IBulk>, new()
    {
        public Guid Id
        {
            get;
            protected set;
        }

        public abstract bool IsReusable
        {
            get;
            set;
        }

        public bool IsPipelined
        {
            get;
            protected set;
        }

        protected RedisConnection Connection
        {
            get;
            set;
        }

        protected List<byte> MessageBuff
        {
            get;
            set;
        }

        protected List<Action<TPkg>> CommandCallbacks
        {
            get;
            set;
        }

        protected Countdown CountdownEvent;

        public MessageQueueBase(RedisConnection connection)
        {
            Id = Guid.NewGuid();
            MessageBuff = new List<byte>();
            CommandCallbacks = new List<Action<TPkg>>();
            Connection = connection;
            CountdownEvent = new Countdown(0);
        }

        #region Common methods.

        protected string CheckOk<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (value is IStatus && value != null && value.ToString().ToUpper() == "OK")
            {
                return value.ToString();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        protected string CheckQueued<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (value is IStatus && value != null && value.ToString().ToUpper() == "QUEUED")
            {
                return value.ToString();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        protected string CheckStatus<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (value is IStatus && value != null)
            {
                return value.ToString();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        protected void CheckError<T>(T value)
            where T : IBulk
        {
            if (value is IError && value != null)
            {
                throw new RedisException(value.ToString());
            }
        }

        protected int CheckInt<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (value is IInteger && value != null)
            {
                return value.ToInt32();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        protected string CheckValue<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (!(value is IStatus || value is IInteger))
            {
                return value == null ? null : value.ToString();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }



        

        public void Wait()
        {
            CountdownEvent.Wait();
        }

        public void Wait(int millisecondsTimeout)
        {
            CountdownEvent.Wait(millisecondsTimeout);
        }

        #endregion

        public abstract MessageQueueBase<TPkg> Open();

        public abstract MessageQueueBase<TPkg> Close();

        public abstract MessageQueueBase<TPkg> Queue(byte[] msg, Action<TPkg> callback);

        public abstract MessageQueueBase<TPkg> Execute();

        public abstract MessageQueueBase<TPkg> Watch(ICollection<string> keys);

        public abstract MessageQueueBase<TPkg> Unwatch();

        public abstract MessageQueueBase<TPkg> Discard();

        //public abstract void QueueCallback(TPkg pkg);
    }
}
