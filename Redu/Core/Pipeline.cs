using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class Pipeline : MessageQueue
    {

        public override bool IsReusable
        {
            get
            {
                return true;
            }
            set
            {
                
            }
        }

        public Pipeline(RedisConnection connection)
            : base(connection)
        {
            IsPipelined = true;
        }

        public override MessageQueueBase<List<IBulk>> Open()
        {
            Connection.Open(Id);

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Close()
        {
            Connection.Close(Id);

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Queue(byte[] msg, Action<List<IBulk>> callback)
        {
            CommandCallbacks.Add(callback);
            MessageBuff.AddRange(msg);

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Execute()
        {
            int totCallbacks = CommandCallbacks.Count;

            CountdownEvent.AddCount(1);
            Connection.Send(Id, MessageBuff.ToArray(), delegate(ReceiveHandler rh)
            {
                for (int i = 0; i < totCallbacks; i++)
                {
                    CommandCallbacks[i](rh.Packages[i]);
                }

                if (!IsReusable)
                {
                    Discard();
                }

                CountdownEvent.Signal();
            }, totCallbacks);

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Watch(ICollection<string> keys)
        {
            throw new NotImplementedException();
        }

        public override MessageQueueBase<List<IBulk>> Unwatch()
        {
            throw new NotImplementedException();
        }

        public override MessageQueueBase<List<IBulk>> Discard()
        {
            CommandCallbacks.Clear();
            MessageBuff.Clear();

            return this;
        }
    }
}
