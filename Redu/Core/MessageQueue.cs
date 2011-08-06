using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public abstract class MessageQueue : MessageQueueBase<List<IBulk>>
    {
        public MessageQueue(RedisConnection connection)
            : base(connection)
        {
        }
    }
}
