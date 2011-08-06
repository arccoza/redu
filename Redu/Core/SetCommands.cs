using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class SetCommands : CommandsBase<SetCommands>
    {
        public SetCommands(RedisConnection connection)
            : base(connection)
        {
        }

        public SetCommands(RedisConnection connection, Countdown countdownEvent)
            : base(connection, countdownEvent)
        {
        }
        
        protected override void Init()
        {
            Strings = new StringCommands(Connection, countdownEvent);
            Hashes = new HashCommands(Connection, countdownEvent);
            Lists = new ListCommands(Connection, countdownEvent);
            Sets = this;
            SortedSets = new SortedSetCommands(Connection, countdownEvent);
            PubSub = new PubSubCommands(Connection, countdownEvent);

            base.Init();
        }

        #region Set commands.

        public SetCommands Add(string key, string member, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SADD", new string[] { key, member });

            return (SetCommands)this;
        }

        public SetCommands Cardinality(string key, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SCARD", new string[] { key });

            return (SetCommands)this;
        }

        public SetCommands Difference<TK>(TK keys, Action<List<string>> callback, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            return Difference(keys, new List<string>(), callback, mq);
        }

        public SetCommands Difference<TK, TV>(TK keys, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "SDIFF", keys);

            return (SetCommands)this;
        }

        public SetCommands Difference<TK>(string destination, TK keys, Action<int> callback, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SDIFFSTORE", new string[] { destination }, keys);

            return (SetCommands)this;
        }

        public SetCommands Intersect<TK>(TK keys, Action<List<string>> callback, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            return Intersect(keys, new List<string>(), callback, mq);
        }

        public SetCommands Intersect<TK, TV>(TK keys, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "SINTER", keys);

            return (SetCommands)this;
        }

        public SetCommands Intersect<TK>(string destination, TK keys, Action<int> callback, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SINTERSTORE", new string[] { destination }, keys);

            return (SetCommands)this;
        }

        public SetCommands IsMember(string key, string member, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SISMEMBER", new string[] { key, member });

            return (SetCommands)this;
        }

        public SetCommands Members(string key, Action<List<string>> callback, MessageQueue mq = null)
        {
            return Members(key, new List<string>(), callback, mq);
        }

        public SetCommands Members<TV>(string key, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "SMEMBERS", new string[] { key });

            return (SetCommands)this;
        }

        public SetCommands Move(string source, string destination, string member, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SMOVE", new string[] { source, destination, member });

            return (SetCommands)this;
        }

        public SetCommands Pop(string key, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SPOP", new string[] { key });

            return (SetCommands)this;
        }

        public SetCommands RandomMember(string key, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SRANDMEMBER", new string[] { key });

            return (SetCommands)this;
        }

        public SetCommands Remove(string key, string member, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SREM", new string[] { key, member });

            return (SetCommands)this;
        }

        public SetCommands Union<TK>(TK keys, Action<List<string>> callback, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            return Union(keys, new List<string>(), callback, mq);
        }

        public SetCommands Union<TK, TV>(TK keys, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "SUNION", keys);

            return (SetCommands)this;
        }

        public SetCommands Union<TK>(string destination, TK keys, Action<int> callback, MessageQueue mq = null)
           where TK : ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SUNIONSTORE", new string[] { destination }, keys);

            return (SetCommands)this;
        }

        #endregion
    }
}
