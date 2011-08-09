using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class ListCommands : CommandsBase<ListCommands>
    {
        public ListCommands(RedisConnection connection)
            : base(connection)
        {
        }

        public ListCommands(RedisConnection connection, Countdown countdownEvent, uint[] selectedDb)
            : base(connection, countdownEvent, selectedDb)
        {
        }

        protected override void Init()
        {
            Strings = new StringCommands(Connection, _countdownEvent, _selectedDb);
            Hashes = new HashCommands(Connection, _countdownEvent, _selectedDb);
            Lists = this;
            Sets = new SetCommands(Connection, _countdownEvent, _selectedDb);
            SortedSets = new SortedSetCommands(Connection, _countdownEvent, _selectedDb);
            PubSub = new PubSubCommands(Connection, _countdownEvent, _selectedDb);

            base.Init();
        }

        #region List commands.

        public ListCommands BlockingPop<TK>(TK keys, TimeSpan timeout, Action<List<string>> callback, bool rightPop = false, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            return BlockingPop(keys, (uint)timeout.TotalSeconds, new List<string>(), callback, rightPop, mq);
        }

        public ListCommands BlockingPop<TK>(TK keys, uint timeout, Action<List<string>> callback, bool rightPop = false, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            return BlockingPop(keys, timeout, new List<string>(), callback, rightPop, mq);
        }

        public ListCommands BlockingPop<TK, TV>(TK keys, TimeSpan timeout, TV values, Action<TV> callback = null, bool rightPop = false, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : class, ICollection<string>
        {
            return BlockingPop(keys, (uint)timeout.TotalSeconds, values, callback, rightPop, mq);
        }

        public ListCommands BlockingPop<TK, TV>(TK keys, uint timeout, TV values, Action<TV> callback = null, bool rightPop = false, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : class, ICollection<string>
        {
            var args = new List<string>();

            args.AddRange(keys);
            args.Add(timeout.ToString());

            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, rightPop ? "BRPOP" : "BLPOP", args);

            return (ListCommands)this;
        }

        public ListCommands BlockingPopPush(string source, string destination, TimeSpan timeout, Action<string> callback = null, MessageQueue mq = null)
        {
            return BlockingPopPush(source, destination, (uint)timeout.TotalSeconds, callback, mq);
        }

        public ListCommands BlockingPopPush(string source, string destination, uint timeout, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "BRPOPLPUSH", new string[] { source, destination, timeout.ToString() });

            return (ListCommands)this;
        }

        public ListCommands Index(string key, int index, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "LINDEX", new string[] { key, index.ToString() });

            return (ListCommands)this;
        }

        public ListCommands Insert(string key, bool before, string pivot, string value, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]));
            }, "LINSERT", new string[] { key, before ? "BEFORE" : "AFTER", pivot, value });

            return (ListCommands)this;
        }

        public ListCommands Length(string key, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]));
            }, "LLEN");

            return (ListCommands)this;
        }

        public ListCommands Pop(string key, Action<string> callback, bool rightPop = false, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckValue(inPkg[0]));
            }, string.Format("{0}POP", rightPop ? "R" : "L"), new string[] { key });

            return (ListCommands)this;
        }

        public ListCommands Push(string key, string value, Action<int> callback, bool rightPush = false, bool listMustExist = false, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]));
            }, string.Format("{0}PUSH{1}", rightPush ? "R" : "L", listMustExist ? "X" : ""), new string[] { key, value });

            return (ListCommands)this;
        }

        public ListCommands Range(string key, int start, int stop, Action<List<string>> callback, MessageQueue mq = null)
        {
            return Range(key, start, stop, callback, mq);
        }

        public ListCommands Range<TV>(string key, int start, int stop, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "LRANGE", new string[] { key, start.ToString(), stop.ToString() });

            return (ListCommands)this;
        }

        public ListCommands Remove(string key, int index, string value, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]));
            }, "LREM", new string[] { key, index.ToString(), value });

            return (ListCommands)this;
        }

        public ListCommands Set(string key, int index, string value, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "LSET", new string[] { key, index.ToString(), value });

            return (ListCommands)this;
        }

        public ListCommands Trim(string key, int start, int stop, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "LTRIM", new string[] { key, start.ToString(), stop.ToString() });

            return (ListCommands)this;
        }

        public ListCommands BlockingPopPush<TV>(string source, string destination, Action<string> callback, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "RPOPLPUSH", new string[] { source, destination });

            return (ListCommands)this;
        }

        #endregion
    }
}
