using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class SortedSetCommands : CommandsBase<SortedSetCommands>
    {
        public SortedSetCommands(RedisConnection connection)
            : base(connection)
        {
        }

        public SortedSetCommands(RedisConnection connection, Countdown countdownEvent, uint[] selectedDb)
            : base(connection, countdownEvent, selectedDb)
        {
        }
        
        protected override void Init()
        {
            Strings = new StringCommands(Connection, _countdownEvent, _selectedDb);
            Hashes = new HashCommands(Connection, _countdownEvent, _selectedDb);
            Lists = new ListCommands(Connection, _countdownEvent, _selectedDb);
            Sets = new SetCommands(Connection, _countdownEvent, _selectedDb);
            SortedSets = this;
            PubSub = new PubSubCommands(Connection, _countdownEvent, _selectedDb);

            base.Init();
        }

        #region SortedSet commands.

        public SortedSetCommands Add(string key, double score, string member, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZADD", new string[] { key, score.ToString(), member });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Cardinality(string key, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZCARD", new string[] { key });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Count(string key, double min, double max, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZCOUNT", new string[] { key, min.ToString(), max.ToString() });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Increment(string key, double increment, string member, Action<double> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZINCRBY", new string[] { key, increment.ToString(), member });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Intersect<TK, TW>(string destination, TK keys, Action<int> callback = null, TW weights = null, Aggregate aggregate = default(Aggregate), MessageQueue mq = null)
            where TK : ICollection<string>
            where TW : class, ICollection<double>
        {
            var numKeys = keys.Count;
            var args = new List<string>();

            args.Add(destination);
            args.Add(numKeys.ToString());
            args.AddRange(keys);

            if (weights != null)
            {
                foreach (var w in weights)
                {
                    args.Add(w.ToString());
                }
            }

            if (aggregate != default(Aggregate))
            {
                args.Add("AGGREGATE");

                switch (aggregate)
                {
                    case Aggregate.Sum:
                        args.Add("SUM");
                        break;

                    case Aggregate.Min:
                        args.Add("MIN");
                        break;

                    case Aggregate.Max:
                        args.Add("MAX");
                        break;
                }
            }

            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZINTERSTORE", args);

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Range<TV>(string key, int start, int stop, TV values, Action<TV> callback = null, bool withscores = false, Order order = Order.Asc, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            var args = new List<string>();
            
            args.Add(key);
            args.Add(start.ToString());
            args.Add(stop.ToString());

            if (withscores)
                args.Add("WITHSCORES");

            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, order == Order.Desc ? "ZREVRANGE" : "ZRANGE", args);

            return (SortedSetCommands)this;
        }

        public SortedSetCommands RangeByScore<TV>(string key, RangeMarker min, RangeMarker max, Action<List<string>> callback, bool withscores = false, Limit limit = null, Order order = Order.Asc, MessageQueue mq = null)
        {
            return RangeByScore(key, min.ToString(), max.ToString(), new List<string>(), callback, withscores, limit, order, mq);
        }

        public SortedSetCommands RangeByScore<TV>(string key, double min, double max, Action<List<string>> callback, bool withscores = false, Limit limit = null, Order order = Order.Asc, MessageQueue mq = null)
        {
            return RangeByScore(key, min.ToString(), max.ToString(), new List<string>(), callback, withscores, limit, order, mq);
        }

        public SortedSetCommands RangeByScore<TV>(string key, RangeMarker min, RangeMarker max, TV values, Action<TV> callback = null, bool withscores = false, Limit limit = null, Order order = Order.Asc, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            return RangeByScore(key, min.ToString(), max.ToString(), values, callback, withscores, limit, order, mq);
        }

        public SortedSetCommands RangeByScore<TV>(string key, double min, double max, TV values, Action<TV> callback = null, bool withscores = false, Limit limit = null, Order order = Order.Asc, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            return RangeByScore(key, min.ToString(), max.ToString(), values, callback, withscores, limit, order, mq);
        }

        protected SortedSetCommands RangeByScore<TV>(string key, string min, string max, TV values, Action<TV> callback = null, bool withscores = false, Limit limit = null, Order order = Order.Asc, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            var args = new List<string>();

            args.Add(key);
            args.Add(min);
            args.Add(max);

            if (withscores)
                args.Add("WITHSCORES");

            if (limit != null)
            {
                args.Add("LIMIT");
                args.Add(limit.Offset.ToString());
                args.Add(limit.Count.ToString());
            }

            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, order == Order.Desc ? "ZREVRANGEBYSCORE" : "ZRANGEBYSCORE", args);

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Rank(string key, string member, Action<int?> callback, Order order = Order.Asc, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, order == Order.Desc ? "ZREVRANK" : "ZRANK", new string[] { key, member });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Remove(string key, string member, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZREM", new string[] { key, member });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands RemoveRangeByRank(string key, int start, int stop, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZREMRANGEBYRANK", new string[] { key, start.ToString(), stop.ToString() });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands RemoveRangeByScore(string key, RangeMarker min, RangeMarker max, Action<int> callback, MessageQueue mq = null)
        {
            return RemoveRangeByScore(key, min.ToString(), max.ToString(), callback, mq);
        }

        public SortedSetCommands RemoveRangeByScore(string key, double min, double max, Action<int> callback, MessageQueue mq = null)
        {
            return RemoveRangeByScore(key, min.ToString(), max.ToString(), callback, mq);
        }

        protected SortedSetCommands RemoveRangeByScore(string key, string min, string max, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZREMRANGEBYSCORE", new string[] { key, min, max });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Score(string key, string member, Action<int?> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZSCORE", new string[] { key, member });

            return (SortedSetCommands)this;
        }

        public SortedSetCommands Union<TK, TW>(string destination, TK keys, Action<int> callback = null, TW weights = null, Aggregate aggregate = default(Aggregate), MessageQueue mq = null)
            where TK : ICollection<string>
            where TW : class, ICollection<double>
        {
            var numKeys = keys.Count;
            var args = new List<string>();

            args.Add(destination);
            args.Add(numKeys.ToString());
            args.AddRange(keys);

            if (weights != null)
            {
                foreach (var w in weights)
                {
                    args.Add(w.ToString());
                }
            }

            if (aggregate != default(Aggregate))
            {
                args.Add("AGGREGATE");

                switch (aggregate)
                {
                    case Aggregate.Sum:
                        args.Add("SUM");
                        break;

                    case Aggregate.Min:
                        args.Add("MIN");
                        break;

                    case Aggregate.Max:
                        args.Add("MAX");
                        break;
                }
            }

            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ZUNIONSTORE", args);

            return (SortedSetCommands)this;
        }

        #endregion
    }
}
