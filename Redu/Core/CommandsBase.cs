using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public abstract class CommandsBase<TRet>
        where TRet : CommandsBase<TRet>
    {
        public RedisConnection Connection
        {
            get;
            protected set;
        }

        public StringCommands Strings
        {
            get;
            set;
        }

        public HashCommands Hashes
        {
            get;
            set;
        }

        public ListCommands Lists
        {
            get;
            set;
        }

        public SetCommands Sets
        {
            get;
            set;
        }

        public SortedSetCommands SortedSets
        {
            get;
            set;
        }

        public PubSubCommands PubSub
        {
            get;
            set;
        }

        protected MessageBuilder Builder
        {
            get { return MessageBuilder.Inst; }
        }

        protected MessageParser Parser
        {
            get { return MessageParser.Inst; }
        }

        protected uint[] _selectedDb;

        protected Countdown _countdownEvent;

        public CommandsBase(RedisConnection connection)
            : this(connection, new Countdown(0), new uint[] { 0 })
        {
            Init();
        }

        public CommandsBase(RedisConnection connection, Countdown countdownEvent, uint[] selectedDb)
        {
            Connection = connection;
            this._countdownEvent = countdownEvent;
            _selectedDb = selectedDb;
        }

        protected virtual void Init()
        {
            Strings.Strings = Strings;
            Strings.Hashes = Hashes;
            Strings.Lists = Lists;
            Strings.Sets = Sets;
            Strings.SortedSets = SortedSets;
            Strings.PubSub = PubSub;

            Hashes.Strings = Strings;
            Hashes.Hashes = Hashes;
            Hashes.Lists = Lists;
            Hashes.Sets = Sets;
            Hashes.SortedSets = SortedSets;
            Hashes.PubSub = PubSub;

            Lists.Strings = Strings;
            Lists.Hashes = Hashes;
            Lists.Lists = Lists;
            Lists.Sets = Sets;
            Lists.SortedSets = SortedSets;
            Lists.PubSub = PubSub;

            Sets.Strings = Strings;
            Sets.Hashes = Hashes;
            Sets.Lists = Lists;
            Sets.Sets = Sets;
            Sets.SortedSets = SortedSets;
            Sets.PubSub = PubSub;

            SortedSets.Strings = Strings;
            SortedSets.Hashes = Hashes;
            SortedSets.Lists = Lists;
            SortedSets.Sets = Sets;
            SortedSets.SortedSets = SortedSets;
            SortedSets.PubSub = PubSub;

            PubSub.Strings = Strings;
            PubSub.Hashes = Hashes;
            PubSub.Lists = Lists;
            PubSub.Sets = Sets;
            PubSub.SortedSets = SortedSets;
            PubSub.PubSub = PubSub;
        }

        #region Connection Commands.

        public TRet Auth(string password)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "AUTH", new string[] { password });

            return (TRet)this;
        }

        public TRet Echo(string message, Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "ECHO", new string[] { message });

            return (TRet)this;
        }

        public TRet Ping(Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                callback(CheckStatus(inPkg));
            }, "PING");

            return (TRet)this;
        }

        public TRet Quit()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "QUIT");

            return (TRet)this;
        }

        public TRet Select(uint index)
        {
            _selectedDb[0] = index;

            //Commander(delegate(List<IBulk> inPkg)
            //{
            //    CheckOk(inPkg);
            //}, "SELECT", new string[] { index.ToString() });

            return (TRet)this;
        }

        #endregion

        #region Key Commands.

        public TRet Del<TK>(TK keys, Action<int> callback)
            where TK : ICollection<string>
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "DEL", keys);

            return (TRet)this;
        }

        public TRet Exists(string key, Action<bool> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "EXISTS", new string[] { key });

            return (TRet)this;
        }

        public TRet Expire(string key, TimeSpan expiry, Action<bool> callback)
        {
            Expire(key, (uint)expiry.TotalSeconds, callback);

            return (TRet)this;
        }

        public TRet Expire(string key, uint expirySeconds, Action<bool> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "EXPIRE", new string[] { key, expirySeconds.ToString() });

            return (TRet)this;
        }

        public TRet ExpireAt(string key, DateTime timestamp, Action<bool> callback)
        {
            ExpireAt(key, (uint)(timestamp - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime()).TotalSeconds, callback);

            return (TRet)this;
        }

        public TRet ExpireAt(string key, uint unixTimestamp, Action<bool> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "EXPIREAT", new string[] { key, unixTimestamp.ToString() });

            return (TRet)this;
        }

        public TRet Keys(string pattern, Action<List<string>> callback)
        {
            return Keys(pattern, new List<string>(), callback);
        }

        public TRet Keys<TK>(string pattern, TK keys, Action<TK> callback = null)
            where TK : class, ICollection<string>
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, keys, callback);
            }, "KEYS", new string[] { pattern });

            return (TRet)this;
        }

        public TRet Move(string key, uint db, Action<bool> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "MOVE", new string[] { key, db.ToString() });

            return (TRet)this;
        }

        //TODO:
        //public TRet Object()
        //{
        //}

        public TRet Persist(string key, Action<bool> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "PERSIST", new string[] { key });

            return (TRet)this;
        }

        public TRet RandomKey(Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "RANDOMKEY");

            return (TRet)this;
        }

        public TRet Rename(string key, string newKey)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "RENAME", new string[] { key, newKey });

            return (TRet)this;
        }

        public TRet Rename(string key, string newKey, Action<bool> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "RENAMENX");

            return (TRet)this;
        }


        //TODO:
        public TRet Sort<TV>(string key, TV values, string by = null, Limit limit = null, IList<string> get = null, Order order = default(Order), bool alpha = false, string store = null, Action<TV> callback = null, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            var sortCmd = new List<string>();

            sortCmd.Add(key);
            if (by != null)
            {
                sortCmd.Add("BY");
                sortCmd.Add(by);
            }
            if (limit != null)
            {
                sortCmd.Add("LIMIT");
                sortCmd.Add(limit.Offset.ToString());
                sortCmd.Add(limit.Count.ToString());
            }
            if (get != null)
            {
                foreach (var g in get)
                {
                    sortCmd.Add("GET");
                    sortCmd.Add(g);
                }
            }
            if (order != default(Order))
            {
                sortCmd.Add(order == Order.Asc ? "ASC" : "DESC");
            }
            if (alpha)
            {
                sortCmd.Add("ALPHA");
            }
            if (store != null)
            {
                sortCmd.Add("STORE");
                sortCmd.Add(store);
            }

            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "SORT", sortCmd);

            return (TRet)this;
        }

        public TRet Ttl(string key, Action<int> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "TTL", new string[] { key });

            return (TRet)this;
        }

        public TRet Type(string key, Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                callback(CheckStatus(inPkg));
            }, "TYPE", new string[] { key });

            return (TRet)this;
        }

        #endregion

        #region Server Commands.

        public TRet BackgroundRewriteAOF()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "BGREWRITEAOF");

            return (TRet)this;
        }

        public TRet BackgroundSave()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "BGSAVE");

            return (TRet)this;
        }

        public TRet ConfigGet(string parameter, Action<Dictionary<string, string>> callback)
        {
            return ConfigGet(parameter, new Dictionary<string, string>(), callback);
        }

        public TRet ConfigGet<TP>(string parameter, TP config, Action<TP> callback = null)
            where TP : class, IDictionary<string, string>
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetPairs(inPkg, config, callback);
            }, "CONFIG", new string[] { "GET", parameter });

            return (TRet)this;
        }

        public TRet ConfigSet<TC>(string parameter, string value)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "CONFIG", new string[] { "SET", parameter, value });

            return (TRet)this;
        }

        public TRet ConfigResetStat()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "CONFIG", new string[] { "RESETSTAT" });

            return (TRet)this;
        }

        public TRet DbSize(Action<int> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "DBSIZE");

            return (TRet)this;
        }

        //public TRet DebugObject(string key, Action<string> callback)
        //{
        //    Commander(delegate(List<IBulk> inPkg)
        //    {
        //        callback(CheckValue(inPkg[0]));
        //    }, "DEBUG OBJECT", new string[] { key });

        //    return (TRet)this;
        //}

        //public TRet DebugSegfault(Action<string> callback)
        //{
        //    Commander(delegate(List<IBulk> inPkg)
        //    {
        //        callback(CheckValue(inPkg[0]));
        //    }, "DEBUG SEGFAULT");

        //    return (TRet)this;
        //}

        public TRet FlushAll()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "FLUSHALL");

            return (TRet)this;
        }

        public TRet FlushDb()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "FLUSHDB");

            return (TRet)this;
        }

        public TRet Info(Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "INFO");

            return (TRet)this;
        }

        public TRet LastSave(Action<int> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "LASTSAVE");

            return (TRet)this;
        }

        public TRet LastSave(Action<DateTime> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                DateTime t = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(CheckInt(inPkg[0]));
                callback(t);
            }, "LASTSAVE");

            return (TRet)this;
        }

        //public TRet Monitor(Action<string> callback)
        //{
        //    Commander(delegate(List<IBulk> inPkg)
        //    {
        //        callback(CheckValue(inPkg[0]));
        //    }, "MONITOR");

        //    return (TRet)this;
        //}

        public TRet Save()
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "Save");

            return (TRet)this;
        }

        public TRet Shutdown(Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                callback(CheckStatus(inPkg));
            }, "SHUTDOWN");

            return (TRet)this;
        }

        public TRet SlaveOf(string host, string port, Action<string> callback)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                callback(CheckStatus(inPkg));
            }, "SLAVEOF", new string[] { host, port });

            return (TRet)this;
        }

        public TRet SlaveOfNone(Action<string> callback)
        {
            SlaveOf("NO", "ONE", callback);

            return (TRet)this;
        }

        //SYNC

        #endregion

        #region Transaction Commands.

        public TRet Execute(MessageQueueBase<List<IBulk>> mq)
        {
            mq.Open().Execute();

            return (TRet)this;
        }

        public TRet Watch<T>(T keys, MessageQueueBase<List<IBulk>> mq)
            where T : ICollection<string>
        {
            mq.Watch(keys);

            return (TRet)this;
        }

        public TRet Unwatch(MessageQueueBase<List<IBulk>> mq)
        {
            mq.Unwatch();

            return (TRet)this;
        }

        public TRet Discard(MessageQueueBase<List<IBulk>> mq)
        {
            mq.Discard();

            return (TRet)this;
        }

        #endregion

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

        protected string CheckOk(List<IBulk> inPkg)
        {
            if (inPkg != null && inPkg[0] != null && inPkg[0] is IStatus && inPkg[0].ToString().ToUpper() == "OK")
            {
                return inPkg[0].ToString();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", inPkg[0] == null ? "NULL" : inPkg[0].ToString(), inPkg[0].GetType().ToString()));
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

        protected string CheckStatus(List<IBulk> inPkg)
        {
            if (inPkg != null && inPkg[0] != null && inPkg[0] is IStatus)
            {
                return inPkg[0].ToString();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", inPkg[0] == null ? "NULL" : inPkg[0].ToString(), inPkg[0].GetType().ToString()));
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

        protected int? CheckNullInt<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (value is IInteger)
            {
                return value == null ? null : (int?)value.ToInt32();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        public double CheckDouble<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (!(value is IStatus || value is IInteger) && value != null)
            {
                return value.ToDouble();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        public double? CheckNullDouble<T>(T value)
            where T : IBulk
        {
            CheckError(value);

            if (!(value is IStatus || value is IInteger))
            {
                return value == null ? null : (double?)value.ToDouble();
            }
            else
            {
                throw new InvalidOperationException(string.Format("Unexpected response. Value: {0}\n Type: {1}", value == null ? "NULL" : value.ToString(), value.GetType().ToString()));
            }
        }

        public string CheckValue<T>(T value)
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

        protected void StdGetValue(List<IBulk> inPkg, Action<bool> callback)
        {
            if (inPkg != null)
            {
                if (inPkg.Count > 0)
                {
                    if (callback != null)
                        callback(CheckInt(inPkg[0]) > 0 ? true : false);
                }
            }
            else
            {
                //Do nothing.
            }
        }

        protected void StdGetValue(List<IBulk> inPkg, Action<bool?> callback)
        {
            if (inPkg != null)
            {
                if (inPkg.Count > 0)
                {
                    if (callback != null)
                    {
                        var r = CheckNullInt(inPkg[0]);
                        callback(r == null ? null : (bool?)(r > 0 ? true : false));
                    }
                }
            }
            else
            {
                if (callback != null)
                    callback(null);
            }
        }

        protected void StdGetValue(List<IBulk> inPkg, Action<int> callback)
        {
            if (inPkg != null)
            {
                if (inPkg.Count > 0)
                {
                    if (callback != null)
                        callback(CheckInt(inPkg[0]));
                }
            }
            else
            {
                //Do nothing.
                //if (callback != null)
                //    callback(0);
            }
        }

        protected void StdGetValue(List<IBulk> inPkg, Action<int?> callback)
        {
            if (inPkg != null)
            {
                if (inPkg.Count > 0)
                {
                    if (callback != null)
                        callback(CheckInt(inPkg[0]));
                }
            }
            else
            {
                if (callback != null)
                    callback(null);
            }
        }

        protected void StdGetValue(List<IBulk> inPkg, Action<double> callback)
        {
            if (inPkg != null)
            {
                if (inPkg.Count > 0)
                {
                    if (callback != null)
                        callback(CheckDouble(inPkg[0]));
                }
            }
            else
            {
                //Do nothing.
                //if (callback != null)
                //    callback(null);
            }
        }

        protected void StdGetValue(List<IBulk> inPkg, Action<double?> callback)
        {
            if (inPkg != null)
            {
                if (inPkg.Count > 0)
                {
                    if (callback != null)
                        callback(CheckNullDouble(inPkg[0]));
                }
            }
            else
            {
                if (callback != null)
                    callback(null);
            }
        }

        protected void StdGetValue(List<IBulk> inPkg, Action<string> callback)
        {
            if (inPkg != null)
            {
                if(inPkg.Count > 0)
                {
                    if (callback != null)
                        callback(CheckValue(inPkg[0]));
                }
            }
            else
            {
                if (callback != null)
                    callback(null);
            }
        }

        protected void StdGetValues<TV>(List<IBulk> inPkg, TV values, Action<TV> callback)
            where TV : class, ICollection<string>
        {
            if (inPkg != null)
            {
                foreach (var item in inPkg)
                {
                    values.Add(CheckValue(item));
                }
            }
            else
            {
                values = null;
            }

            if (callback != null)
                callback(values);
        }

        protected void StdGetPairs<TP>(List<IBulk> inPkg, TP pairs, Action<TP> callback)
            where TP : class, IDictionary<string, string>
        {
            if (inPkg != null)
            {
                if (pairs.Count > 0)
                {
                    var keys = pairs.Keys.ToList();
                    var enKeys = keys.GetEnumerator();

                    foreach (var item in inPkg)
                    {
                        if (enKeys.MoveNext())
                        {
                            pairs[enKeys.Current] = CheckValue(item);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < inPkg.Count; i += 2)
                    {
                        pairs[CheckValue(inPkg[i])] = CheckValue(inPkg[i + 1]);
                    }
                }
            }
            else
            {
                pairs = null;
            }

            if (callback != null)
                callback(pairs);
        }

        protected void Commander(Action<List<IBulk>> callback, string instruction, params ICollection<string>[] tableOfArgs/*argCollections*/)
            //where TPkg : List<IBulk>, new()
        {
            Commander(null, callback, instruction, tableOfArgs);
        }

        protected void Commander(MessageQueue mq, Action<List<IBulk>> callback, string instruction, params ICollection<string>[] tableOfArgs)
            //where TPkg : List<IBulk>, new()
            //where TArgs : ICollection<string>
        {
            List<byte> msg = new List<byte>();

            //MessageBuilder.Inst.Build(msg, instruction, tableOfArgs);

            if (mq != null)
            {
                MessageBuilder.Inst.Build(msg, "SELECT", new string[] { _selectedDb[0].ToString() });
                mq.Queue(msg.ToArray(), delegate(List<IBulk> inPkg)
                {
                    CheckOk(inPkg);
                });

                msg.Clear();

                MessageBuilder.Inst.Build(msg, instruction, tableOfArgs);
                mq.Queue(msg.ToArray(), callback);

                return;
            }
            else
            {
                MessageBuilder.Inst.Build(msg, "SELECT", new string[] { _selectedDb[0].ToString() });
                MessageBuilder.Inst.Build(msg, instruction, tableOfArgs);
            }

            _countdownEvent.AddCount(1);

            Connection.Send(msg.ToArray(), delegate(ReceiveHandler h)
            {
                CheckOk(h.Packages[0]); //Check pipelined db select was ok.
                callback((List<IBulk>)h.Packages[1]); //Send data to callback.

                _countdownEvent.Signal();
            }, 2);
        }

        //protected void Commander(MessageQueue mq, Action<List<IBulk>> callback, string instruction, params IEnumerable<ICollection<string>>[] tablesOfArgs)
        //{
        //    List<byte> msg = new List<byte>();

        //    MessageBuilder.Inst.Build(msg, instruction, tablesOfArgs);

        //    if (mq != null)
        //    {
        //        mq.Queue(msg.ToArray(), callback);

        //        return;
        //    }

        //    countdownEvent.AddCount(1);

        //    Connection.Send(msg.ToArray(), delegate(ReceiveHandler h)
        //    {
        //        callback((List<IBulk>)h.Packages[0]);

        //        countdownEvent.Signal();
        //    });
        //}

        public TRet Wait()
        {
            _countdownEvent.Wait();

            return (TRet)this;
        }

        public TRet Wait(int millisecondsTimeout)
        {
            _countdownEvent.Wait(millisecondsTimeout);

            return (TRet)this;
        }

        #endregion
    }
}
