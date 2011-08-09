using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    using MessageCallback = Action<string, string[], string>;

    public sealed class PubSubCommands : CommandsBase<PubSubCommands>
    {
        public Guid Id
        {
            get;
            private set;
        }

        public int SubscriptionCount
        {
            get { return _subscriptionCount; }
            private set { _subscriptionCount = value; }
        }

        private Dictionary<string, MessageCallback> Subscriptions
        {
            get;
            set;
        }

        private Dictionary<string, MessageCallback> PSubscriptions
        {
            get;
            set;
        }

        private int _subscriptionCount;
        private object _subscriptionLocker;

        public PubSubCommands(RedisConnection connection)
            : base(connection)
        {
            Id = Guid.NewGuid();
            Subscriptions = new Dictionary<string, MessageCallback>();
            PSubscriptions = new Dictionary<string, MessageCallback>();
            _subscriptionLocker = new object();
        }

        public PubSubCommands(RedisConnection connection, Countdown countdownEvent, uint[] selectedDb)
            : base(connection, countdownEvent, selectedDb)
        {
            Id = Guid.NewGuid();
            Subscriptions = new Dictionary<string, MessageCallback>();
            PSubscriptions = new Dictionary<string, MessageCallback>();
            _subscriptionLocker = new object();
        }

        protected override void Init()
        {
            Strings = new StringCommands(Connection, _countdownEvent, _selectedDb);
            Hashes = new HashCommands(Connection, _countdownEvent, _selectedDb);
            Lists = new ListCommands(Connection, _countdownEvent, _selectedDb);
            Sets = new SetCommands(Connection, _countdownEvent, _selectedDb);
            SortedSets = new SortedSetCommands(Connection, _countdownEvent, _selectedDb);
            PubSub = this;

            base.Init();
        }

        public PubSubCommands Open()
        {
            Connection.Open(Id);

            return this;
        }

        public PubSubCommands Close()
        {
            Connection.Close(Id);

            return this;
        }

        #region PubSub commands.

        public PubSubCommands Pub(string channel, string message, Action<int> callback = null)
        {
            Commander(delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "PUBLISH", new string[] { channel, message });

            return (PubSubCommands)this;
        }

        public PubSubCommands Sub<TC>(TC channels, MessageCallback callback)
            where TC : ICollection<string>
        {
            return Subscribe(channels, callback, false);
        }

        public PubSubCommands PSub<TC>(TC patterns, MessageCallback callback)
            where TC : ICollection<string>
        {
            return Subscribe(patterns, callback, true);
        }

        public PubSubCommands Unsub()
        {
            return Unsubscribe<List<string>>(pattern: false);
        }

        public PubSubCommands Unsub(List<string> channels)
        {
            return Unsubscribe<List<string>>(channels: channels, pattern: false);
        }

        public PubSubCommands Unsub(List<string> channels, MessageCallback callback)
        {
            return Unsubscribe<List<string>>(channels: channels, callback: callback, pattern: false);
        }

        public PubSubCommands PUnsub()
        {
            return Unsubscribe<List<string>>(pattern: true);
        }

        public PubSubCommands PUnsub(List<string> channels)
        {
            return Unsubscribe<List<string>>(channels: channels, pattern: true);
        }

        public PubSubCommands PUnsub(List<string> channels, MessageCallback callback)
        {
            return Unsubscribe<List<string>>(channels: channels, callback: callback, pattern: true);
        }

        //public PubSubCommands UnSub<TC>(TC channels)
        //    where TC : ICollection<string>
        //{
        //    return Subscriber<TC, Dictionary<string, string>>("UNSUBSCRIBE", channels, skipReceive: true);
        //}

        

        #endregion

        #region Common methods.

        //protected PubSubCommands Subscriber<TC>(string instruction, TC channels, MessageCallback callback, bool skipReceive = false)
        //    where TC : ICollection<string>
        //{
        //    return Subscriber(instruction, channels, callback, skipReceive);
        //}

        private PubSubCommands Unsubscribe<TC>(TC channels = null, MessageCallback callback = null, bool pattern = false)
            where TC : class, ICollection<string>
        {
            var c = Connection;
            List<byte> outMsg;
            List<string> remChannels = new List<string>();
            bool remAll = false;
            Dictionary<string, MessageCallback> clientSubs;
            string instruction;

            if (!pattern)
            {
                clientSubs = Subscriptions;
                instruction = "unsubscribe";
            }
            else
            {
                clientSubs = PSubscriptions;
                instruction = "punsubscribe";
            }

            lock (clientSubs)
            {
                if (channels == null)
                {
                    //remChannels.AddRange(clientSubs.Keys);
                    remAll = true;
                    clientSubs.Clear();
                }
                else
                {
                    foreach (var channel in channels)
                    {
                        if (clientSubs.ContainsKey(channel))
                        {
                            if (callback == null)
                            {
                                clientSubs[channel] = null;
                                clientSubs.Remove(channel);
                                remChannels.Add(channel);
                            }
                            else
                            {
                                clientSubs[channel] -= callback;

                                if (clientSubs[channel] == null)
                                {
                                    clientSubs.Remove(channel);
                                    remChannels.Add(channel);
                                }
                                else
                                {
                                    callback(instruction, new string[] { channel }, SubscriptionCount.ToString());
                                }
                            }

                        }
                    }
                }
            }

            if (remChannels.Count > 0 || remAll)
            {
                outMsg = new List<byte>();
                MessageBuilder.Inst.Build(outMsg, instruction, remChannels);

                c.Send(Id, outMsg.ToArray()).Wait(30000);
            }

            _countdownEvent.AddCount(1);

            return this;
        }

        private object _handlerRegisterLocker = new object();
        private bool _isHandlerRegistered = false;
        private PubSubCommands Subscribe<TC>(TC channels, MessageCallback callback, bool pattern = false)
            where TC : ICollection<string>
        {
            var c = Connection;
            List<byte> outMsg;
            List<string> newChannels = new List<string>();
            Dictionary<string, MessageCallback> clientSubs;
            string instruction;

            if (!pattern)
            {
                clientSubs = Subscriptions;
                instruction = "subscribe";
            }
            else
            {
                clientSubs = PSubscriptions;
                instruction = "psubscribe";
            }

            lock (clientSubs)
            {
                foreach (var channel in channels)
                {
                    if (!clientSubs.ContainsKey(channel))
                    {
                        newChannels.Add(channel);
                        clientSubs.Add(channel, callback);
                    }
                    else
                    {
                        clientSubs[channel] += callback;
                        callback(instruction, new string[] { channel }, SubscriptionCount.ToString());

                        //if (callback(instruction, channel, SubscriptionCount.ToString())) //Don't add it if it returns false;
                        //{
                        //    clientSubs[channel] += callback;
                        //}
                    }
                }
            }

            if(newChannels.Count > 0)
            {
                outMsg = new List<byte>();
                MessageBuilder.Inst.Build(outMsg, instruction, channels);

                lock (_handlerRegisterLocker)
                {
                    if (_isHandlerRegistered)
                    {
                        c.Send(Id, outMsg.ToArray());//.Wait(30000);
                    }
                    else
                    {
                        _isHandlerRegistered = true;
                        if (!c.Send(Id, outMsg.ToArray(), ProcessMessage).Wait(60000))
                        {
                            throw new TimeoutException("The initial subscription timedout, it is uncertain if it was successful.");
                        }
                    }
                }
            }

            _countdownEvent.AddCount(1);

            return this;
        }

        private void ProcessMessage(ReceiveHandler h)
        {
            var c = Connection;
            string type = "";
            string[] channels = null;
            string message = "";

            lock (_subscriptionLocker)
            {
                foreach (var inPkg in h.Packages)
                {
                    type = inPkg[0].ToString().ToLower();

                    if (type == "subscribe" || type == "psubscribe" || type == "unsubscribe" || type == "punsubscribe")
                    {
                        SubscriptionCount = inPkg[2].ToInt32();
                        channels = new string[] { inPkg[1].ToString() };
                        message = inPkg[2].ToString();
                    }
                    else if (type == "message" || type == "pmessage")
                    {
                        channels = new string[inPkg.Count - 2];
                        //channel = inPkg[1].ToString();

                        for (int i = 0; i < channels.Length; i++)
                        {
                            channels[i] = inPkg[i + 1].ToString();
                        }
                        
                        message = inPkg[inPkg.Count - 1].ToString();
                    }

                    var subs = type[0] == 'p' ? PSubscriptions : Subscriptions;
                    if (channels != null && channels[0] != null && subs.ContainsKey(channels[0]))
                    {
                        subs[channels[0]](type, channels, message);
                    }


                    //var subs = type[0] == 'p' ? PSubscriptions : Subscriptions;
                    //if (subs.ContainsKey(channel))
                    //{
                    //    foreach (var d in subs[channel].GetInvocationList())
                    //    {
                    //        var m = (d as MessageCallback);

                    //        if (!m(type, channel, inPkg[2].ToString()) && (type == "subscribe" || type == "psubscribe" || type == "message" || type == "pmessage"))
                    //        {
                    //            Unsubscribe(new string[] { channel }, m, type[0] == 'p' ? true : false);
                    //        }
                    //    }
                    //}
                }

                if (type == "subscribe" || type == "psubscribe" || type == "unsubscribe" || type == "punsubscribe")
                {
                    _countdownEvent.Signal();
                }

                if (SubscriptionCount > 0)
                {
                    c.Receive(h);
                }
                else
                {
                    lock (_handlerRegisterLocker)
                    {
                        _isHandlerRegistered = false;
                    }
                }
            }
        }

        #endregion
    }
}
