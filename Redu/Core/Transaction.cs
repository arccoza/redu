using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class Transaction : MessageQueue
    {
        private bool _reusable;
        public override bool IsReusable
        {
            get { return IsPipelined && _reusable; }
            set { _reusable = value; }
        }

        protected List<Action<List<IBulk>>> TransCallbacks
        {
            get;
            set;
        }

        protected bool IsInitial;
        protected bool SendingData;
        protected object IsInitialLocker;
        protected bool IsSealed;

        public Transaction(RedisConnection connection, bool pipelined = false, bool reusable = false)
            : base(connection)
        {
            //Open();
            IsPipelined = pipelined;
            _reusable = reusable;
            IsInitial = true;
            IsInitialLocker = new object();
            IsSealed = false;
            SendingData = false;

            TransCallbacks = new List<Action<List<IBulk>>>();
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
            CheckIsSealed();

            //CountdownEvent.Clear();

            CommandCallbacks.Add(callback);

            if (IsPipelined)
            {
                if (IsInitial)
                {
                    MessageBuff.AddRange(Transaction.MULTI);
                    MessageBuff.AddRange(msg);

                    TransCallbacks.Add(delegate(List<IBulk> inPkg)
                    {
                        CheckOk(inPkg[0]);
                    });

                    IsInitial = false;
                }
                else
                {
                    MessageBuff.AddRange(msg);
                }

                TransCallbacks.Add(delegate(List<IBulk> inPkg)
                {
                    CheckQueued(inPkg[0]);
                });
            }
            else
            {
                if (IsInitial)
                {
                    MessageBuff.AddRange(Transaction.MULTI);
                    MessageBuff.AddRange(msg);
                    msg = MessageBuff.ToArray();
                    MessageBuff.Clear();

                    CountdownEvent.AddCount(1);
                    Connection.Send(Id, msg, delegate(ReceiveHandler rh)
                    {
                        if (rh.Packages.Count == 2 && rh.Packages[0].Count == 1)
                        {
                            CheckOk(rh.Packages[0][0]);
                            CheckQueued(rh.Packages[1][0]);
                        }
                        else
                        {
                            //TODO: Add a specific exception.
                            throw new Exception("More data was received than expected for this operation.");
                        }

                        CountdownEvent.Signal();
                    }, 2);

                    IsInitial = false;
                    SendingData = true;
                }
                else
                {
                    CountdownEvent.AddCount(1);
                    Connection.Send(Id, msg, delegate(ReceiveHandler rh)
                    {
                        if (rh.Packages.Count == 1 && rh.Packages[0].Count == 1)
                        {
                            CheckQueued(rh.Packages[0][0]);
                        }
                        else
                        {
                            //TODO: Add a specific exception.
                            throw new Exception("More data was received than expected for this operation.");
                        }

                        CountdownEvent.Signal();
                    });

                    SendingData = true;
                }
            }

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Execute()
        {
            int totCallbacks = CommandCallbacks.Count + TransCallbacks.Count;
            MessageBuff.AddRange(Transaction.EXEC);
            IsSealed = true;
            SendingData = false;

            CountdownEvent.AddCount(1);
            Connection.Send(Id, MessageBuff.ToArray(), delegate(ReceiveHandler rh)
            {
                for (int i = 0; i < totCallbacks; i++)
                {
                    if (i < TransCallbacks.Count)
                    {
                        TransCallbacks[i](rh.Packages[i]);
                    }
                    else
                    {
                        CommandCallbacks[i - TransCallbacks.Count](rh.Packages[i]);
                    }
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
            if (!IsInitial)
            {
                throw new InvalidOperationException("Watch must be called before Queue.");
            }

            if (IsPipelined)
            {
                MessageBuff.AddRange(Transaction.WATCH(keys));

                TransCallbacks.Add(delegate(List<IBulk> inPkg)
                {
                    CheckOk(inPkg[0]);
                });
            }
            else
            {
                Connection.Send(Id, Transaction.WATCH(keys), delegate(ReceiveHandler rh)
                {
                    CheckOk(rh.Packages[0][0]);
                });
            }

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Unwatch()
        {
            if (IsPipelined)
            {
                MessageBuff.AddRange(Transaction.UNWATCH);

                if (IsInitial)
                {
                    TransCallbacks.Add(delegate(List<IBulk> inPkg)
                    {
                        CheckOk(inPkg[0]);
                    });
                }
                else
                {
                    TransCallbacks.Add(delegate(List<IBulk> inPkg)
                    {
                        CheckQueued(inPkg[0]);
                    });
                }
            }
            else
            {
                if (IsInitial)
                {
                    Connection.Send(Id, Transaction.UNWATCH, delegate(ReceiveHandler rh)
                    {
                        CheckOk(rh.Packages[0][0]);
                    });
                }
                else
                {
                    Connection.Send(Id, Transaction.UNWATCH, delegate(ReceiveHandler rh)
                    {
                        CheckQueued(rh.Packages[0][0]);
                    });
                }
            }

            return this;
        }

        public override MessageQueueBase<List<IBulk>> Discard()
        {
            if (SendingData)
            {
                Connection.Send(Id, Transaction.DISCARD, delegate(ReceiveHandler rh)
                {
                    CheckOk(rh.Packages[0][0]);
                });

                SendingData = false;
            }

            //if (!IsReusable)
            //{
                IsInitial = true;
                SendingData = false;
                IsSealed = false;
                TransCallbacks.Clear();
                CommandCallbacks.Clear();
                MessageBuff.Clear();
                
            //}

            Connection.Close(Id);

            return this;
        }

        //protected void CheckInUse()
        //{
        //    if (!CountdownEvent.IsSet && !CountdownEvent.TimedOut)
        //    {
        //        throw new InvalidOperationException("This transaction is still in use.");
        //    }
        //}

        protected void CheckIsSealed()
        {
            if (IsSealed)
            {
                throw new InvalidOperationException("This transaction is sealed.");
            }
        }




        #region Static helpers.

        private static byte[] _multi;
        public static byte[] MULTI
        {
            get 
            {
                if (_multi == null)
                {
                    var l = new List<byte>();
                    MessageBuilder.Inst.Build(l, "MULTI");
                    _multi = l.ToArray();
                }

                return _multi;
            }
        }

        private static byte[] _exec;
        public static byte[] EXEC
        {
            get
            {
                if (_exec == null)
                {
                    var l = new List<byte>();
                    MessageBuilder.Inst.Build(l, "EXEC");
                    _exec = l.ToArray();
                }

                return _exec;
            }
        }

        private static byte[] _unwatch;
        public static byte[] UNWATCH
        {
            get
            {
                if (_unwatch == null)
                {
                    var l = new List<byte>();
                    MessageBuilder.Inst.Build(l, "UNWATCH");
                    _unwatch = l.ToArray();
                }

                return _unwatch;
            }
        }

        private static byte[] _discard;
        public static byte[] DISCARD
        {
            get
            {
                if (_discard == null)
                {
                    var l = new List<byte>();
                    MessageBuilder.Inst.Build(l, "DISCARD");
                    _discard = l.ToArray();
                }

                return _discard;
            }
        }

        public static byte[] WATCH(ICollection<string> keys)
        {
            var l = new List<byte>();

            MessageBuilder.Inst.Build(l, "WATCH", keys);

            return l.ToArray();
        }

        #endregion
    }
}
