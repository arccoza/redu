using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using Redu.Protocol;


namespace Redu
{
    public class RedisConnection : IDisposable
    {
        private string _host;
        private int _port;
        private Socket _socket;
        private EventHandler<SocketAsyncEventArgs> _socketEventHandler;
        private StringBuilder _rbuffer; //Receive buffer.
        private List<byte> _rbuffer2; //Receive buffer.

        public string Host
        {
            get { return _host; }
            protected set { _host = value; }
        }

        public int Port
        {
            get { return _port; }
            protected set { _port = value; }
        }

        public string ConnectionString
        {
            get { return Host + ":" + Port; }
        }

        protected Socket SharedSocket
        {
            get
            {
                return _socket;
            }
            set
            {
                _socket = value;
            }
        }

        public bool IsConnected
        {
            get
            {
                var s = SharedSocket;
                return s != null && s.Connected;
            }
        }

        protected Dictionary<Guid, Socket> Channels
        {
            get;
            set;
        }

        protected Util.ResourcePool<Socket> SocketPool;
        protected Util.ResourcePool<SocketAsyncEventArgs> EventArgsPool;

        public RedisConnection() : this("localhost", 6379)
        {
        }

        public RedisConnection(string connectionString)
        {
            var c = connectionString.Split(':');

            Host = c[0];
            Port = Convert.ToInt32(c[1]);
            Init();
        }

        public RedisConnection(string host, int port)
        {
            Host = host;
            Port = port;
            Channels = new Dictionary<Guid, Socket>();
            Init();
        }

        private void Init()
        {
            SocketPool = new Util.ResourcePool<Socket>(CreateSocket, DeleteSocket, 1, 10);
            EventArgsPool = new Util.ResourcePool<SocketAsyncEventArgs>(CreateEventArg, DeleteEventArg, 5, 30);
            _socketEventHandler = new EventHandler<SocketAsyncEventArgs>(ProcessSocketEvents);
            _rbuffer = new StringBuilder();
            _rbuffer2 = new List<byte>();
        }

        public void Open()
        {
            var s = SharedSocket;

            SocketPool = SocketPool == null ? new Util.ResourcePool<Socket>(CreateSocket, DeleteSocket) : SocketPool;
            EventArgsPool = EventArgsPool == null ? new Util.ResourcePool<SocketAsyncEventArgs>(CreateEventArg, DeleteEventArg) : EventArgsPool;
            
            if (s != null && !s.Connected)
            {
                DeleteSocket(s);
            }
            if (s == null)
            {
                s = CreateSocket();
                s.Connect(Host, Port);
                SharedSocket = s;
            }
        }

        public void Open(Guid channelId)
        {
            Socket s;

            if (!Channels.ContainsKey(channelId) || Channels[channelId] == null)
            {
                s = SocketPool.Request();
                Channels[channelId] = s;
            }
            else
            {
                s = Channels[channelId];
            }

            if (!s.Connected)
            {
                s.Connect(Host, Port);
            }
        }

        public void Close(Guid channelId)
        {
            Socket s;

            if (Channels.ContainsKey(channelId) && Channels[channelId] != null)
            {
                s = Channels[channelId];
                Channels.Remove(channelId);
                SocketPool.Release(s);
            }
        }

        public void Close()
        {
            var s = SharedSocket;
            DeleteSocket(s);
            SharedSocket = null;

            EventArgsPool.Dispose();
            SocketPool.Dispose();

            EventArgsPool = null;
            SocketPool = null;
        }

        protected Socket CreateSocket()
        {
            var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            //s.NoDelay = true;
            s.SendTimeout = -1;
            
            s.UseOnlyOverlappedIO = true;

            return s;
        }

        protected void DeleteSocket(Socket s)
        {
            if (s.Connected)
            {
                s.Shutdown(SocketShutdown.Both);
                s.Disconnect(false);
            }

            s.Close();
            s = null;
        }

        protected SocketAsyncEventArgs CreateEventArg()
        {
            var e = new SocketAsyncEventArgs();
            //e.Completed += _socketEventHandler;
            e.Completed += ProcessSocketEvents; //TODO: Why can you not reuse the delegate object? Research it.

            return e;
        }

        protected void DeleteEventArg(SocketAsyncEventArgs e)
        {
            //e.Completed -= _socketEventHandler;
            e.Completed -= ProcessSocketEvents;
            e.Dispose();
        }

        #region Send() overloads.

        public ReceiveHandler Send(string command, int numOfPackagesToRec = 0)
        {
            return Send(default(Guid), command, numOfPackagesToRec);
        }

        public ReceiveHandler Send(Guid channelId, string command, int numOfPackagesToRec = 0)
        {
            var r = new ReceiveHandler();
            r.ReceivePackageCount = numOfPackagesToRec;
            r.ChannelId = channelId;

            return Send(Encoding.UTF8.GetBytes(command), r);
        }

        public ReceiveHandler Send(byte[] command, int numOfPackagesToRec = 0)
        {
            return Send(default(Guid), command, numOfPackagesToRec);
        }

        public ReceiveHandler Send(Guid channelId, byte[] command, int numOfPackagesToRec = 0)
        {
            var r = new ReceiveHandler();
            r.ReceivePackageCount = numOfPackagesToRec;
            r.ChannelId = channelId;

            return Send(command, r);
        }

        public ReceiveHandler Send(string command, ReceiveCallback callback, int numOfPackagesToRec = 1)
        {
            return Send(default(Guid), command, callback, numOfPackagesToRec);
        }

        public ReceiveHandler Send(Guid channelId, string command, ReceiveCallback callback, int numOfPackagesToRec = 1)
        {
            var r = new ReceiveHandler();
            r.Callback = callback;
            r.ReceivePackageCount = numOfPackagesToRec;
            r.ChannelId = channelId;

            return Send(command, r);
        }

        public ReceiveHandler Send(byte[] command, ReceiveCallback callback, int numOfPackagesToRec = 1)
        {
            return Send(default(Guid), command, callback,numOfPackagesToRec);
        }

        public ReceiveHandler Send(Guid channelId, byte[] command, ReceiveCallback callback, int numOfPackagesToRec = 1)
        {
            var r = new ReceiveHandler();
            r.Callback = callback;
            r.ReceivePackageCount = numOfPackagesToRec;
            r.ChannelId = channelId;
            
            return Send(command, r);
        }

        public ReceiveHandler Send(string command, ReceiveHandler handler)
        {
            return Send(Encoding.UTF8.GetBytes(command), handler);
        }

        #endregion

        public ReceiveHandler Send(byte[] command, ReceiveHandler handler)
        {
            Socket s;

            if (handler == null)
            {
                handler = new ReceiveHandler();
                handler.ReceivePackageCount = 0;
            }

            if (!handler.Busy.WaitOne(0)) //Check if this ReceiveHandler object is currently in use.
            {
                throw new Exception("This ReceiveHandler object is currently in use.");
            }

            
            s = handler.ChannelId == default(Guid) ? SharedSocket : Channels[handler.ChannelId];

            if (s.Connected)
            {
                var e = EventArgsPool.Request();

                e.UserToken = handler;
                handler.Busy.Reset(); //Reset the waithandle for any threads who want to wait on this response.

                e.SetBuffer(command, 0, command.Length);
                
                if (!s.SendAsync(e))
                {
                    ProcessSocketEvents(s, e);
                }
                
                return handler;
            }
            else
            {
                throw new Exception("TODO: Add a specific exception. (Not connected exception)");
            }
        }

        public ReceiveHandler Receive(ReceiveHandler handler)
        {
            if (handler == null)
                return null;

            var s = handler.ChannelId == default(Guid) ? SharedSocket : Channels[handler.ChannelId];
            var e = EventArgsPool.Request();

            if (s.Connected)
            {
                if (handler != null)
                {
                    e.UserToken = handler;
                    handler.Busy.Reset(); //Reset the waithandle for any threads who want to wait on this response.
                }

                e.SetBuffer(new byte[0], 0, 0);

                if (!s.ReceiveAsync(e))
                {
                    ProcessSocketEvents(s, e);
                }

                return handler;
            }
            else
            {
                throw new Exception("TODO: Add a specific exception. (Not connected exception)");
            }
        }

        private object _recLock = new object();
        protected void ProcessSocketEvents(object sender, SocketAsyncEventArgs e)
        {
            var s = sender as Socket;
            var r = e.UserToken as ReceiveHandler;


            //TODO:
            if (e.SocketError != SocketError.Success)
            {
                ProcessError(s, e);
            }

            if (e.LastOperation == SocketAsyncOperation.Send)
            {
                if (r != null && r.ReceivePackageCount > 0)
                {
                    e.SetBuffer(new byte[0], 0, 0);

                    if (!s.ReceiveAsync(e))
                    {
                        ProcessSocketEvents(s, e);
                    }
                }
                else
                {
                    EventArgsPool.Release(e);
                    r.Busy.Set();
                }
            }
            else if (e.LastOperation == SocketAsyncOperation.Receive)
            {
                //var r = e.UserToken as ReceiveHandler;
                byte[] readBuff;
                int parsedLength = 0;

                if (r != null && r.ReceivePackageCount > 0)
                {
                    List<List<IBulk>> lpkg = new List<List<IBulk>>();

                    lock (_recLock)
                    {

                        readBuff = new byte[s.Available];

                        s.Receive(readBuff, SocketFlags.Peek);

                        parsedLength = MessageParser.Inst.Parse(readBuff, lpkg, r.ReceivePackageCount);

                        readBuff = new byte[parsedLength];
                        s.Receive(readBuff);

                        r.Packages = lpkg;

                        lpkg = null;
                    }

                    EventArgsPool.Release(e);

                    if (r.Callback != null)
                    {
                        r.Callback(r);
                    }
                }
                else
                {
                    EventArgsPool.Release(e);
                }
                
                r.Busy.Set();
            }
        }

        protected void ProcessError(object sender, SocketAsyncEventArgs e)
        {

        }

        #region Commands

        public StringCommands Strings
        {
            get { return new StringCommands(this); }
        }
        public HashCommands Hashes
        {
            get { return new HashCommands(this); }
        }
        public ListCommands Lists
        {
            get { return new ListCommands(this); }
        }
        public SetCommands Sets
        {
            get { return new SetCommands(this); }
        }
        public SortedSetCommands SortedSets
        {
            get { return new SortedSetCommands(this); }
        }
        public PubSubCommands PubSub
        {
            get { return new PubSubCommands(this); }
        }

        #endregion

        #region IDisposable

        private bool _disposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    foreach (var channel in Channels)
                    {
                        Close(channel.Key);
                    }

                    SocketPool.Dispose();
                    EventArgsPool.Dispose();

                    Close();
                }

                _disposed = true;
            }
        }

        ~RedisConnection()
        {
            Dispose(false);
        }

        #endregion
    }
    
}
