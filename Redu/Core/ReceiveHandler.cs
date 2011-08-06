using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Redu.Protocol;


namespace Redu
{
    public delegate void ReceiveCallback(ReceiveHandler response);

    public class ReceiveHandler
    {
        public ManualResetEvent Busy
        {
            get;
            protected set;
        }

        public Guid ChannelId
        {
            get;
            set;
        }

        public int ReceivePackageCount
        {
            get;
            set;
        }

        public List<List<IBulk>> Packages
        {
            get;
            set;
        }

        public List<byte> Message
        {
            get;
            set;
        }

        public ReceiveCallback Callback
        {
            get;
            set;
        }

        public ReceiveHandler()
        {
            ChannelId = Guid.Empty;
            ReceivePackageCount = 1;
            Busy = new ManualResetEvent(true);
        }

        public bool Wait()
        {
            return Busy.WaitOne();
        }

        public bool Wait(int millisecondsTimeout)
        {
            return Busy.WaitOne(millisecondsTimeout);
        }
    }
}
