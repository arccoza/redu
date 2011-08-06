using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public sealed class Countdown
    {
        public int CurrentCount
        {
            get { return count; }
        }

        public bool IsSet
        {
            get { return count == 0; }
        }

        public bool TimedOut
        {
            get { return timedOut; }
        }

        private object locker = new object();
        private int count;
        private System.Timers.Timer timer;
        private bool timedOut;

        public Countdown()
        {
            timer = new System.Timers.Timer();
            timer.Elapsed += new System.Timers.ElapsedEventHandler(Timeout);
            timer.Enabled = false;
            timedOut = false;
        }
        
        public Countdown(int initialCount) : this()
        {
            count = initialCount;
        }

        public void Signal()
        {
            AddCount(-1);
        }

        public void Signal(int signalCount)
        {
            if (signalCount > 0)
            {
                signalCount = signalCount * -1;
            }
            else if (signalCount == 0)
            {
                return;
            }

            AddCount(signalCount);
        }

        public void AddCount(int signalCount)
        {
            if (signalCount == 0)
                return;

            lock (locker)
            {
                count += signalCount;
                if (count <= 0)
                    System.Threading.Monitor.PulseAll(locker);
            }
        }

        public void Wait()
        {
            lock (locker)
                while (count > 0)
                    System.Threading.Monitor.Wait(locker);
        }

        public void Wait(int millisecondsTimeout)
        {
            lock (locker)
            {
                timedOut = false;
                timer.Interval = millisecondsTimeout;
                timer.Start();
                
                while (count > 0 && !timedOut)
                    System.Threading.Monitor.Wait(locker);

                timer.Stop();
            }
        }

        private void Timeout(object sender, System.Timers.ElapsedEventArgs ev)
        {
            lock (locker)
            {
                timedOut = true;
                System.Threading.Monitor.PulseAll(locker);
            }
        }

        public void Clear()
        {
            count = 0;
            timedOut = false;
        }
    }
}
