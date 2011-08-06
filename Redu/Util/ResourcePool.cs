using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Util
{
    public class ResourcePool<TResource> : IDisposable
    {
        public delegate TResource CreateResourceDelegate();
        public delegate void DeleteResourceDelegate(TResource r);

        private bool _disposed;
        protected int maxAge = 2;
        private int _minSize = 3;
        private int _maxSize = 5;

        public int MinSize
        {
            get { return _minSize; }
            protected set { _minSize = value; }
        }

        public int MaxSize
        {
            get { return _maxSize; }
            protected set { _maxSize = value; }
        }

        protected Stack<ResourceWrapper> ResourceStack
        {
            get;
            set;
        }

        public int Count
        {
            get { return ResourceStack.Count; }
        }

        public CreateResourceDelegate Create;
        public DeleteResourceDelegate Delete;

        public ResourcePool(CreateResourceDelegate createResourceMethod, DeleteResourceDelegate deleteResourceMethod) : this(createResourceMethod, deleteResourceMethod, 3, 30000)
        {
        }

        public ResourcePool(CreateResourceDelegate createResourceMethod, DeleteResourceDelegate deleteResourceMethod, int minSize, int maxSize)
        {
            Create = createResourceMethod;
            Delete = deleteResourceMethod;
            _minSize = minSize;
            _maxSize = maxSize;
            InitStack();
        }

        protected void InitStack()
        {
            ResourceStack = new Stack<ResourceWrapper>();
            ResourceWrapper rw;

            for (var i = 0; i < _minSize; i++)
            {
                rw = new ResourceWrapper();
                rw.Resource = Create();
                //rw.TimeStamp = 0;
                ResourceStack.Push(rw);
            }
        }

        public TResource Request()
        {
            TResource r;

            lock (ResourceStack)
            {
                if (ResourceStack.Count > 0)
                {
                    r = ResourceStack.Pop().Resource;
                }
                else if (Create != null)
                {
                    r = Create();
                }
                else
                {
                    throw new NullReferenceException("You must assign a create method to the Create delegate before using this object.");
                }
            }

            return r;
        }

        public void Release(TResource r)
        {
            ResourceWrapper rw;
            //long age = 0;

            //if (ResourceStack.Count > 0)
            //{
            //    age = DateTime.UtcNow.Ticks - ((long)ResourceStack.Peek().TimeStamp);
            //}

            lock (ResourceStack)
            {
                if (ResourceStack.Count < _maxSize)
                {
                    rw = new ResourceWrapper();
                    rw.Resource = r;
                    ResourceStack.Push(rw);
                }
                else if (Delete != null)
                {
                    Delete(r);
                }
                else
                {
                    throw new NullReferenceException("You must assign a delete method to the Delete delegate before using this object.");
                }
            }
        }

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
                }

                if (ResourceStack.Count > 0)
                {
                    foreach (var r in ResourceStack)
                    {
                        Delete(r.Resource);
                    }
                }

                Create = null;
                Delete = null;

                _disposed = true;
            }
        }

        ~ResourcePool()
        {
            Dispose(false);
        }

        protected class ResourceWrapper
        {
            public TResource Resource
            {
                get;
                set;
            }

            /*public long TimeStamp
            {
                get;
                set;
            }*/
        }
    }
}
