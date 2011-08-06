using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Protocol
{
    public interface IMessageBuilder
    {
        void Build<T>(IList<T> pkg, out byte[] msg)
            where T : IList<byte>;
        void Build<T>(Func<IList<T>> pkg, out byte[] msg)
            where T : IList<byte>;
    }
}
