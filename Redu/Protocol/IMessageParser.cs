using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Protocol
{
    public interface IMessageParser
    {
        void Parse<TPkg>(IList<byte> msg, IList<TPkg> lpkg)
            where TPkg : class, IList<IBulk>, new();
        void Parse<TPkg>(IList<byte> msg, IList<Action<TPkg>> lapkg)
            where TPkg : class, IList<IBulk>, new();
    }
}
