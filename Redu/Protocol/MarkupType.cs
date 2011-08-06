using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redu.Protocol
{
    public enum MarkupType
    {
        None = 0,
        Status,
        Error,
        Integer,
        Bulk,
        MultiBulk,
        Term
    }
}
