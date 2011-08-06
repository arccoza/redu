using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class StringCommands : CommandsBase<StringCommands>
    {
        public StringCommands(RedisConnection connection)
            : base(connection)
        {
        }

        public StringCommands(RedisConnection connection, Countdown countdownEvent)
            : base(connection, countdownEvent)
        {
        }
        
        protected override void Init()
        {
            Strings = this;
            Hashes = new HashCommands(Connection, countdownEvent);
            Lists = new ListCommands(Connection, countdownEvent);
            Sets = new SetCommands(Connection, countdownEvent);
            SortedSets = new SortedSetCommands(Connection, countdownEvent);
            PubSub = new PubSubCommands(Connection, countdownEvent);

            base.Init();
        }

        #region String commands.
        
        
        /// <summary>
        /// Appends <paramref name="value"/> to the value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the base value.</param>
        /// <param name="value">The value to append to the value at <paramref name="key"/>.</param>
        /// <param name="callback">The response callback, with the length of the string after the append operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Append(string key, string value, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "APPEND", new string[] { key, value });

            return (StringCommands)this;
        }

        /// <summary>
        /// Increment the value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value to increment.</param>
        /// <param name="callback">The response callback, with the value at <paramref name="key"/>.</param>
        /// <param name="increment">The amount by which to increment the value at <paramref name="key"/>, default value is 1.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Incr(string key, Action<int> callback, long increment = 1, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, increment == 1 ? "INCR" : "INCRBY", increment == 1 ? new string[] { key } : new string[] { key, increment.ToString() });

            return (StringCommands)this;
        }

        /// <summary>
        /// Decrement the value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value to increment.</param>
        /// <param name="callback">The response callback, with the value at <paramref name="key"/>.</param>
        /// <param name="decrement">The amount by which to decrement the value at <paramref name="key"/>, default value is 1.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Decr(string key, Action<int> callback, long decrement = 1, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, decrement == 1 ? "DECR" : "DECRBY", decrement == 1 ? new string[] { key } : new string[] { key, decrement.ToString() });

            return (StringCommands)this;
        }

        /// <summary>
        /// Get the value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="callback">The response callback, with value at <paramref name="key"/>.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Get(string key, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "GET", new string[] { key });

            return (StringCommands)this;
        }

        /// <summary>
        /// Get the values of all the given keys.
        /// </summary>
        /// <typeparam name="TK">The key collection type.</typeparam>
        /// <param name="keys">The locations of the values.</param>
        /// <param name="callback">The response callback, with the result of the operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Get<TK>(TK keys, Action<List<string>> callback, MessageQueue mq = null)
            where TK : ICollection<string>
        {
            return Get(keys, new List<string>(), callback, mq);
        }

        /// <summary>
        /// Get the values of all the given keys.
        /// </summary>
        /// <typeparam name="TK">The key collection type.</typeparam>
        /// <typeparam name="TV">The value collection type.</typeparam>
        /// <param name="keys">The locations of the values.</param>
        /// <param name="values">The collection in which to store the values.</param>
        /// <param name="callback">The response callback, with the result of the operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Get<TK, TV>(TK keys, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
            }, "MGET", keys);

            return (StringCommands)this;
        }

        /// <summary>
        /// Get the values of all the given keys.
        /// </summary>
        /// <typeparam name="TP">The key - value pair collection type.</typeparam>
        /// <param name="keyValuePairs">The key - value collection to fill based on it's keys.</param>
        /// <param name="callback">The response callback, with the result of the operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Get<TP>(TP keyValuePairs, Action<TP> callback = null, MessageQueue mq = null)
            where TP : class, IDictionary<string, string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetPairs(inPkg, keyValuePairs, callback);
            }, "MGET", keyValuePairs.Keys);

            return (StringCommands)this;
        }

        /// <summary>
        /// Get the bit value at <paramref name="offset"/> in the string value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="offset">The position of the bit in the value.</param>
        /// <param name="callback">The response callback, with the result of the operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands GetBit(string key, uint offset, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "GETBIT", new string[] { key, offset.ToString() });

            return (StringCommands)this;
        }

        /// <summary>
        /// Get a substring of the string stored at a <paramref name="key"/>. 
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="start">The start index of the substring to extract.</param>
        /// <param name="end">The end index of the string to extract.</param>
        /// <param name="callback">The response callback, with the result of the operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands GetRange(string key, uint start, uint end, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "GETRANGE", new string[] { key, start.ToString(), end.ToString() });

            return (StringCommands)this;
        }

        /// <summary>
        /// Set the string value of <paramref name="key"/> and respond with its old value.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="newValue">The new value to store at <paramref name="key"/>.</param>
        /// <param name="callback">The response callback, with the result of the operation.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands GetSet(string key, string newValue, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "GETSET", new string[] { key, newValue });

            return (StringCommands)this;
        }

        /// <summary>
        /// Set the value at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="value">The value to store.</param>
        /// <param name="expirySeconds">Set how long the key and it's value should live.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Set(string key, string value, uint expirySeconds, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "SETEX", new string[] { key, expirySeconds.ToString(), value });

            return (StringCommands)this;
        }

        /// <summary>
        /// Set the value at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="value">The value to store.</param>
        /// <param name="unique">If true, set the value of <paramref name="key"/>, only if <paramref name="key"/> does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Set(string key, string value, bool unique = false, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, unique ? "SETNX" : "SET", new string[] { key, value });

            return (StringCommands)this;
        }

        /// <summary>
        /// Set multiple keys to multiple values.
        /// </summary>
        /// <typeparam name="TK">The key collection type.</typeparam>
        /// <typeparam name="TV">The value collection type.</typeparam>
        /// <param name="keys">The keys to set.</param>
        /// <param name="values">The values to store at <paramref name="keys"/>.</param>
        /// <param name="unique">If true, set the value of <paramref name="key"/>, only if <paramref name="key"/> does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Set<TK, TV>(TK keys, TV values, bool unique = false, MessageQueue mq = null)
            where TK : ICollection<string>
            where TV : ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, unique ? "MSETNX" : "MSET", keys, values);

            return (StringCommands)this;
        }

        /// <summary>
        /// Set multiple keys to multiple values.
        /// </summary>
        /// <typeparam name="TP">The key - value pair collection type.</typeparam>
        /// <param name="keyValuePairs">The key - value collection to store.</param>
        /// <param name="unique">If true, set the value of <paramref name="key"/>, only if <paramref name="key"/> does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Set<TP>(TP keyValuePairs, bool unique = false, MessageQueue mq = null)
            where TP : IDictionary<string, string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, unique ? "MSETNX" : "MSET", keyValuePairs.Keys, keyValuePairs.Values);

            return (StringCommands)this;
        }

        /// <summary>
        /// Sets or clears the bit at <paramref name="offset"/> in the string value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="offset">The position of the bit in the value.</param>
        /// <param name="newValue">The bit value to set.</param>
        /// <param name="callback">The response callback, with the original value of the bit.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands SetBit(string key, uint offset, bool newValue, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SETBIT", new string[] { key, offset.ToString(), newValue ? "1" : "0" });

            return (StringCommands)this;
        }

        /// <summary>
        /// Overwrite part of a string at <paramref name="key"/> starting at the specified <paramref name="offset"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="offset">The position at which to start overwriting.</param>
        /// <param name="value">The value to write over the range.</param>
        /// <param name="callback">The response callback, with the length of the string.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands SetRange(string key, uint offset, string value, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "SETRANGE", new string[] { key, offset.ToString(), value });

            return (StringCommands)this;
        }

        /// <summary>
        /// Get the length of the value stored at <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The location of the value.</param>
        /// <param name="callback">The response callback, with the length of the string.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public StringCommands Length(string key, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
            }, "STRLEN", new string[] { key });

            return (StringCommands)this;
        }

        #endregion
    }
}
