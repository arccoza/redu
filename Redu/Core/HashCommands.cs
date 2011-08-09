using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Redu.Util;
using Redu.Protocol;


namespace Redu
{
    public class HashCommands : CommandsBase<HashCommands>
    {
        public HashCommands(RedisConnection connection)
            : base(connection)
        {
        }

        public HashCommands(RedisConnection connection, Countdown countdownEvent, uint[] selectedDb)
            : base(connection, countdownEvent, selectedDb)
        {
        }

        protected override void Init()
        {
            Strings = new StringCommands(Connection, _countdownEvent, _selectedDb);
            Hashes = this;
            Lists = new ListCommands(Connection, _countdownEvent, _selectedDb);
            Sets = new SetCommands(Connection, _countdownEvent, _selectedDb);
            SortedSets = new SortedSetCommands(Connection, _countdownEvent, _selectedDb);
            PubSub = new PubSubCommands(Connection, _countdownEvent, _selectedDb);

            base.Init();
        }

        #region Hash commands.

        /// <summary>
        /// Delete the <paramref name="field"/> from the hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="field">The field in the hash.</param>
        /// <param name="callback">The response callback, true if field was present in the hash and is now removed, false if field does not exist in the hash, or key does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Del(string key, string field, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]) > 0 ? true : false);
            }, "HDEL", new string[] { key, field });

            return (HashCommands)this;
        }

        /// <summary>
        /// Determine if <paramref name="field"/> exists in the hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="field">The field in the hash.</param>
        /// <param name="callback">The response callback, true if the hash contains field, false if the hash does not contain field, or key does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Exists(string key, string field, Action<bool> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]) > 0 ? true : false);
            }, "HEXISTS", new string[] { key });

            return (HashCommands)this;
        }

        /// <summary>
        /// Get the value of <paramref name="field"/> in the hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="field">The field in the hash.</param>
        /// <param name="callback">The response callback, with the value stored at <paramref name="field"/>.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Get(string key, string field, Action<string> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckValue(inPkg[0]));
            }, "HGET", new string[] { key, field });

            return (HashCommands)this;
        }

        /// <summary>
        /// Get the values of all the given hash <paramref name="fields"/>.
        /// </summary>
        /// <typeparam name="TF">The field collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="fields">The collection of fields.</param>
        /// <param name="callback">The response callback, with the values stored at <paramref name="fields"/>.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Get<TF>(string key, TF fields, Action<List<string>> callback, MessageQueue mq = null)
            where TF : ICollection<string>
        {
            return Get(key, fields, new List<string>(), callback, mq);
        }

        /// <summary>
        /// Get the values of all the given hash <paramref name="fields"/>.
        /// </summary>
        /// <typeparam name="TF">The field collection type.</typeparam>
        /// <typeparam name="TV">The value collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="fields">The collection of fields.</param>
        /// <param name="values">The collection of values to be filled.</param>
        /// <param name="callback">The response callback, with the values stored at <paramref name="fields"/>.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Get<TF, TV>(string key, TF fields, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TF : ICollection<string>
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
                //var vals = values;
                //foreach (var item in inPkg)
                //{
                //    vals.Add(CheckValue(item));
                //}

                //if(callback != null)
                //    callback(vals);
            }, "HMGET", new string[] { key }, fields);

            return (HashCommands)this;
        }

        /// <summary>
        /// Get the values of all the given hash <paramref name="fields"/>.
        /// </summary>
        /// <typeparam name="TP">The field - value pair collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="fieldValuePairs">The field - value collection to fill based on it's fields.</param>
        /// <param name="callback">The response callback, with the field - value pair collection.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Get<TP>(string key, TP fieldValuePairs, Action<TP> callback = null, MessageQueue mq = null)
            where TP : class, IDictionary<string, string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetPairs(inPkg, fieldValuePairs, callback);
                //var pairs = fieldValuePairs;
                //var keys = pairs.Keys.ToList();
                //var enKeys = keys.GetEnumerator();

                //foreach (var item in inPkg)
                //{
                //    if (enKeys.MoveNext())
                //    {
                //        pairs[enKeys.Current] = CheckValue(item);
                //    }
                //    else
                //    {
                //        break;
                //    }
                //}

                //if (callback != null)
                //    callback(pairs);
            }, "HMGET", new string[] { key }, fieldValuePairs.Keys);

            return (HashCommands)this;
        }

        //public HashCommands GetAll(string key, Action<List<string>> callback, MessageQueue mq = null)
        //{
        //    return GetAll(key, new List<string>(), callback, mq);
        //}

        //public HashCommands GetAll(string key, ICollection<string> values, Action<ICollection<string>> callback = null, MessageQueue mq = null)
        //    //where TV : ICollection<string>
        //{
        //    Commander(mq, delegate(List<IBulk> inPkg)
        //    {
        //        var vals = values;
        //        foreach (var item in inPkg)
        //        {
        //            vals.Add(CheckValue(item));
        //        }

        //        if (callback != null)
        //            callback(vals);
        //    }, "HGETALL", new string[] { key });

        //    return (HashCommands)this;
        //}

        /// <summary>
        /// Get all the fields and values in a hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="callback">The response callback, with the field - value pair collection.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands GetAll(string key, Action<Dictionary<string, string>> callback, MessageQueue mq = null)
        {
            return GetAll(key, new Dictionary<string, string>(), callback, mq);
        }

        /// <summary>
        /// Get all the fields and values in a hash.
        /// </summary>
        /// <typeparam name="TP">The field - value pair collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="fieldValuePairs">The field - value collection to be filled.</param>
        /// <param name="callback">The response callback, with the field - value pair collection.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands GetAll<TP>(string key, TP fieldValuePairs, Action<TP> callback = null, MessageQueue mq = null)
            where TP : class, IDictionary<string, string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetPairs(inPkg, fieldValuePairs, callback);
                //if (inPkg != null)
                //{
                //    var pairs = fieldValuePairs;

                //    for (int i = 0; i < inPkg.Count; i += 2)
                //    {
                //        pairs[CheckValue(inPkg[i])] = CheckValue(inPkg[i + 1]);
                //    }

                //    if (callback != null)
                //        callback(pairs);
                //}
                //else
                //{
                //    callback(null);
                //}
            }, "HGETALL", new string[] { key });

            return (HashCommands)this;
        }

        /// <summary>
        /// Increment/decrement the integer value of the hash <paramref name="field"/> by the given number.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="field">The field to increment/decrement.</param>
        /// <param name="callback">The response callback, with the value at <paramref name="field"/> after the increment operation.</param>
        /// <param name="increment">The amount by which to increment/decrement <paramref name="field"/>.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Incr(string key, string field, Action<int> callback, long increment = 1, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]));
            }, "HINCRBY", new string[] { key, field, increment.ToString() });

            return (HashCommands)this;
        }

        /// <summary>
        /// Get all the fields in the hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="callback">The response callback, with the collection of fields.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands GetKeys(string key, Action<List<string>> callback, MessageQueue mq = null)
        {
            return GetKeys(key, new List<string>(), callback, mq);
        }

        /// <summary>
        /// Get all the fields in the hash.
        /// </summary>
        /// <typeparam name="TK">The fields collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="fields">The collection of fields to be filled.</param>
        /// <param name="callback">The response callback, with the collection of fields.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands GetKeys<TK>(string key, TK fields, Action<TK> callback = null, MessageQueue mq = null)
            where TK : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, fields, callback);
                //var vals = fields;
                //foreach (var item in inPkg)
                //{
                //    vals.Add(CheckValue(item));
                //}

                //if (callback != null)
                //    callback(vals);
            }, "HKEYS", new string[] { key });

            return (HashCommands)this;
        }

        /// <summary>
        /// Get the number of fields in the hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="callback">The response callback, with number of fields in the hash, or 0 when key does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Length(string key, Action<int> callback, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]));
            }, "HLEN", new string[] { key });

            return (HashCommands)this;
        }

        /// <summary>
        /// Set the string <paramref name="value"/> of the hash <paramref name="field"/>.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="field">The field in the hash.</param>
        /// <param name="value">The value to store.</param>
        /// <param name="callback">The response callback, true if field is a new field in the hash and value was set, false if field already exists in the hash and the value was updated.</param>
        /// <param name="unique">If true, set the value of <paramref name="field"/>, only if <paramref name="field"/> does not exist.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Set(string key, string field, string value, Action<bool> callback, bool unique = false, MessageQueue mq = null)
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValue(inPkg, callback);
                //callback(CheckInt(inPkg[0]) > 0 ? true : false);
            }, unique ? "HSETNX" : "HSET", new string[] { key, field, value });

            return (HashCommands)this;
        }

        /// <summary>
        /// Set multiple hash fields to multiple values.
        /// </summary>
        /// <typeparam name="TF">The field collection type.</typeparam>
        /// <typeparam name="TV">The value collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="fields">The fields to set.</param>
        /// <param name="values">The values to store at <paramref name="fields"/>.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Set<TF, TV>(string key, TF fields, TV values, MessageQueue mq = null)
            where TF : ICollection<string>
            where TV : ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "HMSET", new string[] { key }, fields, values);

            return (HashCommands)this;
        }

        /// <summary>
        /// Set multiple hash fields to multiple values.
        /// </summary>
        /// <typeparam name="TP">The field - value collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="keyValuePairs">The fields and values to set.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands Set<TP>(string key, TP keyValuePairs, MessageQueue mq = null)
            where TP : IDictionary<string, string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                CheckOk(inPkg);
            }, "HMSET", new string[] { key }, keyValuePairs.Keys, keyValuePairs.Values);

            return (HashCommands)this;
        }

        /// <summary>
        /// Get all the values in the hash.
        /// </summary>
        /// <param name="key">The location of the hash.</param>
        /// <param name="callback">The response callback, with the collection of values.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands GetValues(string key, Action<List<string>> callback, MessageQueue mq = null)
        {
            return GetValues(key, new List<string>(), callback, mq);
        }

        /// <summary>
        /// Get all the values in the hash.
        /// </summary>
        /// <typeparam name="TV">The value collection type.</typeparam>
        /// <param name="key">The location of the hash.</param>
        /// <param name="values">The collection of values to be filled.</param>
        /// <param name="callback">The response callback, with the collection of values.</param>
        /// <param name="mq">Supply a Transaction or Pipeline object to enable transactions or pipelining.</param>
        /// <returns>The current object, for method chaining.</returns>
        public HashCommands GetValues<TV>(string key, TV values, Action<TV> callback = null, MessageQueue mq = null)
            where TV : class, ICollection<string>
        {
            Commander(mq, delegate(List<IBulk> inPkg)
            {
                StdGetValues(inPkg, values, callback);
                //var vals = values;
                //foreach (var item in inPkg)
                //{
                //    vals.Add(CheckValue(item));
                //}

                //if (callback != null)
                //    callback(vals);
            }, "HVALS", new string[] { key });

            return (HashCommands)this;
        }

        #endregion
    }
}
