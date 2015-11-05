## redu

**NO LONGER USED OR MAINTAINED** but if you find something useful have at it.

**redu** is a complete C# **redis** client, it has been developed for .NET 3.5 and MONO 2.6.7. The library has been designed to be asynchronous and fluid. Call as many commands as you like in a nice chained style, then call Wait() to block the thread and wait for a result. The API is very simple to use, but changes may happen, hopefully nothing drastic. See an example bellow.

###Example
The only class you really need to be interested in is *RedisConnection*, you may initialize it without parameters to use the default options. You can also wrap it up in a *using* block to automatically dispose of it. 

    using(var c = new RedisConnection())
    {
        string name = "Hank";
        string surname = "Venture";
        
        c.Open(); //You must explicitly open the connection.
        c.Strings.Get("user1:name", n => { name = n; })
            .Set("user1:surname", surname)
            .Wait(30000);
    }



### Features
All standard redis commands are supported.

* String, Hash, Set, SortedSet etc commands.
* Transactions and pipelining available.
* PubSub.
* Fluid, chained commands.
* Use transactions, commands and PubSub simultaneously on one connection.
* All commands are asynchronous.

