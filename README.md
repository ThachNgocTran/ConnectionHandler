# ConnectionHandler
Connection Handler for clients toward a variety of servers (Amazon S3, Amazon DynamoDB, MongoDB, RServe, SQL Server, ElasticSearch)

When making connections toward many servers (like above), I recognized that many of them share the same approach:

- Typically, the Server Providers offer some Client written for Java, in which they take care themselves of connection pooling for efficiency. Each server may be different a little bit to each other (e.g. optimization, time-out, etc.); so they would be better to be handled by the Server Providers, rather than put that burden on developers' shoulder.
- The client usually follow Singleton pattern. "Static" to initialize the single class if not available. "Instance" holding the raw client is used because the object holds "state".
- Dev can expose the "raw" client class to outside world (more flexible but dangerous), or they can control this through wrapping around the "raw" client and only expose needed functions.

This kind of approach is quick, simple, ideal for small-scale projects. No distributed load-balancing or complex stuff!

Just copy/paste and replace with real login credentials.

Updated: 25/07/2017
Exposing the "client" objects is one thing, using them is another thing. So I added some examples.

Updated: 02/08/2017
Example on invoking javascript code inside MongoDB Server. Like copy/paste javascript into MongoDB Console.

Have fun!
