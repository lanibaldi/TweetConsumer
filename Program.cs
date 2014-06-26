using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;

namespace TweetConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            // Thread count
            int threadCount = 1;
            string threadCountAsString = ConfigurationManager.AppSettings["ThreadCount"];
            if (!string.IsNullOrEmpty(threadCountAsString))
                threadCount = int.Parse(threadCountAsString);

            string queueName = ConfigurationManager.AppSettings["QueueName"];

            string collectionName = ConfigurationManager.AppSettings["CollectionName"];
            if (string.IsNullOrEmpty(collectionName))
                collectionName = "tweets";

            bool removeAll = args.Length > 1 && args[1] == "-clear";

            while (true)
            {
                try
                {
                    //Message msg = ReceiveTweet(queueName);
                    //if (args.Length > 0 && msg != null)
                    //{
                    //    if (args[0] == "-mongodb")
                    //    {
                    //        Consume(args, msg, collectionName, removeAll);
                    //    }
                    //}

                    if (args.Length > 0)
                    {
                        if (args[0] == "-mongodb")
                        {
                            // Launch a bunch of threads to consume messages in the queue
                            List<Thread> threads = new List<Thread>(threadCount);
                            for (int i = 1; i <= threadCount; i++)
                            {
                                var threadReceiver = new ConsumerThread();
                                Thread thread = threadReceiver.Start(string.Concat("T", i.ToString()),
                                    queueName, collectionName, removeAll);
                                threads.Add(thread);
                            }
                            foreach (Thread thread in threads)
                                thread.Join();                                 
                        }                   
                    }

                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Error in polling for tweets", e);
                }
                Console.WriteLine("Sleeping...");
                Thread.Sleep(5000);
            }
        }

        /*
        private static Message ReceiveTweet(string queueName)
        { 
            if (!MessageQueue.Exists(queueName))
            {
                Console.WriteLine("Queue {0} does not exist.", queueName);
                return null;
            }

            Message msg = null;
            try
            {
                MessageQueue queue = new MessageQueue(queueName);
                queue.Formatter = new System.Messaging.XmlMessageFormatter(new String[] { "System.String,mscorlib" });
                using (var ts = new TransactionScope())
                {
                    msg = queue.Receive(TimeSpan.FromSeconds(10), MessageQueueTransactionType.Automatic);
                    ts.Complete();
                }

                Console.WriteLine(msg.Label);
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
            return msg;
        }

        private static void Consume(Message msg, string collectionName, bool removeAll)
        {
            try
            {
                string connectionString = "mongodb://localhost";
                var client = new MongoClient(connectionString);
                var server = client.GetServer();
                var database = server.GetDatabase("tcat"); // "tcat" is the name 
                if (!database.CollectionExists(collectionName))
                {
                    database.CreateCollection(collectionName);
                }
                var collection = database.GetCollection(collectionName);
                if (collection != null)
                {
                    if (removeAll)
                        collection.RemoveAll();

                    try
                    {
                        var my_tweet = new MyTweet();
                        string content = msg.Body as string;
                        JsonConvert.PopulateObject("{" + content + "}", my_tweet);

                        var doc = new MongoDB.Bson.BsonDocument();
                        var elems = new List<MongoDB.Bson.BsonElement>();
                        elems.Add(new MongoDB.Bson.BsonElement("CreationDate", DateTime.Parse(my_tweet.Created_at)));
                        elems.Add(new MongoDB.Bson.BsonElement("Id", my_tweet.Id));
                        elems.Add(new MongoDB.Bson.BsonElement("Text", my_tweet.Text));
                        elems.Add(new MongoDB.Bson.BsonElement("User", my_tweet.User));
                        doc.AddRange(elems);

                        collection.Insert(doc);
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(msg.Body);
                        throw new Exception(e.Message);
                    }
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
        }
        */
    }

    [JsonObject(MemberSerialization.OptOut)]
    public class MyTweet 
    {
        [JsonProperty]
        public long Id { get; set; }
         [JsonProperty]
        public string Text { get; set; }
        [JsonProperty]
        public string Created_at { get; set; }
        [JsonProperty]
        public string User { get; set; }

        [JsonProperty]
        public List<string> HashTag { get; set; }
        [JsonProperty]
        public List<string> Mention { get; set; }
        [JsonProperty]
        public List<string> Url { get; set; }
    }

    /// <summary>
    /// Thread consuming the messages in the queue
    /// </summary>
    internal class ConsumerThread
    {
        private Thread pThread = null;
        private string queueName;
        private string collName;
        private bool removeAll;

        /// <summary>
        /// Constructor
        /// </summary>
        public ConsumerThread()
        {
            removeAll = false;
        }

        /// <summary>
        /// Start the thread
        /// </summary>
        /// <param name="threadName"></param>
        /// <param name="queueName"></param>
        /// <param name="collName"></param>
        /// <param name="rm"></param>
        /// <returns></returns>
        internal Thread Start(string threadName, string queueName, string collName, bool rm)
        {
            this.queueName = queueName;
            this.collName = collName;
            this.removeAll = rm;

            ThreadStart threadStart = new System.Threading.ThreadStart(Run);
            pThread = new Thread(threadStart);
            pThread.Name = threadName;
            pThread.Start();
            return pThread;
        }

        private static Message ReceiveTweet(string queueName)
        {
            if (!MessageQueue.Exists(queueName))
            {
                Console.WriteLine("Queue {0} does not exist.", queueName);
                return null;
            }

            Message msg = null;
            try
            {
                MessageQueue queue = new MessageQueue(queueName);
                queue.Formatter = new System.Messaging.XmlMessageFormatter(new String[] { "System.String,mscorlib" });
                using (var ts = new TransactionScope())
                {
                    msg = queue.Receive(TimeSpan.FromSeconds(10), MessageQueueTransactionType.Automatic);
                    ts.Complete();
                }

                Console.WriteLine(string.Format("{0} - {1}", 
                    DateTime.Now.ToString("s"), 
                    string.IsNullOrEmpty(msg.Label) ? "NONE" : msg.Label));
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
            return msg;
        }

        /// <summary>
        /// Execute the message consumption
        /// </summary>
        private void Run()
        {
            Message msg = ReceiveTweet(queueName);            
            Consume(msg, collName, removeAll);
        }

        /// <summary>
        /// Consume the message by available collection
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="collectionName"></param>
        /// <param name="removeAll"></param>
        private static void Consume(Message msg, string collectionName, bool removeAll)
        {
            if (msg == null)
                return;

            try
            {
                string connectionString = "mongodb://localhost";
                var client = new MongoClient(connectionString);
                var server = client.GetServer();
                var database = server.GetDatabase("tcat"); // "tcat" is the name 
                if (!database.CollectionExists(collectionName))
                {
                    database.CreateCollection(collectionName);
                }
                
                var collection = database.GetCollection(collectionName);
                if (collection != null)
                {
                    if (removeAll)
                        collection.RemoveAll();

                    try
                    {
                        var my_tweet = new MyTweet();
                        string content = msg.Body as string;
                        if (!string.IsNullOrEmpty(content))
                        {                                                                                 
                            string json = "{" + content + "}";       
                            JsonConvert.PopulateObject(json, my_tweet);

                            // remove mentions from content
                            foreach (var mention in my_tweet.Mention)
                            {
                                int index = content.IndexOf(mention);
                                content = content.Remove(index, mention.Length);
                            }
                            
                            // remove urls from content
                            foreach (var url in my_tweet.Url)
                            {
                                int index = content.IndexOf(url);
                                content = content.Remove(index, url.Length);
                            }

                            var doc = new MongoDB.Bson.BsonDocument();
                            var elems = new List<MongoDB.Bson.BsonElement>();
                            elems.Add(new MongoDB.Bson.BsonElement("CreationDate", DateTime.Parse(my_tweet.Created_at)));
                            elems.Add(new MongoDB.Bson.BsonElement("Id", my_tweet.Id));
                            elems.Add(new MongoDB.Bson.BsonElement("Text", my_tweet.Text));
                            elems.Add(new MongoDB.Bson.BsonElement("User", my_tweet.User));
                            //elems.Add(new MongoDB.Bson.BsonElement("HashTag", string.Join("|", my_tweet.HashTag)));
                            //elems.Add(new MongoDB.Bson.BsonElement("Mention", string.Join("|", my_tweet.Mention)));
                            //elems.Add(new MongoDB.Bson.BsonElement("Url", string.Join("|", my_tweet.Url)));
                            doc.AddRange(elems);
                            
                            doc.Add("HashTag", new MongoDB.Bson.BsonArray(my_tweet.HashTag));
                            doc.Add("Mention", new MongoDB.Bson.BsonArray(my_tweet.Mention));
                            doc.Add("Url", new MongoDB.Bson.BsonArray(my_tweet.Url));

                            collection.Insert(doc);                            
                        }

                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(msg.Body);
                        throw new Exception(e.Message);
                    }
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message);
            }
        }
    }

}
