using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Storage.MongoDB
{
    public class MongoDBSpanStore : ISpanStore
    {
        private readonly MongoClient client;
        private readonly IMongoDatabase db;
        private readonly IMongoCollection<BsonDocument> traces;
        private readonly IMongoCollection<BsonDocument> servicesIndex;
        private readonly TimeSpan spanTTL;

        public MongoDBSpanStore(string url, TimeSpan spanTTL)
        {
            var mongoUrl = new MongoUrl(url);
            client = new MongoClient(mongoUrl);
            db = client.GetDatabase(mongoUrl.DatabaseName);
            traces = db.GetCollection<BsonDocument>("traces");
            servicesIndex = db.GetCollection<BsonDocument>("servicesIndex");
            this.spanTTL = spanTTL;
        }

        //public async Task Accept(IEnumerable<Span> spans)
        //{
        //    foreach (var span in spans)
        //    {
        //        var builder = Builders<BsonDocument>.Update;
        //        var update = builder.Set("expiresAt", DateTime.Now.Add(spanTTL))
        //            .AddToSet("spans", new BsonDocument(new List<BsonElement> {
        //                new BsonElement("id", span.id), 
        //                new BsonElement("parentId", span.parentId), 
        //                new BsonElement("name", span.name)
        //            }))
        //            .PushEach("annotations", span.annotations.Select(a => new BsonDocument(new List<BsonElement> {
        //                new BsonElement("span", span.id), 
        //                new BsonElement("timestamp", a.timestamp),
        //                new BsonElement("value", a.value), 
        //                new BsonElement("host", EndpointToDocument(a.endpoint))
        //            })).ToList())
        //           .PushEach("binaryAnnotations", span.binaryAnnotations.Select(a => new BsonDocument(new List<BsonElement> {
        //                new BsonElement("span", span.id), 
        //                new BsonElement("key", a.key),
        //                new BsonElement("value", a.value), 
        //                new BsonElement("type", a.type), 
        //                new BsonElement("host", EndpointToDocument(a.endpoint))
        //            })).ToList());
        //        var filter = Builders<BsonDocument>.Filter.Eq("Id", span.traceId);
        //        var result = await traces.UpdateOneAsync(filter, update, new UpdateOptions() { IsUpsert = true });
        //        var t = result.ModifiedCount;
        //    }
        //}

        public Task Accept(IEnumerable<Span> spans)
        {
            var tasks = new List<Task>();
            foreach (var span in spans)
            {
                var builder = Builders<BsonDocument>.Update;
                var update = builder.Set("expiresAt", DateTime.Now.Add(spanTTL))
                    .AddToSet("spans", new BsonDocument(new List<BsonElement> {
                        new BsonElement("id", span.id), 
                        new BsonElement("parentId", span.parentId), 
                        new BsonElement("name", span.name)
                    }))
                    .PushEach("annotations", span.annotations.Select(a => new BsonDocument(new List<BsonElement> {
                        new BsonElement("span", span.id), 
                        new BsonElement("timestamp", a.timestamp),
                        new BsonElement("value", a.value), 
                        new BsonElement("host", EndpointToDocument(a.endpoint))
                    })).ToList())
                   .PushEach("binaryAnnotations", span.binaryAnnotations.Select(a => new BsonDocument(new List<BsonElement> {
                        new BsonElement("span", span.id), 
                        new BsonElement("key", a.key),
                        new BsonElement("value", a.value), 
                        new BsonElement("type", a.type), 
                        new BsonElement("host", EndpointToDocument(a.endpoint))
                    })).ToList());
                var filter = Builders<BsonDocument>.Filter.Eq("Id", span.traceId);
                tasks.Add(traces.UpdateOneAsync(filter, update, new UpdateOptions() { IsUpsert = true }));
            }
            return Task.WhenAll(tasks);
        }

        private BsonDocument EndpointToDocument(Endpoint endpoint)
        {
            return endpoint == null
                ? new BsonDocument()
                : new BsonDocument(new List<BsonElement> { 
                    new BsonElement("ipv4", endpoint.ipv4),
                    new BsonElement("port", endpoint.port),
                    new BsonElement("service", endpoint.serviceName.ToLower())
                });
        }

        public Task<IEnumerable<IEnumerable<Span>>> GetTraces(QueryRequest request)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<IEnumerable<Span>>> GetTracesByIds(IEnumerable<long> traceIds)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<string>> GetServiceNames()
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<string>> GetSpanNames(string serviceName)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback)
        {
            throw new NotImplementedException();
        }
    }
}
