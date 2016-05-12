using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Zipkin.Storage.Cassandra;

namespace Zipkin.WebApi
{
    public class ZipkinCassandraProperties
    {
        public string Keyspace { get; set; }
        public string ContactPoints { get; set; }
        public string LocalDc { get; set; }
        public int MaxConnections { get; set; }
        public bool EnsureSchema { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int MaxTraceCols { get; set; }
        public int BucketCount { get; set; }
        public int SpanTtl { get; set; }
        public int IndexTtl { get; set; }

        public ZipkinCassandraProperties()
        {
            Keyspace = "zipkin";
            ContactPoints = "localhost";
            MaxConnections = 8;
            EnsureSchema = true;
            MaxTraceCols = 100000;
            BucketCount = 10;
            SpanTtl = (int)TimeSpan.FromDays(7).TotalSeconds;
            IndexTtl = (int)TimeSpan.FromDays(3).TotalSeconds;
        }

        public CassandraStorage.Builder ToBuilder()
        {
            return new CassandraStorage.Builder()
                .Keyspace(Keyspace)
                .ContactPoints(ContactPoints)
                .LocalDc(LocalDc)
                .MaxConnections(MaxConnections)
                .EnsureSchema(EnsureSchema)
                .Username(Username)
                .Password(Password)
                .SpanTtl(SpanTtl)
                .IndexTtl(IndexTtl);
        }
    }
}