using Cassandra;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Zipkin.Storage.Cassandra
{
    public class CassandraSpanStore
    {
        public static readonly String KEYSPACE = "zipkin";
        public static readonly short BUCKETS = 10;

        private readonly ISession session;
        private readonly PreparedStatement selectTraces;
        private readonly PreparedStatement insertSpan;
        private readonly PreparedStatement selectDependencies;
        private readonly PreparedStatement insertDependencies;
        private readonly PreparedStatement selectServiceNames;
        private readonly PreparedStatement insertServiceName;
        private readonly PreparedStatement selectSpanNames;
        private readonly PreparedStatement insertSpanName;
        private readonly PreparedStatement selectTraceIdsByServiceName;
        private readonly PreparedStatement insertTraceIdByServiceName;
        private readonly PreparedStatement selectTraceIdsBySpanName;
        private readonly PreparedStatement insertTraceIdBySpanName;
        private readonly PreparedStatement selectTraceIdsByAnnotations;
        private readonly PreparedStatement insertTraceIdByAnnotation;
        private readonly PreparedStatement selectTraceIdsBySpanDuration;
        private readonly PreparedStatement insertTraceIdBySpanDuration;

        public CassandraSpanStore(string keyspace, Cluster cluster, bool ensureSchema)
        {
            session = cluster.Connect(keyspace);
            
            insertSpan = session.Prepare("");

        }

        public Task Accept(IEnumerable<Span> spans)
        {
            throw new NotImplementedException();
        }
    }
}
