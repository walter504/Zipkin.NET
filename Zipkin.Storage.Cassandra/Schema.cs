using System;
using System.Collections.Generic;
using Common.Logging;
using Cassandra;

namespace Zipkin.Storage.Cassandra
{
    public static class Schema
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(Schema));

        private static readonly string SCHEMA = "database.cassandra-schema-cql3.txt";
        public static readonly string KEYSPACE = "zipkin";
        
        public static Dictionary<string, string> ReadMetadata(ISession session)
        {
            var metadata = new Dictionary<string, string>();
            var keyspaceMetadata = GetKeyspaceMetadata(session.Keyspace, session.Cluster);

            IDictionary<string, int> replication = keyspaceMetadata.Replication;
            if ("SimpleStrategy" == keyspaceMetadata.StrategyClass && 1 == replication["replication_factor"])
            {
                log.Warn("running with RF=1, this is not suitable for production. Optimal is 3+");
            }
            IDictionary<string, string> tracesCompaction = keyspaceMetadata.GetTableMetadata("traces").Options.CompactionOptions;
            metadata.Add("traces.compaction.class", tracesCompaction["class"]);
            return metadata;
        }

        private static KeyspaceMetadata GetKeyspaceMetadata(string keyspace, ICluster cluster)
        {
            var keyspaceMetadata = cluster.Metadata.GetKeyspace(keyspace);

            if (keyspaceMetadata == null)
            {
                throw new InvalidOperationException(string.Format(
                    "Cannot read keyspace metadata for give keyspace: {0} and cluster: {1}",
                    keyspace, cluster.Metadata.ClusterName));
            }
            return keyspaceMetadata;
        }

        public static void EnsureExists(string keyspace, ISession session)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            string name = assembly.GetName().Name;
            System.IO.Stream stream = assembly.GetManifestResourceStream(name + "." + SCHEMA);
            try
            {
                using (var reader = new System.IO.StreamReader(stream, System.Text.Encoding.UTF8))
                {
                    foreach (var cmd in reader.ReadToEnd().Split(';'))
                    {
                        var cql = cmd.Trim();
                        if (!string.IsNullOrEmpty(cql))
                        {
                            session.Execute(cql.Replace(" " + KEYSPACE, " " + keyspace));
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                log.Error(ex.Message, ex);
            }
            finally
            {
                if (stream != null)
                {
                    stream.Dispose();
                }
            }
        }
    }
}
