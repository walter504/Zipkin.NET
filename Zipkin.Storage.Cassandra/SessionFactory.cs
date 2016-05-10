using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Internal;

namespace Zipkin.Storage.Cassandra
{
    public abstract class SessionFactory
    {
        public abstract ISession Create(CassandraStorage storage);

        public sealed class Default : SessionFactory
        {

            /// <summary>
            /// Creates a session and ensures schema if configured. Closes the cluster and session if any
            /// exception occurred.
            /// </summary>
            /// <param name="cassandra"></param>
            /// <returns></returns>
            public override ISession Create(CassandraStorage cassandra)
            {
                Cluster cluster = BuildCluster(cassandra);
                ISession session = null;
                try
                {

                    if (cassandra.ensureSchema)
                    {
                        session = cluster.Connect();
                        Schema.EnsureExists(cassandra.keyspace, session);
                        session.Execute("USE " + cassandra.keyspace);
                        return session;
                    }
                    else
                    {
                        return cluster.Connect(cassandra.keyspace);
                    }
                }
                catch (Exception ex)
                {
                    if (session != null)
                    {
                        session.Dispose();
                    }

                    throw ex;
                }
            }

            static Cluster BuildCluster(CassandraStorage cassandra)
            {
                var builder = Cluster.Builder();
                var contactPoints = ParseContactPoints(cassandra);
                int defaultPort = FindConnectPort(contactPoints);
                builder.AddContactPoints(contactPoints);
                // This ends up protocolOptions.port
                builder.WithPort(defaultPort);
                if (cassandra.username != null && cassandra.password != null)
                {
                    builder.WithCredentials(cassandra.username, cassandra.password);
                }
                //builder.WithRetryPolicy(ZipkinRetryPolicy.Instance);
                builder.WithLoadBalancingPolicy(new TokenAwarePolicy(
                    cassandra.localDc != null
                        ? new DCAwareRoundRobinPolicy(cassandra.localDc) as ILoadBalancingPolicy
                        : new RoundRobinPolicy()));
                builder.WithPoolingOptions(new PoolingOptions().SetMaxConnectionsPerHost(
                    HostDistance.Local, cassandra.maxConnections
                ));
                return builder.Build();
            }

            static List<IPEndPoint> ParseContactPoints(CassandraStorage cassandra)
            {
                var result = new List<IPEndPoint>();
                foreach (var contactPoint in cassandra.contactPoints.Split(','))
                {
                    HostAndPort parsed = HostAndPort.FromString(contactPoint);
                    result.Add(new IPEndPoint(parsed.HostLong, parsed.GetPortOrDefault(9042)));
                }
                return result;
            }

            /// <summary>
            /// Returns the consistent port across all contact points or 9042
            /// </summary>
            /// <param name="contactPoints"></param>
            /// <returns></returns>
            static int FindConnectPort(List<IPEndPoint> contactPoints)
            {
                var ports = new List<int>();
                foreach (var contactPoint in contactPoints)
                {
                    ports.Add(contactPoint.Port);
                }
                return ports.Count == 1 ? ports[0] : 9042;
            }
        }
    }
}
