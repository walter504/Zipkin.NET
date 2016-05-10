using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Internal;

namespace Zipkin.Storage.Cassandra
{
    public class CassandraStorage
    {
        public sealed class Builder
        {
            public string keyspace = "zipkin";
            public string contactPoints = "localhost";
            public string localDc;
            public int maxConnections = 8;
            public bool ensureSchema = true;
            public string username;
            public string password;
            public int maxTraceCols = 100000;
            public int bucketCount = 10;
            public int spanTtl = TimeSpan.FromDays(7).Seconds;
            public int indexTtl = TimeSpan.FromDays(3).Seconds;
            public SessionFactory sessionFactory = new SessionFactory.Default();

            /** Override to control how sessions are created. */
            public Builder SessionFactory(SessionFactory sessionFactory)
            {
                this.sessionFactory = Ensure.ArgumentNotNull(sessionFactory, "sessionFactory");
                return this;
            }

            /** Keyspace to store span and index data. Defaults to "zipkin" */
            public Builder Keyspace(string keyspace)
            {
                this.keyspace = Ensure.ArgumentNotNull(keyspace, "keyspace");
                return this;
            }

            /** Comma separated list of hosts / IPs part of Cassandra cluster. Defaults to localhost */
            public Builder ContactPoints(string contactPoints)
            {
                this.contactPoints = Ensure.ArgumentNotNull(contactPoints, "contactPoints");
                return this;
            }

            /**
             * Name of the datacenter that will be considered "local" for latency load balancing. When
             * unset, load-balancing is round-robin.
             */
            public Builder LocalDc(string localDc)
            {
                this.localDc = localDc;
                return this;
            }

            /** Max pooled connections per datacenter-local host. Defaults to 8 */
            public Builder MaxConnections(int maxConnections)
            {
                this.maxConnections = maxConnections;
                return this;
            }

            /**
             * Ensures that schema exists, if enabled tries to execute script io.zipkin:zipkin-cassandra-core/cassandra-schema-cql3.txt.
             * Defaults to true.
             */
            public Builder EnsureSchema(bool ensureSchema)
            {
                this.ensureSchema = ensureSchema;
                return this;
            }

            /** Will throw an exception on startup if authentication fails. No default. */
            public Builder Username(string username)
            {
                this.username = username;
                return this;
            }

            /** Will throw an exception on startup if authentication fails. No default. */
            public Builder Password(string password)
            {
                this.password = password;
                return this;
            }

            /**
             * Spans have multiple values for the same id. For example, a client and server contribute to
             * the same span id. When searching for spans by id, the amount of results may be larger than
             * the ids. This defines a threshold which accommodates this situation, without looking for an
             * unbounded number of results.
             */
            public Builder MaxTraceCols(int maxTraceCols)
            {
                this.maxTraceCols = maxTraceCols;
                return this;
            }

            /** Time-to-live in seconds for span data. Defaults to 604800 (7 days) */
            public Builder SpanTtl(int spanTtl)
            {
                this.spanTtl = spanTtl;
                return this;
            }

            /** Time-to-live in seconds for index data. Defaults to 259200 (3 days) */
            public Builder IndexTtl(int indexTtl)
            {
                this.indexTtl = indexTtl;
                return this;
            }

            public CassandraStorage Build()
            {
                return new CassandraStorage(this);
            }
        }

        public readonly int maxTraceCols;
        public readonly int indexTtl;
        public readonly int spanTtl;
        public readonly int bucketCount;
        public readonly string contactPoints;
        public readonly int maxConnections;
        public readonly string localDc;
        public readonly string username;
        public readonly string password;
        public readonly bool ensureSchema;
        public readonly string keyspace;
        public readonly Lazy<ISession> session;

        // eagerly makes network connections, so lazy
        private Lazy<Repository> lazyRepository;

        CassandraStorage(Builder builder)
        {
            this.contactPoints = builder.contactPoints;
            this.maxConnections = builder.maxConnections;
            this.localDc = builder.localDc;
            this.username = builder.username;
            this.password = builder.password;
            this.ensureSchema = builder.ensureSchema;
            this.keyspace = builder.keyspace;
            this.maxTraceCols = builder.maxTraceCols;
            this.indexTtl = builder.indexTtl;
            this.spanTtl = builder.spanTtl;
            this.bucketCount = builder.bucketCount;
            this.session = new Lazy<ISession>(() => builder.sessionFactory.Create(this));
            lazyRepository = new Lazy<Repository>(() => new Repository(session.Value));
        }

        public ISpanStore SpanStore
        {
            get
            {
                return new CassandraSpanStore(lazyRepository, spanTtl, indexTtl, maxTraceCols);
            }
        }

        public IDependencyStore DependencyStore
        {
            get
            {
                return new CassandraDependencyStore(lazyRepository);
            }
        }
        

    }
}
