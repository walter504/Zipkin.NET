using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Common.Logging;
using System.Threading;
using Zipkin.Internal;

namespace Zipkin.Storage.Cassandra
{
    public class CassandraSpanStore
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(CassandraSpanStore));

        private static readonly long WRITTEN_NAMES_TTL = 60 * 60 * 1000;

        // Time window covered by a single bucket of the Span Duration Index, in seconds. Default: 1hr
        private static readonly long DURATION_INDEX_BUCKET_WINDOW_SECONDS = 60 * 60;

        public static readonly short BUCKETS = 10;

        private readonly int spanTtl;
        private readonly int indexTtl;
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
        private readonly Dictionary<string, string> metadata;

        private const string selectTracesQuery = "SELECT trace_id, span FROM traces WHERE trace_id IN :trace_id LIMIT :limit;";
        private const string insertSpanQuery = "INSERT INTO traces(trace_id, ts, span_name, span) VALUES(:trace_id, :ts, :span_name, :span) USING TTL :ttl;";
        private const string selectDependenciesQuery = "SELECT dependencies FROM dependencies WHERE day IN :days;";
        private const string insertDependenciesQuery = "INSERT INTO dependencies(day, dependencies) VALUES(:day, :dependencies);";
        private const string selectServiceNamesQuery = "SELECT service_name FROM service_names;";
        private const string insertServiceNameQuery = "INSERT INTO service_names(service_name) VALUES(:service_name) USING TTL :ttl;";
        private const string selectSpanNamesQuery = "SELECT span_name FROM span_names WHERE service_name = :service_name AND bucket = :bucket LIMIT :limit;";
        private const string insertSpanNameQuery = "INSERT INTO span_names(service_name, bucket, span_name) VALUES(:service_name, :bucket, :span_name) USING TTL :ttl;";
        private const string selectTraceIdsByServiceNameQuery = "SELECT ts, trace_id FROM service_name_index"
                + " WHERE service_name = :service_name AND bucket = :bucket"
                + " AND ts >= :start_ts AND ts <= :end_ts"
                + " LIMIT :limit ORDER BY ts DESC;";
        private const string insertTraceIdByServiceNameQuery = "INSERT INTO service_name_index(service_name, bucket, ts, trace_id)"
                + " VALUES(:service_name, :bucket, :ts, :trace_id) USING TTL :ttl;";
        private const string selectTraceIdsBySpanNameQuery = "SELECT ts, trace_id FROM service_span_name_index"
                + " WHERE service_span_name = :service_span_name AND bucket = :bucket"
                + " AND ts >= :start_ts AND ts <= :end_ts"
                + " LIMIT :limit ORDER BY ts DESC;";
        private const string insertTraceIdBySpanNameQuery = "INSERT INTO service_span_name_index(service_span_name, ts, trace_id)"
                + " VALUES(:service_span_name, :ts, :trace_id) USING TTL :ttl;";
        private const string selectTraceIdsByAnnotationsQuery = "SELECT ts, trace_id FROM annotations_index"
                + " WHERE annotation = :annotation AND bucket = :bucket"
                + " AND ts >= :start_ts AND ts <= :end_ts"
                + " LIMIT :limit ORDER BY ts DESC;";
        private const string insertTraceIdByAnnotationQuery = "INSERT INTO annotations_index(annotation, bucket, ts, trace_id)"
                + " VALUES(:annotation, :bucket, :ts, :trace_id) USING TTL :ttl;";
        private const string selectTraceIdsBySpanDurationQuery = "SELECT duration, ts, trace_id FROM span_duration_index"
                + " WHERE service_name = :service_name  AND span_name = :span_name AND bucket = :bucket"
                + " AND duration >= :min_duration AND duration <= :max_duration"
                + " ORDER BY duration;";
        private const string insertTraceIdBySpanDurationQuery = "INSERT INTO span_duration_index(service_name, span_name, bucket, duration, ts, trace_id)"
                + " VALUES(:service_name, :span_name, :bucket, :duration, :ts, :trace_id) USING TTL :ttl;";

        private readonly ThreadLocal<ISet<string>> writtenNames = new ThreadLocalSet();

        class ThreadLocalSet : ThreadLocal<ISet<string>>
        {
            private long cacheInterval = ToCacheInterval(DateTime.Now.Ticks / 10000);

            public ISet<string> Value
            {
                get
                {
                    long newCacheInterval = ToCacheInterval(DateTime.Now.Ticks / 10000);
                    if (cacheInterval != newCacheInterval)
                    {
                        cacheInterval = newCacheInterval;
                        base.Value = new HashSet<string>();
                    }
                    return base.Value;
                }
                set
                {
                    base.Value = value;
                }
            }

            private static long ToCacheInterval(long ms)
            {
                return ms / WRITTEN_NAMES_TTL;
            }
        };

        public CassandraSpanStore(string keyspace, Cluster cluster, bool ensureSchema)
        {
            session = cluster.Connect(keyspace);
            this.metadata = Schema.ReadMetadata(session);

            selectTraces = session.Prepare(selectTracesQuery);
            insertSpan = session.Prepare(insertSpanQuery);
            selectDependencies = session.Prepare(selectDependenciesQuery);
            insertDependencies = session.Prepare(insertDependenciesQuery);
            selectServiceNames = session.Prepare(selectServiceNamesQuery);
            insertServiceName = session.Prepare(insertServiceNameQuery);
            selectSpanNames = session.Prepare(selectSpanNamesQuery);
            insertSpanName = session.Prepare(insertSpanNameQuery);
            selectTraceIdsByServiceName = session.Prepare(selectTraceIdsByServiceNameQuery);
            insertTraceIdByServiceName = session.Prepare(insertTraceIdByServiceNameQuery);
            selectTraceIdsBySpanName = session.Prepare(selectTraceIdsBySpanNameQuery);
            insertTraceIdBySpanName = session.Prepare(insertTraceIdBySpanNameQuery);
            selectTraceIdsByAnnotations = session.Prepare(selectTraceIdsByAnnotationsQuery);
            insertTraceIdByAnnotation = session.Prepare(insertTraceIdByAnnotationQuery);
            selectTraceIdsBySpanDuration = session.Prepare(selectTraceIdsBySpanDurationQuery);
            insertTraceIdBySpanDuration = session.Prepare(insertTraceIdBySpanDurationQuery);


        }

        public async Task Accept(IEnumerable<Span> spans)
        {
            foreach (var span in spans)
            {
                await StoreSpan(span.traceId,
                    span.timestamp ?? 0L,
                    CreateSpanColumnName(span),
                    Codec.THRIFT.WriteSpan(span),
                    spanTtl);

            }
        }

        private Task StoreSpan(long traceId, long timestamp, string spanName, byte[] span, int ttl)
        {
            try
            {
                if (0 == timestamp && metadata["traces.compaction.class"].Contains("DateTieredCompactionStrategy"))
                {
                    log.WarnFormat("Span {0} in trace {1} had no timestamp. "
                            + "If this happens a lot consider switching back to SizeTieredCompactionStrategy for "
                            + "{2}.traces", spanName, traceId, Schema.KEYSPACE);
                }

                BoundStatement bound = insertSpan.Bind(new
                {
                    trace_id = traceId,
                    ts = timestamp,
                    span_name = spanName,
                    span = span,
                    ttl = ttl
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertSpan(traceId, timestamp, spanName, span, ttl));
                }

                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertSpan(traceId, timestamp, spanName, span, ttl), ex);
                throw ex;
            }
        }

        private string DebugInsertSpan(long traceId, long timestamp, string spanName, byte[] span, int ttl)
        {
            return insertSpanQuery
                .Replace(":trace_id", traceId.ToString())
                .Replace(":ts", timestamp.ToString())
                .Replace(":span_name", spanName)
                .Replace(":span", Convert.ToBase64String(span))
                .Replace(":ttl_", ttl.ToString());
        }

        /// <summary>
        /// Get the available trace information from the storage system.
        /// Spans in trace should be sorted by the first annotation timestamp
        /// in that span. First event should be first in the spans list.
        /// </summary>
        /// <param name="traceIds"></param>
        /// <param name="limit"></param>
        /// <returns>
        /// The return list will contain only spans that have been found, thus
        /// the return list may not match the provided list of ids.
        /// </returns>
        public async Task<Dictionary<long, List<Span>>> GetSpansByTraceIds(long[] traceIds, int limit)
        {
            if (traceIds.Length == 0)
            {
                return await Task.FromResult(new Dictionary<long, List<Span>>());
            }

            try
            {
                BoundStatement bound = selectTraces.Bind(new { trace_id = traceIds, limit = limit });

                bound.SetPageSize(int.MaxValue);

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectTraces(traceIds, limit));
                }

                var result = await session.ExecuteAsync(bound);
                var spans = new Dictionary<long, List<Span>>();
                foreach (var row in result.GetRows())
                {
                    long traceId = row.GetValue<long>("trace_id");
                    if (!spans.ContainsKey(traceId))
                    {
                        spans.Add(traceId, new List<Span>());
                    }
                    spans[traceId].Add(Codec.THRIFT.ReadSpan(row.GetValue<byte[]>("span")));
                }
                return spans;
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectTraces(traceIds, limit), ex);
                throw ex;
            }
        }

        private string DebugSelectTraces(long[] traceIds, int limit)
        {
            return selectTracesQuery
                .Replace(":trace_id", traceIds.ToString())
                .Replace(":limit", limit.ToString());
        }

        public Task StoreDependencies(long epochDayMillis, byte[] dependencies)
        {
            DateTimeOffset startFlooredToDay = Util.FromUnixTimeMilliseconds(epochDayMillis);
            try
            {
                BoundStatement bound = insertDependencies.Bind(new
                {
                    day = startFlooredToDay,
                    dependencies = dependencies
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertDependencies(startFlooredToDay, dependencies));
                }
                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertDependencies(startFlooredToDay, dependencies));
                throw ex;
            }
        }

        private string DebugInsertDependencies(DateTimeOffset startFlooredToDay, byte[] dependencies)
        {
            return insertDependenciesQuery
                .Replace(":day", startFlooredToDay.ToString())
                .Replace(":dependencies", Convert.ToBase64String(dependencies));
        }

        public async Task<List<DependencyLink>> GetDependencies(long endTs, long? lookback)
        {
            DateTimeOffset endDate = Util.FromUnixTimeMilliseconds(endTs).Date;
            DateTimeOffset startDate = Util.FromUnixTimeMilliseconds(endTs - lookback ?? 0).Date;

            List<DateTimeOffset> days = GetDays(startDate, endDate);
            try
            {
                BoundStatement bound = selectDependencies.Bind(new { days = days });
                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectDependencies(days));
                }
                var result = await session.ExecuteAsync(bound);
                var dependencies = new List<DependencyLink>();
                foreach (var row in result.GetRows())
                {
                    dependencies.AddRange(Codec.THRIFT.ReadDependencyLinks(row.GetValue<byte[]>("dependencies")));
                }
                return dependencies;
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectDependencies(days), ex);
                throw ex;
            }
        }

        private string DebugSelectDependencies(List<DateTimeOffset> days)
        {
            return selectDependenciesQuery.Replace(":days", days.ToString());
        }

        public Task StoreServiceName(string serviceName, int ttl)
        {
            if (writtenNames.Value.Add(serviceName))
            {
                try
                {
                    BoundStatement bound = insertServiceName.Bind(new { service_name = serviceName, ttl = ttl });

                    if (log.IsDebugEnabled)
                    {
                        log.Debug(DebugInsertServiceName(serviceName, ttl));
                    }

                    return session.ExecuteAsync(bound);
                }
                catch (Exception ex)
                {
                    log.Error("failed " + DebugInsertServiceName(serviceName, ttl), ex);
                    writtenNames.Value.Remove(serviceName);
                    throw ex;
                }
            }
            else
            {
                return Task.FromResult(0);
            }
        }

        private string DebugInsertServiceName(string serviceName, int ttl)
        {
            return insertServiceNameQuery
                .Replace(":service_name", serviceName)
                .Replace(":ttl", ttl.ToString());
        }

        public Task StoreSpanName(string serviceName, string spanName, int ttl)
        {
            if (writtenNames.Value.Add(serviceName + "––" + spanName))
            {
                try
                {
                    BoundStatement bound = insertSpanName.Bind(new
                    {
                        service_name = serviceName,
                        bucket = 0,
                        span_name = spanName,
                        ttl = ttl
                    });

                    if (log.IsDebugEnabled)
                    {
                        log.Debug(DebugInsertSpanName(serviceName, spanName, ttl));
                    }
                    return session.ExecuteAsync(bound);
                }
                catch (Exception ex)
                {
                    log.Error("failed " + DebugInsertSpanName(serviceName, spanName, ttl), ex);
                    writtenNames.Value.Remove(serviceName + "––" + spanName);
                    throw ex;
                }
            }
            else
            {
                return Task.FromResult(0);
            }
        }

        private string DebugInsertSpanName(string serviceName, string spanName, int ttl)
        {
            return insertSpanNameQuery
                .Replace(":service_name", serviceName)
                .Replace(":span_name", spanName)
                .Replace(":ttl", ttl.ToString());
        }

        private string CreateSpanColumnName(Span span)
        {
            return string.Format("{0}_{1}_{2}", span.id, span.annotations.GetHashCode(), span.binaryAnnotations.GetHashCode());
        }

        private static List<DateTimeOffset> GetDays(DateTimeOffset from, DateTimeOffset to)
        {
            var days = new List<DateTimeOffset>();
            for (var time = from; time <= to; time = time.AddDays(1))
            {
                days.Add(time);
            }
            return days;
        }
    }
}
