﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Common.Logging;
using System.Threading;
using Zipkin.Internal;
using Zipkin.Adjuster;

namespace Zipkin.Storage.Cassandra
{
    public class Repository
    {
        public static readonly int BUCKETS = 10;

        private static readonly ILog log = LogManager.GetLogger(typeof(Repository));

        private static readonly Random RAND = new Random();

        private static readonly int[] ALL_BUCKETS = (new int[BUCKETS]).Select((it, idx) => idx).ToArray();

        private static readonly long WRITTEN_NAMES_TTL = 60 * 60 * 1000;

        // Time window covered by a single bucket of the Span Duration Index, in seconds. Default: 1hr
        private static readonly long DURATION_INDEX_BUCKET_WINDOW_SECONDS = 60 * 60;

        private readonly ISession session;
        private readonly PreparedStatement selectTraces;
        private readonly PreparedStatement insertSpan;
        private readonly PreparedStatement selectDependencies;
        private readonly PreparedStatement insertDependencies;
        private readonly PreparedStatement selectServiceNames;
        private readonly PreparedStatement insertServiceName;
        private readonly PreparedStatement selectSpanNames;
        private readonly PreparedStatement insertSpanName;
        private readonly PreparedStatement selectTraceIdsByServiceNames;
        private readonly PreparedStatement insertTraceIdByServiceName;
        private readonly PreparedStatement selectTraceIdsBySpanName;
        private readonly PreparedStatement insertTraceIdBySpanName;
        private readonly PreparedStatement selectTraceIdsByAnnotations;
        private readonly PreparedStatement insertTraceIdByAnnotation;
        private readonly PreparedStatement selectTraceIdsBySpanDuration;
        private readonly PreparedStatement insertTraceIdBySpanDuration;

        private readonly PreparedStatement selectTraceIdsByUpdateTime;
        private readonly PreparedStatement insertTraceIdByUpdateTime;

        private readonly Dictionary<string, string> metadata;
        private readonly Func<RowSet, Dictionary<long, long>> traceIdToTimestamp;

        private const string selectTracesQuery = "SELECT trace_id, span FROM traces WHERE trace_id IN :trace_id LIMIT :limit_;";
        private const string insertSpanQuery = "INSERT INTO traces(trace_id, ts, span_name, span) VALUES(:trace_id, :ts, :span_name, :span) USING TTL :ttl_;";
        private const string selectDependenciesQuery = "SELECT dependencies FROM dependencies WHERE day IN :days;";
        private const string insertDependenciesQuery = "INSERT INTO dependencies(day, dependencies) VALUES(:day, :dependencies);";
        private const string selectServiceNamesQuery = "SELECT service_name FROM service_names;";
        private const string insertServiceNameQuery = "INSERT INTO service_names(service_name) VALUES(:service_name) USING TTL :ttl_;";
        private const string selectSpanNamesQuery = "SELECT span_name FROM span_names WHERE service_name = :service_name AND bucket = :bucket LIMIT :limit_;";
        private const string insertSpanNameQuery = "INSERT INTO span_names(service_name, bucket, span_name) VALUES(:service_name, :bucket, :span_name) USING TTL :ttl_;";
        private const string selectTraceIdsByServiceNamesQuery = "SELECT ts, trace_id FROM service_name_index"
                + " WHERE service_name IN :service_name AND bucket IN :bucket"
                + " AND ts >= :start_ts AND ts <= :end_ts"
                + " ORDER BY ts DESC LIMIT :limit_;";
        private const string insertTraceIdByServiceNameQuery = "INSERT INTO service_name_index(service_name, bucket, ts, trace_id)"
                + " VALUES(:service_name, :bucket, :ts, :trace_id) USING TTL :ttl_;";
        private const string selectTraceIdsBySpanNameQuery = "SELECT ts, trace_id FROM service_span_name_index"
                + " WHERE service_span_name = :service_span_name"
                + " AND ts >= :start_ts AND ts <= :end_ts"
                + " ORDER BY ts DESC LIMIT :limit_;";
        private const string insertTraceIdBySpanNameQuery = "INSERT INTO service_span_name_index(service_span_name, ts, trace_id)"
                + " VALUES(:service_span_name, :ts, :trace_id) USING TTL :ttl_;";
        private const string selectTraceIdsByAnnotationsQuery = "SELECT ts, trace_id FROM annotations_index"
                + " WHERE annotation = :annotation AND bucket IN :bucket"
                + " AND ts >= :start_ts AND ts <= :end_ts"
                + " ORDER BY ts DESC LIMIT :limit_;";
        private const string insertTraceIdByAnnotationQuery = "INSERT INTO annotations_index(annotation, bucket, ts, trace_id)"
                + " VALUES(:annotation, :bucket, :ts, :trace_id) USING TTL :ttl_;";
        private const string selectTraceIdsBySpanDurationQuery = "SELECT duration, ts, trace_id FROM span_duration_index"
                + " WHERE service_name = :service_name  AND span_name = :span_name AND bucket = :bucket"
                + " AND duration >= :min_duration AND duration <= :max_duration"
                + " ORDER BY duration DESC";
        private const string insertTraceIdBySpanDurationQuery = "INSERT INTO span_duration_index(service_name, span_name, bucket, duration, ts, trace_id)"
                + " VALUES(:service_name, :span_name, :bucket, :duration, :ts, :trace_id) USING TTL :ttl_;";

        private const string selectTraceIdsByUpdateTimeQuery = "SELECT trace_id FROM trace_updatetime WHERE updatetime = :updatetime ALLOW FILTERING;";
        private const string insertTraceIdByUpdateTimeQuery = "INSERT INTO trace_updatetime(trace_id, updatetime)"
                + " VALUES(:trace_id, :updatetime) USING TTL :ttl_;";

        private readonly ThreadLocal<ISet<string>> writtenNames = new ThreadLocalSet();

        class ThreadLocalSet : ThreadLocal<ISet<string>>
        {
            private long cacheInterval = ToCacheInterval(DateTime.Now.Ticks / 10000);

            public ThreadLocalSet()
                : base(() => new HashSet<string>())
            {
            }

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

        public Repository(ISession session)
        {
            this.session = session;
            this.metadata = Schema.ReadMetadata(session);

            selectTraces = session.Prepare(selectTracesQuery);
            insertSpan = session.Prepare(insertSpanQuery);
            selectDependencies = session.Prepare(selectDependenciesQuery);
            insertDependencies = session.Prepare(insertDependenciesQuery);
            selectServiceNames = session.Prepare(selectServiceNamesQuery);
            insertServiceName = session.Prepare(insertServiceNameQuery);
            selectSpanNames = session.Prepare(selectSpanNamesQuery);
            insertSpanName = session.Prepare(insertSpanNameQuery);
            selectTraceIdsByServiceNames = session.Prepare(selectTraceIdsByServiceNamesQuery);
            insertTraceIdByServiceName = session.Prepare(insertTraceIdByServiceNameQuery);
            selectTraceIdsBySpanName = session.Prepare(selectTraceIdsBySpanNameQuery);
            insertTraceIdBySpanName = session.Prepare(insertTraceIdBySpanNameQuery);
            selectTraceIdsByAnnotations = session.Prepare(selectTraceIdsByAnnotationsQuery);
            insertTraceIdByAnnotation = session.Prepare(insertTraceIdByAnnotationQuery);
            selectTraceIdsBySpanDuration = session.Prepare(selectTraceIdsBySpanDurationQuery);
            insertTraceIdBySpanDuration = session.Prepare(insertTraceIdBySpanDurationQuery);

            selectTraceIdsByUpdateTime = session.Prepare(selectTraceIdsByUpdateTimeQuery);
            insertTraceIdByUpdateTime = session.Prepare(insertTraceIdByUpdateTimeQuery);

            traceIdToTimestamp = new Func<RowSet, Dictionary<long, long>>((input) =>
            {
                var traceIdsToTimestamps = new Dictionary<long, long>();
                foreach (var row in input.GetRows())
                {
                    traceIdsToTimestamps[row.GetValue<long>("trace_id")] = 
                        Util.ToUnixTimeMilliseconds(row.GetValue<DateTimeOffset>("ts").DateTime);
                }
                return traceIdsToTimestamps;
            });
        }

        public async Task StoreSpan(long traceId, long timestamp, string spanName, byte[] span, int ttl)
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
                    ttl_ = ttl
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertSpan(traceId, timestamp, spanName, span, ttl));
                }

                var result = await session.ExecuteAsync(bound);
                foreach(var row in result.GetRows())
                {
                    var t = row.GetHashCode();
                }
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
        /// <param name="limit_"></param>
        /// <returns>
        /// The return list will contain only spans that have been found, thus
        /// the return list may not match the provided list of ids.
        /// </returns>
        public async Task<List<List<Span>>> GetSpansByTraceIds(long[] traceIds, int limit)
        {
            if (traceIds.Length == 0)
            {
                return await Task.FromResult(new List<List<Span>>());
            }

            try
            {
                BoundStatement bound = selectTraces.Bind(new { trace_id = traceIds, limit_ = limit });
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
                return spans.Select(kvp => kvp.Value).ToList();
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
                .Replace(":limit_", limit.ToString());
        }

        public Task StoreDependencies(long epochDayMillis, byte[] dependencies)
        {
            DateTimeOffset startFlooredToDay = Util.FromUnixTimeMilliseconds(epochDayMillis).Date;
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

        public async Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback)
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
                    BoundStatement bound = insertServiceName.Bind(new { service_name = serviceName, ttl_ = ttl });

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
                .Replace(":ttl_", ttl.ToString());
        }

        public async Task<IEnumerable<string>> GetServiceNames()
        {
            try
            {
                BoundStatement bound = selectServiceNames.Bind();

                if (log.IsDebugEnabled)
                {
                    log.Debug(selectServiceNamesQuery);
                }

                var result = await session.ExecuteAsync(bound);
                var serviceNames = new List<string>();
                foreach (var row in result.GetRows())
                {
                    serviceNames.Add(row.GetValue<string>("service_name"));
                }
                return serviceNames;
            }
            catch (Exception ex)
            {
                log.Error("failed " + selectServiceNamesQuery, ex);
                throw ex;
            }
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
                        ttl_ = ttl
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
                .Replace(":ttl_", ttl.ToString());
        }

        public async Task<IEnumerable<string>> GetSpanNames(string serviceName)
        {
            serviceName = serviceName.ToLower(); // service names are always lowercase!
            try
            {
                if (!string.IsNullOrEmpty(serviceName))
                {
                    BoundStatement bound = selectSpanNames.Bind(new { service_name = serviceName, bucket = 0, limit_ = 1000 });

                    if (log.IsDebugEnabled)
                    {
                        log.Debug(DebugSelectSpanNames(serviceName));
                    }

                    var result = await session.ExecuteAsync(bound);
                    var spanNames = new List<string>();
                    foreach (var row in result.GetRows())
                    {
                        spanNames.Add(row.GetValue<string>("span_name"));
                    }
                    return spanNames;
                }
                else
                {
                    return new List<string>();
                }
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectSpanNames(serviceName), ex);
                throw ex;
            }
        }

        private string DebugSelectSpanNames(string serviceName)
        {
            return selectSpanNamesQuery.Replace(":service_name", serviceName);
        }

        public Task StoreTraceIdByServiceName(string serviceName, long timestamp, long traceId, int ttl)
        {
            try
            {
                BoundStatement bound = insertTraceIdByServiceName.Bind(new
                {
                    service_name = serviceName,
                    bucket = RAND.Next(BUCKETS),
                    ts = Util.FromUnixTimeMicroseconds(timestamp),
                    trace_id = traceId,
                    ttl_ = ttl
                });
                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertTraceIdByServiceName(serviceName, timestamp, traceId, ttl));
                }

                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertTraceIdByServiceName(serviceName, timestamp, traceId, ttl), ex);
                throw ex;
            }
        }

        private string DebugInsertTraceIdByServiceName(string serviceName, long timestamp, long traceId, int ttl)
        {
            return insertTraceIdByServiceNameQuery
                .Replace(":service_name", serviceName)
                .Replace(":ts", Util.FromUnixTimeMicroseconds(timestamp).ToString())
                .Replace(":trace_id", traceId.ToString())
                .Replace(":ttl_", ttl.ToString());
        }

        public async Task<Dictionary<long, long>> GetTraceIdsByServiceNames(IEnumerable<string> serviceNames, long endTs, long lookback, int limit)
        {
            long startTs = endTs - lookback;

            try
            {
                BoundStatement bound = selectTraceIdsByServiceNames.Bind(new
                {
                    service_name = serviceNames,
                    bucket = ALL_BUCKETS,
                    start_ts = Util.FromUnixTimeMicroseconds(startTs),
                    end_ts = Util.FromUnixTimeMicroseconds(endTs),
                    limit_ = limit
                });
                bound.SetPageSize(int.MaxValue);

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectTraceIdsByServiceName(serviceNames, startTs, endTs, limit));
                }

                var result = await session.ExecuteAsync(bound);
                return traceIdToTimestamp(result);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectTraceIdsByServiceName(serviceNames, startTs, endTs, limit), ex);
                throw ex;
            }
        }

        private string DebugSelectTraceIdsByServiceName(IEnumerable<string> serviceNames, long startTs, long endTs, int limit)
        {
            return selectTraceIdsByServiceNamesQuery
                .Replace(":service_name", serviceNames.ToString())
                .Replace(":start_ts", Util.FromUnixTimeMicroseconds(startTs).ToString())
                .Replace(":end_ts", Util.FromUnixTimeMicroseconds(endTs).ToString())
                .Replace(":limit_", limit.ToString());
        }

        public Task StoreTraceIdBySpanName(string serviceName, string spanName, long timestamp, long traceId, int ttl)
        {
            try
            {
                string serviceSpanName = serviceName + "." + spanName;

                BoundStatement bound = insertTraceIdBySpanName.Bind(new
                {
                    service_span_name = serviceSpanName,
                    ts = Util.FromUnixTimeMicroseconds(timestamp),
                    trace_id = traceId,
                    ttl_ = ttl
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertTraceIdBySpanName(serviceSpanName, timestamp, traceId, ttl));
                }
                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertTraceIdBySpanName(serviceName, timestamp, traceId, ttl), ex);
                throw ex;
            }
        }

        private string DebugInsertTraceIdBySpanName(string serviceSpanName, long timestamp, long traceId, int ttl)
        {
            return insertTraceIdBySpanNameQuery
                    .Replace(":service_span_name", serviceSpanName)
                    .Replace(":ts", Util.FromUnixTimeMicroseconds(timestamp).ToString())
                    .Replace(":trace_id", traceId.ToString())
                    .Replace(":ttl_", ttl.ToString());
        }

        public async Task<Dictionary<long, long>> GetTraceIdsBySpanName(string serviceName, string spanName, long endTs, long lookback, int limit)
        {
            string serviceSpanName = serviceName + "." + spanName;
            long startTs = endTs - lookback;
            try
            {
                BoundStatement bound = selectTraceIdsBySpanName.Bind(new
                {
                    service_span_name = serviceSpanName,
                    start_ts = Util.FromUnixTimeMicroseconds(startTs),
                    end_ts = Util.FromUnixTimeMicroseconds(endTs),
                    limit_ = limit
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectTraceIdsBySpanName(serviceSpanName, startTs, endTs, limit));
                }

                var result = await session.ExecuteAsync(bound);
                return traceIdToTimestamp(result);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectTraceIdsBySpanName(serviceSpanName, startTs, endTs, limit), ex);
                throw ex;
            }
        }

        private string DebugSelectTraceIdsBySpanName(string serviceSpanName, long startTs, long endTs, int limit)
        {
            return selectTraceIdsBySpanNameQuery
                .Replace(":service_span_name", serviceSpanName)
                .Replace(":start_ts", Util.FromUnixTimeMicroseconds(startTs).ToString())
                .Replace(":end_ts", Util.FromUnixTimeMicroseconds(endTs).ToString())
                .Replace(":limit_", limit.ToString());
        }

        public Task StoreTraceIdByAnnotation(byte[] annotationKey, long timestamp, long traceId, int ttl)
        {
            try
            {
                BoundStatement bound = insertTraceIdByAnnotation.Bind(new
                {
                    annotation = annotationKey,
                    bucket = RAND.Next(BUCKETS),
                    ts = Util.FromUnixTimeMicroseconds(timestamp),
                    trace_id = traceId,
                    ttl_ = ttl
                });
                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertTraceIdByAnnotation(annotationKey, timestamp, traceId, ttl));
                }
                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertTraceIdByAnnotation(annotationKey, timestamp, traceId, ttl), ex);
                throw ex;
            }
        }

        private string DebugInsertTraceIdByAnnotation(byte[] annotationKey, long timestamp, long traceId, int ttl)
        {
            return insertTraceIdByAnnotationQuery
                .Replace(":annotation", Convert.ToBase64String(annotationKey))
                .Replace(":ts", Util.FromUnixTimeMicroseconds(timestamp).ToString())
                .Replace(":trace_id", traceId.ToString())
                .Replace(":ttl_", ttl.ToString());
        }

        public async Task<Dictionary<long, long>> GetTraceIdsByAnnotation(byte[] annotationKey, long endTs, long lookback, int limit)
        {
            long startTs = endTs - lookback;
            try
            {
                BoundStatement bound = selectTraceIdsByAnnotations.Bind(new
                {
                    annotation = annotationKey,
                    bucket = ALL_BUCKETS,
                    start_ts = Util.FromUnixTimeMicroseconds(startTs),
                    end_ts = Util.FromUnixTimeMicroseconds(endTs),
                    limit_ = limit
                });
                bound.SetPageSize(int.MaxValue);

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectTraceIdsByAnnotations(annotationKey, startTs, endTs, limit));
                }
                var result = await session.ExecuteAsync(bound);
                return traceIdToTimestamp(result);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectTraceIdsByAnnotations(annotationKey, startTs, endTs, limit), ex);
                throw ex;
            }
        }

        private string DebugSelectTraceIdsByAnnotations(byte[] annotationKey, long startTs, long endTs, int limit)
        {
            return selectTraceIdsByAnnotationsQuery
                .Replace(":annotation", Convert.ToBase64String(annotationKey))
                .Replace(":start_ts", Util.FromUnixTimeMicroseconds(startTs).ToString())
                .Replace(":end_ts", Util.FromUnixTimeMicroseconds(endTs).ToString())
                .Replace(":limit_", limit.ToString());
        }

        public Task StoreTraceIdByDuration(string serviceName, string spanName, long timestamp, long duration, long traceId, int ttl)
        {
            try
            {
                BoundStatement bound = insertTraceIdBySpanDuration.Bind(new
                {
                    service_name = serviceName,
                    span_name = spanName,
                    bucket = DurationIndexBucket(timestamp),
                    ts = Util.FromUnixTimeMicroseconds(timestamp),
                    duration = duration,
                    trace_id = traceId,
                    ttl_ = ttl
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertTraceIdBySpanDuration(serviceName, spanName, timestamp, duration, traceId, ttl));
                }
                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertTraceIdBySpanDuration(serviceName, spanName, timestamp, duration, traceId, ttl));
                throw ex;
            }
        }

        private string DebugInsertTraceIdBySpanDuration(string serviceName, string spanName, long timestamp, long duration, long traceId, int ttl)
        {
            return insertTraceIdBySpanDurationQuery
                .Replace(":service_name", serviceName)
                .Replace(":span_name", spanName)
                .Replace(":bucket", DurationIndexBucket(timestamp).ToString())
                .Replace(":ts", Util.FromUnixTimeMicroseconds(timestamp).ToString())
                .Replace(":duration", duration.ToString())
                .Replace(":trace_id", traceId.ToString())
                .Replace(":ttl_", ttl.ToString());
        }

        /** Returns a map of trace id to timestamp (in microseconds) */
        public async Task<Dictionary<long, long>> GetTraceIdsByDuration(QueryRequest request, int ttl)
        {
            long oldestData = (Util.CurrentTimeMilliseconds() - (long)(ttl * 1000)) * 1000;
            long startTs = Math.Max((request.endTs - request.lookback) * 1000, oldestData);
            long endTs = Math.Max(request.endTs * 1000, oldestData);

            int startBucket = DurationIndexBucket(startTs);
            int endBucket = DurationIndexBucket(endTs);
            if (startBucket > endBucket)
            {
                throw new ArgumentException("Start bucket (" + startBucket + ") > end bucket (" + endBucket + ")");
            }

            var tasks = new List<Task<List<DurationRow>>>();
            for (int i = startBucket; i <= endBucket; i++)
            {
                tasks.Add(OneBucketDurationQuery(request, i, startTs, endTs));
            }
            var result = await Task.WhenAll(tasks);
            var traceIdsToTimestamps = new Dictionary<long, long>();
            foreach (var row in result.SelectMany(rs => rs))
            {
                long oldValue;
                if (!traceIdsToTimestamps.TryGetValue(row.trace_id, out oldValue) || oldValue > row.timestamp)
                {
                    traceIdsToTimestamps[row.trace_id] = row.timestamp;
                }
            }
            return traceIdsToTimestamps;
        }

        private async Task<List<DurationRow>> OneBucketDurationQuery(QueryRequest request, int bucket, long startTs, long endTs)
        {
            string serviceName = request.serviceName;
            string spanName = request.spanName ?? "";
            long minDuration = request.minDuration.Value;
            long maxDuration = request.maxDuration ?? long.MaxValue;
            int limit = request.limit;

            try
            {
                BoundStatement bound = selectTraceIdsBySpanDuration.Bind(new
                {
                    service_name = serviceName,
                    span_name = spanName,
                    bucket = bucket,
                    max_duration = maxDuration,
                    min_duration = minDuration
                });
                // optimistically setting fetch size to 'limit_' here. Since we are likely to filter some results
                // because their timestamps are out of range, we may need to fetch again.
                // TODO figure out better strategy
                bound.SetPageSize(limit);

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectTraceIdsByDuration(bucket, serviceName, spanName, minDuration, maxDuration, limit));
                }

                var result = await session.ExecuteAsync(bound);
                return result.GetRows().Select(r => new DurationRow(r))
                    .Where(r => r.timestamp >= startTs && r.timestamp <= endTs)
                    .Take(limit).ToList();
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectTraceIdsByDuration(bucket, serviceName, spanName, minDuration, maxDuration, limit), ex);
                throw ex;
            }
        }

        private string DebugSelectTraceIdsByDuration(int bucket, string serviceName, string spanName, long minDuration, long maxDuration, int limit)
        {
            return selectTraceIdsBySpanDurationQuery
                .Replace(":service_name", serviceName)
                .Replace(":span_name", spanName)
                .Replace(":bucket", bucket.ToString())
                .Replace(":max_duration", maxDuration.ToString())
                .Replace(":min_duration", minDuration.ToString())
                .Replace(":limit_", limit.ToString());
        }

        public Task StoreTraceIdByUpdateTime(long traceId, DateTimeOffset updatetime, int ttl)
        {
            var minute = new DateTime(updatetime.Year, updatetime.Month, updatetime.Day, updatetime.Hour, updatetime.Minute, 0);
            try
            {
                BoundStatement bound = insertTraceIdByUpdateTime.Bind(new
                {
                    trace_id = traceId,
                    updatetime = minute,
                    ttl_ = ttl
                });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugInsertTraceIdByUpdateTime(traceId, minute, ttl));
                }
                return session.ExecuteAsync(bound);
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugInsertTraceIdByUpdateTime(traceId, minute, ttl), ex);
                throw ex;
            }
        }
        private string DebugInsertTraceIdByUpdateTime(long traceId, DateTimeOffset updatetime, int ttl)
        {
            return selectTraceIdsBySpanDurationQuery
                .Replace(":trace_id", traceId.ToString())
                .Replace(":updatetime", updatetime.ToString())
                .Replace(":ttl_", ttl.ToString());
        }

        public async Task<List<long>> GetTraceIdsByUpdateTime(DateTimeOffset updatetime)
        {
            var minute = new DateTime(updatetime.Year, updatetime.Month, updatetime.Day, updatetime.Hour, updatetime.Minute, 0);
            try
            {
                BoundStatement bound = selectTraceIdsByUpdateTime.Bind(new { updatetime = minute });

                if (log.IsDebugEnabled)
                {
                    log.Debug(DebugSelectTraceIdsByUpdateTime(minute));
                }
                var result = await session.ExecuteAsync(bound);
                return result.GetRows().Select(r => r.GetValue<long>("trace_id")).ToList();
            }
            catch (Exception ex)
            {
                log.Error("failed " + DebugSelectTraceIdsByUpdateTime(minute), ex);
                throw ex;
            }
        }
        private string DebugSelectTraceIdsByUpdateTime(DateTimeOffset updatetime)
        {
            return selectTraceIdsBySpanDurationQuery
                .Replace(":updatetime", updatetime.ToString());
        }

        private int DurationIndexBucket(long ts)
        {
            // if the window constant has microsecond precision, the division produces negative values
            return (int)((ts / DURATION_INDEX_BUCKET_WINDOW_SECONDS) / 1000000);
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

        class DurationRow
        {
            public long trace_id;
            public long duration;
            public long timestamp; // inflated back to microseconds

            public DurationRow(Row row)
            {
                trace_id = row.GetValue<long>("trace_id");
                duration = row.GetValue<long>("duration");
                timestamp = Util.ToUnixTimeMicroseconds(row.GetValue<DateTimeOffset>("ts").DateTime);
            }

            public override string ToString()
            {
                return string.Format("trace_id={0}, duration={1}, timestamp={2}", trace_id, duration, timestamp);
            }
        }
    }
}