using System;
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
    public class CassandraSpanStore : ISpanStore
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(CassandraSpanStore));

        private readonly int spanTtl;
        private readonly int indexTtl;
        private readonly int maxTraceCols;
        private readonly Lazy<Repository> lazyRepository;

        private Repository Repo
        {
            get
            {
                return lazyRepository.Value;
            }
        }

        public CassandraSpanStore(Lazy<Repository> lazyRepository, int spanTtl, int indexTtl, int maxTraceCols)
        {
            this.lazyRepository = lazyRepository;
            this.spanTtl = spanTtl;
            this.indexTtl = indexTtl;
            this.maxTraceCols = maxTraceCols;
        }

        public Task Accept(IEnumerable<Span> spans)
        {
            var tasks = new List<Task>();
            foreach (var s in spans)
            {
                var span = ApplyTimestampAndDuration.Apply(s);
                tasks.Add(Repo.StoreSpan(span.traceId,
                    span.timestamp ?? 0L,
                    CreateSpanColumnName(span),
                    Codec.THRIFT.WriteSpan(span),
                    spanTtl));
                tasks.Add(Repo.StoreTraceIdByUpdateTime(span.traceId, DateTime.Now, indexTtl));
                foreach (var serviceName in span.ServiceNames)
                {
                    // SpanStore.GetServiceNames
                    tasks.Add(Repo.StoreServiceName(serviceName, indexTtl));
                    if (!string.IsNullOrEmpty(span.name))
                    {
                        // SpanStore.GetSpanNames
                        tasks.Add(Repo.StoreSpanName(serviceName, span.name, indexTtl));
                    }
                    if (span.timestamp.HasValue)
                    {
                        // QueryRequest.serviceName
                        tasks.Add(Repo.StoreTraceIdByServiceName(serviceName, span.timestamp.Value, span.traceId, indexTtl));
                        if (!string.IsNullOrEmpty(span.name))
                        {
                            // QueryRequest.spanName
                            tasks.Add(Repo.StoreTraceIdBySpanName(serviceName, span.name, span.timestamp.Value, span.traceId, indexTtl));
                        }

                        // QueryRequest.min/maxDuration
                        if (span.duration != null)
                        {
                            // Contract for StoreTraceIdByDuration is to store the span twice, once with
                            // the span name and another with empty string.
                            tasks.Add(Repo.StoreTraceIdByDuration(serviceName, span.name,
                                span.timestamp.Value, span.duration.Value, span.traceId, indexTtl));
                            if (!string.IsNullOrEmpty(span.name))
                            {
                                tasks.Add(Repo.StoreTraceIdByDuration(serviceName, "",
                                    span.timestamp.Value, span.duration.Value, span.traceId, indexTtl));
                            }
                        }
                    }
                }
                if (span.timestamp.HasValue)
                {
                    foreach (var a in span.annotations)
                    {
                        if (a.endpoint != null && !string.IsNullOrEmpty(a.endpoint.serviceName))
                        {
                            tasks.Add(Repo.StoreTraceIdByAnnotation(AnnotationKey(a.endpoint.serviceName, a.value, null),
                                span.timestamp.Value, span.traceId, indexTtl));
                        }
                    }
                    foreach (var ba in span.binaryAnnotations)
                    {
                        if (ba.type == AnnotationType.STRING
                            && ba.endpoint != null
                            && !string.IsNullOrEmpty(ba.endpoint.serviceName))
                        {
                            tasks.Add(Repo.StoreTraceIdByAnnotation(AnnotationKey(ba.endpoint.serviceName, ba.key, null),
                                span.timestamp.Value, span.traceId, indexTtl));
                            tasks.Add(Repo.StoreTraceIdByAnnotation(AnnotationKey(ba.endpoint.serviceName, ba.key, ba.value),
                                span.timestamp.Value, span.traceId, indexTtl));
                        }
                    }
                }
            }

            return Task.WhenAll(tasks);
        }

        public async Task<IEnumerable<IEnumerable<Span>>> GetTraces(QueryRequest request)
        {
            Task<Dictionary<long, long>> traceIdToTimestamp;
            if (request.minDuration != null || request.maxDuration != null)
            {
                traceIdToTimestamp = Repo.GetTraceIdsByDuration(request, indexTtl);
            }
            else if (request.spanName != null)
            {
                traceIdToTimestamp = Repo.GetTraceIdsBySpanName(request.serviceName, request.spanName,
                    request.endTs * 1000, request.lookback * 1000, request.limit);
            }
            else if (request.serviceName != null)
            {
                traceIdToTimestamp = Repo.GetTraceIdsByServiceNames(new List<string> { request.serviceName },
                    request.endTs * 1000, request.lookback * 1000, request.limit);
            }
            else
            {
                //checkArgument(selectTraceIdsByServiceNames != null,
                //    "getTraces without serviceName requires Cassandra 2.2 or later");
                var serviceNames = await Repo.GetServiceNames();
                traceIdToTimestamp = Repo.GetTraceIdsByServiceNames(serviceNames,
                            request.endTs * 1000, request.lookback * 1000, request.limit);
            }

            var taskKeySetsToIntersect = new List<Task<Dictionary<long, long>>>();
            foreach (var a in request.annotations)
            {
                taskKeySetsToIntersect.Add(Repo.GetTraceIdsByAnnotation(AnnotationKey(request.serviceName, a, null),
                    request.endTs * 1000, request.lookback * 1000, request.limit));
            }
            foreach (var ba in request.binaryAnnotations)
            {
                taskKeySetsToIntersect.Add(Repo.GetTraceIdsByAnnotation(
                    AnnotationKey(request.serviceName, ba.Key, System.Text.Encoding.UTF8.GetBytes(ba.Value)),
                    request.endTs * 1000, request.lookback * 1000, request.limit));
            }
            IEnumerable<long> traceIds;
            if (taskKeySetsToIntersect.Count == 0)
            {
                traceIds = (await traceIdToTimestamp).Keys.Take(request.limit);
            }
            else
            {
                taskKeySetsToIntersect.Add(traceIdToTimestamp);
                var traceIdsToTimestamps = await Task.WhenAll(taskKeySetsToIntersect);
                var groupTraceIds = traceIdsToTimestamps.Select(kvp => kvp.Keys.AsEnumerable()).ToList();
                traceIds = groupTraceIds.Aggregate(groupTraceIds[0], (acc, next) => acc.Intersect(next));
            }

            var result = await Repo.GetSpansByTraceIds(traceIds.Take(request.limit).ToArray(), maxTraceCols);
            return result.Select(ss => MergeById.Apply(ss)).Select(ss => CorrectForClockSkew.Apply(ss));
        }

        public async Task<IEnumerable<Span>> GetTrace(long traceId)
        {
            var trace = await GetRawTrace(traceId);
            return trace == null || trace.Count() == 0
                ? null
                : CorrectForClockSkew.Apply(MergeById.Apply(trace));
        }

        public async Task<IEnumerable<Span>> GetRawTrace(long traceId)
        {
            var traces = await Repo.GetSpansByTraceIds(new long[] {traceId}, maxTraceCols);
            return traces.Count == 0 ? null : traces[0];
        }


        public Task<IEnumerable<string>> GetServiceNames()
        {
            return Repo.GetServiceNames();
        }

        public Task<IEnumerable<string>> GetSpanNames(string serviceName)
        {
            return Repo.GetSpanNames(serviceName);
        }

        public Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback)
        {
            throw new NotImplementedException();
        }

        private string CreateSpanColumnName(Span span)
        {
            return string.Format("{0}_{1}_{2}", span.id, span.annotations.GetHashCode(), span.binaryAnnotations.GetHashCode());
        }

        private byte[] AnnotationKey(string serviceName, string annotation, byte[] value)
        {
            var key = serviceName + ":" + annotation;
            if (value != null)
            {
                key += ":";
            }
            var buffer = System.Text.Encoding.UTF8.GetBytes(key);
            if (value != null)
            {
                buffer = buffer.Concat(value).ToArray();
            }
            return buffer;
        }

    }
}
