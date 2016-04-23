using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zipkin.Core;
using Dapper;
using Zipkin.Core.Internal;
using Zipkin.DbStore.Models;
using System.Configuration;
using MySql.Data.MySqlClient;

namespace Zipkin.DbStore
{
    public class DbSpanStore : ISpanStore
    {
        public readonly string sqlConnectionString = ConfigurationManager.ConnectionStrings["zipkin"].ConnectionString;

        private IDbConnection OpenConnection()
        {
            IDbConnection conn = new MySqlConnection(sqlConnectionString);
            conn.Open();
            return conn;
        }

        public void Accept(IEnumerable<Span> spans)
        {
            var spanEntities = new List<zipkin_spans>();
            var annoEntities = new List<zipkin_annotations>();
            foreach (var span in spans)
            {
                ApplyTimestampAndDuration.Apply(span);
                long? binaryAnnotationTimestamp = span.timestamp;
                if (!binaryAnnotationTimestamp.HasValue)
                {
                    // fallback if we have no timestamp, yet
                    binaryAnnotationTimestamp = Util.CurrentTimeMilliseconds() * 1000;
                }

                spanEntities.Add(new zipkin_spans()
                {
                    trace_id = span.traceId,
                    id = span.id,
                    name = span.name,
                    parent_id = span.parentId,
                    debug = span.debug,
                    start_ts = span.timestamp,
                    duration = span.duration
                });

                annoEntities.AddRange(span.annotations.Select(a =>
                {
                    var entity = new zipkin_annotations()
                    {
                        trace_id = span.traceId,
                        span_id = span.id,
                        a_key = a.value,
                        a_type = -1,
                        a_timestamp = a.timestamp
                    };
                    if (a.endpoint != null)
                    {
                        entity.endpoint_service_name = a.endpoint.serviceName;
                        entity.endpoint_ipv4 = a.endpoint.ipv4;
                        entity.endpoint_port = a.endpoint.port;
                    }
                    return entity;
                }).ToList());

                annoEntities.AddRange(span.binaryAnnotations.Select(a =>
                {
                    var entity = new zipkin_annotations()
                    {
                        trace_id = span.traceId,
                        span_id = span.id,
                        a_key = a.key,
                        a_value = a.value,
                        a_type = (int)a.type,
                        a_timestamp = binaryAnnotationTimestamp
                    };
                    if (a.endpoint != null)
                    {
                        entity.endpoint_service_name = a.endpoint.serviceName;
                        entity.endpoint_ipv4 = a.endpoint.ipv4;
                        entity.endpoint_port = a.endpoint.port;
                    }
                    return entity;
                }).ToList());
            }
            using (IDbConnection conn = OpenConnection())
            using (IDbTransaction transaction = conn.BeginTransaction())
            {
                conn.Execute(@"insert into zipkin_spans(trace_id,id,name,parent_id,debug,start_ts,duration) 
                    values(@trace_id,@id,@name,@parent_id,@debug,@start_ts,@duration)
                    on duplicate key update name=@name, start_ts=@start_ts, duration=@duration", spanEntities, transaction);

                conn.Execute(@"insert ignore into zipkin_annotations(trace_id, span_id, a_key, a_value, a_type, a_timestamp,endpoint_ipv4, endpoint_port, endpoint_service_name) 
                    values(@trace_id,@span_id,@a_key,@a_value,@a_type,@a_timestamp,@endpoint_ipv4,@endpoint_port,@endpoint_service_name)", annoEntities, transaction);

                transaction.Commit();
            }
        }

        public IEnumerable<IEnumerable<Span>> GetTracesByIds(IEnumerable<long> traceIds)
        {
            return 0 == traceIds.Count() ? Enumerable.Empty<IEnumerable<Span>>() : GetTraces(null, traceIds);
        }

        public IEnumerable<IEnumerable<Span>> GetTraces(QueryRequest request)
        {
            return GetTraces(request, null);
        }

        private IEnumerable<IEnumerable<Span>> GetTraces(QueryRequest request, IEnumerable<long> traceIds)
        {
            var spansWithoutAnnotations = new Dictionary<long, List<Span>>();
            var dbAnnotations = new Dictionary<KeyValuePair<long, long>, List<zipkin_annotations>>();
            using (IDbConnection conn = OpenConnection())
            {
                if (request != null)
                {
                    traceIds = conn.Query<long>(GetTraceIdQuery(request));
                }
                if (traceIds.Count() != 0)
                {
                    spansWithoutAnnotations = conn.Query<zipkin_spans>("select * from zipkin_spans where trace_id in @traceIds order by start_ts", new { traceIds = traceIds })
                        .Select(s =>
                        {
                            return new Span(
                                s.trace_id,
                                s.name,
                                s.id,
                                s.parent_id,
                                s.start_ts,
                                s.duration,
                                debug: s.debug
                            );
                        }).GroupBy(s => s.traceId).ToDictionary(g => g.Key, g => g.ToList());

                    dbAnnotations = conn.Query<zipkin_annotations>("select * from zipkin_annotations where trace_id in @traceIds order by a_timestamp, a_key", new { traceIds = traceIds })
                        .GroupBy(a => new KeyValuePair<long, long>(a.trace_id, a.span_id)).ToDictionary(g => g.Key, g => g.ToList());
                }
            }

            List<List<Span>> result = new List<List<Span>>();
            foreach (var spans in spansWithoutAnnotations.Values)
            {
                List<Span> trace = new List<Span>();
                foreach (Span span in spans)
                {
                    var key = new KeyValuePair<long, long>(span.traceId, span.id);
                    if (dbAnnotations.ContainsKey(key))
                    {
                        foreach (var a in dbAnnotations[key])
                        {
                            Endpoint endpoint = null;
                            if (!string.IsNullOrEmpty(a.endpoint_service_name)
                                && a.endpoint_ipv4.HasValue && a.endpoint_port.HasValue)
                            {
                                endpoint = new Endpoint()
                                {
                                    serviceName = a.endpoint_service_name,
                                    ipv4 = a.endpoint_ipv4.Value,
                                    port = a.endpoint_port.Value
                                };
                            }
                            if (-1 == a.a_type)
                            {
                                span.annotations.Add(new Annotation(a.a_timestamp.Value, a.a_key, endpoint));
                            }
                            else
                            {
                                span.binaryAnnotations.Add(new BinaryAnnotation(a.a_key, a.a_value, (AnnotationType)a.a_type, endpoint));
                            }
                        }
                    }
                    trace.Add(span);
                }
                //trace = CorrectForClockSkew.apply(trace);
                result.Add(trace);
            }
            result.Sort((left, right) => right[0].CompareTo(left[0]));
            return result;
        }

        private string GetTraceIdQuery(QueryRequest request)
        {
            long endTs = (request.endTs > 0 && request.endTs != long.MaxValue)
                ? request.endTs * 1000 : Util.CurrentTimeMilliseconds() * 1000;

            var query = new StringBuilder();
            query.Append(@"select distinct s.trace_id from zipkin_spans as s
                    join zipkin_annotations as a
                        on s.trace_id = a.trace_id and s.id = a.span_id");
            var keyToTables = new Dictionary<string, string>();
            int i = 0;
            foreach (var key in request.binaryAnnotations.Keys)
            {
                keyToTables.Add(key, "a" + i++);
                query.Append(Join(keyToTables[key], key, (int)AnnotationType.STRING));
            }
            foreach (var key in request.annotations)
            {
                keyToTables.Add(key, "a" + i++);
                query.Append(Join(keyToTables[key], key, -1));
            }
            query.Append(" where s.start_ts >= ");
            query.Append(endTs - request.lookback * 1000);
            query.Append(" and s.start_ts < ");
            query.Append(endTs);
            query.Append(" and a.endpoint_service_name = '");
            query.Append(request.serviceName);
            query.Append("'");
            if (!string.IsNullOrEmpty(request.spanName))
            {
                query.Append(" and s.name = '");
                query.Append(request.spanName);
                query.Append("'");
            }
            if (request.minDuration.HasValue)
            {
                query.Append(" and s.duration >= ");
                query.Append(request.minDuration);
            }
            if (request.maxDuration.HasValue)
            {
                query.Append(" and s.duration < ");
                query.Append(request.maxDuration);
            }
            foreach (var kvp in request.binaryAnnotations)
            {
                query.AppendFormat(" and {0}.a_value = '{1}'", keyToTables[kvp.Key], kvp.Value);
            }
            query.Append(" order by s.start_ts desc limit ");
            query.Append(request.limit);
            return query.ToString();
        }

        private string Join(string joinTable, string key, int type)
        {
            return string.Format(@" join zipkin_annotations as {0}
                on s.trace_id = {0}.trace_id
                    and s.id = {0}.span_id
                    and {0}.a_type = " + type
                + " and {0}.a_key = '" + key + "'", joinTable);
        }

        public IEnumerable<string> GetServiceNames()
        {
            using (IDbConnection conn = OpenConnection())
            {
                return conn.Query<string>(@"select distinct endpoint_service_name 
                    from zipkin_annotations 
                    where endpoint_service_name is not null and endpoint_service_name != ''");
            }
        }

        public IEnumerable<string> GetSpanNames(string serviceName)
        {
            using (IDbConnection conn = OpenConnection())
            {
                return conn.Query<string>(@"select distinct s.name
                    from zipkin_spans as s
                        join zipkin_annotations as a on s.trace_id = a.trace_id and s.id = a.span_id
                    where a.endpoint_service_name = @serviceName", new { serviceName = serviceName });
            }
        }

        public IEnumerable<DependencyLink> GetDependencies(long endTs, long? lookback)
        {
            endTs = endTs * 1000;
            using (IDbConnection conn = OpenConnection())
            {
                var parentChild = conn.Query(@"select trace_id, parent_id, id
                        from zipkin_spans 
                        where parent_id is not null
                            and start_ts <= " + endTs
                        + (lookback.HasValue ? (" and start_ts > " + (endTs - lookback.Value * 1000)) : ""))
                    .Select(r => new { trace_id = (long)r.trace_id, parent_id = (long)r.parent_id, id = (long)r.id})
                    .GroupBy(r => r.trace_id).ToDictionary(g => g.Key, g => g.ToList());

                var traceSpanServiceName = conn.Query(@"select distinct trace_id, span_id, endpoint_service_name
                        from zipkin_annotations 
                        where trace_id in @traceIds
                            and a_key in ('sr', 'sa') 
                            and endpoint_service_name is not null
                        group by trace_id, span_id", new { traceIds = parentChild.Keys.ToArray() })
                    .ToDictionary(r => new KeyValuePair<long, long>(r.trace_id, r.span_id), r => (string)r.endpoint_service_name);

                // links are merged by mapping to parent/child and summing corresponding links
                var dictLink = new Dictionary<KeyValuePair<string, string>, long>();

                parentChild.Values.SelectMany(p => p).ToList().ForEach(r => {
                    string parent;
                    if (traceSpanServiceName.TryGetValue(new KeyValuePair<long, long>(r.trace_id, r.parent_id), out parent))
                    {
                        string child;
                        if (traceSpanServiceName.TryGetValue(new KeyValuePair<long, long>(r.trace_id, r.id), out child))
                        {
                            var kvp = new KeyValuePair<string, string>(parent, child);
                            if (dictLink.ContainsKey(kvp))
                            {
                                dictLink[kvp] += 1;
                            }
                            else
                            {
                                dictLink.Add(kvp, 1L);
                            }
                        }
                    }
                });
                return dictLink.Select(kvp => new DependencyLink(kvp.Key.Key, kvp.Key.Value, kvp.Value));
            }
        }
    }
}
