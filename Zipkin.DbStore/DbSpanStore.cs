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
            using (IDbConnection conn = OpenConnection())
            using (IDbTransaction transaction = conn.BeginTransaction())
            {
                foreach (var span in spans)
                {
                    ApplyTimestampAndDuration.Apply(span);
                    long? binaryAnnotationTimestamp = span.timestamp;
                    if (!binaryAnnotationTimestamp.HasValue)
                    {
                        // fallback if we have no timestamp, yet
                        binaryAnnotationTimestamp = Util.GetCurrentTimeStamp();
                    }
                    var spanEntity = new zipkin_spans()
                    {
                        trace_id = span.traceId,
                        id = span.id,
                        name = span.name,
                        parent_id = span.parentId,
                        debug = span.debug,
                        start_ts = span.timestamp.Value,
                        duration = span.duration.Value
                    };
                    conn.Execute(@"replace into zipkin_spans(trace_id,id,name,parent_id,debug,start_ts,duration) 
                        values(@trace_id,@id,@name,@parent_id,@debug,@start_ts,@duration)", spanEntity, transaction);

                    foreach (var annotation in span.annotations)
                    {
                        var annotationEntity = new zipkin_annotations()
                        {
                            trace_id = span.traceId,
                            span_id = span.id,
                            a_key = annotation.value,
                            a_type = -1,
                            a_timestamp = annotation.timestamp
                        };
                        if (annotation.endpoint != null)
                        {
                            annotationEntity.endpoint_service_name = annotation.endpoint.serviceName;
                            annotationEntity.endpoint_ipv4 = annotation.endpoint.ipv4;
                            annotationEntity.endpoint_port = annotation.endpoint.port;
                        }
                        conn.Execute(@"replace into zipkin_annotations(trace_id, span_id, a_key, a_type, a_timestamp,endpoint_ipv4, endpoint_port, endpoint_service_name) 
                            values(@trace_id,@span_id,@a_key,@a_type,@a_timestamp,@endpoint_ipv4,@endpoint_port,@endpoint_service_name)", annotationEntity, transaction);
                    }
                    foreach (var annotation in span.binaryAnnotations)
                    {
                        var annotationEntity = new zipkin_annotations()
                        {
                            trace_id = span.traceId,
                            span_id = span.id,
                            a_key = annotation.key,
                            a_value = annotation.value,
                            a_type = (int)annotation.type,
                            a_timestamp = binaryAnnotationTimestamp
                        };
                        if (annotation.endpoint != null)
                        {
                            annotationEntity.endpoint_service_name = annotation.endpoint.serviceName;
                            annotationEntity.endpoint_ipv4 = annotation.endpoint.ipv4;
                            annotationEntity.endpoint_port = annotation.endpoint.port;
                        }
                        conn.Execute(@"replace into zipkin_annotations(trace_id, span_id, a_key, a_type, a_timestamp,endpoint_ipv4, endpoint_port, endpoint_service_name) 
                            values(@trace_id,@span_id,@a_key,@a_type,@a_timestamp,@endpoint_ipv4,@endpoint_port,@endpoint_service_name)", annotationEntity, transaction);
                    }
                }
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
            IDictionary<long, IEnumerable<Span>> spansWithoutAnnotations;
            IDictionary<KeyValuePair<long, long>, IEnumerable<zipkin_annotations>> dbAnnotations;
            using (IDbConnection conn = OpenConnection())
            {
                if (request != null)
                {
                    traceIds = GetTraceIdsByQuery(request);
                }
                spansWithoutAnnotations = conn.Query<zipkin_spans>("select * from zipkin_spans where trace_id in @traceIds", new { traceIds = traceIds })
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
                    }).GroupBy(s => s.traceId).ToDictionary(g => g.Key, g => g.AsEnumerable());

                dbAnnotations = conn.Query<zipkin_annotations>("select * from zipkin_annotations where trace_id in @traceIds order by a_timestamp, a_key", new { traceIds = traceIds })
                    .GroupBy(a => new KeyValuePair<long, long>(a.trace_id, a.span_id)).ToDictionary(g => g.Key, g => g.AsEnumerable());
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
                            Endpoint endpoint = new Endpoint()
                            {
                                serviceName = a.endpoint_service_name,
                                ipv4 = a.endpoint_ipv4 ?? 0,
                                port = a.endpoint_port ?? 0
                            };
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

        private IEnumerable<long> GetTraceIdsByQuery(QueryRequest request)
        {
            long endTs = (request.endTs > 0 && request.endTs != long.MaxValue)
                ? request.endTs * 1000 : Util.GetCurrentTimeStamp() * 1000;
            string condition = " where s.start_ts > " + request.lookback * 1000
                + " and s.start_ts <= " + endTs;
            if (!string.IsNullOrEmpty(request.spanName))
            {
                condition += " and s.name = '" + request.spanName + "'";
            }
            if (request.minDuration.HasValue && request.maxDuration.HasValue)
            {
                condition += " and s.duration > " + request.minDuration + " and s.start_ts <= " + request.maxDuration;
            }
            string joinSql = string.Empty;
            foreach (var pair in request.binaryAnnotations)
            {
                joinSql += " or (a.a_type = " + (int)AnnotationType.STRING +
                    " and a.a_key = '" + pair.Key + " and a.a_value = '" + pair.Value + "')";
            }
            foreach (var key in request.annotations)
            {
                joinSql += " or (a.a_type = -1 and a.a_key = '" + key + "')";
            }
            if (!string.IsNullOrEmpty(joinSql))
            {
                joinSql = " join zipkin_annotations as a on s.trace_id = a.trace_id and s.id = a.span_id and (1=2 " + joinSql + ")";
            }
            string query = "select distinct s.trace_id from zipkin_spans as s" + joinSql + condition;

            using (IDbConnection conn = OpenConnection())
            {
                return conn.Query<long>(query);
            }
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

        public IEnumerable<DependencyLink> GetDependencies(long endTs, long lookback)
        {
            return new List<DependencyLink>();
        }
    }

}
