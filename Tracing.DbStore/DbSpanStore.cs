using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tracing.Core;
using Dapper;
using Tracing.Core.Internal;
using Tracing.DbStore.Models;

namespace Tracing.DbStore
{
    public class DbSpanStore
    {
        public readonly string sqlConnectionString = "server=127.0.0.1;database=tracing;uid=root;pwd=123456;charset='gbk'";

        private SqlConnection OpenConnection()
        {
            SqlConnection conn = new SqlConnection(sqlConnectionString);
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
                    conn.Execute(@"replace to zipkin_spans(trace_id,id,name,parent_id,debug,start_ts,duration) 
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
                        conn.Execute(@"replace to zipkin_annotations(trace_id, span_id, a_key, a_type, a_timestamp,endpoint_ipv4, endpoint_port, endpoint_service_name) 
                            values(@trace_id,@ span_id,@ a_key,@ a_type,@ a_timestamp,@endpoint_ipv4,@ endpoint_port,@ endpoint_service_name)", annotationEntity, transaction);
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
                        conn.Execute(@"replace to zipkin_annotations(trace_id, span_id, a_key, a_type, a_timestamp,endpoint_ipv4, endpoint_port, endpoint_service_name) 
                            values(@trace_id,@ span_id,@ a_key,@ a_type,@ a_timestamp,@endpoint_ipv4,@ endpoint_port,@ endpoint_service_name)", annotationEntity, transaction);
                    }
                }
                transaction.Commit();
            }
        }

        private List<List<Span>> GetTraces(QueryRequest request, IEnumerable<long> traceIds)
        {
            IDictionary<long, List<Span>> spansWithoutAnnotations;
            IDictionary<KeyValuePair<long, long>, List<zipkin_annotations>> dbAnnotations;
            using (IDbConnection conn = OpenConnection())
            using (IDbTransaction transaction = conn.BeginTransaction())
            {
                if (request != null)
                {
                    traceIds = GetTraceIdsByQuery(request);
                }
                spansWithoutAnnotations = conn.Query<zipkin_spans>("select * from zipkin_spans where trace_id in @traceIds", new { traceIds = traceIds })
                    .Select(s =>
                    {
                        return new Span()
                        {
                            traceId = s.trace_id,
                            id = s.id,
                            name = s.name,
                            parentId = s.parent_id,
                            timestamp = s.start_ts,
                            duration = s.duration,
                            debug = s.debug
                        };
                    }).GroupBy(s => s.traceId).ToDictionary(g => g.Key, g => g.ToList());

                dbAnnotations = conn.Query<zipkin_annotations>("select * from zipkin_annotations where trace_id in @traceId order by a_timestamp, a_key", new { traceIds = traceIds })
                    .GroupBy(a => new KeyValuePair<long, long>(1, 1)).ToDictionary(g => g.Key, g => g.ToList());
            }

            List<List<Span>> result = new List<List<Span>>();
            foreach (List<Span> spans in spansWithoutAnnotations.Values)
            {
                List<Span> trace = new List<Span>();
                foreach (Span s in spans)
                {
                    Span span = new Span();
                    var key = new KeyValuePair<long, long>(s.traceId, s.id);
                    if (dbAnnotations.ContainsKey(key))
                    {
                        foreach (var a in dbAnnotations[key])
                        {
                            Endpoint endpoint = new Endpoint()
                            {
                                serviceName = a.endpoint_service_name,
                                ipv4 = a.endpoint_ipv4.Value,
                                port = a.endpoint_port
                            };
                            if (-1 == a.a_type)
                            {
                                span.annotations.Add(new Annotation(a.a_timestamp.Value, a.a_key, endpoint));
                            }
                            else
                            {
                                span.binaryAnnotations.Add(new BinaryAnnotation(a.a_key, a.a_value, (BinaryAnnotation.Type)a.a_type, endpoint));
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
            return new long[] { 1 };
        }
    }

}
