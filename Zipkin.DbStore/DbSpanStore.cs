﻿using System;
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
                        binaryAnnotationTimestamp = Util.CurrentTimeMilliSeconds() * 1000;
                    }
                    var spanEntity = new zipkin_spans()
                    {
                        trace_id = span.traceId,
                        id = span.id,
                        name = span.name,
                        parent_id = span.parentId,
                        debug = span.debug,
                        start_ts = span.timestamp,
                        duration = span.duration
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
                    traceIds = conn.Query<long>(GetTraceIdQuery(request));
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
                ? request.endTs * 1000 : Util.CurrentTimeMilliSeconds() * 1000;

            var query = new StringBuilder();
            query.Append("select distinct s.trace_id from zipkin_spans as s"
                + " join zipkin_annotations as a"
                + " on s.trace_id = a.trace_id"
                + " and s.id = a.span_id");
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
            query.Append(" order by s.start_ts desc limit ");
            query.Append(request.limit);
            return query.ToString();
        }

        private string Join(string joinTable, string key, int type)
        {
            return string.Format(" join zipkin_annotations as {0}"
                + " on s.trace_id = {0}.trace_id"
                + " and s.id = {0}.span_id"
                + " and {0}.a_type = " + (int)AnnotationType.STRING
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

        public IEnumerable<DependencyLink> GetDependencies(long endTs, long lookback)
        {
            return new List<DependencyLink>();
        }
    }

}
