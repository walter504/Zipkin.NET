using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Zipkin.Storage
{
    public interface ISpanStore
    {
        /// <summary>
        /// Sinks the given spans, ignoring duplicate annotations.
        /// </summary>
        /// <param name="spans"></param>
        /// <returns></returns>
        Task Accept(IEnumerable<Span> spans);

        /// <summary>
        /// Get the available trace information from the storage system. Spans in trace are sorted by the
        /// first annotation timestamp in that span. First event should be first in the spans list.
        ///
        /// <p/> Results are sorted in order of the first span's timestamp, and contain up to {@link
        /// QueryRequest#limit} elements.
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        Task<IEnumerable<IEnumerable<Span>>> GetTraces(QueryRequest request);

        /// <summary>
        /// Get the available trace information from the storage system. Spans in trace are sorted by the
        /// first annotation timestamp in that span. First event should be first in the spans list.
        /// </summary>
        /// <param name="id"></param>
        /// <returns>a list of spans with the same {@link Span#traceId}, or null if not present.</returns>
        Task<IEnumerable<Span>> GetTrace(long id);

        /// <summary>
        /// Retrieves spans that share a trace id, as returned from backend data store queries, with no
        /// ordering expectation.
        ///
        /// <p>This is different, but related to {@link #getTrace}. {@link #getTrace} cleans data by
        /// merging spans, adding timestamps and performing clock skew adjustment. This feature is for
        /// debugging zipkin logic or zipkin instrumentation.
        /// 
        /// </summary>
        /// <param name="traceId"></param>
        /// <returns>a list of spans with the same {@link Span#traceId}, or null if not present.</returns>
        Task<IEnumerable<Span>> GetRawTrace(long traceId);

        /// <summary>
        /// Get all the {@link Endpoint#serviceName service names}.
        /// <p/> Results are sorted lexicographically
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<string>> GetServiceNames();

        /// <summary>
        /// Get all the span names for a particular {@link Endpoint#serviceName}.
        /// </summary>
        /// <param name="serviceName"></param>
        /// <returns>Results are sorted lexicographically</returns>
        Task<IEnumerable<string>> GetSpanNames(string serviceName);
    }
}
