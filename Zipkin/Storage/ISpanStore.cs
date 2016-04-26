using System;
using System.Collections.Generic;

namespace Zipkin.Storage
{
    public interface ISpanStore
    {
        /**
         * Sinks the given spans, ignoring duplicate annotations.
         */
        void Accept(IEnumerable<Span> spans);

        /**
         * Get the available trace information from the storage system. Spans in trace are sorted by the
         * first annotation timestamp in that span. First event should be first in the spans list.
         *
         * <p/> Results are sorted in order of the first span's timestamp, and contain up to {@link
         * QueryRequest#limit} elements.
         */
        IEnumerable<IEnumerable<Span>> GetTraces(QueryRequest request);

        /**
         * Get the available trace information from the storage system. Spans in trace are sorted by the
         * first annotation timestamp in that span. First event should be first in the spans list.
         *
         * <p/> Results are sorted in order of the first span's timestamp, and contain less elements than
         * trace IDs when corresponding traces aren't available.
         */
        IEnumerable<IEnumerable<Span>> GetTracesByIds(IEnumerable<long> traceIds);

        /**
         * Get all the {@link Endpoint#serviceName service names}.
         *
         * <p/> Results are sorted lexicographically
         */
        IEnumerable<String> GetServiceNames();

        /**
         * Get all the span names for a particular {@link Endpoint#serviceName}.
         *
         * <p/> Results are sorted lexicographically
         */
        IEnumerable<String> GetSpanNames(String serviceName);

        /**
         * Returns dependency links derived from spans.
         *
         * <p/>Implementations may bucket aggregated data, for example daily. When this is the case, endTs
         * may be floored to align with that bucket, for example midnight if daily. lookback applies to
         * the original endTs, even when bucketed. Using the daily example, if endTs was 11pm and lookback
         * was 25 hours, the implementation would query against 2 buckets.
         *
         * @param endTs only return links from spans where {@link Span#timestamp} are at or before this
         *              time in epoch milliseconds.
         * @param lookback only return links from spans where {@link Span#timestamp} are at or after
         *                 (endTs - lookback) in milliseconds. Defaults to endTs.
         * @return dependency links in an interval contained by (endTs - lookback) or empty if none are
         *         found
         */
        IEnumerable<DependencyLink> GetDependencies(long endTs, long? lookback);
    }
}
