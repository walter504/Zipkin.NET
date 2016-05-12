using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Zipkin.Storage
{
    public interface IDependencyStore
    {
        /// <summary>
        /// Returns dependency links derived from spans in the [[SpanStore]].
        /// 
        /// Implementations may bucket aggregated data, for example daily. When this is the case, endTs
        /// may be floored to align with that bucket, for example midnight if daily. lookback applies to
        /// the original endTs, even when bucketed. Using the daily example, if endTs was 11pm and lookback
        /// was 25 hours, the implementation would query against 2 buckets.
        /// </summary>
        /// <param name="endTs">
        /// only return links from spans where [[com.twitter.zipkin.common.Span.timestamp]]
        /// are at or before this time in epoch milliseconds.
        /// </param>
        /// <param name="lookback">
        /// only return links from spans where [[com.twitter.zipkin.common.Span.timestamp]]
        /// are at or after (endTs - lookback) in milliseconds. Defaults to endTs.
        /// </param>
        /// <returns>
        /// dependency links in an interval contained by (endTs - lookback) or 
        /// empty if none are found
        /// </returns>
        Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback);
        Task StoreDependencies(long epochDayMillis, IEnumerable<DependencyLink> links);
    }

    public class NullDependencyStore : IDependencyStore
    {
        public virtual Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback)
        {
            return Task.FromResult((new List<DependencyLink>()).AsEnumerable());
        }
        public virtual Task StoreDependencies(long epochDayMillis, IEnumerable<DependencyLink> links)
        {
            return Task.FromResult(0);
        }
    }
}
