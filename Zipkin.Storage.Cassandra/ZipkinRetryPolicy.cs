using System;
using Cassandra;

namespace Zipkin.Storage.Cassandra
{
    public class ZipkinRetryPolicy : IRetryPolicy
    {
        public static readonly ZipkinRetryPolicy Instance = new ZipkinRetryPolicy();

        private ZipkinRetryPolicy()
        {
        }

        public RetryDecision OnReadTimeout(IStatement statement, ConsistencyLevel cl,
            int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }

        public RetryDecision OnWriteTimeout(IStatement statement, ConsistencyLevel cl, string writeType,
            int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }

        public RetryDecision OnUnavailable(IStatement statement, ConsistencyLevel cl, int requiredReplica,
            int aliveReplica, int nbRetry)
        {
            return RetryDecision.Retry(ConsistencyLevel.One);
        }
    }
}
