using System;
using System.Collections.Generic;
using System.Linq;

namespace Zipkin.Adjuster
{
    public class CorrectForClockSkew
    {
        public static List<Span> Apply(List<Span> spans)
        {
            var root = spans.FirstOrDefault(s => !s.parentId.HasValue);
            if (root == null)
            {
                return spans;
            }
            else
            {
                var tree = SpanNode.Create(root, spans);
                Adjust(tree, null);
                return tree.ToSpans();
            }
        }

        /**
         * Recursively adjust the timestamps on the span tree. Root span is the reference point, all
         * children's timestamps gets adjusted based on that span's timestamps.
         */
        private static void Adjust(SpanNode node, ClockSkew skewFromParent)
        {
            // adjust skew for the endpoint brought over from the parent span
            if (skewFromParent != null)
            {
                node.span = AdjustTimestamps(node.span, skewFromParent);
            }
            // Is there any skew in the current span?
            var skew = GetClockSkew(node.span);
            if (skew != null)
            {
                // the current span's skew may be a different endpoint than skewFromParent, adjust again.
                node.span = AdjustTimestamps(node.span, skew);
                // propagate skew to any children
                foreach (SpanNode child in node.children)
                {
                    Adjust(child, skew);
                }
            }
        }

        /** If any annotation has an IP with skew associated, adjust accordingly. */
        private static Span AdjustTimestamps(Span span, ClockSkew clockSkew)
        {
            Annotation[] annotations = null;
            var length = span.annotations.Count;
            for (int i = 0; i < length; i++)
            {
                var a = span.annotations[i];
                if (a.endpoint == null) continue;
                if (clockSkew.endpoint.ipv4 == a.endpoint.ipv4)
                {
                    if (annotations == null)
                    {
                        annotations = span.annotations.ToArray();
                    }
                    annotations[i] = new Annotation(a.timestamp - clockSkew.skew, a.value, a.endpoint);
                }
            }
            if (annotations == null) return span;
            // reset timestamp and duration as if there's skew, these will change.
            long first = annotations[0].timestamp;
            long last = annotations[length - 1].timestamp;
            long duration = last - first;
            return new Span(span.traceId, span.name, span.id, span.parentId, first,
                duration, span.annotations, span.binaryAnnotations, span.debug);
        }

        private static ClockSkew GetClockSkew(Span span)
        {
            var annotations = span.annotations.ToDictionary(a => a.value, a => a);

            var clientSend = GetTimestamp(annotations, Constants.ClientSend);
            var clientRecv = GetTimestamp(annotations, Constants.ClientRecv);
            var serverRecv = GetTimestamp(annotations, Constants.ServerRecv);
            var serverSend = GetTimestamp(annotations, Constants.ServerSend);

            if (!clientSend.HasValue || !clientRecv.HasValue || !serverRecv.HasValue || !serverSend.HasValue)
            {
                return null;
            }

            var server = annotations[Constants.ServerRecv].endpoint;
            server = server == null ? annotations[Constants.ServerSend].endpoint : server;
            if (server == null) return null;

            var clientDuration = clientRecv.Value - clientSend.Value;
            var serverDuration = serverSend.Value - serverRecv.Value;

            // There is only clock skew if CS is after SR or CR is before SS
            var csAhead = clientSend < serverRecv;
            var crAhead = clientRecv > serverSend;
            if (serverDuration > clientDuration || (csAhead && crAhead))
            {
                return null;
            }
            long latency = (clientDuration - serverDuration) / 2;
            long skew = serverRecv.Value - latency - clientSend.Value;
            if (skew != 0L)
            {
                return new ClockSkew(server, skew);
            }
            return null;
        }

        private static long? GetTimestamp(Dictionary<string, Annotation> annotations, string value)
        {
            Annotation anno = null;
            annotations.TryGetValue(value, out anno);
            return anno == null ? null : new Nullable<long>(anno.timestamp);
        }

        class ClockSkew
        {
            public Endpoint endpoint { get; set; }
            public long skew { get; set; }
            public ClockSkew(Endpoint endpoint, long skew)
            {
                this.endpoint = endpoint;
                this.skew = skew;
            }
        }
    }
}
