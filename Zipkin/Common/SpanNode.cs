using System;
using System.Collections.Generic;
using System.Linq;
using Zipkin.Internal;

namespace Zipkin
{
    public class SpanNode
    {
        public Span span { get; set; }
        public List<SpanNode> children { get; set; }
        private SpanNode(Span span)
        {
            this.span = Ensure.ArgumentNotNull(span, "span");
            this.children = new List<SpanNode>();
        }

        private void AddChild(SpanNode node)
        {
            children.Add(node);
        }

        public static SpanNode Create(Span span, IEnumerable<Span> spans)
        {
            SpanNode rootNode = new SpanNode(span);

            // Initialize nodes representing the trace tree
            var idToNode = new Dictionary<long, SpanNode>();
            foreach (var s in spans)
            {
                if (s.parentId.HasValue && s != span)
                {
                    idToNode.Add(s.id, new SpanNode(s));
                }
            }

            // Collect the parent-child relationships between all spans.
            var idToParent = new Dictionary<long, long>();
            foreach (var kvp in idToNode)
            {
                idToParent.Add(kvp.Key, kvp.Value.span.parentId.Value);
            }

            // Materialize the tree using parent - child relationships
            foreach (var kvp in idToParent)
            {
                var node = idToNode[kvp.Key];
                SpanNode parent;
                if (idToNode.TryGetValue(kvp.Value, out parent))
                {
                    parent.AddChild(node);
                }
                else
                {
                    rootNode.AddChild(node);
                }
            }
            return rootNode;
        }

        public List<Span> ToSpans()
        {
            var result = new List<Span>();
            result.Add(span);
            if (children != null)
            {
                foreach (var child in children.OrderBy(c => c.span).ToList())
                {
                    result.AddRange(child.ToSpans());
                }
            }
            return result;
        }

        public Dictionary<long, int> Depths(int startDepth)
        {
            return this.children.Aggregate(new Dictionary<long, int>() { { span.id, startDepth } }, (prevMap, child) => prevMap.Union(child.Depths(startDepth + 1)).ToDictionary(kvp => kvp.Key, kvp => kvp.Value));
        }
    }
}
