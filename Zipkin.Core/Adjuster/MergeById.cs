using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class MergeById
    {
        public static IList<Span> Apply(IList<Span> spans)
        {
            var result = new List<Span>();
            var spanIdToSpans = new Dictionary<long, List<Span>>();
            foreach (var span in spans)
            {
                if (!spanIdToSpans.ContainsKey(span.id))
                {
                    spanIdToSpans.Add(span.id, new List<Span>());
                }
                spanIdToSpans[span.id].Add(span);
            }

            foreach (List<Span> spansToMerge in spanIdToSpans.Values)
            {
                if (spansToMerge.Count == 1)
                {
                    result.Add(spansToMerge[0]);
                }
                else
                {
                    //Span.Builder builder = new Span.Builder(spansToMerge.get(0));
                    for (int i = 1; i < spansToMerge.Count; i++)
                    {
                        //builder.merge(spansToMerge.get(i));
                    }
                    //result.add(builder.build());
                }
            }

            // Apply timestamp so that sorting will be helpful
            for (int i = 0; i < result.Count; i++)
            {
                result[i] = ApplyTimestampAndDuration.Apply(result[i]);
            }
            return Util.SortedList(result);
        }
    }
}
