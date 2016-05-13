﻿using System;
using System.Collections.Generic;
using Zipkin.Internal;

namespace Zipkin.Adjuster
{
    public class MergeById
    {
        /// <summary>
        /// Merge all the spans with the same id. This is used by span stores who store
        /// partial spans and need them collated at query time.
        /// </summary>
        /// <param name="spans"></param>
        /// <returns></returns>
        public static List<Span> Apply(IEnumerable<Span> spans)
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

            foreach (var spansToMerge in spanIdToSpans.Values)
            {
                if (spansToMerge.Count == 1)
                {
                    result.Add(spansToMerge[0]);
                }
                else
                {
                    var builder = spansToMerge[0].ToBuilder();
                    for (int i = 1; i < spansToMerge.Count; i++)
                    {
                        builder.Merge(spansToMerge[i]);
                    }
                    result.Add(builder.Build());
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
