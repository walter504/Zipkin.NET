using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tracing.Core
{
    public class Span : IComparable<Span>
    {
        public long traceId;
        public string name;
        public long id;
        public long? parentId;
        public long? timestamp;
        public long? duration;
        public readonly IList<Annotation> annotations;
        public readonly IList<BinaryAnnotation> binaryAnnotations;
        public bool? debug;

        Span(long traceId,
           string name,
           long id,
           long? parentId,
           long? timestamp,
           long? duration,
           Collection<Annotation> annotations,
           Collection<BinaryAnnotation> binaryAnnotations,
           bool? debug)
        {
            this.traceId = traceId;
            this.name = name.ToLower(); //checkNotNull(name, "name").toLowerCase();
            this.id = id;
            this.parentId = parentId;
            this.timestamp = timestamp;
            this.duration = duration;
            this.annotations = Util.SortedList(annotations);
            this.binaryAnnotations = new ReadOnlyCollection<BinaryAnnotation>(binaryAnnotations);
            this.debug = debug;
        }

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is Span)
            {
                Span that = (Span)o;
                return (this.traceId == that.traceId)
                    && (this.name.Equals(that.name))
                    && (this.id == that.id)
                    && Util.Equal(this.parentId, that.parentId)
                    && Util.Equal(this.timestamp, that.timestamp)
                    && Util.Equal(this.duration, that.duration)
                    && (this.annotations.Equals(that.annotations))
                    && (this.binaryAnnotations.Equals(that.binaryAnnotations))
                    && Util.Equal(this.debug, that.debug);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int h = 1;
            h *= 1000003;
            h ^= (int)((traceId >> 32) ^ traceId);
            h *= 1000003;
            h ^= name.GetHashCode();
            h *= 1000003;
            h ^= (int)((id >> 32) ^ id);
            h *= 1000003;
            h ^= (parentId == null) ? 0 : parentId.GetHashCode();
            h *= 1000003;
            h ^= (timestamp == null) ? 0 : timestamp.GetHashCode();
            h *= 1000003;
            h ^= (duration == null) ? 0 : duration.GetHashCode();
            h *= 1000003;
            h ^= annotations.GetHashCode();
            h *= 1000003;
            h ^= binaryAnnotations.GetHashCode();
            h *= 1000003;
            h ^= (debug == null) ? 0 : debug.GetHashCode();
            return h;
        }

        public int CompareTo(Span that)
        {
            if (this == that) return 0;
            int byTimestamp =
                (this.timestamp.HasValue ? this.timestamp.Value : long.MinValue)
                .CompareTo(that.timestamp.HasValue ? that.timestamp : long.MinValue);
            if (byTimestamp != 0) return byTimestamp;
            return this.name.CompareTo(that.name);
        }
    }
}
