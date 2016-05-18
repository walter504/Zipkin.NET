using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Zipkin.Internal;

namespace Zipkin
{
    public class Span : IComparable<Span>
    {
        public long traceId { get; private set; }
        public string name { get; private set; }
        public long id { get; private set; }
        public long? parentId { get; private set; }
        public long? timestamp { get; private set; }
        public long? duration { get; private set; }
        public List<Annotation> annotations { get; private set; }
        public List<BinaryAnnotation> binaryAnnotations { get; private set; }
        public bool? debug { get; private set; }

        private Span(Builder builder)
        {
            this.traceId = builder.traceId;
            this.name = Ensure.ArgumentNotNull(builder.name, "name").ToLower();
            this.id = builder.id;
            this.parentId = builder.parentId;
            this.timestamp = builder.timestamp;
            this.duration = builder.duration;
            this.annotations = Util.SortedList(builder.annotations);
            this.binaryAnnotations = Util.SortedList(builder.binaryAnnotations);
            this.debug = builder.debug;
        }

        public Builder ToBuilder()
        {
            return new Builder(this);
        }

        public static Builder NewBuilder()
        {
            return new Builder();
        }

        public sealed class Builder
        {
            internal long traceId;
            internal string name;
            internal long id;
            internal long? parentId;
            internal long? timestamp;
            internal long? duration;
            internal HashSet<Annotation> annotations;
            internal HashSet<BinaryAnnotation> binaryAnnotations;
            internal bool? debug;

            internal Builder()
            {
            }

            internal Builder(Span source)
            {
                this.traceId = source.traceId;
                this.name = source.name;
                this.id = source.id;
                this.parentId = source.parentId;
                this.timestamp = source.timestamp;
                this.duration = source.duration;
                if (source.annotations.Count != 0)
                {
                    this.annotations = new HashSet<Annotation>(source.annotations);
                }
                if (source.binaryAnnotations.Count != 0)
                {
                    this.binaryAnnotations = new HashSet<BinaryAnnotation>(source.binaryAnnotations);
                }
                this.debug = source.debug;
            }

            public Builder Merge(Span that)
            {
                if (string.IsNullOrEmpty(this.name) || this.name == "unknown")
                {
                    this.name = that.name;
                }
                if (this.parentId == null)
                {
                    this.parentId = that.parentId;
                }

                // Single timestamp makes duration easy: just choose max
                if (this.timestamp == null || that.timestamp == null || this.timestamp.Value == that.timestamp)
                {
                    this.timestamp = this.timestamp != null ? this.timestamp : that.timestamp;
                    if (this.duration == null)
                    {
                        this.duration = that.duration;
                    }
                    else if (that.duration != null)
                    {
                        this.duration = Math.Max(this.duration.Value, that.duration.Value);
                    }
                }
                else
                {
                    // duration might need to be recalculated, since we have 2 different timestamps
                    long thisEndTs = this.duration != null ? this.timestamp.Value + this.duration.Value : this.timestamp.Value;
                    long thatEndTs = that.duration != null ? that.timestamp.Value + that.duration.Value : that.timestamp.Value;
                    this.timestamp = Math.Min(this.timestamp.Value, that.timestamp.Value);
                    this.duration = Math.Max(thisEndTs, thatEndTs) - this.timestamp.Value;
                }

                foreach (Annotation a in that.annotations)
                {
                    AddAnnotation(a);
                }
                foreach (BinaryAnnotation a in that.binaryAnnotations)
                {
                    AddBinaryAnnotation(a);
                }
                if (this.debug == null)
                {
                    this.debug = that.debug;
                }
                return this;
            }

            /** @see Span#name */
            public Builder Name(String name)
            {
                this.name = name;
                return this;
            }

            /** @see Span#traceId */
            public Builder TraceId(long traceId)
            {
                this.traceId = traceId;
                return this;
            }

            /** @see Span#id */
            public Builder Id(long id)
            {
                this.id = id;
                return this;
            }

            /** @see Span#parentId */
            public Builder ParentId(long? parentId)
            {
                this.parentId = parentId;
                return this;
            }

            /** @see Span#timestamp */
            public Builder Timestamp(long? timestamp)
            {
                this.timestamp = timestamp;
                return this;
            }

            /** @see Span#duration */
            public Builder Duration(long? duration)
            {
                this.duration = duration;
                return this;
            }

            /**
             * Replaces currently collected annotations.
             *
             * @see Span#annotations
             */
            public Builder Annotations(IEnumerable<Annotation> annotations)
            {
                this.annotations = new HashSet<Annotation>(annotations);
                return this;
            }

            /** @see Span#annotations */
            public Builder AddAnnotation(Annotation annotation)
            {
                if (annotations == null)
                {
                    annotations = new HashSet<Annotation>();
                }
                annotations.Add(annotation);
                return this;
            }

            /**
             * Replaces currently collected binary annotations.
             *
             * @see Span#binaryAnnotations
             */
            public Builder BinaryAnnotations(IEnumerable<BinaryAnnotation> binaryAnnotations)
            {
                this.binaryAnnotations = new HashSet<BinaryAnnotation>(binaryAnnotations);
                return this;
            }

            /** @see Span#binaryAnnotations */
            public Builder AddBinaryAnnotation(BinaryAnnotation binaryAnnotation)
            {
                if (binaryAnnotations == null)
                {
                    binaryAnnotations = new HashSet<BinaryAnnotation>();
                }
                binaryAnnotations.Add(binaryAnnotation);
                return this;
            }

            /** @see Span#debug */
            public Builder Debug(bool? debug)
            {
                this.debug = debug;
                return this;
            }

            public Span Build()
            {
                return new Span(this);
            }
        }

        public IEnumerable<Annotation> clientSideAnnotations
        {
            get
            {
                return annotations.Where(a => Constants.CoreClient.Contains(a.value)).ToList();
            }
        }

        public IEnumerable<Annotation> serverSideAnnotations
        {
            get
            {
                return annotations.Where(a => Constants.CoreServer.Contains(a.value)).ToList();
            }
        }

        public IEnumerable<string> ServiceNames
        {
            get
            {
                return this.Endpoints.Where(e => !string.IsNullOrEmpty(e.serviceName)).Select(e => e.serviceName).Distinct();
            }
        }

        public string ServiceName
        {
            get
            {
                // Most authoritative is the label of the server's endpoint
                var name = binaryAnnotations.Where(ba => ba.key == Constants.ServerAddr).Select(ba => ba.serviceName).FirstOrDefault();
                if (string.IsNullOrEmpty(name))
                {
                    // Next, the label of any server annotation, logged by an instrumented server
                    name = serverSideAnnotations.Select(a => a.serviceName).FirstOrDefault();
                    if (string.IsNullOrEmpty(name))
                    {
                        // Next is the label of the client's endpoint
                        name = binaryAnnotations.Where(ba => ba.key == Constants.ClientAddr).Select(ba => ba.serviceName).FirstOrDefault();
                        if (string.IsNullOrEmpty(name))
                        {
                            // Next is the label of any client annotation, logged by an instrumented client
                            name = clientSideAnnotations.Select(a => a.serviceName).FirstOrDefault();
                            if (string.IsNullOrEmpty(name))
                            {
                                // Finally is the label of the local component's endpoint
                                name = binaryAnnotations.Where(ba => ba.key == Constants.LocalComponent).Select(ba => ba.serviceName).FirstOrDefault();
                            }
                        }
                    }
                }
                return name;
            }
        }

        public IEnumerable<Endpoint> Endpoints
        {
            get
            {
                return this.annotations.Select(a => a.endpoint).Concat(this.binaryAnnotations.Select(ba => ba.endpoint)).Distinct();
            }
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
            int byTimestamp = (this.timestamp ?? long.MinValue).CompareTo(that.timestamp ?? long.MinValue);
            if (byTimestamp != 0) return byTimestamp;
            return this.name.CompareTo(that.name);
        }
    }
}
