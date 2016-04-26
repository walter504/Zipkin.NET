using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Zipkin.Internal;

namespace Zipkin
{
    public class Span : IComparable<Span>
    {
        public long traceId { get; set; }
        public string name { get; set; }
        public long id { get; set; }
        public long? parentId { get; set; }
        public long? timestamp { get; set; }
        public long? duration { get; set; }
        public List<Annotation> annotations { get; set; }
        public List<BinaryAnnotation> binaryAnnotations { get; set; }
        public bool? debug { get; set; }

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
                return this.Endpoints.Where(e => !string.IsNullOrEmpty(e.serviceName)).Select(e => e.serviceName);
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
                return this.annotations.Select(a => a.endpoint).Concat(this.binaryAnnotations.Select(ba => ba.endpoint));
            } 
        }

        public Span()
        {
        }

        public Span(long traceId,
            string name,
            long id,
            long? parentId = null,
            long? timestamp = null,
            long? duration = null,
            IList<Annotation> annotations = null,
            IList<BinaryAnnotation> binaryAnnotations = null,
            bool? debug = null)
        {
            this.traceId = traceId;
            this.name = name.ToLower(); //checkNotNull(name, "name").toLowerCase();
            this.id = id;
            this.parentId = parentId;
            this.timestamp = timestamp;
            this.duration = duration;
            this.annotations = annotations == null ? new List<Annotation>() : Util.SortedList(annotations);
            this.binaryAnnotations = binaryAnnotations == null ? new List<BinaryAnnotation>() : new ReadOnlyCollection<BinaryAnnotation>(binaryAnnotations).ToList();
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
            int byTimestamp = (this.timestamp ?? long.MinValue).CompareTo(that.timestamp ?? long.MinValue);
            if (byTimestamp != 0) return byTimestamp;
            return this.name.CompareTo(that.name);
        }
    }
}
