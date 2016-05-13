using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Zipkin.Internal;

namespace Zipkin
{
    public sealed class ThriftCodec : Codec
    {
        // break vs decode huge structs, like > 1MB strings or 10k spans in a trace.
        const int STRING_LENGTH_LIMIT = 1 * 1024 * 1024;
        const int CONTAINER_LENGTH_LIMIT = 10 * 1000;
        // break vs recursing infinitely when Skipping data
        static int MAX_SKIP_DEPTH = 2147483647;

        // taken from org.apache.thrift.protocol.TType
        const byte TYPE_STOP = 0;
        const byte TYPE_BOOL = 2;
        const byte TYPE_BYTE = 3;
        const byte TYPE_DOUBLE = 4;
        const byte TYPE_I16 = 6;
        const byte TYPE_I32 = 8;
        const byte TYPE_I64 = 10;
        const byte TYPE_STRING = 11;
        const byte TYPE_STRUCT = 12;
        const byte TYPE_MAP = 13;
        const byte TYPE_SET = 14;
        const byte TYPE_LIST = 15;

        public Span ReadSpan(BufferReader bytes)
        {
            return Read(SPAN_ADAPTER, bytes);
        }

        public override Span ReadSpan(byte[] bytes)
        {
            using (MemoryStream stream = new MemoryStream(bytes))
            using (BufferReader buffer = new BufferReader(stream))
            {
                return Read(SPAN_ADAPTER, buffer);
            }
        }

        public override byte[] WriteSpan(Span value)
        {
            return Write(SPAN_ADAPTER, value);
        }

        public override List<Span> ReadSpans(byte[] bytes)
        {
            using (MemoryStream stream = new MemoryStream(bytes))
            using (BufferReader buffer = new BufferReader(stream))
            {
                return Read(SPANS_ADAPTER, buffer);
            }
        }

        public override byte[] WriteSpans(List<Span> value)
        {
            return Write(SPANS_ADAPTER, value);
        }

        public override byte[] WriteTraces(List<List<Span>> value)
        {
            return Write(TRACES_ADAPTER, value);
        }

        public List<DependencyLink> ReadDependencyLinks(BufferReader bytes)
        {
            return Read(DEPENDENCY_LINKS_ADAPTER, bytes);
        }

        public override List<DependencyLink> ReadDependencyLinks(byte[] bytes)
        {
            using (MemoryStream stream = new MemoryStream(bytes))
            using (BufferReader buffer = new BufferReader(stream))
            {
                return Read(DEPENDENCY_LINKS_ADAPTER, buffer);
            }
        }

        public override byte[] WriteDependencyLinks(List<DependencyLink> value)
        {
            return Write(DEPENDENCY_LINKS_ADAPTER, value);
        }


        static IThriftAdapter<Endpoint> ENDPOINT_ADAPTER = new EndpointThriftAdapter();

        class EndpointThriftAdapter : IThriftAdapter<Endpoint>
        {
            readonly Field IPV4 = new Field(TYPE_I32, 1);
            readonly Field PORT = new Field(TYPE_I16, 2);
            readonly Field SERVICE_NAME = new Field(TYPE_STRING, 3);

            public Endpoint Read(BufferReader bytes)
            {
                Endpoint endpoint = new Endpoint();
                Field field;

                while (true)
                {
                    field = Field.Read(bytes);
                    if (field.Type == TYPE_STOP) break;

                    if (field.Equals(IPV4))
                    {
                        endpoint.ipv4 = bytes.ReadInt32();
                    }
                    else if (field.Equals(PORT))
                    {
                        endpoint.port = bytes.ReadInt16();
                    }
                    else if (field.Equals(SERVICE_NAME))
                    {
                        endpoint.serviceName = ReadUtf8(bytes);
                    }
                    else
                    {
                        Skip(bytes, field.Type);
                    }
                }
                return endpoint;
            }

            public void Write(Endpoint value, BufferWriter buffer)
            {
                IPV4.Write(buffer);
                buffer.Write(value.ipv4);

                PORT.Write(buffer);
                buffer.Write(value.port);

                SERVICE_NAME.Write(buffer);
                WriteUtf8(buffer, value.serviceName);

                buffer.Write(TYPE_STOP);
            }
        };

        static IThriftAdapter<Annotation> ANNOTATION_ADAPTER = new AnnotationThriftAdapter();
        class AnnotationThriftAdapter : IThriftAdapter<Annotation>
        {

            readonly Field TIMESTAMP = new Field(TYPE_I64, 1);
            readonly Field VALUE = new Field(TYPE_STRING, 2);
            readonly Field ENDPOINT = new Field(TYPE_STRUCT, 3);

            public Annotation Read(BufferReader bytes)
            {
                Annotation anno = new Annotation();
                Field field;
                while (true)
                {
                    field = Field.Read(bytes);
                    if (field.Type == TYPE_STOP) break;

                    if (field.Equals(TIMESTAMP))
                    {
                        anno.timestamp = bytes.ReadInt64();
                    }
                    else if (field.Equals(VALUE))
                    {
                        anno.value = ReadUtf8(bytes);
                    }
                    else if (field.Equals(ENDPOINT))
                    {
                        anno.endpoint = ENDPOINT_ADAPTER.Read(bytes);
                    }
                    else
                    {
                        Skip(bytes, field.Type);
                    }
                }
                return anno;
            }

            public void Write(Annotation value, BufferWriter buffer)
            {
                TIMESTAMP.Write(buffer);
                buffer.Write(value.timestamp);

                if (value.value != null)
                {
                    VALUE.Write(buffer);
                    WriteUtf8(buffer, value.value);
                }

                if (value.endpoint != null)
                {
                    ENDPOINT.Write(buffer);
                    ENDPOINT_ADAPTER.Write(value.endpoint, buffer);
                }
                buffer.Write(TYPE_STOP);
            }
        };

        static IThriftAdapter<BinaryAnnotation> BINARY_ANNOTATION_ADAPTER = new BinaryAnnotationThriftAdapter();
        class BinaryAnnotationThriftAdapter : IThriftAdapter<BinaryAnnotation>
        {
            readonly Field KEY = new Field(TYPE_STRING, 1);
            readonly Field VALUE = new Field(TYPE_STRING, 2);
            readonly Field TYPE = new Field(TYPE_I32, 3);
            readonly Field ENDPOINT = new Field(TYPE_STRUCT, 4);

            public BinaryAnnotation Read(BufferReader bytes)
            {
                BinaryAnnotation ba = new BinaryAnnotation();
                Field field;

                while (true)
                {
                    field = Field.Read(bytes);
                    if (field.Type == TYPE_STOP) break;

                    if (field.Equals(KEY))
                    {
                        ba.key = ReadUtf8(bytes);
                    }
                    else if (field.Equals(VALUE))
                    {
                        ba.value = ReadByteArray(bytes);
                    }
                    else if (field.Equals(TYPE))
                    {
                        ba.type = (AnnotationType)bytes.ReadInt32();
                    }
                    else if (field.Equals(ENDPOINT))
                    {
                        ba.endpoint = ENDPOINT_ADAPTER.Read(bytes);
                    }
                    else
                    {
                        Skip(bytes, field.Type);
                    }
                }
                return ba;
            }

            public void Write(BinaryAnnotation value, BufferWriter buffer)
            {
                KEY.Write(buffer);
                WriteUtf8(buffer, value.key);

                VALUE.Write(buffer);
                buffer.Write(value.value.Length);
                buffer.Write(value.value);

                TYPE.Write(buffer);
                buffer.Write((int)value.type);

                if (value.endpoint != null)
                {
                    ENDPOINT.Write(buffer);
                    ENDPOINT_ADAPTER.Write(value.endpoint, buffer);
                }

                buffer.Write(TYPE_STOP);
            }
        };


        static IThriftAdapter<List<Annotation>> ANNOTATIONS_ADAPTER = new ListAdapter<Annotation>(ANNOTATION_ADAPTER);
        static IThriftAdapter<List<BinaryAnnotation>> BINARY_ANNOTATIONS_ADAPTER = new ListAdapter<BinaryAnnotation>(BINARY_ANNOTATION_ADAPTER);

        static IThriftAdapter<Span> SPAN_ADAPTER = new SpanThriftAdapter();
        static IThriftAdapter<List<Span>> SPANS_ADAPTER = new ListAdapter<Span>(SPAN_ADAPTER);
        static IThriftAdapter<List<List<Span>>> TRACES_ADAPTER = new ListAdapter<List<Span>>(SPANS_ADAPTER);
        class SpanThriftAdapter : IThriftAdapter<Span>
        {
            readonly Field TRACE_ID = new Field(TYPE_I64, 1);
            readonly Field NAME = new Field(TYPE_STRING, 3);
            readonly Field ID = new Field(TYPE_I64, 4);
            readonly Field PARENT_ID = new Field(TYPE_I64, 5);
            readonly Field ANNOTATIONS = new Field(TYPE_LIST, 6);
            readonly Field BINARY_ANNOTATIONS = new Field(TYPE_LIST, 8);
            readonly Field DEBUG = new Field(TYPE_BOOL, 9);
            readonly Field TIMESTAMP = new Field(TYPE_I64, 10);
            readonly Field DURATION = new Field(TYPE_I64, 11);

            public Span Read(BufferReader bytes)
            {
                var builder = Span.NewBuilder();
                Field field;

                while (true)
                {
                    field = Field.Read(bytes);
                    if (field.Type == TYPE_STOP) break;

                    if (field.Equals(TRACE_ID))
                    {
                        builder.TraceId(bytes.ReadInt64());
                    }
                    else if (field.Equals(NAME))
                    {
                        builder.Name(ReadUtf8(bytes));
                    }
                    else if (field.Equals(ID))
                    {
                        builder.Id(bytes.ReadInt64());
                    }
                    else if (field.Equals(PARENT_ID))
                    {
                        builder.ParentId(bytes.ReadInt64());
                    }
                    else if (field.Equals(ANNOTATIONS))
                    {
                        builder.Annotations(ANNOTATIONS_ADAPTER.Read(bytes));
                    }
                    else if (field.Equals(BINARY_ANNOTATIONS))
                    {
                        builder.BinaryAnnotations(BINARY_ANNOTATIONS_ADAPTER.Read(bytes));
                    }
                    else if (field.Equals(DEBUG))
                    {
                        builder.Debug(bytes.ReadByte() == 1);
                    }
                    else if (field.Equals(TIMESTAMP))
                    {
                        builder.Timestamp(bytes.ReadInt64());
                    }
                    else if (field.Equals(DURATION))
                    {
                        builder.Duration(bytes.ReadInt64());
                    }
                    else
                    {
                        Skip(bytes, field.Type);
                    }
                }
                return builder.Build();
            }

            public void Write(Span value, BufferWriter buffer)
            {
                TRACE_ID.Write(buffer);
                buffer.Write(value.traceId);

                NAME.Write(buffer);
                WriteUtf8(buffer, value.name);

                ID.Write(buffer);
                buffer.Write(value.id);

                if (value.parentId != null)
                {
                    PARENT_ID.Write(buffer);
                    buffer.Write(value.parentId.Value);
                }

                ANNOTATIONS.Write(buffer);
                ANNOTATIONS_ADAPTER.Write(value.annotations, buffer);

                BINARY_ANNOTATIONS.Write(buffer);
                BINARY_ANNOTATIONS_ADAPTER.Write(value.binaryAnnotations, buffer);

                if (value.debug != null)
                {
                    DEBUG.Write(buffer);
                    buffer.Write(value.debug ?? false ? 1 : 0);
                }

                if (value.timestamp != null)
                {
                    TIMESTAMP.Write(buffer);
                    buffer.Write(value.timestamp.Value);
                }

                if (value.duration != null)
                {
                    DURATION.Write(buffer);
                    buffer.Write(value.duration.Value);
                }

                buffer.Write(TYPE_STOP);
            }

            public override string ToString()
            {
                return "Span";
            }
        };

        static IThriftAdapter<DependencyLink> DEPENDENCY_LINK_ADAPTER = new DependencyLinkThriftAdapter();
        static IThriftAdapter<List<DependencyLink>> DEPENDENCY_LINKS_ADAPTER = new ListAdapter<DependencyLink>(DEPENDENCY_LINK_ADAPTER);
        class DependencyLinkThriftAdapter : IThriftAdapter<DependencyLink>
        {

            readonly Field PARENT = new Field(TYPE_STRING, 1);
            readonly Field CHILD = new Field(TYPE_STRING, 2);
            readonly Field CALL_COUNT = new Field(TYPE_I64, 4);

            public DependencyLink Read(BufferReader bytes)
            {
                DependencyLink dl = new DependencyLink();
                Field field;

                while (true)
                {
                    field = Field.Read(bytes);
                    if (field.Type == TYPE_STOP) break;

                    if (field.Equals(PARENT))
                    {
                        dl.parent = ReadUtf8(bytes);
                    }
                    else if (field.Equals(CHILD))
                    {
                        dl.child = ReadUtf8(bytes);
                    }
                    else if (field.Equals(CALL_COUNT))
                    {
                        dl.callCount = bytes.ReadInt64();
                    }
                    else
                    {
                        Skip(bytes, field.Type);
                    }
                }

                return dl;
            }

            public void Write(DependencyLink value, BufferWriter buffer)
            {
                PARENT.Write(buffer);
                WriteUtf8(buffer, value.parent);

                CHILD.Write(buffer);
                WriteUtf8(buffer, value.child);

                CALL_COUNT.Write(buffer);
                buffer.Write(value.callCount);

                buffer.Write(TYPE_STOP);
            }

            public override string ToString()
            {
                return "DependencyLink";
            }
        };


        interface IThriftWriter<T>
        {
            void Write(T value, BufferWriter buffer);
        }

        interface IThriftReader<T>
        {
            T Read(BufferReader bytes);
        }

        interface IThriftAdapter<T> : IThriftReader<T>, IThriftWriter<T>
        {
        }


        static T Read<T>(IThriftReader<T> Reader, BufferReader bytes)
        {
            if (!bytes.BaseStream.CanRead)
            {
                throw new ArgumentException(string.Format("Empty input Reading {0}", Reader));
            }
            try
            {
                return Reader.Read(bytes);
            }
            catch (Exception e)
            {
                throw ExceptionReading(Reader.ToString(), bytes, e);
            }
        }

        /** Inability to encode is a programming bug. */
        static byte[] Write<T>(IThriftWriter<T> Writer, T value)
        {
            MemoryStream stream = null;
            byte[] bytes;
            try
            {
                stream = new MemoryStream();
                BufferWriter buffer = new BufferWriter(stream);
                Writer.Write(value, buffer);
                bytes = stream.ToArray();
            }
            catch (Exception e)
            {
                bytes = new byte[0];
                //throw new AssertionError("Could not Write " + value + " as TBinary", e);
            }
            finally
            {
                if (stream != null)
                {
                    stream.Dispose();
                }
            }
            return bytes;
        }

        static List<T> ReadList<T>(IThriftReader<T> Reader, BufferReader bytes)
        {
            byte ignoredType = bytes.ReadByte();
            int length = GuardLength(bytes, CONTAINER_LENGTH_LIMIT);
            List<T> result = new List<T>(length);
            for (int i = 0; i < length; i++)
            {
                result.Add(Reader.Read(bytes));
            }
            return result;
        }

        static void WriteList<T>(IThriftWriter<T> Writer, List<T> value, BufferWriter buffer)
        {
            WriteListBegin(buffer, value.Count);
            for (int i = 0, length = value.Count; i < length; i++)
            {
                Writer.Write(value[i], buffer);
            }
        }

        sealed class ListAdapter<T> : IThriftAdapter<List<T>>
        {
            readonly IThriftAdapter<T> adapter;

            public ListAdapter(IThriftAdapter<T> adapter)
            {
                this.adapter = adapter;
            }

            public List<T> Read(BufferReader bytes)
            {
                return ReadList(adapter, bytes);
            }

            public void Write(List<T> value, BufferWriter buffer)
            {
                WriteList(adapter, value, buffer);
            }

            public string ToString()
            {
                return "List<" + adapter + ">";
            }
        }

        static ArgumentException ExceptionReading(string type, BufferReader bytes, Exception e)
        {
            string cause = e.Message == null ? "Error" : e.Message;
            string message = String.Format("{0} Reading {1} from BufferReader: {2}", cause, type, bytes);
            throw new ArgumentException(message, e);
        }


        private class Field
        {
            private readonly byte type;
            private readonly int id;

            public byte Type { get { return type; } }
            public int Id { get { return id; } }

            public Field(byte type, int id)
            {
                this.type = type;
                this.id = id;
            }

            public void Write(BufferWriter buffer)
            {
                buffer.Write(type);
                buffer.Write((Int16)id);
            }

            public static Field Read(BufferReader buffer)
            {
                byte type = buffer.ReadByte();
                return new Field(type, type == TYPE_STOP ? TYPE_STOP : buffer.ReadInt16());
            }

            public bool Equals(Field that)
            {
                return this.type == that.type && this.id == that.id;
            }
        }

        static void Skip(BufferReader bytes, byte type)
        {
            Skip(bytes, type, MAX_SKIP_DEPTH);
        }

        static void Skip(BufferReader bytes, byte type, int maxDepth)
        {
            if (maxDepth <= 0) throw new InvalidOperationException("Maximum Skip depth exceeded");
            switch (type)
            {
                case TYPE_BOOL:
                case TYPE_BYTE:
                    Skip(bytes, 1);
                    break;
                case TYPE_I16:
                    Skip(bytes, 2);
                    break;
                case TYPE_I32:
                    Skip(bytes, 4);
                    break;
                case TYPE_DOUBLE:
                case TYPE_I64:
                    Skip(bytes, 8);
                    break;
                case TYPE_STRING:
                    int size = GuardLength(bytes, STRING_LENGTH_LIMIT);
                    Skip(bytes, size);
                    break;
                case TYPE_STRUCT:
                    while (true)
                    {
                        Field field = Field.Read(bytes);
                        if (field.Type == TYPE_STOP) return;
                        Skip(bytes, field.Type, maxDepth - 1);
                    }
                case TYPE_MAP:
                    byte keyType = bytes.ReadByte();
                    byte valueType = bytes.ReadByte();
                    for (int i = 0, length = GuardLength(bytes, CONTAINER_LENGTH_LIMIT); i < length; i++)
                    {
                        Skip(bytes, keyType, maxDepth - 1);
                        Skip(bytes, valueType, maxDepth - 1);
                    }
                    break;
                case TYPE_SET:
                case TYPE_LIST:
                    byte elemType = bytes.ReadByte();
                    for (int i = 0, length = GuardLength(bytes, CONTAINER_LENGTH_LIMIT); i < length; i++)
                    {
                        Skip(bytes, elemType, maxDepth - 1);
                    }
                    break;
                default: // types that don't need explicit Skipping
                    break;
            }
        }

        static void Skip(BufferReader bytes, int count)
        {
            bytes.BaseStream.Seek(8 * count, SeekOrigin.Current);
        }

        static byte[] ReadByteArray(BufferReader bytes)
        {
            return bytes.ReadBytes(GuardLength(bytes, STRING_LENGTH_LIMIT));
        }

        static string ReadUtf8(BufferReader bytes)
        {
            return Encoding.UTF8.GetString(ReadByteArray(bytes));
        }

        static int GuardLength(BufferReader bytes, int limit)
        {
            int length = bytes.ReadInt32();
            if (length > limit)
            { // don't allocate massive arrays
                throw new InvalidOperationException(length + " > " + limit + ": possibly malformed thrift");
            }
            return length;
        }

        static void WriteListBegin(BufferWriter buffer, int size)
        {
            buffer.Write(TYPE_STRUCT);
            buffer.Write(size);
        }

        static void WriteUtf8(BufferWriter buffer, string str)
        {
            var temp = Encoding.UTF8.GetBytes(str);
            buffer.Write(temp.Length);
            buffer.Write(temp);
        }
    }
}
