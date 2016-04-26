using System;
using System.Text;

namespace Zipkin.Json
{
    public class JsonBinaryAnnotation
    {
        public string key { get; set; }
        public object value { get; set; }
        public string annotationType { get; set; }
        public JsonEndpoint endpoint { get; set; }

        public JsonBinaryAnnotation()
        {
        }

        public JsonBinaryAnnotation(BinaryAnnotation ba)
        {
            key = ba.key;
            annotationType = ba.type.ToString();
            endpoint = new JsonEndpoint(ba.endpoint);
            switch(ba.type)
            {
                case AnnotationType.BOOL: value = BitConverter.ToBoolean(ba.value, 0); break;
                case AnnotationType.BYTES: value = Convert.ToBase64String(ba.value); break;
                case AnnotationType.I16: value = BitConverter.ToInt16(ba.value, 0); break;
                case AnnotationType.I32: value = BitConverter.ToInt32(ba.value, 0); break;
                case AnnotationType.I64: value = BitConverter.ToInt64(ba.value, 0); break;
                case AnnotationType.DOUBLE: value = BitConverter.ToDouble(ba.value, 0); break;
                case AnnotationType.STRING: value = Encoding.UTF8.GetString (ba.value); break;
                default: throw new Exception(string.Format("Unsupported annotation type: {0}", ba));
            }
        }

        public BinaryAnnotation Invert()
        {
            return new BinaryAnnotation(key, GetByteBuffer(), (AnnotationType)Enum.Parse(typeof(AnnotationType), annotationType), endpoint.Invert());
        }

        private byte[] GetByteBuffer()
        {
            try
            {
                switch ((AnnotationType)Enum.Parse(typeof(AnnotationType), annotationType))
                {
                    case AnnotationType.BOOL: return BitConverter.GetBytes((bool)value);
                    case AnnotationType.BYTES: return Convert.FromBase64String((string)value);
                    case AnnotationType.I16: return BitConverter.GetBytes((Int16)value);
                    case AnnotationType.I32: return BitConverter.GetBytes((Int32)value);
                    case AnnotationType.I64: return BitConverter.GetBytes((Int64)value);
                    case AnnotationType.DOUBLE: return BitConverter.GetBytes((double)value);
                    case AnnotationType.STRING: return Encoding.UTF8.GetBytes(value.ToString());
                    default: throw new Exception(string.Format("Unsupported annotation type: {0}", this));
                }
            }
            catch (Exception e)
            {
                throw new Exception(string.Format("Error parsing json binary annotation: {0}", e));
            }
        }

    }
}
