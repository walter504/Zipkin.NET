using System;
using System.IO;

namespace Zipkin.Core.Internal
{
    public class BufferReader : BinaryReader
    {
        private readonly bool isBigEndian;

        public BufferReader(Stream input, bool isBigEndian = true)
            :base(input)
        {
            this.isBigEndian = isBigEndian;
        }

        public override short ReadInt16()
        {
            if (isBigEndian)
            {
                var bytes = this.ReadBytes(2);
                unchecked
                {
                    return (short)(
                        ((short)(bytes[0] & 0xff) << 8) |
                        ((short)(bytes[1] & 0xff)));
                }
            }
            return base.ReadInt16();
        }

        public override int ReadInt32()
        {
            if (isBigEndian)
            {
                var bytes = this.ReadBytes(4);
                unchecked
                {
                    return (int)(
                        ((int)(bytes[0] & 0xff) << 24) |
                        ((int)(bytes[1] & 0xff) << 16) |
                        ((int)(bytes[2] & 0xff) << 8) |
                        ((int)(bytes[3] & 0xff)));
                }
            }
            return base.ReadInt32();
        }

        public override long ReadInt64()
        {
            if (isBigEndian)
            {
                var bytes = this.ReadBytes(8);
                unchecked
                {
                    return (long)(
                        ((long)(bytes[0] & 0xff) << 56) |
                        ((long)(bytes[1] & 0xff) << 48) |
                        ((long)(bytes[2] & 0xff) << 40) |
                        ((long)(bytes[3] & 0xff) << 32) |
                        ((long)(bytes[4] & 0xff) << 24) |
                        ((long)(bytes[5] & 0xff) << 16) |
                        ((long)(bytes[6] & 0xff) << 8) |
                        ((long)(bytes[7] & 0xff)));
                }
            }
            return base.ReadInt64();
        }
    }
}
