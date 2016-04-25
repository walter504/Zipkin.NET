using System;
using System.IO;

namespace Zipkin.Core.Internal
{
    public class BufferWriter : BinaryWriter
    {
        private readonly bool isBigEndian;

        public BufferWriter(Stream input, bool isBigEndian = true)
            :base(input)
        {
            this.isBigEndian = isBigEndian;
        }

        public override void Write(short value)
        {
            if (isBigEndian)
            {
                byte[] bytes = new byte[2];
                bytes[0] = (byte)(0xff & (value >> 8));
                bytes[1] = (byte)(0xff & value);
                base.Write(bytes);
            }
            else
            {
                base.Write(value);
            }
        }

        public override void Write(int value)
        {
            if (isBigEndian)
            {
                byte[] bytes = new byte[4];
                bytes[0] = (byte)(0xff & (value >> 24));
                bytes[1] = (byte)(0xff & (value >> 16));
                bytes[2] = (byte)(0xff & (value >> 8));
                bytes[3] = (byte)(0xff & value);
                base.Write(bytes);
            }
            else
            {
                base.Write(value);
            }
        }

        public override void Write(long value)
        {
            if (isBigEndian)
            {
                byte[] bytes = new byte[8];
                bytes[0] = (byte)(0xff & (value >> 56));
                bytes[1] = (byte)(0xff & (value >> 48));
                bytes[2] = (byte)(0xff & (value >> 40));
                bytes[3] = (byte)(0xff & (value >> 32));
                bytes[4] = (byte)(0xff & (value >> 24));
                bytes[5] = (byte)(0xff & (value >> 16));
                bytes[6] = (byte)(0xff & (value >> 8));
                bytes[7] = (byte)(0xff & value);
                base.Write(bytes);
            }
            else
            {
                base.Write(value);
            }
        }
    }
}
