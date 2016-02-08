using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Core
{
    public class DependencyLink
    {
        public readonly string parent;

        public readonly string child;

        public readonly long callCount;

        DependencyLink(string parent, string child, long callCount)
        {
            this.parent = Ensure.ArgumentNotNull(parent, "parent").ToLower();
            this.child = Ensure.ArgumentNotNull(child, "child").ToLower();
            this.callCount = callCount;
        }


        //public string tostring() {
        //  return JsonCodec.DEPENDENCY_LINK_ADAPTER.toJson(this);
        //}

        public override bool Equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (o is DependencyLink)
            {
                DependencyLink that = (DependencyLink)o;
                return (this.parent.Equals(that.parent))
                    && (this.child.Equals(that.child))
                    && (this.callCount == that.callCount);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int h = 1;
            h *= 1000003;
            h ^= parent.GetHashCode();
            h *= 1000003;
            h ^= child.GetHashCode();
            h *= 1000003;
            h ^= (int)((callCount >> 32) ^ callCount);
            return h;
        }
    }
}
