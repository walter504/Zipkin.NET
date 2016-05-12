using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zipkin.Storage.Cassandra
{
    public class CassandraDependencyStore : IDependencyStore
    {
        private readonly Lazy<Repository> lazyRepository;

        private Repository Repo
        {
            get
            {
                return lazyRepository.Value;
            }
        }

        public CassandraDependencyStore(Lazy<Repository> lazyRepository)
        {
            this.lazyRepository = lazyRepository;
        }

        public async Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback)
        {
            var links = (await Repo.GetDependencies(endTs, lookback)).ToList();
            return links.GroupBy(dl => new KeyValuePair<string, string>(dl.parent, dl.child)).Select(g => new DependencyLink()
            {
                parent = g.Key.Key,
                child = g.Key.Value,
                callCount = g.ToList().Sum(dl => dl.callCount)
            });
        }
        public virtual Task StoreDependencies(long epochDayMillis, IEnumerable<DependencyLink> links)
        {
            return Repo.StoreDependencies(epochDayMillis, Codec.THRIFT.WriteDependencyLinks(links.ToList()));
        }
    }
}
