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

        public Task<IEnumerable<DependencyLink>> GetDependencies(long endTs, long? lookback)
        {
            return Repo.GetDependencies(endTs, lookback);
        }
        public Task StoreDependencies(Dependencies dependencies)
        {
            return Task.FromResult(0);
        }
    }
}
