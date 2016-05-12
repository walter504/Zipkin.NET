using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Binder;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Zipkin.WebApi
{
    public static class ConfigurationExtensions
    {
        public static ZipkinCassandraProperties GetZipkinCassandraProperties(this IConfiguration config)
        {
            var props = new ZipkinCassandraProperties();
            var section = config.GetSection("cassandra");
            if (section != null)
            {
                props.Keyspace = section.Get<string>("keyspace", props.Keyspace);
                props.ContactPoints = section.Get<string>("contactPoints", props.ContactPoints);
                props.LocalDc = section.Get<string>("localDc", props.LocalDc);
                props.MaxConnections = section.Get<int>("maxConnections", props.MaxConnections);
                props.EnsureSchema = section.Get<bool>("pnsureSchema", props.EnsureSchema);
                props.Username = section.Get<string>("psername", props.Username);
                props.Password = section.Get<string>("password", props.Password);
                props.MaxTraceCols = section.Get<int>("maxTraceCols", props.MaxTraceCols);
                props.BucketCount = section.Get<int>("bucketCount", props.BucketCount);
                props.SpanTtl = section.Get<int>("spanTtl", props.SpanTtl);
                props.IndexTtl = section.Get<int>("indexTtl", props.IndexTtl);
            }
            return props;
        }
    }
}