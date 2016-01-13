using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tracing.Core;
using Dapper;

namespace Tracing.DbStore
{
    public class DbSpanStore
    {
        public readonly string sqlConnectionString = "server=127.0.0.1;database=tracing;uid=root;pwd=123456;charset='gbk'";

        private SqlConnection OpenConnection()
        {
            SqlConnection conn = new SqlConnection(sqlConnectionString);
            conn.Open();
            return conn;
        }

        public void Accept(IEnumerable<Span> spans)
        {
            using (IDbConnection conn = OpenConnection())
            {
                
            }
        }
    }
              
}
