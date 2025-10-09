using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class PreparedSql
    {
        public SqlCommand Sql { get; set; }
        
        public PreparedSql(SqlConnection sqlConn, SqlTransaction sqlTrans)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO Titles (Id, TypeId, PrimaryTitle, " +
                "OriginalTitle, IsAdult, StartYear, EndYear, RuntimeMinutes) " +
                "VALUES (");
            sb.Append("@Id, ");
            sb.Append("@TitleType, ");
            sb.Append("@PrimaryTitle, ");
            sb.Append("@OriginalTitle, ");
            sb.Append("@IsAdult, ");
            sb.Append("@StartYear, ");
            sb.Append("@EndYear, ");
            sb.Append("@RuntimeMinutes");
            sb.Append(");");

            Sql = new SqlCommand(sb.ToString(), sqlConn, sqlTrans);

            Sql.Parameters.Add(new SqlParameter("@Id", System.Data.SqlDbType.Int));
            Sql.Parameters.Add(new SqlParameter("@TitleType", System.Data.SqlDbType.Int));
            Sql.Parameters.Add(new SqlParameter("@PrimaryTitle", System.Data.SqlDbType.NVarChar, 255));
            Sql.Parameters.Add(new SqlParameter("@OriginalTitle", System.Data.SqlDbType.NVarChar, 255));
            Sql.Parameters.Add(new SqlParameter("@IsAdult", System.Data.SqlDbType.Bit));
            Sql.Parameters.Add(new SqlParameter("@StartYear", System.Data.SqlDbType.SmallInt));
            Sql.Parameters.Add(new SqlParameter("@EndYear", System.Data.SqlDbType.SmallInt));
            Sql.Parameters.Add(new SqlParameter("@RuntimeMinutes", System.Data.SqlDbType.Int));

            Sql.Prepare();
        }

        public void InsertTitle(Title title)
        {
            Sql.Parameters["@Id"].Value = title.Id;
            Sql.Parameters["@TitleType"].Value = title.TitleType;
            Sql.Parameters["@PrimaryTitle"].Value = title.PrimaryTitle;
            Sql.Parameters["@OriginalTitle"].Value = (object?)title.OriginalTitle ?? DBNull.Value;
            Sql.Parameters["@IsAdult"].Value = title.IsAdult;
            Sql.Parameters["@StartYear"].Value = (object?)title.StartYear ?? DBNull.Value;
            Sql.Parameters["@EndYear"].Value = (object?)title.EndYear ?? DBNull.Value;
            Sql.Parameters["@RuntimeMinutes"].Value = (object?)title.RuntimeMinutes ?? DBNull.Value;

            Sql.ExecuteNonQuery();
        }
    }
}
