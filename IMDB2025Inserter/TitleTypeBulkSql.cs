using Microsoft.Data.SqlClient;
using System.Data;
namespace IMDB2025Inserter;

public class TitleTypeBulkSql{
    private readonly Dictionary<string,int> genreIdCache = new();
    public DataTable TitleGenreDataTable { get; set; } 

    public TitleTypeBulkSql() {
        TitleGenreDataTable = new DataTable();
        TitleGenreDataTable.Columns.Add("TitleId", typeof(int));
        TitleGenreDataTable.Columns.Add("GenreId", typeof(int));
    }

    public void InsertTitleGenre(Title title,SqlConnection sqlConn,SqlTransaction sqlTrans) {
        DataRow? row = null;

        foreach(string s in title.Genres){
            row = TitleGenreDataTable.NewRow();
            row["TitleId"] = title.Id;
            row["GenreId"] = GetGenreId(s,sqlConn,sqlTrans);
            TitleGenreDataTable.Rows.Add(row);
        }
    }

    public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans) {
        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
            bulkCopy.DestinationTableName = "TitleGenres";
            bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
            bulkCopy.ColumnMappings.Add("GenreId", "GenreId");
            bulkCopy.WriteToServer(TitleGenreDataTable);
        }
    }

    public int GetGenreId(string genreName, SqlConnection sqlConn,SqlTransaction sqlTrans){
        if(genreIdCache.TryGetValue(genreName, out int cacheId)){
            return cacheId;
        }

        string cmd = $"select id from genres where genre = @genre";
        SqlCommand sqlComm = new(cmd,sqlConn,sqlTrans);
        sqlComm.Parameters.AddWithValue("@genre",genreName);
        int result = Convert.ToInt32(sqlComm.ExecuteScalar());
        genreIdCache[genreName] = result;
        return result;
    }
}
