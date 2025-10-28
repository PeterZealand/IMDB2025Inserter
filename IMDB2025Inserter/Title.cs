using Microsoft.Data.SqlClient;
using System.Text;

namespace IMDB2025Inserter {
    public class Title {
        // string connectionString = "Server=localhost;Database=IMDB;" +
        //     "integrated security=True;TrustServerCertificate=True;";
        public int Id { get; set; }
        public int TypeId { get; set; }
        public string? PrimaryTitle { get; set; }
        public string? OriginalTitle { get; set; }
        public bool IsAdult { get; set; }
        public int? StartYear { get; set; }
        public int? EndYear { get; set; }
        public int? RuntimeMinutes { get; set; }
        public List<string> Genres { get; set; }

        public Title() {
            Genres = new List<string>();
        }

        public string ToSQL(SqlConnection sqlConn, SqlTransaction sqlTrans) {
            // SqlConnection sqlConn = new SqlConnection(connectionString);
            // sqlConn.Open();
            //
            // SqlTransaction sqlTrans = sqlConn.BeginTransaction();
            StringBuilder sb = new StringBuilder();

            int? genreId = 0;
            List<int> genreRes = new();

            int maxId = 0;

            SqlCommand cmd = new SqlCommand($"select max(id) from genres",sqlConn,sqlTrans);
            maxId = Convert.ToInt32(cmd.ExecuteScalar());

            foreach(string s in Genres){
                cmd = new SqlCommand(
                        $"select id from genres where genre = '{s}'",sqlConn,sqlTrans);

                genreId = Convert.ToInt32(cmd.ExecuteScalar());

                if(genreId == 0){
                    cmd = new SqlCommand("SET IDENTITY_INSERT Genres ON;", sqlConn, sqlTrans);
                    cmd.ExecuteNonQuery();

                    cmd = new SqlCommand(
                            $"insert into genres (id,genre) values({++maxId},'{s}')",sqlConn,sqlTrans);
                    cmd.ExecuteNonQuery();

                    cmd = new SqlCommand("SET IDENTITY_INSERT Genres OFF;", sqlConn, sqlTrans);
                    cmd.ExecuteNonQuery();

                    cmd = new SqlCommand(
                            $"select id from genres where genre = '{s}'",sqlConn,sqlTrans);
                    genreId = Convert.ToInt32(cmd.ExecuteScalar());
                    genreRes.Add((int)genreId);
                }
                else{
                    genreRes.Add((int)genreId);
                }
            }

            sqlConn.Close();

            sb.Append($"INSERT INTO Titles (Id, TypeId, PrimaryTitle, " +
                $"OriginalTitle, IsAdult, StartYear, EndYear, RuntimeMinutes) " +
                $"VALUES (");
            sb.Append($"{Id}, ");
            sb.Append($"{TypeId}, ");
            sb.Append($"'{PrimaryTitle?.Replace("'", "''")}', ");
            sb.Append(OriginalTitle != null ? $"'{OriginalTitle.Replace("'", "''")}', " : "NULL, ");
            sb.Append($"{(IsAdult ? 1 : 0)}, ");
            sb.Append(StartYear.HasValue ? $"{StartYear.Value}, " : "NULL, ");
            sb.Append(EndYear.HasValue ? $"{EndYear.Value}, " : "NULL, ");
            sb.Append(RuntimeMinutes.HasValue ? $"{RuntimeMinutes.Value}" : "NULL");
            sb.Append(");");

            foreach (int genreid in genreRes) {
               sb.AppendLine();
               sb.Append($"INSERT INTO TitleGenres (TitleId, GenreId) VALUES ({Id}, {genreid})");
            }
            // Console.WriteLine(sb.ToString());
            return sb.ToString();
        }

        private int GetGenreId(){
            return 0;
        }

        public override string? ToString() {
            return $"{Id} - {TypeId} - {PrimaryTitle} - {OriginalTitle} - {IsAdult} - {StartYear} - {EndYear} - {RuntimeMinutes}";
        }
    }
}
