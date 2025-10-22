using IMDB2025Inserter;
using Microsoft.Data.SqlClient;
using System.Diagnostics;

string connectionString = "Server=localhost;Database=IMDB;integrated security=True;TrustServerCertificate=True;";
string titlesFile = "C:\\temp\\title.basics.tsv\\title.basics.tsv";
// string namesFile = "C:\\temp\\name.basics.tsv\\name.basics.tsv";
string principalsFile = "C:\\temp\\title.Principals.tsv\\title.Principals.tsv";

using var sqlConn = new SqlConnection(connectionString);
sqlConn.Open();

SqlTransaction sqlTrans = sqlConn.BeginTransaction();
SqlCommand cmd = new SqlCommand("SET IDENTITY_INSERT Titles ON;", sqlConn, sqlTrans);
cmd.ExecuteNonQuery();

BulkSql bulkSql = new();
PrincipalsBulkSql principalsBulkSql = new();

Dictionary<string, int> TitleTypes = new();
Dictionary<string, int> TitleGenres = new();

Stopwatch sw = Stopwatch.StartNew();

int batchSize = 25000;
int count = 0;

// Title();
Principals();

cmd = new SqlCommand("SET IDENTITY_INSERT Titles OFF;", sqlConn,sqlTrans);
cmd.ExecuteNonQuery();
sqlConn.Close();

Console.WriteLine($"Done. Inserted {count:N0} titles in {sw.Elapsed.TotalMinutes:F2} minutes.");

void Principals(){
    foreach(string line in File.ReadLines(principalsFile).Skip(1)){
        string[] values = line.Split("\t");
        if(values.Length != 6) continue;

        Principals principals = new(){
            TitleId = Int32.Parse(values[0][2..]),
            Ordering = Int32.Parse(values[1]),
            NameId = Int32.Parse(values[2][2..]),
            Category = values[3],
            Job = values[4] == "\\N" ? null : values[4],
            Characters = values[5] == "\\N" ? null : values[5]
        };

        principalsBulkSql.InsertPrincipals(principals);
        count++;

        if (count % batchSize == 0) {
            principalsBulkSql.InsertIntoDB(sqlConn, sqlTrans);
            principalsBulkSql.princibleDataTable.Clear();
            sqlTrans.Rollback();
            sqlTrans = sqlConn.BeginTransaction();
            Console.WriteLine($"{count:N0} principals inserted...");
        }
    }

    // Final batch
    if (principalsBulkSql.princibleDataTable.Rows.Count > 0) {
        principalsBulkSql.InsertIntoDB(sqlConn, sqlTrans);
        sqlTrans.Rollback();
        sqlTrans.Dispose();
    }
}

void Title(){
    foreach (string line in File.ReadLines(titlesFile).Skip(1)){
        string[] values = line.Split('\t');
        if (values.Length != 9) continue;

        if (!TitleTypes.ContainsKey(values[1]))
            AddTitleType(values[1], sqlConn, sqlTrans, TitleTypes);

        Title title = new Title {
            Id = int.Parse(values[0][2..]),
            TypeId = TitleTypes[values[1]],
            PrimaryTitle = values[2],
            OriginalTitle = values[3] == "\\N" ? null : values[3],
            IsAdult = values[4] == "1",
            StartYear = values[5] == "\\N" ? null : int.Parse(values[5]),
            EndYear = values[6] == "\\N" ? null : int.Parse(values[6]),
            RuntimeMinutes = values[7] == "\\N" ? null : int.Parse(values[7])
        };

        if(values[8] != "\\N"){
            string[] genres = values[8].Split(",");

            foreach(string ge in genres){
                string genre = ge.Trim();
                if(genre.Length == 0) continue;

                if(!TitleGenres.ContainsKey(genre)){
                    AddTitleGenre(genre, sqlConn, sqlTrans, TitleGenres);
                }

                title.Genres.Add(genre);
            }
        }

        bulkSql.InsertTitle(title);
        count++;

        if (count % batchSize == 0) {
            bulkSql.InsertIntoDB(sqlConn, sqlTrans);
            bulkSql.TitleDataTable.Clear();
            sqlTrans.Rollback();
            sqlTrans = sqlConn.BeginTransaction();
            Console.WriteLine($"{count:N0} titles inserted...");
        }
    }

    // Final batch
    if (bulkSql.TitleDataTable.Rows.Count > 0) {
        bulkSql.InsertIntoDB(sqlConn, sqlTrans);
        sqlTrans.Rollback();
        sqlTrans.Dispose();
    }
}

void AddTitleType(string titleType, SqlConnection conn, SqlTransaction trans, Dictionary<string, int> cache) {
    SqlCommand sqlComm = new("INSERT INTO TitleTypes (TypeName) VALUES (@name); SELECT SCOPE_IDENTITY();", conn, trans);
    sqlComm.Parameters.AddWithValue("@name", titleType);
    int newId = Convert.ToInt32(sqlComm.ExecuteScalar());
    cache[titleType] = newId;
}

void AddTitleGenre(string genre, SqlConnection conn, SqlTransaction trans, Dictionary<string, int> cache) {
    SqlCommand sqlComm = new("select id from genres where genre = @genre", conn,trans);
    sqlComm.Parameters.AddWithValue("@genre",genre);
    int result = Convert.ToInt32(sqlComm.ExecuteScalar());

    if(result != 0){
        cache[genre] = result;
        return;
    }

    sqlComm = new("insert into genres (genre) values (@genre); select SCOPE_IDENTITY();",conn,trans);
    sqlComm.Parameters.AddWithValue("@genre", genre);
    int newId = Convert.ToInt32(sqlComm.ExecuteScalar());
    cache[genre] = newId;
}
