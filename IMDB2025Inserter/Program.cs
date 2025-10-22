using IMDB2025Inserter;
using Microsoft.Data.SqlClient;
using System.Diagnostics;

string connectionString = "Server=localhost;Database=IMDB;integrated security=True;TrustServerCertificate=True;";
string titlesFile = "C:\\temp\\title.basics.tsv";
string namesFile = "C:\\temp\\name.basics.tsv";
string principalsFile = "C:\\temp\\title.Principals.tsv";

using var sqlConn = new SqlConnection(connectionString);
sqlConn.Open();

SqlTransaction sqlTrans = sqlConn.BeginTransaction();
SqlCommand cmd = new SqlCommand("SET IDENTITY_INSERT Titles ON;", sqlConn, sqlTrans);
cmd.ExecuteNonQuery();

BulkSql bulkSql = new();
PrincipalsBulkSql principalsBulkSql = new();
NameBulkSqL namesSql = new();

//Titles
Dictionary<string, int> TitleTypes = new();
Dictionary<string, int> TitleGenres = new();

//Names
Dictionary<string,int> NamesPrimaryProfessions = new();
Dictionary<string,int> NamesKnownForTitles = new();
Dictionary<string,int> NamesKnownForNames = new();

Stopwatch sw = Stopwatch.StartNew();

int batchSize = 25000;
int count = 0;

// Title();
Principals();

cmd = new SqlCommand("SET IDENTITY_INSERT Titles OFF;", sqlConn,sqlTrans);
cmd.ExecuteNonQuery();

Names();

sqlConn.Close();

Console.WriteLine($"Done. Inserted {count:N0} titles in {sw.Elapsed.TotalMinutes:F2} minutes.");

void Names(){
    foreach(string line in File.ReadLines(namesFile).Skip(1)){
        string[] values = line.Split("\t");
        if(values.Length != 6) continue;
        // foreach(string s in values){
        //     Console.WriteLine(s);
        // }

        Name name = new(){
            Id = Int32.Parse(values[0][2..]),
            PrimaryName = values[1],
            BirthYear = values[2] == "\\N" ? null : Int32.Parse(values[2]),
            DeathYear = values[3] == "\\N" ? null : Int32.Parse(values[3])
        };

        namesSql.InsertName(name);
        count++;

        if(values[4] != "\\N"){
            string[] primaryProfessions = values[4].Split(",");

            foreach(string pro in primaryProfessions){
                string profession = pro.Trim();
                if(profession.Length == 0) continue;

                if(!NamesPrimaryProfessions.ContainsKey(profession)){
                    AddPrimaryProfessions(profession, sqlConn, sqlTrans, NamesPrimaryProfessions);
                }

                name.PrimaryProfessions.Add(pro);
            }
        }

        if(values[5] != "\\N"){
            string[] knownForSplit = values[5].Split(",");

            foreach(string s in knownForSplit){
                if(!NamesKnownForTitles.ContainsKey(s)){
                    AddKnownForNames(values[0][2..],s[2..],sqlConn,sqlTrans, NamesKnownForNames,NamesKnownForTitles);
                }
            }
        }

        if (count % batchSize == 0) {
            namesSql.InsertIntoDB(sqlConn, sqlTrans);
            namesSql.NameDataTable.Clear();
            sqlTrans.Rollback();
            sqlTrans = sqlConn.BeginTransaction();
            Console.WriteLine($"{count:N0} names inserted...");
        }
    }

    // Final batch
    if (namesSql.NameDataTable.Rows.Count > 0) {
        namesSql.InsertIntoDB(sqlConn, sqlTrans);
        sqlTrans.Rollback();
        sqlTrans.Dispose();
    }
}

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

void AddPrimaryProfessions(string profession, SqlConnection conn, SqlTransaction trans, Dictionary<string,int> cache){
    SqlCommand sqlComm = new("insert into professions (professions) values (@profession); select scope_identity();", conn,trans);
    sqlComm.Parameters.AddWithValue("@profession",profession);
    int res = Convert.ToInt32(sqlComm.ExecuteScalar());

    cache[profession] = res;
}

void AddKnownForNames(string nameid,string titleid, SqlConnection conn,SqlTransaction trans, Dictionary<string,int> namesCache,Dictionary<string,int> titleCache){
    SqlCommand sqlComm = new("insert into namesknownfor values (@nameid,@titleid); select scope_identity();",conn,trans);
    sqlComm.Parameters.AddWithValue("@nameid", nameid);
    sqlComm.Parameters.AddWithValue("@titleid", titleid);

    object result = sqlComm.ExecuteScalar();

    if(result != null && result != DBNull.Value){
        int res = Convert.ToInt32(sqlComm.ExecuteScalar());

        namesCache[nameid] = res;
        titleCache[titleid] = res;
    }

    // int resTitleId = Convert.ToInt32(sqlComm.ExecuteScalar());

    // sqlComm = new("insert into namesknownfor (titleid) values (@titleid); select scope_identity();",conn,trans);
    // sqlComm.Parameters.AddWithValue("@titleid", titleid);
}

// void AddKnownForTitles(string titleId, SqlConnection conn, SqlTransaction trans, Dictionary<string, int> cache){
// }
