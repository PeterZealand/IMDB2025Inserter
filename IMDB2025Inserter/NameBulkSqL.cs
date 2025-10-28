using Microsoft.Data.SqlClient;
using System.Data;

namespace IMDB2025Inserter {
    public class NameBulkSqL {
        private Dictionary<string,int> namesProfessions = new();
        public DataTable NameDataTable { get; set; }
        public DataTable NamePrimaryProfessions {get;set;}
        public DataTable NameKnownForTable {get;set;}
        public DataTable ProfessionsTable {get;set;} 

        public NameBulkSqL() {
            NameDataTable = new DataTable();
            NamePrimaryProfessions = new();
            NameKnownForTable = new();
            ProfessionsTable = new();

            NameDataTable.Columns.Add("Id", typeof(int));
            NameDataTable.Columns.Add("PrimaryName", typeof(string));
            NameDataTable.Columns.Add("BirthYear", typeof(int));
            NameDataTable.Columns.Add("DeathYear", typeof(int));

            NamePrimaryProfessions.Columns.Add("NameId",typeof(int));
            NamePrimaryProfessions.Columns.Add("ProfessionId",typeof(int));

            NameKnownForTable.Columns.Add("NameId",typeof(int));
            NameKnownForTable.Columns.Add("TitleId",typeof(int));

            ProfessionsTable.Columns.Add("Professions",typeof(string));
        }

        public void InsertName(Name name) {
            DataRow row = NameDataTable.NewRow();
            row["Id"] = name.Id;
            row["PrimaryName"] = name.PrimaryName;
            row["BirthYear"] = (object?)name.BirthYear ?? DBNull.Value;
            row["DeathYear"] = (object?)name.DeathYear ?? DBNull.Value;
            NameDataTable.Rows.Add(row);
        }

        public void InsertProfessions(Name name){
            DataRow? row = null;

            foreach(string profession in name.PrimaryProfessions){
                row = ProfessionsTable.NewRow();
                row["Professions"] = profession;
                ProfessionsTable.Rows.Add(row);
            }
        }

        public void InsertNamesProfessions(Name name,SqlConnection sqlConn, SqlTransaction sqlTrans){
            DataRow? row = null;

            foreach(string profession in name.PrimaryProfessions){
                row = NamePrimaryProfessions.NewRow();
                row["NameId"] = name.Id;
                row["ProfessionId"] = GetProfessionId(profession,sqlConn,sqlTrans);
                NamePrimaryProfessions.Rows.Add(row);
            }
        }

        public void InsertKnownFor(Name name){
            DataRow? row = null;

            foreach(string title in name.KnownForTitles){
                int titleId = Int32.Parse(title[2..]);

                row = NameKnownForTable.NewRow();
                row["NameId"] = name.Id;
                row["TitleId"] = titleId;
                NameKnownForTable.Rows.Add(row);
            }
        }
         
        public void InsertNameIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "Names";
                bulkCopy.ColumnMappings.Add("Id", "Id");
                bulkCopy.ColumnMappings.Add("PrimaryName", "PrimaryName");
                bulkCopy.ColumnMappings.Add("BirthYear", "BirthYear");
                bulkCopy.ColumnMappings.Add("DeathYear", "DeathYear");
                bulkCopy.WriteToServer(NameDataTable);
            }
        }

        public void InsertIntoDBProfessions(SqlConnection sqlConn, SqlTransaction sqlTrans) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "Professions";
                bulkCopy.ColumnMappings.Add("Professions","Professions");
                bulkCopy.WriteToServer(ProfessionsTable);
            }
        }

        public void InsertIntoDBKnownFor(SqlConnection sqlConn, SqlTransaction sqlTrans){
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "NamesKnownFor";
                bulkCopy.ColumnMappings.Add("NameId","NameId");
                bulkCopy.ColumnMappings.Add("TitleId","TitleId");
                bulkCopy.WriteToServer(NameKnownForTable);
            }
        }

        public void InsertIntoDBNamesProfessions(SqlConnection sqlConn, SqlTransaction sqlTrans){
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "NamesProfessions";
                bulkCopy.ColumnMappings.Add("NameId","NameId");
                bulkCopy.ColumnMappings.Add("ProfessionId","ProfessionId");
                bulkCopy.WriteToServer(NamePrimaryProfessions);
            }
        }

        public int GetProfessionId(string profession, SqlConnection sqlConn,SqlTransaction sqlTrans){
            if(namesProfessions.TryGetValue(profession, out int cacheId)){
                return cacheId;
            }

            string cmd = $"select id from professions where profession = @profession";
            SqlCommand sqlComm = new(cmd,sqlConn,sqlTrans);
            sqlComm.Parameters.AddWithValue("@profession",profession);
            int result = Convert.ToInt32(sqlComm.ExecuteScalar());
            namesProfessions[profession] = result;
            return result;
        }
    }
}
