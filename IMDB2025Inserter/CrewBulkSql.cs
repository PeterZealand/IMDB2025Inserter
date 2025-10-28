using Microsoft.Data.SqlClient;
using System.Data;

namespace IMDB2025Inserter {
    public class CrewBulkSql {
        public DataTable CrewDataTableDirectors { get; set; }
        public DataTable CrewDataTableWriters { get; set; }

        public CrewBulkSql() {
            CrewDataTableDirectors = new();
            CrewDataTableWriters = new DataTable();

            CrewDataTableDirectors.Columns.Add("TitleId",typeof(int));
            CrewDataTableDirectors.Columns.Add("NameId",typeof(int));

            CrewDataTableWriters.Columns.Add("TitleId", typeof(int));
            CrewDataTableWriters.Columns.Add("NameId", typeof(int));

        }

        public void InsertCrewDirectors(Crew crew) {
            DataRow? row = null;

            foreach(string s in crew.Directors){
                int nameId = Int32.Parse(s);

                row = CrewDataTableDirectors.NewRow();
                row["TitleId"] = crew.TitleId;
                row["NameId"] = nameId;
                CrewDataTableDirectors.Rows.Add(row);
            }
        }

        public void InsertCrewWriters(Crew crew) {
            DataRow? row = null;

            foreach(string s in crew.Writers){
                int nameId = Int32.Parse(s);

                row = CrewDataTableWriters.NewRow();
                row["TitleId"] = crew.TitleId;
                row["NameId"] = nameId;
                CrewDataTableWriters.Rows.Add(row);
            }
        }

        public void InsertIntoDBDirectors(SqlConnection sqlConn, SqlTransaction sqlTrans) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "CrewDirector";
                bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                bulkCopy.ColumnMappings.Add("NameId", "NameId");
                bulkCopy.WriteToServer(CrewDataTableDirectors);
            }
        }

        public void InsertIntoDBWriters(SqlConnection sqlConn, SqlTransaction sqlTrans) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "CrewWriter";
                bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                bulkCopy.ColumnMappings.Add("NameId", "NameId");
                bulkCopy.WriteToServer(CrewDataTableWriters);
            }
        }
    }
}
