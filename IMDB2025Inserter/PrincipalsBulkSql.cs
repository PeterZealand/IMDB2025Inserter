using Microsoft.Data.SqlClient;
using System.Data;

namespace IMDB2025Inserter {
    public class PrincipalsBulkSql {
        public DataTable princibleDataTable { get; set; }

        public PrincipalsBulkSql() {
            princibleDataTable = new DataTable();
            princibleDataTable.Columns.Add("TitleId", typeof(int));
            princibleDataTable.Columns.Add("Ordering", typeof(int));
            princibleDataTable.Columns.Add("NameId", typeof(int));
            princibleDataTable.Columns.Add("Category", typeof(string));
            princibleDataTable.Columns.Add("Job", typeof(string));
            princibleDataTable.Columns.Add("Characters", typeof(string));
        }

        public void InsertPrincipals(Principals principals) {
            DataRow row = princibleDataTable.NewRow();
            row["TitleId"] = principals.TitleId;
            row["Ordering"] = principals.Ordering;
            row["NameId"] = principals.NameId;
            row["Category"] = principals.Category;
            row["Job"] = (object?)principals.Job ?? DBNull.Value;
            row["Characters"] = (object?)principals.Characters ?? DBNull.Value;
            princibleDataTable.Rows.Add(row);
        }

        public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans)) {
                bulkCopy.DestinationTableName = "Principals";
                bulkCopy.ColumnMappings.Add("TitleId", "TitleId");
                bulkCopy.ColumnMappings.Add("Ordering", "Ordering");
                bulkCopy.ColumnMappings.Add("NameId", "NameId");
                bulkCopy.ColumnMappings.Add("Category", "Category");
                bulkCopy.ColumnMappings.Add("Job", "Job");
                bulkCopy.ColumnMappings.Add("Characters", "Characters");
                bulkCopy.WriteToServer(princibleDataTable); 
            }
        }
    }
}
