using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class CrewBulkSql
    {
        public DataTable CrewDataTable { get; set; }
        public CrewBulkSql()
        {
            CrewDataTable = new DataTable();
            CrewDataTable.Columns.Add("Id", typeof(int));
        }
        public void InsertCrew(Crew crew)
        {
            DataRow row = CrewDataTable.NewRow();
            row["Id"] = crew.Id;
            CrewDataTable.Rows.Add(row);
        }
        public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans))
            {
                bulkCopy.DestinationTableName = "Crews";
                bulkCopy.ColumnMappings.Add("Id", "Id");
                bulkCopy.WriteToServer(CrewDataTable);
            }
        }
    }
}
