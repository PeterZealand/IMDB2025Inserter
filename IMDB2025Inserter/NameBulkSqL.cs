using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class NameBulkSqL
    {
        public DataTable NameDataTable { get; set; }

        public NameBulkSqL()
        {
            NameDataTable = new DataTable();
            NameDataTable.Columns.Add("Id", typeof(int));
            NameDataTable.Columns.Add("PrimaryName", typeof(string));
            NameDataTable.Columns.Add("BirthYear", typeof(int));
            NameDataTable.Columns.Add("DeathYear", typeof(int));
        }
        public void InsertName(Name name)
        {
            DataRow row = NameDataTable.NewRow();
            row["Id"] = name.Id;
            row["PrimaryName"] = name.PrimaryName;
            row["BirthYear"] = (object?)name.BirthYear ?? DBNull.Value;
            row["DeathYear"] = (object?)name.DeathYear ?? DBNull.Value;
            NameDataTable.Rows.Add(row);
        }
         
        public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans))
            {
                bulkCopy.DestinationTableName = "Names";
                bulkCopy.ColumnMappings.Add("Id", "Id");
                bulkCopy.ColumnMappings.Add("PrimaryName", "PrimaryName");
                bulkCopy.ColumnMappings.Add("BirthYear", "BirthYear");
                bulkCopy.ColumnMappings.Add("DeathYear", "DeathYear");
                bulkCopy.WriteToServer(NameDataTable);
            }
        }
    }
}
