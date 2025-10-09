using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class BulkSql
    {
        public DataTable TitleDataTable { get; set; } 
        public BulkSql() {
            TitleDataTable = new DataTable();
            TitleDataTable.Columns.Add("Id", typeof(int));
            TitleDataTable.Columns.Add("TypeId", typeof(int));
            TitleDataTable.Columns.Add("PrimaryTitle", typeof(string));
            TitleDataTable.Columns.Add("OriginalTitle", typeof(string));
            TitleDataTable.Columns.Add("IsAdult", typeof(bool));
            TitleDataTable.Columns.Add("StartYear", typeof(short));
            TitleDataTable.Columns.Add("EndYear", typeof(short));
            TitleDataTable.Columns.Add("RuntimeMinutes", typeof(int));
        }

        public void InsertTitle(Title title)
        {
            DataRow row = TitleDataTable.NewRow();
            row["Id"] = title.Id;
            row["TypeId"] = title.TitleType;
            row["PrimaryTitle"] = title.PrimaryTitle;
            row["OriginalTitle"] = (object?)title.OriginalTitle ?? DBNull.Value;
            row["IsAdult"] = title.IsAdult;
            row["StartYear"] = (object?)title.StartYear ?? DBNull.Value;
            row["EndYear"] = (object?)title.EndYear ?? DBNull.Value;
            row["RuntimeMinutes"] = (object?)title.RuntimeMinutes ?? DBNull.Value;
            TitleDataTable.Rows.Add(row);
        }

        public void InsertIntoDB(SqlConnection sqlConn, SqlTransaction sqlTrans) {
        
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, sqlTrans))
            {
                bulkCopy.DestinationTableName = "Titles";
                bulkCopy.ColumnMappings.Add("Id", "Id");
                bulkCopy.ColumnMappings.Add("TypeId", "TypeId");
                bulkCopy.ColumnMappings.Add("PrimaryTitle", "PrimaryTitle");
                bulkCopy.ColumnMappings.Add("OriginalTitle", "OriginalTitle");
                bulkCopy.ColumnMappings.Add("IsAdult", "IsAdult");
                bulkCopy.ColumnMappings.Add("StartYear", "StartYear");
                bulkCopy.ColumnMappings.Add("EndYear", "EndYear");
                bulkCopy.ColumnMappings.Add("RuntimeMinutes", "RuntimeMinutes");
                bulkCopy.WriteToServer(TitleDataTable);
            }
        }
    }
}
