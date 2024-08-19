package main 
import (
	"database/sql"
	
	"fmt"
	"log"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
)


//structure for COClogs database
type TableDB2 struct{
	DOCUMENTID int64 `db:"DocID"`
	EXPORTDATE sql.NullTime `db:"ExportDate"`
	DOCUMENTTYPE sql.NullString `db:"DocumentType"`
	DOCUMENTNAME sql.NullString `db:"DocumentName"`
	LOCALSIZE sql.NullInt64 `db:"LocalSize"`
	BLOBSIZE sql.NullInt64 `db:"BlobSize"`
	LOCALMD5 sql.NullString `db:"LocalMD5"`
	BLOBMD5 sql.NullString `db:"BlobMD5"`
	MD5VERIFICATIONDATE sql.NullTime `db:"MD5VerificationDate"`
	BLOBCONTAINERNAME sql.NullString `db:"BlobContainerName"`
	BLOBPATH sql.NullString `db:"BlobPath"`
	EXPORTACCOUNT sql.NullString `db:"ExportAccount"`
	EXPORTCONNECTIVITYACCOUNT sql.NullString `db:"ExportConnectivityAccount"`
	EXPORTSTORAGEACCOUNT sql.NullString `db:"ExportStorageAccount"`
	INDEXNAME sql.NullString `db:"IndexName"`
	INDEXLOCALMD5 sql.NullString `db:"IndexLocalMD5"`
	INDEXBLOBMD5 sql.NullString `db:"IndexBlobMD5"`



}

//structure for A10985 database
type TableDB1 struct{
	DOCUMENTID     int64          `db:"DOCUMENTID"`
	SNGLPRINCIP    sql.NullString `db:"HYP_SNGLPRINCIP"`
	BOOKINGYEAR    sql.NullString `db:"HYP_BOOKINGYEAR"`
	SELCOMPANY     sql.NullString `db:"HYP_SELCOMPANY"`
	SELDOKART      sql.NullString `db:"HYP_SELDOKART"`
	VOUNO_ALPHA    sql.NullString `db:"HYP_VOUNO_ALPHA"`
	VOUDAT         sql.NullTime   `db:"HYP_VOUDAT"`
	INVREFNO       sql.NullString `db:"HYP_INVREFNO"`
	MONTH          sql.NullInt64  `db:"HYP_MONTH"`
	SFCNAME        sql.NullString `db:"HYP_SFCNAME"`
	SFCNO          sql.NullInt64  `db:"HYP_SFCNO"`
	YMCODE         sql.NullInt64  `db:"HYP_YMCODE"`
	COSTCENTER     sql.NullInt64  `db:"HYP_COSTCENTER"`
	VOYAGE         sql.NullInt64  `db:"HYP_VOYAGE"`
	DIRECTION      sql.NullString `db:"HYP_DIRECTION"`
	ORIGIN         sql.NullString `db:"HYP_ORIGIN"`
	DESTINATION    sql.NullString `db:"HYP_DESTINATION"`
	BL             sql.NullString `db:"HYP_BL"`
	UBLI           sql.NullString `db:"HYP_UBLI"`
	REMARK         sql.NullString `db:"HYP_REMARK"`
	ARCHDAT        sql.NullTime   `db:"HYP_ARCHDAT"`
	ORIGDOCID      sql.NullInt64  `db:"HYP_ORIGDOCID"`
	ORIGDBID       sql.NullInt64  `db:"HYP_ORIGDBID"`
	VESSEL_ALPHA   sql.NullString `db:"HYP_VESSEL_ALPHA"`
	CODER          sql.NullInt64  `db:"HYP_CODER"`
	STATION        sql.NullString `db:"HYP_STATION"`
	BOOKINGNUMBER  sql.NullString `db:"HYP_BOOKINGNUMBER"`
	VESSELNAME     sql.NullString `db:"HYP_VESSELNAME"`
	GUID           sql.NullString `db:"HYP_GUID"`
	SFCNO_SAP      sql.NullString `db:"HYP_SFCNO_SAP"`
	COSTCENTER_SAP sql.NullString `db:"HYP_COSTCENTER_SAP"`
	YMCODE_SAP     sql.NullString `db:"HYP_YMCODE_SAP"`
	AGREEMENT_NO   sql.NullString `db:"HYP_AGREEMENT_NO"`
	MTY_RELEASE_NO sql.NullString `db:"HYP_MTY_RELEASE_NO"`
	JOBORDERNUMBER sql.NullString `db:"HYP_JOBORDERNUMBER"`
}

//structre for the resultant db
type CombinedTable struct {
	
	DB1_DOCUMENTID     int64          `db:"DB1_DOCUMENTID"`
	DB1_SNGLPRINCIP    sql.NullString `db:"DB1_SNGLPRINCIP"`
	DB1_BOOKINGYEAR    sql.NullString `db:"DB1_BOOKINGYEAR"`
	DB1_SELCOMPANY     sql.NullString `db:"DB1_SELCOMPANY"`
	DB1_SELDOKART      sql.NullString `db:"DB1_SELDOKART"`
	DB1_VOUNO_ALPHA    sql.NullString `db:"DB1_VOUNO_ALPHA"`
	DB1_VOUDAT         sql.NullTime   `db:"DB1_VOUDAT"`
	DB1_INVREFNO       sql.NullString `db:"DB1_INVREFNO"`
	DB1_MONTH          sql.NullInt64  `db:"DB1_MONTH"`
	DB1_SFCNAME        sql.NullString `db:"DB1_SFCNAME"`
	DB1_SFCNO          sql.NullInt64  `db:"DB1_SFCNO"`
	DB1_YMCODE         sql.NullInt64  `db:"DB1_YMCODE"`
	DB1_COSTCENTER     sql.NullInt64  `db:"DB1_COSTCENTER"`
	DB1_VOYAGE         sql.NullInt64  `db:"DB1_VOYAGE"`
	DB1_DIRECTION      sql.NullString `db:"DB1_DIRECTION"`
	DB1_ORIGIN         sql.NullString `db:"DB1_ORIGIN"`
	DB1_DESTINATION    sql.NullString `db:"DB1_DESTINATION"`
	DB1_BL             sql.NullString `db:"DB1_BL"`
	DB1_UBLI           sql.NullString `db:"DB1_UBLI"`
	DB1_REMARK         sql.NullString `db:"DB1_REMARK"`
	DB1_ARCHDAT        sql.NullTime   `db:"DB1_ARCHDAT"`
	DB1_ORIGDOCID      sql.NullInt64  `db:"DB1_ORIGDOCID"`
	DB1_ORIGDBID       sql.NullInt64  `db:"DB1_ORIGDBID"`
	DB1_VESSEL_ALPHA   sql.NullString `db:"DB1_VESSEL_ALPHA"`
	DB1_CODER          sql.NullInt64  `db:"DB1_CODER"`
	DB1_STATION        sql.NullString `db:"DB1_STATION"`
	DB1_BOOKINGNUMBER  sql.NullString `db:"DB1_BOOKINGNUMBER"`
	DB1_VESSELNAME     sql.NullString `db:"DB1_VESSELNAME"`
	DB1_GUID           sql.NullString `db:"DB1_GUID"`
	DB1_SFCNO_SAP      sql.NullString `db:"DB1_SFCNO_SAP"`
	DB1_COSTCENTER_SAP sql.NullString `db:"DB1_COSTCENTER_SAP"`
	DB1_YMCODE_SAP     sql.NullString `db:"DB1_YMCODE_SAP"`
	DB1_AGREEMENT_NO   sql.NullString `db:"DB1_AGREEMENT_NO"`
	DB1_MTY_RELEASE_NO sql.NullString `db:"DB1_MTY_RELEASE_NO"`
	DB1_JOBORDERNUMBER sql.NullString `db:"DB1_JOBORDERNUMBER"`

	// Columns from TableDB2
	DB2_DOCUMENTID              int64         `db:"DB2_DOCUMENTID"`
	DB2_EXPORTDATE              sql.NullTime  `db:"DB2_EXPORTDATE"`
	DB2_DOCUMENTTYPE            sql.NullString `db:"DB2_DOCUMENTTYPE"`
	DB2_DOCUMENTNAME            sql.NullString `db:"DB2_DOCUMENTNAME"`
	DB2_LOCALSIZE               sql.NullInt64  `db:"DB2_LOCALSIZE"`
	DB2_BLOBSIZE                sql.NullInt64  `db:"DB2_BLOBSIZE"`
	DB2_LOCALMD5                sql.NullString `db:"DB2_LOCALMD5"`
	DB2_BLOBMD5                 sql.NullString `db:"DB2_BLOBMD5"`
	DB2_MD5VERIFICATIONDATE     sql.NullTime   `db:"DB2_MD5VERIFICATIONDATE"`
	DB2_BLOBCONTAINERNAME       sql.NullString `db:"DB2_BLOBCONTAINERNAME"`
	DB2_BLOBPATH                sql.NullString `db:"DB2_BLOBPATH"`
	DB2_EXPORTACCOUNT           sql.NullString `db:"DB2_EXPORTACCOUNT"`
	DB2_EXPORTCONNECTIVITYACCOUNT sql.NullString `db:"DB2_EXPORTCONNECTIVITYACCOUNT"`
	DB2_EXPORTSTORAGEACCOUNT    sql.NullString `db:"DB2_EXPORTSTORAGEACCOUNT"`
	DB2_INDEXNAME               sql.NullString `db:"DB2_INDEXNAME"`
	DB2_INDEXLOCALMD5           sql.NullString `db:"DB2_INDEXLOCALMD5"`
	DB2_INDEXBLOBMD5            sql.NullString `db:"DB2_INDEXBLOBMD5"`
}



//connection establishment
func getSQLClient(database string) *sqlx.DB{
	server := "dgarchive-migration-sqlserver.database.windows.net"
	username := "r7V4OjohvkX7Wb7x"
	password := "ZzPeArSqmN3BD7Cj6xUsJM"
	connString := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=30",
		username, password, server, database)

	db, err := sqlx.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Error creating connection pool: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}

	return db
}
func assertNotNil(obj interface{}, message string) {
    if obj == nil {
        log.Fatal(message)
    }
}

var database1 = "A1_20240521"
var database2 = "COCLogs_X_A1_20240521"

//fetching data for A1
func fetchAndPrintDataDB1(db *sqlx.DB, tableName string) ([]TableDB1, error) {
	// Using TOP 10 to limit the number of rows retrieved
	query := fmt.Sprintf("SELECT TOP 2 * FROM %s", tableName)
	var rows []TableDB1
	err := db.Select(&rows, query)
	if err != nil {
		log.Fatalf("Error querying table %s in db1: %v", tableName, err)
	}
		return rows,nil

}

//fetching data from COC
func fetchAndPrintDataDB2(db *sqlx.DB, tableName string) ([]TableDB2, error){
	// Using TOP 10 to limit the number of rows retrieved
	query := fmt.Sprintf("SELECT TOP 2 * FROM %s", tableName)
	var rows []TableDB2
	err := db.Select(&rows, query)
	if err != nil {
		log.Fatalf("Error querying table %s in db2: %v", tableName, err)
	}
	return rows, nil
	
}

//joining data
func joinAndPrintData(db1Data []TableDB1, db2Data []TableDB2) {
	db2Map := make(map[int64]TableDB2)
	for _, row := range db2Data {
		db2Map[row.DOCUMENTID] = row
	}

	fmt.Println("DB2 Map Keys:")
	for key := range db2Map {
		fmt.Println(key)
	}

	var combinedData []CombinedTable
	for _, row1 := range db1Data {
		if row2, exists := db2Map[row1.DOCUMENTID]; exists {
			combinedRow := CombinedTable{
				DB1_DOCUMENTID:     row1.DOCUMENTID,
				DB1_SNGLPRINCIP:    row1.SNGLPRINCIP,
				DB1_BOOKINGYEAR:    row1.BOOKINGYEAR,
				DB1_SELCOMPANY:     row1.SELCOMPANY,
				DB1_SELDOKART:      row1.SELDOKART,
				DB1_VOUNO_ALPHA:    row1.VOUNO_ALPHA,
				DB1_VOUDAT:         row1.VOUDAT,
				DB1_INVREFNO:       row1.INVREFNO,
				DB1_MONTH:          row1.MONTH,
				DB1_SFCNAME:        row1.SFCNAME,
				DB1_SFCNO:          row1.SFCNO,
				DB1_YMCODE:         row1.YMCODE,
				DB1_COSTCENTER:     row1.COSTCENTER,
				DB1_VOYAGE:         row1.VOYAGE,
				DB1_DIRECTION:      row1.DIRECTION,
				DB1_ORIGIN:         row1.ORIGIN,
				DB1_DESTINATION:    row1.DESTINATION,
				DB1_BL:             row1.BL,
				DB1_UBLI:           row1.UBLI,
				DB1_REMARK:         row1.REMARK,
				DB1_ARCHDAT:        row1.ARCHDAT,
				DB1_ORIGDOCID:      row1.ORIGDOCID,
				DB1_ORIGDBID:       row1.ORIGDBID,
				DB1_VESSEL_ALPHA:   row1.VESSEL_ALPHA,
				DB1_CODER:          row1.CODER,
				DB1_STATION:        row1.STATION,
				DB1_BOOKINGNUMBER:  row1.BOOKINGNUMBER,
				DB1_VESSELNAME:     row1.VESSELNAME,
				DB1_GUID:           row1.GUID,
				DB1_SFCNO_SAP:      row1.SFCNO_SAP,
				DB1_COSTCENTER_SAP: row1.COSTCENTER_SAP,
				DB1_YMCODE_SAP:     row1.YMCODE_SAP,
				DB1_AGREEMENT_NO:   row1.AGREEMENT_NO,
				DB1_MTY_RELEASE_NO: row1.MTY_RELEASE_NO,
				DB1_JOBORDERNUMBER: row1.JOBORDERNUMBER,

				DB2_DOCUMENTID:              row2.DOCUMENTID,
				DB2_EXPORTDATE:              row2.EXPORTDATE,
				DB2_DOCUMENTTYPE:            row2.DOCUMENTTYPE,
				DB2_DOCUMENTNAME:            row2.DOCUMENTNAME,
				DB2_LOCALSIZE:               row2.LOCALSIZE,
				DB2_BLOBSIZE:                row2.BLOBSIZE,
				DB2_LOCALMD5:                row2.LOCALMD5,
				DB2_BLOBMD5:                 row2.BLOBMD5,
				DB2_MD5VERIFICATIONDATE:     row2.MD5VERIFICATIONDATE,
				DB2_BLOBCONTAINERNAME:       row2.BLOBCONTAINERNAME,
				DB2_BLOBPATH:                row2.BLOBPATH,
				DB2_EXPORTACCOUNT:           row2.EXPORTACCOUNT,
				DB2_EXPORTCONNECTIVITYACCOUNT: row2.EXPORTCONNECTIVITYACCOUNT,
				DB2_EXPORTSTORAGEACCOUNT:    row2.EXPORTSTORAGEACCOUNT,
				DB2_INDEXNAME:               row2.INDEXNAME,
				DB2_INDEXLOCALMD5:           row2.INDEXLOCALMD5,
				DB2_INDEXBLOBMD5:            row2.INDEXBLOBMD5,
			}
			combinedData = append(combinedData, combinedRow)
		}
	}

	fmt.Println("Combined Data:")
	for i, row := range combinedData {
		fmt.Printf("Row %d: DB1 - DOCUMENTID: %d, SNGLPRINCIP: %v, BOOKINGYEAR: %v, SELCOMPANY: %v, SELDOKART: %v, VOUNO_ALPHA: %v, VOUDAT: %v, INVREFNO: %v, MONTH: %v, SFCNAME: %v, SFCNO: %v, YMCODE: %v, COSTCENTER: %v, VOYAGE: %v, DIRECTION: %v, ORIGIN: %v, DESTINATION: %v, BL: %v, UBLI: %v, REMARK: %v, ARCHDAT: %v, ORIGDOCID: %v, ORIGDBID: %v, VESSEL_ALPHA: %v, CODER: %v, STATION: %v, BOOKINGNUMBER: %v, VESSELNAME: %v, GUID: %v, SFCNO_SAP: %v, COSTCENTER_SAP: %v, YMCODE_SAP: %v, AGREEMENT_NO: %v, MTY_RELEASE_NO: %v, JOBORDERNUMBER: %v | DB2 - DOCUMENTID: %d, EXPORTDATE: %v, DOCUMENTTYPE: %v, DOCUMENTNAME: %v, LOCALSIZE: %v, BLOBSIZE: %v, LOCALMD5: %v, BLOBMD5: %v, MD5VERIFICATIONDATE: %v, BLOBCONTAINERNAME: %v, BLOBPATH: %v, EXPORTACCOUNT: %v, EXPORTCONNECTIVITYACCOUNT: %v, EXPORTSTORAGEACCOUNT: %v, INDEXNAME: %v, INDEXLOCALMD5: %v, INDEXBLOBMD5: %v\n",
			i+1, row.DB1_DOCUMENTID, row.DB1_SNGLPRINCIP, row.DB1_BOOKINGYEAR, row.DB1_SELCOMPANY, row.DB1_SELDOKART, row.DB1_VOUNO_ALPHA,
			row.DB1_VOUDAT, row.DB1_INVREFNO, row.DB1_MONTH, row.DB1_SFCNAME, row.DB1_SFCNO, row.DB1_YMCODE, row.DB1_COSTCENTER,
			row.DB1_VOYAGE, row.DB1_DIRECTION, row.DB1_ORIGIN, row.DB1_DESTINATION, row.DB1_BL, row.DB1_UBLI, row.DB1_REMARK,
			row.DB1_ARCHDAT, row.DB1_ORIGDOCID, row.DB1_ORIGDBID, row.DB1_VESSEL_ALPHA, row.DB1_CODER, row.DB1_STATION,
			row.DB1_BOOKINGNUMBER, row.DB1_VESSELNAME, row.DB1_GUID, row.DB1_SFCNO_SAP, row.DB1_COSTCENTER_SAP, row.DB1_YMCODE_SAP,
			row.DB1_AGREEMENT_NO, row.DB1_MTY_RELEASE_NO, row.DB1_JOBORDERNUMBER,
			row.DB2_DOCUMENTID, row.DB2_EXPORTDATE, row.DB2_DOCUMENTTYPE, row.DB2_DOCUMENTNAME, row.DB2_LOCALSIZE,
			row.DB2_BLOBSIZE, row.DB2_LOCALMD5, row.DB2_BLOBMD5, row.DB2_MD5VERIFICATIONDATE, row.DB2_BLOBCONTAINERNAME,
			row.DB2_BLOBPATH, row.DB2_EXPORTACCOUNT, row.DB2_EXPORTCONNECTIVITYACCOUNT, row.DB2_EXPORTSTORAGEACCOUNT,
			row.DB2_INDEXNAME, row.DB2_INDEXLOCALMD5, row.DB2_INDEXBLOBMD5)
	}
}



func main(){
	
	db1  := getSQLClient(database1)
	assertNotNil(db1, "a1  is nil")
	fmt.Println("Successfully connected to database 1:", database1)
	db2 := getSQLClient(database2)
	assertNotNil(db2, "coc db is  nil")
	fmt.Println("Successfully connected to database 2:", database2)

	db1Data, err := fetchAndPrintDataDB1(db1, "HYP_A1_CAT00985")
	if err != nil {
		log.Fatal(err)
	}
	

	db2Data, err := fetchAndPrintDataDB2(db2, "HYP_A1_CAT00985_COC")
	if err != nil {
		log.Fatal(err)
	}

	joinAndPrintData(db1Data, db2Data)


}