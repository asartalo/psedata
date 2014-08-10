package psedata

import (
	"database/sql"
	"fmt"
	// Used internally by database/sql for db driver
	_ "github.com/asartalo/pq"
	"time"
)

type DailyRecord struct {
	symbol string
	date   time.Time
	open   float64
	high   float64
	low    float64
	close  float64
	vol    int
}

type dRow struct {
	symbol string
	date   time.Time
	open   float64
	high   float64
	low    float64
	close  float64
	vol    int
}

// String returns a string representation of the DailyRecord
func (data *DailyRecord) String() string {
	return fmt.Sprintf(
		"%s,%s,%f,%f,%f,%f,%d", data.Symbol(), data.Date().Format("2006-01-02"),
		data.Open(), data.High(), data.Low(), data.Close(), data.Volume(),
	)
}

// Symbol returns the symbol for the data row
func (data *DailyRecord) Symbol() string {
	return data.symbol
}

// Date returns the date for the data row
func (data *DailyRecord) Date() time.Time {
	return data.date
}

// Open returns the opening price for that date
func (data *DailyRecord) Open() float64 {
	return data.open
}

// High returns the highest price for that date
func (data *DailyRecord) High() float64 {
	return data.high
}

// Low returns the lowest price for that date
func (data *DailyRecord) Low() float64 {
	return data.low
}

// Close returns the closing price for that date
func (data *DailyRecord) Close() float64 {
	return data.close
}

// Volume returns the trading volume for that date
func (data *DailyRecord) Volume() int {
	return data.vol
}

type scanner interface {
	Scan(...interface{}) error
}

func (data *DailyRecord) importData(scanner scanner) error {
	return scanner.Scan(&data.symbol, &data.date, &data.open, &data.high, &data.low, &data.close, &data.vol)
}

type DailyRecords interface {
	Next() (*DailyRecord, error)
}

// NewDailyRecord creates a new DailyRecord from parameters
func NewDailyRecord(symbol string, date time.Time, open float64, high float64, low float64, close float64, vol int) DailyRecord {
	return DailyRecord{symbol, date, open, high, low, close, vol}
}

func NewDailyRecordS(d dRow) DailyRecord {
	return DailyRecord{d.symbol, d.date, d.open, d.high, d.low, d.close, d.vol}
}

// ConnectionInfo represents a database connection information and credentials.
type ConnectionInfo struct {
	dbname   string
	user     string
	password string
	host     string
	port     int
}

// ConnectStringTemplateDb returns a connection string to be used with sql.Open() that connects to tmeplate1 db which is a default db.
// Useful for creating databases, or inspecting a db when you're not sure if it exists.
func (info ConnectionInfo) ConnectStringTemplateDb() string {
	return info.connectStringGeneral("template1")
}

// ConnectString returns a connecction string to be used with sql.Open()
func (info ConnectionInfo) ConnectString() string {
	return info.connectStringGeneral(info.dbname)
}

func (info ConnectionInfo) connectStringGeneral(dbname string) string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%d sslmode=disable",
		info.user, info.password, dbname, info.host, info.port,
	)
}

// The PseDb interface for CRUD operations and useful shortcuts for retrieving day trades data.
type PseDb interface {
	DbStore() *sql.DB
	Close() error
	NewData(DailyRecord) error
	DailyRecordFor(string, time.Time) (DailyRecord, error)
	AllDailyRecordFor(string) ([]DailyRecord, error)
	ImportDaylies(DailyRecords) error
}

type pseDbS struct {
	info           ConnectionInfo
	db             *sql.DB
	insertDataStmt *sql.Stmt
	selectDailyStr string
}

func (pseDb pseDbS) Close() error {
	return pseDb.db.Close()
}

func (pseDb *pseDbS) createTables() (err error) {
	db, err := sql.Open("postgres", pseDb.info.ConnectString())
	pseDb.db = db
	if err != nil {
		return err
	}
	_, err = pseDb.db.Exec(
		`CREATE TABLE day_trades (` +
			`symbol varchar(5) NOT NULL, ` +
			`date   date NOT NULL, ` +
			`open   numeric(14, 5) NOT NULL, ` +
			`high   numeric(14, 5) NOT NULL, ` +
			`low    numeric(14, 5) NOT NULL, ` +
			`close  numeric(14, 5) NOT NULL, ` +
			`vol    int NOT NULL)`,
	)
	return err
}

func (pseDb *pseDbS) NewData(data DailyRecord) error {
	stmt, err := pseDb.insertStatement()
	if err != nil {
		return err
	}

	_, err = stmt.Exec(data.symbol, data.date, data.open, data.high, data.low, data.close, data.vol)
	if err != nil {
		return err
	}
	return nil
}

func (pseDb *pseDbS) ImportDaylies(rows DailyRecords) error {
	for {
		data, err := rows.Next()
		if data == nil {
			break
		}
		if err != nil {
			return err
		}
		err = pseDb.NewData(*data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pseDb *pseDbS) selectDailyString() string {
	if pseDb.selectDailyStr == "" {
		pseDb.selectDailyStr = `SELECT symbol, date, open, high, low, close, vol FROM day_trades WHERE `
	}
	return pseDb.selectDailyStr
}

func (pseDb *pseDbS) AllDailyRecordFor(symbol string) ([]DailyRecord, error) {
	var all []DailyRecord
	rows, err := pseDb.DbStore().Query(pseDb.selectDailyString()+`symbol = $1`, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		data := new(DailyRecord)
		if err := data.importData(rows); err != nil {
			return nil, err
		}
		all = append(all, *data)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return all, nil
}

func (pseDb *pseDbS) DailyRecordFor(symbol string, date time.Time) (DailyRecord, error) {
	data := new(DailyRecord)
	err := data.importData(
		pseDb.DbStore().QueryRow(pseDb.selectDailyString()+`symbol = $1 AND date = $2`, symbol, date),
	)
	if err != nil {
		return *data, err
	}
	return *data, nil
}

func (pseDb *pseDbS) insertStatement() (*sql.Stmt, error) {
	if pseDb.insertDataStmt == nil {
		var err error
		pseDb.insertDataStmt, err = pseDb.DbStore().Prepare(
			`INSERT INTO day_trades (symbol, date, open, high, low, close, vol) ` +
				`VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		)
		if err != nil {
			return nil, err
		}
	}
	return pseDb.insertDataStmt, nil
}

// DbStore returns the underlying database object
func (pseDb *pseDbS) DbStore() *sql.DB {
	return pseDb.db
}

// CreateDb creates the underlying PostGreSQL datastore.
func CreateDb(info ConnectionInfo) (PseDb, error) {
	db, err := sql.Open("postgres", info.ConnectStringTemplateDb())
	defer db.Close()
	pseDb := new(pseDbS)
	pseDb.info = info

	if err != nil {
		return pseDb, err
	}

	_, err = db.Exec(`CREATE DATABASE ` + info.dbname)

	if err != nil {
		return pseDb, err
	}

	_, err = db.Exec(`ALTER DATABASE ` + info.dbname + ` SET TIME ZONE 'Asia/Manila'`)

	if err != nil {
		return pseDb, err
	}

	err = pseDb.createTables()
	return pseDb, err
}

// func ImportCSVHistorical(file) {
// }
