package psedata

import (
	"database/sql"
	"fmt"
	// Used internally by database/sql for db driver
	_ "github.com/asartalo/pq"
	"time"
)

type DataRow struct {
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

// String returns a string representation of the DataRow
func (data *DataRow) String() string {
	return fmt.Sprintf(
		"%s,%s,%f,%f,%f,%f,%d", data.symbol, data.date.Format("2006-01-02"),
		data.open, data.high, data.low, data.close, data.vol,
	)
}

type DataRows interface {
	Next() *DataRow
}

// NewDataRow creates a new DataRow from parameters
func NewDataRow(symbol string, date time.Time, open float64, high float64, low float64, close float64, vol int) DataRow {
	return DataRow{symbol, date, open, high, low, close, vol}
}

func NewDataRowS(d dRow) DataRow {
	return DataRow{d.symbol, d.date, d.open, d.high, d.low, d.close, d.vol}
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
	NewData(DataRow) error
	GetAllDataFor(string) ([]DataRow, error)
	Import(DataRows) error
}

type pseDbS struct {
	info           ConnectionInfo
	db             *sql.DB
	insertDataStmt *sql.Stmt
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

func (pseDb *pseDbS) NewData(data DataRow) error {
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

func (pseDb *pseDbS) Import(rows DataRows) error {
	for {
		data := rows.Next()
		if data == nil {
			break
		}
		err := pseDb.NewData(*data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pseDb *pseDbS) GetAllDataFor(symbol string) ([]DataRow, error) {
	var all []DataRow
	d := pseDb.DbStore()
	rows, err := d.Query(
		`SELECT symbol, date, open, high, low, close, vol FROM day_trades WHERE symbol = $1`,
		symbol,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		data := new(DataRow)
		if err := rows.Scan(&data.symbol, &data.date, &data.open, &data.high, &data.low, &data.close, &data.vol); err != nil {
			return nil, err
		}
		all = append(all, *data)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return all, nil
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
