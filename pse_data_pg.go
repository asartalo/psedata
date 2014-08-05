package pse_data_pg

import (
	"database/sql"
	"fmt"
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

type ConnectionInfo struct {
	dbname   string
	user     string
	password string
	host     string
	port     int
}

func (info ConnectionInfo) ConnectStringTemplateDb() string {
	return info.connectStringGeneral("template1")
}

func (info ConnectionInfo) ConnectString() string {
	return info.connectStringGeneral(info.dbname)
}

func (info ConnectionInfo) connectStringGeneral(dbname string) string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%d sslmode=disable",
		info.user, info.password, dbname, info.host, info.port,
	)
}

type PseDb interface {
	DbStore() *sql.DB
	Close() error
}

type pseDbS struct {
	info ConnectionInfo
	db   *sql.DB
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

func (pseDb *pseDbS) DbStore() *sql.DB {
	return pseDb.db
}

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
	err = pseDb.createTables()
	return pseDb, err
}

// func ImportCSVHistorical(file) {
// }
