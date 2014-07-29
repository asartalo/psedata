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

type PseDb struct {
	db *sql.DB
}

func (pseDb PseDb) Close() {
	pseDb.db.Close()
}

func CreateDb(info ConnectionInfo) (pseDb PseDb, err error) {
	db, err := sql.Open("postgres", info.ConnectStringTemplateDb())
	pseDb = PseDb{}

	if err != nil {
		return pseDb, err
	}

	_, err = db.Query(`CREATE DATABASE ` + info.dbname)

	if err != nil {
		db.Close()
		return pseDb, err
	}
	db.Close()
	return pseDb, nil
}

// func ImportCSVHistorical(file) {
// }
