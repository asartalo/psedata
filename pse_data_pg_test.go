package pse_data_pg

import (
	"database/sql"
	"fmt"
	_ "github.com/asartalo/pq"
	"github.com/stretchr/testify/suite"
	"testing"
)

var connInfo = ConnectionInfo{
	dbname:   "pse_data_pg_test",
	user:     "pse_test",
	password: "pse_test",
	host:     "localhost",
	port:     5432,
}

func dropTestDb(t *testing.T) {
	db, err := sql.Open("postgres", "user=pse_test password=pse_test dbname=postgres host=localhost port=5432 sslmode=disable")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = db.Query("DROP DATABASE IF EXISTS " + connInfo.dbname)
	if err != nil {
		t.Fatalf(err.Error())
	}
	db.Close()
}

func haltIfError(t *testing.T, msg string, err error) {
	if err != nil {
		t.Fatalf(fmt.Sprintf(msg+" %s", err.Error()))
	}
}

type ConnectionInfoTestSuite struct {
	suite.Suite
}

func (suite *ConnectionInfoTestSuite) TestConnectStringTemplateDb() {
	expected := "user=pse_test password=pse_test dbname=template1 host=localhost port=5432 sslmode=disable"
	suite.Equal(expected, connInfo.ConnectStringTemplateDb())
}

func (suite *ConnectionInfoTestSuite) TestConnectString() {
	expected := "user=pse_test password=pse_test dbname=pse_data_pg_test host=localhost port=5432 sslmode=disable"
	suite.Equal(expected, connInfo.ConnectString())
}

func TestConnectionInfoTestSuite(t *testing.T) {
	suite.Run(t, new(ConnectionInfoTestSuite))
}

type DbTestSuite struct {
	suite.Suite
}

func (suite *DbTestSuite) SetupTest() {
	dropTestDb(suite.T())
}

func (suite *DbTestSuite) TearDownSuite() {
	dropTestDb(suite.T())
}

func (suite *DbTestSuite) TestCreateNewDb() {
	_, err := CreateDb(connInfo)

	haltIfError(suite.T(), "It error'd!", err)

	// Test creation
	db, err := sql.Open("postgres", connInfo.ConnectStringTemplateDb())
	haltIfError(suite.T(), "There's an error connecting to the test db: %s", err)

	rows, err := db.Query(`SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('pse_data_pg_test')`)
	haltIfError(suite.T(), "Error checking db: %s", err)

	firstRow := rows.Next()
	suite.NotNil(firstRow, "Database was not created")

	rows.Close()
	db.Close()
}

func TestDbTestSuite(t *testing.T) {
	suite.Run(t, new(DbTestSuite))
}
