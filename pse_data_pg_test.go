package pse_data_pg

import (
	"database/sql"
	_ "github.com/asartalo/pq"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

var sharedConnInfo = ConnectionInfo{
	dbname:   "pse_data_pg_test",
	user:     "pse_test",
	password: "pse_test",
	host:     "localhost",
	port:     5432,
}

func TestConnectionInfo(t *testing.T) {
	Convey("Given a ConnectionInfo", t, func() {
		connInfo := sharedConnInfo

		Convey("When getting the template db connect string", func() {
			str := connInfo.ConnectStringTemplateDb()

			Convey("It should have correct value", func() {
				expected := "user=pse_test password=pse_test dbname=template1 host=localhost port=5432 sslmode=disable"
				So(str, ShouldEqual, expected)
			})
		})

		Convey("When obtaining the normal db connect string", func() {
			str := connInfo.ConnectString()

			Convey("It should have correct value", func() {
				expected := "user=pse_test password=pse_test dbname=pse_data_pg_test host=localhost port=5432 sslmode=disable"
				So(str, ShouldEqual, expected)
			})
		})
	})
}

func dropTestDb(t *testing.T) {
	db, err := sql.Open("postgres", "user=pse_test password=pse_test dbname=postgres host=localhost port=5432 sslmode=disable")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = db.Query("DROP DATABASE IF EXISTS " + sharedConnInfo.dbname)
	if err != nil {
		t.Fatalf(err.Error())
	}
	db.Close()
}

func TestPseDb(t *testing.T) {
	Convey("Given there is no db yet ", t, func() {
		dropTestDb(t)

		Convey("When a db is created", func() {
			pseDb, err := CreateDb(sharedConnInfo)
			Reset(func() {
				pseDb.Close()
			})

			Convey("There should be no errors", func() {
				So(err, ShouldBeNil)
			})

			Convey("The database should exist", func() {
				var table string
				db, _ := sql.Open("postgres", "user=pse_test password=pse_test dbname=postgres host=localhost port=5432 sslmode=disable")
				defer db.Close()
				err := db.QueryRow(`SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('pse_data_pg_test')`).Scan(&table)
				So(err, ShouldBeNil)
				So(table, ShouldEqual, "pse_data_pg_test")
			})

			Convey("The day_trades table should be present.", func() {
				d := pseDb.DbStore()
				var table string
				err := d.QueryRow(`SELECT table_name FROM information_schema.tables WHERE table_schema='public'`).Scan(&table)
				So(err, ShouldBeNil)
				So(table, ShouldEqual, "day_trades")
			})
		})
	})

	Convey("Given a db", t, func() {
		CreateDb(sharedConnInfo)

		Convey("When a data is added", func() {

		})
	})
}
