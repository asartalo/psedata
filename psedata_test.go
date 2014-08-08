package psedata

import (
	"database/sql"
	_ "github.com/asartalo/pq"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var sharedConnInfo = ConnectionInfo{
	dbname:   "pse_data_pg_test",
	user:     "pse_test",
	password: "pse_test",
	host:     "localhost",
	port:     5432,
}

type genericRows struct {
	pos        int
	collection []DataRow
}

func (rows *genericRows) Next() *DataRow {
	last := (len(rows.collection) - 1)
	defer func() {
		rows.pos++
	}()
	if rows.pos > last {
		return nil
	}

	return &rows.collection[rows.pos]

}

func (rows *genericRows) Append(data DataRow) {
	rows.collection = append(rows.collection, data)
}

func getGenericDate() time.Time {
	d, _ := time.Parse(dFmt, "Apr 20, 1980")
	return d
}

var dFmt string = "Jan 2, 2006"
var err error
var date time.Time = getGenericDate()
var rawData = struct {
	symbol string
	date   time.Time
	open   float64
	high   float64
	low    float64
	close  float64
	vol    int
}{
	symbol: "AAA",
	date:   date,
	open:   10.0,
	high:   12.0,
	low:    9.1,
	close:  11.0,
	vol:    100,
}
var genericData = NewDataRowS(rawData)

func TestDataRow(t *testing.T) {
	Convey("Given a DataRow", t, func() {

		Convey("When exported as string", func() {
			str := genericData.String()

			Convey("It should be properly formatted", func() {
				expected := "AAA,1980-04-20,10.000000,12.000000,9.100000,11.000000,100"
				So(str, ShouldEqual, expected)
			})
		})
	})
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
		dropTestDb(t)
		pseDb, _ := CreateDb(sharedConnInfo)
		Reset(func() {
			pseDb.Close()
		})

		Convey("When a data is added", func() {
			input := genericData
			pseDb.NewData(input)

			Convey("The data should be saved on db", func() {
				d := pseDb.DbStore()
				var symbol string
				var date2 time.Time
				var open, high, low, close float64
				var vol int
				err := d.QueryRow(
					`SELECT symbol, date, open, high, low, close, vol FROM day_trades limit 1`,
				).Scan(&symbol, &date2, &open, &high, &low, &close, &vol)
				So(err, ShouldBeNil)
				So(symbol, ShouldEqual, input.symbol)
				So(date2.Format(dFmt), ShouldEqual, date.Format(dFmt))
				So(open, ShouldEqual, 10.0)
				So(high, ShouldEqual, 12.0)
				So(low, ShouldEqual, 9.1)
				So(close, ShouldEqual, 11.0)
				So(vol, ShouldEqual, 100)
			})

			Convey("The data should be retrievable", func() {
				result, err := pseDb.GetAllDataFor("AAA")
				So(err, ShouldBeNil)
				So(len(result), ShouldEqual, 1)
				saved := result[0]
				// For some reason, Dates are not equal when using ShouldEqual
				So(saved.date.Equal(input.date), ShouldBeTrue)
				So(saved.String(), ShouldEqual, input.String())
			})
		})

		Convey("When DataRows are imported", func() {
			rows := new(genericRows)
			data1 := genericData
			data2 := genericData
			data1.symbol = "AA1"
			data2.symbol = "AA2"
			rows.Append(data1)
			rows.Append(data2)

			pseDb.Import(rows)

			Convey("The data should be saved", func() {
				result1, _ := pseDb.GetAllDataFor("AA1")
				result2, _ := pseDb.GetAllDataFor("AA2")
				So(len(result1), ShouldEqual, 1)
				So(result1[0].String(), ShouldEqual, data1.String())
				So(result2[0].String(), ShouldEqual, data2.String())
			})
		})
	})
}
