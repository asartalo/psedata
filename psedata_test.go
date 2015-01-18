package psedata

import (
	_ "github.com/asartalo/pq"
	"github.com/jmoiron/sqlx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var sharedConnInfo = ConnectionInfo{
	DbName:   "pse_data_pg_test",
	User:     "pse_test",
	Password: "pse_test",
	Host:     "localhost",
	Port:     5432,
}

type genericRows struct {
	pos        int
	collection []DailyRecord
}

func (rows *genericRows) Next() (*DailyRecord, error) {
	last := (len(rows.collection) - 1)
	defer func() {
		rows.pos++
	}()
	if rows.pos > last {
		return nil, nil
	}

	return &rows.collection[rows.pos], nil
}

func (rows *genericRows) Append(data DailyRecord) {
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
var genericData = NewDailyRecordS(rawData)

func TestDailyRecord(t *testing.T) {
	Convey("Given a DailyRecord", t, func() {

		Convey("When exported as string", func() {
			str := genericData.String()

			Convey("It should be properly formatted", func() {
				expected := "AAA,1980-04-20,10.000000,12.000000,9.100000,11.000000,100"
				So(str, ShouldEqual, expected)
			})
		})

		Convey("When values are retrieved", func() {
			Convey("Values should match those on initialization", func() {
				So(genericData.Symbol(), ShouldEqual, "AAA")
				So(genericData.Date().Equal(date), ShouldBeTrue)
				So(genericData.Open(), ShouldEqual, 10.0)
				So(genericData.High(), ShouldEqual, 12.0)
				So(genericData.Low(), ShouldEqual, 9.1)
				So(genericData.Close(), ShouldEqual, 11.0)
				So(genericData.Volume(), ShouldEqual, 100)
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
	db, err := sqlx.Open("postgres", "user=pse_test password=pse_test dbname=postgres host=localhost port=5432 sslmode=disable")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = db.Query("DROP DATABASE IF EXISTS " + sharedConnInfo.DbName)
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
				db, _ := sqlx.Open("postgres", "user=pse_test password=pse_test dbname=postgres host=localhost port=5432 sslmode=disable")
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
				So(symbol, ShouldEqual, input.Symbol())
				So(date2.Format(dFmt), ShouldEqual, date.Format(dFmt))
				So(open, ShouldEqual, 10.0)
				So(high, ShouldEqual, 12.0)
				So(low, ShouldEqual, 9.1)
				So(close, ShouldEqual, 11.0)
				So(vol, ShouldEqual, 100)
			})

			Convey("The data should be retrievable", func() {
				result, err := pseDb.AllDailyRecordFor("AAA")
				So(err, ShouldBeNil)
				So(len(result), ShouldEqual, 1)
				saved := result[0]
				// For some reason, Dates are not equal when using ShouldEqual
				So(saved.Date().Equal(input.date), ShouldBeTrue)
				So(saved.String(), ShouldEqual, input.String())
			})
		})

		Convey("When DailyRecords are imported", func() {
			// Prepare Data
			d := time.Now()
			rawData1 := rawData
			rawData2 := rawData
			rawData1.symbol = "AA1"
			rawData2.symbol = "AA2"
			rawData3 := rawData2
			rawData3.date = d

			data1 := NewDailyRecordS(rawData1)
			data2 := NewDailyRecordS(rawData2)
			data3 := NewDailyRecordS(rawData3)

			rows := new(genericRows)
			rows.Append(data1)
			rows.Append(data2)
			rows.Append(data3)

			pseDb.ImportDaylies(rows)

			Convey("The data should be saved", func() {
				result1, _ := pseDb.AllDailyRecordFor("AA1")
				result2, _ := pseDb.AllDailyRecordFor("AA2")
				So(len(result1), ShouldEqual, 1)
				So(result1[0].String(), ShouldEqual, data1.String())
				So(result2[0].String(), ShouldEqual, data2.String())
				So(result2[1].String(), ShouldEqual, data3.String())
			})

			Convey("The data can be obtained by date", func() {
				saved, _ := pseDb.DailyRecordFor("AA2", d)
				So(saved.String(), ShouldEqual, data3.String())
			})

		})
	})
}
