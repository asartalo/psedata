package importer

import (
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
	"time"
)

func TestImportingCsvHistoricalRecords(t *testing.T) {
	Convey("Given a Historical CSV File content", t, func() {
		csvStr := "<NAME>,<DATE>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,\n" +
			"SEVN,20131213,101.00000,101.00000,99.50000,100.00000,4940,\n" +
			"SEVN,20131217,100.00000,100.00000,100.00000,100.00000,740,\n" +
			"SEVN,20131218,99.50000,99.50000,99.50000,99.50000,90,\n"
		csvF := strings.NewReader(csvStr)

		Convey("When imported", func() {
			records, err := NewCsvImporter().ImportHistoricalRecords(csvF)

			Convey("There should be no errors", func() {
				So(err, ShouldBeNil)
			})

			Convey("Result should contain data imported", func() {
				data1, _ := records.Next()
				data2, _ := records.Next()
				data3, _ := records.Next()

				So(data1, ShouldNotBeNil)

				So(data1.Symbol(), ShouldEqual, "SEVN")
				So(data2.Volume(), ShouldEqual, 740)

				// Date
				fmt := "Jan 2 2006"
				date, _ := time.Parse(fmt, "Dec 18 2013")
				So(data3.Date().Format(fmt), ShouldEqual, date.Format(fmt))

				So(data1.Open(), ShouldEqual, 101.0)
				So(data1.High(), ShouldEqual, 101.0)
				So(data1.Low(), ShouldEqual, 99.5)
				So(data1.Close(), ShouldEqual, 100.0)

			})
		})
	})
}
