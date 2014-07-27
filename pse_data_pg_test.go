package pse_data_pg

import (
	"testing"
	"time"
)

func TestSettingAndGetting(t *testing.T) {
	date, err := time.Parse("2006-01-02", "2013-05-17")
	if err != nil {
		t.Fatal(err)
	}
	row := DataRow{
		symbol: "AUB",
		date:   date,
		open:   101.50000,
		high:   105.10000,
		low:    101.50000,
		close:  104.00000,
		vol:    19989300,
	}

	if row.date != date {
		t.Fatalf("Date was not set for row.")
	}
}
