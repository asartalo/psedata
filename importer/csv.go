package importer

import (
	ecsv "encoding/csv"
	pse "github.com/asartalo/psedata"
	"io"
	"strconv"
	"time"
)

type importedRecords struct {
	csvReader *ecsv.Reader
}

func toFloats(strings ...string) ([]float64, error) {
	var floats []float64
	for _, s := range strings {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return floats, err
		}
		floats = append(floats, f)
	}
	return floats, nil
}

func (recs *importedRecords) Next() (*pse.DailyRecord, error) {
	for {
		row, err := recs.csvReader.Read()
		if err != nil {
			return nil, err
		}

		date, err := time.Parse("20060102", row[1])
		if err != nil {
			return nil, err
		}

		f, err := toFloats(row[2], row[3], row[4], row[5])
		if err != nil {
			return nil, err
		}

		vol, err := strconv.ParseInt(row[6], 10, 0)
		if err != nil {
			return nil, err
		}
		d := pse.NewDailyRecord(
			row[0], date,
			f[0], f[1], f[2], f[3], int(vol),
		)
		return &d, nil
	}

	return nil, nil
}

type Csv interface {
	ImportHistoricalRecords(io.Reader) (pse.DailyRecords, error)
}

type csv struct{}

func (csv *csv) ImportHistoricalRecords(r io.Reader) (pse.DailyRecords, error) {
	records := new(importedRecords)
	records.csvReader = ecsv.NewReader(r)
	records.csvReader.Comment = '<'
	return records, nil
}

func NewCsvImporter() Csv {
	return new(csv)
}
