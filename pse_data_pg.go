package pse_data_pg

import "time"

type DataRow struct {
	symbol string
	date   time.Time
	open   float64
	high   float64
	low    float64
	close  float64
	vol    int
}
