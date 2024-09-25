package main

import (
	"database/sql"
	"encoding/csv"
	"os"
	"slices"
	"strings"

	"github.com/charmbracelet/log"
)

type CsvToDb struct {
	Db          *sql.DB
	CsvFilename string
	TableName   string
}

func (c *CsvToDb) Load() error {
	records, err := c.readCsv()
	if err != nil {
		log.Errorf("Error in reading csv %s", c.CsvFilename)
		return err
	}

	schema := c.csvToSqlTable(records[0], records[1:])
	
	if _, err = c.Db.Exec(schema); err != nil {
		log.Errorf("Error in creating sql table")
		return err
	}

	err = c.batchCsvLoad(records[0], records[1:])
	return err
}

// NOTE: file needs to fit in memory
func (c *CsvToDb) readCsv() ([][]string, error) {
	f, err := os.Open(c.CsvFilename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	log.Infof("CSV header: %s", records[0])

	return records, nil
}

// TODO: handle not null / null
// TODO: detect appropriate types (everything should *not* be text)
// TODO: also maybe escape illegal column names etc...
func (c *CsvToDb) csvToSqlTable(fieldNames []string, _data [][]string) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(c.TableName)
	sb.WriteString(" (\n")

	for i, name := range fieldNames {
		if i != 0 {
			sb.WriteString(", \n")
		}

		sb.WriteRune('\t')
		sb.WriteString(name)
	}

	sb.WriteString("\n);")

	schema := sb.String()

	log.Infof("Created schema from CSV:\n%s", schema)

	return schema
}

func (c *CsvToDb) batchCsvLoad(fieldNames []string, data [][]string) error {
	batchSize := (900 / len(fieldNames))
	batchCount := (len(data) / batchSize) + 1

	log.Infof("Loading %d rows of data, %d fields per row", len(data), len(fieldNames))
	log.Debugf("Row count: %d", len(fieldNames))
	log.Debugf("Row length: %d", len(data))
	log.Debugf("Batch size: %d", batchSize)
	log.Debugf("Batch count: %d", batchCount)

	for i := range batchCount {
		start := batchSize * i
		end := min(len(data), batchSize*(i+1))

		err := c.loadCsv(fieldNames, data[start:end])
		if err != nil {
			log.Errorf("Error at iteration i: %d, batchCount: %d", i, batchCount)
			return err
		}
	}

	return nil
}

// TODO: handle different datatypes
// WARNING: this crashes at more than ~1000 data points (rows * row length)
func (c *CsvToDb) loadCsv(fieldNames []string, data [][]string) error {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(c.TableName)
	sb.WriteRune('(')

	for i, n := range fieldNames {
		if i != 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(n)
	}
	sb.WriteString(") VALUES ")

	for i := range len(data) {
		if i != 0 {
			sb.WriteString(", ")
		}

		sb.WriteRune('(')
		for j := range len(fieldNames) {
			if j != 0 {
				sb.WriteString(", ")
			}

			sb.WriteString("? ")
		}
		sb.WriteRune(')')
	}
	sb.WriteRune(';')

	q := sb.String()

	flatData := slices.Concat(data...)
	d := make([]interface{}, len(flatData))
	for i := range flatData {
		d[i] = flatData[i]
	}

	_, err := c.Db.Exec(q, d...)
	if err != nil {
		return err
	}

	return nil
}
