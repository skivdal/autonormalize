package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"os"
	"slices"
	"strings"

	"github.com/charmbracelet/log"
	_ "modernc.org/sqlite"
)

// NOTE: file needs to fit in memory
func readCsv(filename string) ([][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
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
func csvToSqlTable(tableName string, fieldNames []string, _data [][]string) (string, error) {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(tableName)
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

	return schema, nil
}

// TODO: handle different datatypes
// WARNING: this crashes at more than ~1000 data points (rows * row length)
func loadCsv(db *sql.DB, tableName string, fieldNames []string, data [][]string) error {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
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

	_, err := db.Exec(q, d...)
	if err != nil {
		return err
	}

	return nil
}

func batchCsvLoad(db *sql.DB, tableName string, fieldNames []string, data [][]string) error {
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

		err := loadCsv(db, tableName, fieldNames, data[start:end])
		if err != nil {
			log.Errorf("Error at iteration i: %d, batchCount: %d", i, batchCount)
			return err
		}
	}

	return nil
}

type config struct {
	logLevel       log.Level
	csvFilename    string
	sqliteLocation string
	mainTableName  string
}

func main() {
	flag.Parse()

	unnamedArgs := flag.Args()
	if len(unnamedArgs) != 1 {
		log.Fatalf("Incorrect number of arguments provided. Expecting a csv file as the only argument")
	}

	cfg := &config{
		logLevel:       log.DebugLevel,
		csvFilename:    unnamedArgs[0],
		sqliteLocation: ":memory:",
		mainTableName:  "csvimport",
	}

	log.SetLevel(cfg.logLevel)

	records, err := readCsv(cfg.csvFilename)

	db, err := sql.Open("sqlite", cfg.sqliteLocation)
	if err != nil {
		log.Fatalf("Could not open sqlite %s", err)
	}
	defer db.Close()

	schema, err := csvToSqlTable(cfg.mainTableName, records[0], records[1:])

	if _, err = db.Exec(schema); err != nil {
		log.Fatalf("Could not create sqlite table %s", err)
	}

	batchCsvLoad(db, cfg.mainTableName, records[0], records[1:])
}
