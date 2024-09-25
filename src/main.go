package main

import (
	"database/sql"
	"flag"

	"github.com/charmbracelet/log"
	_ "modernc.org/sqlite"
)

type config struct {
	LogLevel       log.Level
	CsvFilename    string
	SqliteLocation string
	MainTableName  string
}

func main() {
	// Get config
	flag.Parse()

	unnamedArgs := flag.Args()
	if len(unnamedArgs) != 1 {
		log.Fatalf("Incorrect number of arguments provided. Expecting a csv file as the only argument")
	}

	cfg := &config{
		LogLevel:       log.DebugLevel,
		CsvFilename:    unnamedArgs[0],
		SqliteLocation: ":memory:",
		MainTableName:  "csvimport",
	}

	log.SetLevel(cfg.LogLevel)

	// Open database
	db, err := sql.Open("sqlite", cfg.SqliteLocation)
	if err != nil {
		log.Fatalf("Could not open sqlite %s", err)
	}
	defer db.Close()

	// Load data
	c := &CsvToDb{
		Db:          db,
		CsvFilename: cfg.CsvFilename,
		TableName:   cfg.MainTableName,
	}

	err = c.Load()
	if err != nil {
		log.Fatalf("Could not load data from CSV to sqlite: %s", err)
	}

	// 1st Normal Form omitted. Maybe look for JSON fields in the future...

	// 2nd Normal Form
	n := &Nf2{Db: db}
	script, err := n.RecommendUpdate()
	if err != nil {
		log.Errorf("Could not generate script for 2NF: %s", err)
	}

	_, err = db.Exec(script)
	if err != nil {
		log.Fatalf("Could not execute script for 2NF: %s", err)
	}
}
