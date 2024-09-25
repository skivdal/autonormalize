package main

import (
	"database/sql"

	"github.com/charmbracelet/log"
)

type Nf2 struct {
	Db *sql.DB
}

// Traverses the database, returning a sql script for altering table definitions and transferring data
func (n *Nf2) RecommendUpdate() (string, error) {
	log.Info("Running tests for 2NF...")
	return "", nil;
}

