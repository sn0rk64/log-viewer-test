package main

import (
	"fmt"
	"github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"os"
)

const (
	host     = "postgres"
	port     = 5432
	user     = "logger"
	password = "testpass"
	dbname   = "logger"
)

func main() {
	db, err := dbInit()
	if err != nil {
		fatal(fmt.Errorf("failed to connect to database: %v", err))
	}
	defer db.Close()

	if err = runMigrate(db); err != nil {
		fatal(fmt.Errorf("failed to migrate: %v", err))
	}

	if err = runServer(db); err != nil {
		fatal(fmt.Errorf("failed to run server: %v", err))
	}
}

func dbInit() (*sqlx.DB, error) {
	db, err := sqlx.Connect("pgx", fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, password, host, port, dbname))
	if err != nil {
		return nil, err
	}
	return db, nil
}

func runMigrate(db *sqlx.DB) error {
	_, err := db.Query("SELECT * from logs")
	if err != nil && err.(*pgconn.PgError).Code == "42P01" {
		query := `CREATE TABLE logs (
			id serial PRIMARY KEY,
			type SMALLINT NOT NULL,
			message TEXT NOT NULL,
			ts TIMESTAMP default clock_timestamp()
		);
		
		CREATE INDEX ts_index on logs (ts);
		CREATE INDEX ts_type_index on logs(ts,type);
		`
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "migration passed")
		return nil
	}
	return err
}

func fatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
