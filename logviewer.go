package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNoLogs = errors.New("logs not found")
)

type Log struct {
	Id        uint64
	Ts        time.Time
	Type      uint8
	Message   string
	CreatedAt time.Time
}

type Filter [3]string

func (f Filter) Query() string {
	if f[0] == "" || f[1] == "" || f[2] == "" {
		return ""
	}
	return fmt.Sprintf("%s %s %s", f[0], f[1], f[2])
}

type Sort struct {
	Field     string
	Direction string
	Limit     int
}

func (s Sort) Query() string {
	if s.Field == "" {
		return " ORDER BY id ASC"
	}
	if !(strings.ToLower(s.Direction) == "asc" || strings.ToLower(s.Direction) == "desc") {
		s.Direction = "asc"
	}
	return fmt.Sprintf(" ORDER BY %s %s", s.Field, s.Direction)
}

type Cursor string

func (c Cursor) Query(sort Sort) string {
	cursor := c
	if cursor == "" {
		cursor = c.defaultVal(sort)
	}

	op := ">"
	if sort.Direction == "desc" {
		op = "<"
	}

	if sort.Field == "ts" {
		cursor = "'" + cursor + "'"
	}

	return fmt.Sprintf("%s %s %s", sort.Field, op, cursor)
}

func (c Cursor) defaultVal(sort Sort) Cursor {
	switch sort.Field {
	case "id":
		if sort.Direction == "asc" {
			return "0"
		} else {
			return ""
		}
	case "ts":
		if sort.Direction == "asc" {
			return "-infinity"
		} else {
			return "infinity"
		}
	}
	return ""
}

func GetLogs(db *sqlx.DB, cursor Cursor, sort Sort, filters []Filter) (logs []Log, err error) {
	if sort.Field == "" || sort.Direction != "asc" && sort.Direction != "desc" {
		return logs, fmt.Errorf("invalid sort")
	}

	query := "SELECT * FROM logs WHERE " + cursor.Query(sort)
	for _, f := range filters {
		query += " AND " + f.Query()
	}
	query += sort.Query()
	if sort.Limit != 0 {
		query += " LIMIT " + strconv.Itoa(sort.Limit)
	}
	if sort.Direction == "desc" {
		query = fmt.Sprintf("SELECT * FROM (%s) as x ORDER BY id ASC", query)
	}

	rows, err := db.Queryx(query)
	if err != nil {
		return logs, fmt.Errorf("failed to query to database: %v", err)
	}

	for rows.Next() {
		var log Log
		err := rows.StructScan(&log)
		if err != nil {
			return logs, fmt.Errorf("failed to scan struct: %v", err)
		}
		logs = append(logs, log)
	}

	return logs, nil
}

func GetTotalRows(db *sqlx.DB) (int, error) {
	var total int
	row := db.QueryRow("SELECT count(*) FROM logs")

	if errors.Is(row.Err(), sql.ErrNoRows) {
		return 0, nil
	}
	if row.Err() != nil {
		return 0, fmt.Errorf("failed to query to database: %v", row.Err())
	}

	if err := row.Scan(&total); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNoLogs
		}
		return 0, fmt.Errorf("failed to scan total rows: %v", err)
	}

	return total, nil
}

func GetCursorPosition(db *sqlx.DB, cursor Cursor, sort Sort) (int, error) {
	var position int
	if cursor == "" {
		cursor = cursor.defaultVal(sort)
	}
	if sort.Field == "ts" {
		cursor = "'" + cursor + "'"
	}
	query := fmt.Sprintf(
		"SELECT count(*) FROM (SELECT * FROM logs WHERE %s < %s %s) as x",
		sort.Field,
		cursor,
		sort.Query(),
	)
	row := db.QueryRow(query)
	if err := row.Scan(&position); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNoLogs
		}
		return 0, fmt.Errorf("failed to scan position: %v", err)
	}
	return position, nil
}

func GetCursor(db *sqlx.DB, page int, sort Sort, filters []Filter) (Cursor, error) {
	var cursor interface{}
	if sort.Field == "" || sort.Direction != "asc" && sort.Direction != "desc" {
		return "", fmt.Errorf("invalid sort")
	}
	offset := (page - 1) * sort.Limit
	if sort.Direction == "asc" {
		offset--
	}

	query := fmt.Sprintf("SELECT %s FROM logs", sort.Field)
	for i, f := range filters {
		if i == 0 {
			query += " WHERE " + f.Query()
		} else {
			query += " AND " + f.Query()
		}
	}
	query += fmt.Sprintf(" OFFSET %d LIMIT 1", offset)
	row := db.QueryRow(query)

	if err := row.Scan(&cursor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNoLogs
		}
		return "", fmt.Errorf("failed to scan position: %v", err)
	}

	switch cursor.(type) {
	case int64:
		return Cursor(strconv.FormatInt(cursor.(int64), 10)), nil
	case time.Time:
		return Cursor(cursor.(time.Time).Format(time.RFC3339Nano)), nil
	}
	return "", fmt.Errorf("could not convert scan result to Cursor")
}
