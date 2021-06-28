package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"html/template"
	"math"
	"net/http"
	"strconv"
	"time"
)

var defaultSort = Sort{
	Field:     "id",
	Direction: "asc",
	Limit:     100,
}

type log struct {
	Log
	TsPretty string
}

type tmplData struct {
	FromCursor string
	ToCursor   string
	Logs       []log
}

func runServer(db *sqlx.DB) error {
	http.HandleFunc("/", indexHandler(db))
	http.HandleFunc("/showPage", showPageHandler(db))
	http.HandleFunc("/totalRows", totalRowsHandler(db))
	http.HandleFunc("/pageNumber", pageNumberHandler(db))
	return http.ListenAndServe(":8080", nil)
}

func indexHandler(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		var filters []Filter
		var cursor Cursor
		q := r.URL.Query()
		sort := defaultSort

		cursor = Cursor(q.Get("cursor"))
		if limit := q.Get("Limit"); limit != "" {
			sort.Limit, err = strconv.Atoi(limit)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to convert Limit to int: %v", err), http.StatusInternalServerError)
				return
			}
		}
		if sortField := q.Get("sort_field"); sortField != "" {
			sort.Field = sortField
		}
		if sortDirection := q.Get("sort_direction"); sortDirection != "" {
			sort.Direction = sortDirection
		}
		if typeFilter := q.Get("type_filter"); typeFilter != "" {
			filters = append(filters, Filter{"type", "=", typeFilter})
		}

		tmpl := template.Must(template.ParseFiles("index.html"))

		data, err := getLogList(db, cursor, sort, filters)
		if err != nil && !errors.Is(err, ErrNoLogs) {
			http.Error(w, fmt.Sprintf("failed to get log list: %v", err), http.StatusInternalServerError)
			return
		}

		err = tmpl.Execute(w, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func showPageHandler(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		var filters []Filter
		page := 1
		q := r.URL.Query()
		sort := defaultSort

		if limit := q.Get("Limit"); limit != "" {
			sort.Limit, err = strconv.Atoi(limit)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to convert Limit to int: %v", err), http.StatusInternalServerError)
				return
			}
		}
		if sortField := q.Get("sort_field"); sortField != "" {
			sort.Field = sortField
		}
		if sortDirection := q.Get("sort_direction"); sortDirection != "" {
			sort.Direction = sortDirection
		}
		if typeFilter := q.Get("type_filter"); typeFilter != "" {
			filters = append(filters, Filter{"type", "=", typeFilter})
		}
		if pg := q.Get("page"); pg != "" {
			page, err = strconv.Atoi(pg)
			if err != nil {
				http.Error(w, fmt.Sprintf("failed to convert page to int: %v", err), http.StatusInternalServerError)
				return
			}
		}

		cursor, err := GetCursor(db, page, sort, filters)
		if err != nil {
			if errors.Is(err, ErrNoLogs) {
				http.Redirect(w, r, "/", http.StatusMovedPermanently)
				return
			}
			http.Error(w, fmt.Sprintf("failed to get cursor: %v", err), http.StatusInternalServerError)
			return
		}

		tmpl := template.Must(template.ParseFiles("index.html"))

		data, err := getLogList(db, cursor, sort, filters)
		if err != nil {
			if errors.Is(err, ErrNoLogs) {
				http.Redirect(w, r, "/", http.StatusMovedPermanently)
				return
			}
			http.Error(w, fmt.Sprintf("failed to get log list: %v", err), http.StatusInternalServerError)
			return
		}

		err = tmpl.Execute(w, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func totalRowsHandler(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		totalRows, err := GetTotalRows(db)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to get total rows: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := map[string]int{
			"totalRows": totalRows,
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, fmt.Sprintf("failed to encode json: %v", err), http.StatusInternalServerError)
			return
		}
	}
}

func pageNumberHandler(db *sqlx.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var reqData struct {
			Sort   Sort
			Cursor Cursor
		}
		reqData.Sort = defaultSort
		page := 1

		err := json.NewDecoder(r.Body).Decode(&reqData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		curPos, err := GetCursorPosition(db, reqData.Cursor, reqData.Sort)
		if err != nil && !errors.Is(err, ErrNoLogs) {
			http.Error(w, fmt.Sprintf("failed to get cursor position: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		page = int(math.Ceil(float64(curPos) / float64(reqData.Sort.Limit)))
		if reqData.Sort.Direction == "asc" {
			page++
		}

		resp := map[string]int{
			"page": page,
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, fmt.Sprintf("failed to encode json: %v", err), http.StatusInternalServerError)
			return
		}
	}
}

func getLogList(db *sqlx.DB, cursor Cursor, sort Sort, filters []Filter) (tmplData, error) {
	var data tmplData
	logs, err := GetLogs(db, cursor, sort, filters)
	if err != nil {
		return data, fmt.Errorf("failed to get logs: %v", err)
	}

	if len(logs) == 0 {
		return data, ErrNoLogs
	}

	first := logs[0]
	last := logs[len(logs)-1]

	switch sort.Field {
	case "id":
		data.FromCursor = strconv.FormatUint(first.Id, 10)
		data.ToCursor = strconv.FormatUint(last.Id, 10)
	case "ts":
		data.FromCursor = first.Ts.Format(time.RFC3339Nano)
		data.ToCursor = last.Ts.Format(time.RFC3339Nano)
	}

	for _, l := range logs {
		data.Logs = append(data.Logs, log{
			Log:      l,
			TsPretty: l.Ts.Format("2006-01-02 15:04:05.000"),
		})
	}

	return data, nil
}
