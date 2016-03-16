package ksql

import (
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"testing"
	"time"
)

func getPGHost() string {
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "192.168.1.10"
	}
	return host
}

func openTestConn(t *testing.T) error {
	db, err := New("test", "postgres", fmt.Sprintf("postgres://postgres:postgres@%s/test?sslmode=disable", getPGHost()))
	if err != nil {
		return err
	}
	_, err = db.Exec("drop table if exists people")
	if err != nil {
		return err
	}
	_, err = db.Exec("create table people (id integer not null,name text not null,married boolean not null,ratio double precision not null,last_modified timestamp not null,primary key(id))")
	if err != nil {
		return err
	}
	_, err = db.Exec("insert into people values (1,'john doe','t',3.14,'2016-01-02 03:04:05')")
	if err != nil {
		return err
	}
	return nil
}

func TestNew(t *testing.T) {
	defer Close()
	err := openTestConn(t)
	if err != nil {
		t.Fatal(err)
	}
	err = openTestConn(t)
	if err != ErrDupConnName {
		t.Fatalf("should have received a duplicate database name error")
	}
}

func TestDBQuery(t *testing.T) {
	err := openTestConn(t)
	if err != nil {
		t.Fatal(err)
	}
	defer Close()
	db, ok := Get("test")
	if !ok {
		t.Fatalf("database \"test\" not found!")
	}
	rows, err := db.Query("select * from people")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected a row of results, got none!")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	v1, err := rows.GetInteger("id")
	if err != nil {
		t.Errorf("failed to GetInteger on \"id\" column")
	}
	if v1 != 1 {
		t.Errorf("expected 1 for \"id\", got %d", v1)
	}
	v2, err := rows.GetString("name")
	if err != nil {
		t.Errorf("failed to GetString on \"name\" column")
	}
	if v2 != "john doe" {
		t.Errorf("expected \"john doe\" for \"name\", got \"%s\"", v2)
	}
	v3, err := rows.GetBoolean("married")
	if err != nil {
		t.Errorf("failed to GetBoolean on \"married\" column")
	}
	if v3 != true {
		t.Errorf("expected true for \"married\", got %v", v3)
	}
	v4, err := rows.GetDouble("ratio")
	if err != nil {
		t.Errorf("failed to GetDouble on \"ratio\" column")
	}
	if v4 != 3.14 {
		t.Errorf("expected 3.14 for \"ratio\", got %v", v4)
	}
	v5, err := rows.GetTime("last_modified")
	if err != nil {
		t.Errorf("failed to GetTime on \"last_modified\" column")
	}
	if !v5.Equal(time.Date(2016, time.January, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("expected 2016-01-02 03:04:05 for \"last_modified\", got %v", v5)
	}
}

func TestDBScan(t *testing.T) {
	err := openTestConn(t)
	if err != nil {
		t.Fatal(err)
	}
	defer Close()
	db, ok := Get("test")
	if !ok {
		t.Fatalf("database \"test\" not found!")
	}
	rows, err := db.Query("select * from people")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected a row of results, got none!")
	}
	var v1 int32
	var v2 string
	var v3 bool
	var v4 float64
	var v5 time.Time
	err = rows.Scan(&v1, &v2, &v3, &v4, &v5)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 1 {
		t.Errorf("expected 1 for \"id\", got %d", v1)
	}
	if v2 != "john doe" {
		t.Errorf("expected \"john doe\" for \"name\", got \"%s\"", v2)
	}
	if v3 != true {
		t.Errorf("expected true for \"married\", got %v", v3)
	}
	if v4 != 3.14 {
		t.Errorf("expected 3.14 for \"ratio\", got %v", v4)
	}
	if !v5.Equal(time.Date(2016, time.January, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("expected 2016-01-02 03:04:05 for \"last_modified\", got %v", v5)
	}
}

func TestDBQueryRow(t *testing.T) {
	err := openTestConn(t)
	if err != nil {
		t.Fatal(err)
	}
	defer Close()
	db, ok := Get("test")
	if !ok {
		t.Fatalf("database \"test\" not found!")
	}
	row := db.QueryRow("select * from people where id=1")
	v1, err := row.GetInteger("id")
	if err != nil {
		t.Errorf("failed to GetInteger on \"id\" column")
	}
	if v1 != 1 {
		t.Errorf("expected 1 for \"id\", got %d", v1)
	}
	v2, err := row.GetString("name")
	if err != nil {
		t.Errorf("failed to GetString on \"name\" column")
	}
	if v2 != "john doe" {
		t.Errorf("expected \"john doe\" for \"name\", got \"%s\"", v2)
	}
	v3, err := row.GetBoolean("married")
	if err != nil {
		t.Errorf("failed to GetBoolean on \"married\" column")
	}
	if v3 != true {
		t.Errorf("expected true for \"married\", got %v", v3)
	}
	v4, err := row.GetDouble("ratio")
	if err != nil {
		t.Errorf("failed to GetDouble on \"ratio\" column")
	}
	if v4 != 3.14 {
		t.Errorf("expected 3.14 for \"ratio\", got %v", v4)
	}
	v5, err := row.GetTime("last_modified")
	if err != nil {
		t.Errorf("failed to GetTime on \"last_modified\" column")
	}
	if !v5.Equal(time.Date(2016, time.January, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("expected 2016-01-02 03:04:05 for \"last_modified\", got %v", v5)
	}
}

func TestDBScanRow(t *testing.T) {
	err := openTestConn(t)
	if err != nil {
		t.Fatal(err)
	}
	defer Close()
	db, ok := Get("test")
	if !ok {
		t.Fatalf("database \"test\" not found!")
	}
	row := db.QueryRow("select * from people where id=1")
	var v1 int32
	var v2 string
	var v3 bool
	var v4 float64
	var v5 time.Time
	err = row.Scan(&v1, &v2, &v3, &v4, &v5)
	if err != nil {
		t.Fatal(err)
	}
	if v1 != 1 {
		t.Errorf("expected 1 for \"id\", got %d", v1)
	}
	if v2 != "john doe" {
		t.Errorf("expected \"john doe\" for \"name\", got \"%s\"", v2)
	}
	if v3 != true {
		t.Errorf("expected true for \"married\", got %v", v3)
	}
	if v4 != 3.14 {
		t.Errorf("expected 3.14 for \"ratio\", got %v", v4)
	}
	if !v5.Equal(time.Date(2016, time.January, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("expected 2016-01-02 03:04:05 for \"last_modified\", got %v", v5)
	}
}
