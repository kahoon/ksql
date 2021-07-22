// A simple extension to the golang database/sql package that facilitates getting
// row columns by name. The goal is to keep the existing sql package interface
// intact, yet allow the use of additional methods to satisfy the added functionality.
// This allows a seamless swap of the database/sql package with this one. Methods not list
// are directly inherited from database/sql
package ksql

import (
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Global pool of open databases to save from having to keep pointer references
var (
	poolMu sync.RWMutex
	pool   map[string]*DB
)

// Errors
var (
	ErrNoRows                      = sql.ErrNoRows
	ErrDupConnName                 = errors.New("ksql: duplicate database connection name")
	ErrColumnNotFound              = errors.New("ksql: column not found in result")
	ErrInvalidColumnTypeConversion = errors.New("ksql: invalid column type conversion")
)

func init() {
	pool = make(map[string]*DB)
}

// Get list of names of open database connections
func Databases() []string {
	poolMu.RLock()
	defer poolMu.RUnlock()
	var list []string
	for name := range pool {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// Get an open database connection by name
func Get(name string) (*DB, bool) {
	poolMu.RLock()
	defer poolMu.RUnlock()
	db, ok := pool[name]
	return db, ok
}

// Open a new database connection, and save the reference by name
func New(name, driver, dsn string) (*DB, error) {
	poolMu.Lock()
	defer poolMu.Unlock()
	// check if the name already exists
	if _, dup := pool[name]; dup {
		return nil, ErrDupConnName
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	pool[name] = &DB{db}
	return pool[name], nil
}

// Manage an already open database, and save the reference by name
func NewWithDB(name string, db *sql.DB) (*DB, error) {
	poolMu.Lock()
	defer poolMu.Unlock()
	// check if the name already exists
	if _, dup := pool[name]; dup {
		return nil, ErrDupConnName
	}
	pool[name] = &DB{db}
	return pool[name], nil
}

// Close all open databases connections.
func Close() {
	poolMu.Lock()
	defer poolMu.Unlock()
	for key := range pool {
		err := pool[key].DB.Close()
		if err == nil {
			delete(pool, key)
		}
	}
}

// Inherit database/sql.DB
type DB struct {
	*sql.DB
}

// Close this database connection
func (db *DB) Close() error {
	poolMu.Lock()
	defer poolMu.Unlock()
	err := db.DB.Close()
	if err != nil {
		return err
	}
	for key := range pool {
		if db == pool[key] {
			delete(pool, key)
			break
		}
	}
	return nil
}

func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

func (db *DB) Prepare(query string) (*Stmt, error) {
	stmt, err := db.DB.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt}, nil
}

func (db *DB) Query(query string, args ...interface{}) (*Rows, error) {
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows}, nil
}

func (db *DB) QueryRow(query string, args ...interface{}) *Row {
	rows, err := db.Query(query, args...)
	return &Row{rows: rows, err: err}
}

// Inherit database/sql.Rows
type Rows struct {
	*sql.Rows
	err     error
	columns []string
	loader  []interface{}
	values  map[string]interface{}
}

func (rs *Rows) Err() error {
	if err := rs.Rows.Err(); err != nil {
		return err
	}
	if err := rs.err; err != nil {
		return err
	}
	return nil
}

func (rs *Rows) Next() bool {
	if rs.err != nil {
		return false
	}
	if more := rs.Rows.Next(); !more {
		return false
	}
	if rs.columns == nil {
		if rs.columns, rs.err = rs.Rows.Columns(); rs.err != nil {
			return false
		}
		rs.loader = make([]interface{}, len(rs.columns))
		for i := range rs.loader {
			rs.loader[i] = new(interface{})
		}
		rs.values = make(map[string]interface{})
	}
	if rs.err = rs.Rows.Scan(rs.loader...); rs.err != nil {
		return false
	}
	for i := range rs.columns {
		rs.values[rs.columns[i]] = *(rs.loader[i]).(*interface{})
	}
	return true
}

func validateRows(rs *Rows, column string) error {
	if err := rs.Err(); err != nil {
		return err
	}
	if rs.values == nil {
		return ErrNoRows
	}
	if _, ok := rs.values[column]; !ok {
		return ErrColumnNotFound
	}
	return nil
}

func convertToInt(value interface{}) (int64, error) {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(value).Int(), nil
	}
	return 0, ErrInvalidColumnTypeConversion
}

func convertToDouble(value interface{}) (float64, error) {
	switch value.(type) {
	case float32, float64:
		return reflect.ValueOf(value).Float(), nil
	}
	return 0, ErrInvalidColumnTypeConversion
}

func convertToString(value interface{}) (string, error) {
	switch value.(type) {
	case string:
		return value.(string), nil
	}
	return "", ErrInvalidColumnTypeConversion
}

func convertToTime(value interface{}) (time.Time, error) {
	switch value.(type) {
	case time.Time:
		return value.(time.Time), nil
	}
	return time.Time{}, ErrInvalidColumnTypeConversion
}

// Get the boolean value in this row by column name
func (rs *Rows) GetBoolean(column string) (bool, error) {
	value, ok := rs.values[column].(bool)
	if !ok {
		return false, ErrInvalidColumnTypeConversion
	}
	return value, nil
}

// Get the integer  value in this row by column name
func (rs *Rows) GetInteger(column string) (int64, error) {
	if err := validateRows(rs, column); err != nil {
		return 0, err
	}
	value, err := convertToInt(rs.values[column])
	if err != nil {
		return 0, err
	}
	return value, nil
}

// Get the float value in this row by column name
func (rs *Rows) GetDouble(column string) (float64, error) {
	if err := validateRows(rs, column); err != nil {
		return 0, err
	}
	value, err := convertToDouble(rs.values[column])
	if err != nil {
		return 0, err
	}
	return value, nil
}

// Get the string value in this row by column name
func (rs *Rows) GetString(column string) (string, error) {
	if err := validateRows(rs, column); err != nil {
		return "", err
	}
	value, err := convertToString(rs.values[column])
	if err != nil {
		return "", err
	}
	return value, nil
}

// Get the time.Time value in this row by column name
func (rs *Rows) GetTime(column string) (time.Time, error) {
	if err := validateRows(rs, column); err != nil {
		return time.Time{}, err
	}
	value, err := convertToTime(rs.values[column])
	if err != nil {
		return time.Time{}, err
	}
	return value, nil
}

type Row struct {
	err  error
	next bool
	rows *Rows
}

func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	if err := r.rows.Close(); err != nil {
		return err
	}
	return nil
}

func next(r *Row) error {
	if r.next {
		return nil
	}
	r.next = true
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}
	if err := r.rows.Close(); err != nil {
		return err
	}
	return nil
}

// Get the boolean value in this row by column name
func (r *Row) GetBoolean(column string) (bool, error) {
	if err := next(r); err != nil {
		return false, err
	}
	return r.rows.GetBoolean(column)
}

// Get the integer value in this row by column name
func (r *Row) GetInteger(column string) (int64, error) {
	if err := next(r); err != nil {
		return 0, err
	}
	return r.rows.GetInteger(column)
}

// Get the float value in this row by column name
func (r *Row) GetDouble(column string) (float64, error) {
	if err := next(r); err != nil {
		return 0, err
	}
	return r.rows.GetDouble(column)
}

// Get the string value in this row by column name
func (r *Row) GetString(column string) (string, error) {
	if err := next(r); err != nil {
		return "", err
	}
	return r.rows.GetString(column)
}

// Get the time.Time value in this row by column name
func (r *Row) GetTime(column string) (time.Time, error) {
	if err := next(r); err != nil {
		return time.Time{}, err
	}
	return r.rows.GetTime(column)
}

type Stmt struct {
	*sql.Stmt
}

func (s *Stmt) Query(args ...interface{}) (*Rows, error) {
	rows, err := s.Stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows}, nil
}

func (s *Stmt) QueryRow(args ...interface{}) *Row {
	rows, err := s.Query(args...)
	return &Row{rows: rows, err: err}
}

type Tx struct {
	*sql.Tx
}

func (tx *Tx) Prepare(query string) (*Stmt, error) {
	stmt, err := tx.Tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{stmt}, nil
}

func (tx *Tx) Query(query string, args ...interface{}) (*Rows, error) {
	rows, err := tx.Tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: rows}, nil
}

func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	rows, err := tx.Query(query, args...)
	return &Row{rows: rows, err: err}
}

func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	return &Stmt{tx.Tx.Stmt(stmt.Stmt)}
}
