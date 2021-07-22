# ksql

[![GoDoc](https://godoc.org/github.com/kahoon/ksql?status.png)](https://godoc.org/github.com/kahoon/ksql)
[![Build Status](https://travis-ci.org/kahoon/ksql.svg?branch=master)](https://travis-ci.org/kahoon/ksql) [![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/kahoon/ksql/master/LICENSE)

A simple extension to the golang database/sql package that facilitates getting row columns by name. The goal is to keep the existing sql package interface intact, yet allow the use of additional methods to satisfy the added functionality.  This allows a seamless swap of the database/sql package with this one.

Feature highlights:

* Internal pool of database connections that allows getting a connection by name, and not have to keep a global pointer
* Support for queries with arbitrary number of result columns, get what you need.
* Get the last row of results even after the `Rows` are closed.


## Install

```
  go get github.com/kahoon/ksql
```
  
## Usage

```
package main

import (
	"fmt"
	"log"

	sql "github.com/kahoon/ksql"
	_ "github.com/lib/pq"
)

// >>CHANGE<<
const (
	HOSTNAME string = "192.168.1.10"
	DATABASE        = "test"
	USER            = "postgres"
	PASSWORD        = "postgres"
)

var (
	drop   = "drop table if exists people"
	schema = "create table people (id integer not null,name text not null,married boolean not null,last_modified timestamp not null,primary key(id))"
	data   = []string{
		"insert into people values (1,'John Doe','f','1980-12-01 01:02:03')",
		"insert into people values (2,'Jane Doe','t','1999-12-01 01:02:03')",
	}
	query = "select * from people"
)

func populate() {
	db, ok := sql.Get("master")
	if !ok {
		log.Fatalf("Database doesn't exist!\n")
	}
	_, err := db.Exec(drop)
	if err != nil {
        log.Fatal(err)
    }
	_, err = db.Exec(schema)
	if err != nil {
		log.Fatal(err)
	}
	for _, value := range data {
		_, err = db.Exec(value)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	// create a connection and name it
	db, err := sql.New("master", "postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", USER, PASSWORD, HOSTNAME, DATABASE))
	if err != nil {
		log.Fatal(err)
	}
	// close all open database on exit
	defer sql.Close()
	// populate the schema
	populate()
	// query the data
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("select failed %v", err)
	}
	for rows.Next() {
		id, err := rows.GetInteger("id")
		if err != nil {
			log.Fatal(err)
		}
		name, err := rows.GetString("name")
		if err != nil {
			log.Fatal(err)
		}
		last, err := rows.GetTime("last_modified")
		if err != nil {
			log.Fatal(err)
		}
		log.Println(id, name, last)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	row := db.QueryRow("select * from people where id=1")
	id, err := row.GetInteger("id")
	if err != nil {
		log.Fatal(err)
	}
	name, err := row.GetString("name")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(id,name)
}

```
