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
	defer rows.Close()
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
	row := db.QueryRow("select * from people where id=1")
	id, err := row.GetInteger("id")
	if err != nil {
		log.Fatal(err)
	}
	name, err := row.GetString("name")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(id, name)
}
