// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package sqlx_test

import (
	"log"

	sqltrace "github.com/DataDog/dd-trace-go/contrib/database/sql/v2"
	sqlxtrace "github.com/DataDog/dd-trace-go/contrib/jmoiron/sqlx/v2"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func ExampleOpen() {
	// Register informs the sqlxtrace package of the driver that we will be using in our program.
	// It uses a default service name, in the below case "postgres.db". To use a custom service
	// name use RegisterWithServiceName.
	sqltrace.Register("postgres", &pq.Driver{}, sqltrace.WithService("my-service"))
	db, err := sqlxtrace.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// All calls through sqlx API will then be traced.
	query, args, err := sqlx.In("SELECT * FROM users WHERE level IN (?);", []int{4, 6, 7})
	if err != nil {
		log.Fatal(err)
	}
	query = db.Rebind(query)
	rows, err := db.Queryx(query, args...)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}
