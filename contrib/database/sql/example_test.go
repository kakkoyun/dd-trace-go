// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package sql_test

import (
	"context"
	"log"

	sqltrace "github.com/DataDog/dd-trace-go/contrib/database/sql/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	sqlite "github.com/mattn/go-sqlite3" // Setup application to use Sqlite
)

func Example() {
	// The first step is to register the driver that we will be using.
	sqltrace.Register("postgres", &pq.Driver{})

	// Followed by a call to Open.
	db, err := sqltrace.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// Then, we continue using the database/sql package as we normally would, with tracing.
	rows, err := db.Query("SELECT name FROM users WHERE age=?", 27)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func Example_context() {
	tracer.Start()
	defer tracer.Stop()

	// Register the driver that we will be using (in this case mysql) under a custom service name.
	sqltrace.Register("mysql", &mysql.MySQLDriver{}, sqltrace.WithService("my-db"))

	// Open a connection to the DB using the driver we've just registered with tracing.
	db, err := sqltrace.Open("mysql", "user:password@/dbname")
	if err != nil {
		log.Fatal(err)
	}

	// Create a root span, giving name, server and resource.
	span, ctx := tracer.StartSpanFromContext(context.Background(), "my-query",
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.ServiceName("my-db"),
		tracer.ResourceName("initial-access"),
	)

	// Subsequent spans inherit their parent from context.
	rows, err := db.QueryContext(ctx, "SELECT * FROM city LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}
	rows.Close()
	span.Finish(tracer.WithError(err))
}

func Example_sqlite() {
	// Register the driver that we will be using (in this case Sqlite) under a custom service name.
	sqltrace.Register("sqlite", &sqlite.SQLiteDriver{}, sqltrace.WithService("sqlite-example"))

	// Open a connection to the DB using the driver we've just registered with tracing.
	db, err := sqltrace.Open("sqlite", "./test.db")
	if err != nil {
		log.Fatal(err)
	}

	// Create a root span, giving name, server and resource.
	span, ctx := tracer.StartSpanFromContext(context.Background(), "my-query",
		tracer.SpanType("example"),
		tracer.ServiceName("sqlite-example"),
		tracer.ResourceName("initial-access"),
	)

	// Subsequent spans inherit their parent from context.
	rows, err := db.QueryContext(ctx, "SELECT * FROM city LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}
	rows.Close()
	span.Finish(tracer.WithError(err))
}

func Example_dbmPropagation() {
	// The first step is to set the dbm propagation mode when registering the driver. Note that this can also
	// be done on sqltrace.Open for more granular control over the feature.
	sqltrace.Register("postgres", &pq.Driver{}, sqltrace.WithDBMPropagation(tracer.DBMPropagationModeFull))

	// Followed by a call to Open.
	db, err := sqltrace.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// Then, we continue using the database/sql package as we normally would, with tracing.
	rows, err := db.Query("SELECT name FROM users WHERE age=?", 27)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}

func Example_dbStats() {
	// Register the driver with the WithDBStats option to enable DBStats metric polling
	sqltrace.Register("postgres", &pq.Driver{}, sqltrace.WithDBStats())
	// Followed by a call to Open.
	db, err := sqltrace.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	// Tracing and metric polling is now enabled. Metrics  will be submitted to Datadog with the prefix `datadog.tracer.sql`
	rows, err := db.Query("SELECT name FROM users WHERE age=?", 27)
	if err != nil {
		log.Fatal(err)
	}
	rows.Close()
}
