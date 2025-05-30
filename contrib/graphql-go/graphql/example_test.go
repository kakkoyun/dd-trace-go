// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package graphql_test

import (
	"log"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"

	ddgraphql "github.com/DataDog/dd-trace-go/contrib/graphql-go/graphql/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

func Example() {
	tracer.Start()
	defer tracer.Stop()

	schema, err := ddgraphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"hello": &graphql.Field{
					Name: "hello",
					Type: graphql.NewNonNull(graphql.String),
				},
			},
		}),
	})
	if err != nil {
		panic(err)
	}

	http.Handle("/query", handler.New(&handler.Config{Schema: &schema}))
	log.Fatal(http.ListenAndServe(":8080", nil))

	// then:
	// $ curl -XPOST -d '{"query": "{ hello }"}' localhost:8080/query
}
