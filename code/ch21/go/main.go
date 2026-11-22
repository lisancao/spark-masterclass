// Chapter 21: the five busiest pickup zones in June 2026, from the Go client.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/spark-connect-go/v40/spark/sql"
	F "github.com/apache/spark-connect-go/v40/spark/sql/functions"
)

func main() {
	remote := os.Getenv("SPARK_REMOTE")
	if remote == "" {
		remote = "sc://localhost:15106"
	}
	t0 := time.Now()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	must(err)
	defer spark.Stop()
	// The session has no Version() method, so the version comes from SQL.
	v, err := spark.Sql(ctx, "SELECT split(version(), ' ')[0]")
	must(err)
	rows, err := v.Collect(ctx)
	must(err)
	fmt.Printf("spark-connect-go v0.1.0 -> server %v at %s\n", rows[0].At(0), remote)

	trips, err := spark.Read().Format("parquet").Load(os.Getenv("TRIPS"))
	must(err)
	pickup := F.Col("tpep_pickup_datetime")
	// The Column type has no And(), so the two bounds are two filters.
	june, err := trips.Filter(ctx, pickup.Ge(F.StringLit("2026-06-01")))
	must(err)
	june, err = june.Filter(ctx, pickup.Lt(F.StringLit("2026-07-01")))
	must(err)
	byZone, err := june.GroupBy(F.Col("PULocationID")).Agg(ctx,
		F.Count(F.Int64Lit(1)).Alias("trips"),
		F.Round(F.Avg(F.Col("fare_amount")), 2).Alias("avg_fare"))
	must(err)
	top, err := byZone.OrderBy(ctx, F.Col("trips").Desc())
	must(err)
	must(top.Limit(ctx, 5).Show(ctx, 5, false))
	fmt.Printf("wall time %.2f s\n", time.Since(t0).Seconds())
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
