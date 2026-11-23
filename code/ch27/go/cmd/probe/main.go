// Chapter 27: the same capability probes in every client. Each line: case, result, detail.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apache/spark-connect-go/v40/spark/sql"
	F "github.com/apache/spark-connect-go/v40/spark/sql/functions"
	"github.com/apache/spark-connect-go/v40/spark/sql/types"
	"github.com/apache/spark-connect-go/v40/spark/sql/utils"
)

func report(name string, detail string, err error) {
	switch {
	case errors.Is(err, errNotOffered):
		fmt.Printf("%-12s n/o     %s\n", name, detail)
	case err != nil:
		fmt.Printf("%-12s breaks  %s\n", name, strings.SplitN(err.Error(), "\n", 2)[0])
	default:
		fmt.Printf("%-12s works   %s\n", name, detail)
	}
}

var errNotOffered = errors.New("not offered")

func main() {
	ctx := context.Background()
	remote := os.Getenv("SPARK_REMOTE")
	if remote == "" {
		remote = "sc://localhost:15106"
	}
	trips, work := os.Getenv("TRIPS"), os.Getenv("WORK")
	spark, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		panic(err)
	}
	defer spark.Stop()
	fmt.Println("go (spark-connect-go v0.1.0) -> server 4.3.0")
	rng := func(n int) sql.DataFrame {
		df, err := spark.Sql(ctx, fmt.Sprintf("SELECT id FROM range(%d)", n))
		if err != nil {
			panic(err)
		}
		return df
	}

	report(func() (string, string, error) {
		df, err := spark.Sql(ctx, "SELECT 1 AS x, 'a' AS y")
		if err != nil {
			return "sql", "", err
		}
		rows, err := df.Collect(ctx)
		if err != nil {
			return "sql", "", err
		}
		return "sql", fmt.Sprint(rows[0].Values()), nil
	}())
	report(func() (string, string, error) {
		df, err := spark.Read().Format("parquet").Load(trips)
		if err != nil {
			return "schema", "", err
		}
		s, err := df.Schema(ctx)
		if err != nil {
			return "schema", "", err
		}
		return "schema", fmt.Sprintf("%d columns, first %s", len(s.Fields), s.Fields[0].Name), nil
	}())
	report(func() (string, string, error) {
		// A LONG column takes a Go int: an int64 value panics in CreateDataFrame (v0.1.0).
		schema := types.StructOf(types.NewStructField("id", types.LONG), types.NewStructField("city", types.STRING))
		df, err := spark.CreateDataFrame(ctx, [][]any{{1, "Toronto"}, {2, "Hanoi"}}, schema)
		if err != nil {
			return "local_data", "", err
		}
		n, err := df.Count(ctx)
		return "local_data", fmt.Sprintf("count %d", n), err
	}())
	report(func() (string, string, error) {
		if err := rng(5).CreateOrReplaceTempView(ctx, "five"); err != nil {
			return "temp_view", "", err
		}
		df, err := spark.Sql(ctx, "SELECT count(*) AS n FROM five")
		if err != nil {
			return "temp_view", "", err
		}
		rows, err := df.Collect(ctx)
		if err != nil {
			return "temp_view", "", err
		}
		return "temp_view", fmt.Sprint(rows[0].Values()), nil
	}())
	report(func() (string, string, error) {
		path := work + "/go"
		if err := rng(10).Write().Mode("overwrite").Format("parquet").Save(ctx, path); err != nil {
			return "write_read", "", err
		}
		df, err := spark.Read().Format("parquet").Load(path)
		if err != nil {
			return "write_read", "", err
		}
		n, err := df.Count(ctx)
		return "write_read", fmt.Sprintf("read back %d rows", n), err
	}())
	report(func() (string, string, error) {
		if err := spark.Config().Set(ctx, "spark.sql.shuffle.partitions", "7"); err != nil {
			return "config", "", err
		}
		v, err := spark.Config().Get(ctx, "spark.sql.shuffle.partitions")
		return "config", fmt.Sprintf("%q", v), err
	}())
	report(func() (string, string, error) {
		df, err := spark.Read().Format("parquet").Load(trips)
		if err != nil {
			return "error", "", err
		}
		bad, err := df.Select(ctx, F.Col("revenue"))
		if err == nil {
			_, err = bad.Collect(ctx)
		}
		if err == nil {
			return "error", "", errors.New("no error")
		}
		s := err.Error()
		class, state := "?", "?"
		if strings.Contains(s, "UNRESOLVED_COLUMN") {
			class = "UNRESOLVED_COLUMN"
		}
		if strings.Contains(s, "42703") {
			state = "42703"
		}
		return "error", fmt.Sprintf("%s, SQLSTATE %s", class, state), nil
	}())
	report(func() (string, string, error) {
		ptr, err := rng(1000).ToArrow(ctx) // returns *arrow.Table, a pointer to an interface
		if err != nil {
			return "arrow", "", err
		}
		tbl := *ptr
		return "arrow", fmt.Sprintf("arrow.Table, %d chunk(s), %d rows", len(tbl.Column(0).Data().Chunks()), tbl.NumRows()), nil
	}())
	report(func() (string, string, error) {
		plan, err := rng(3).Explain(ctx, utils.ExplainModeSimple)
		if err != nil {
			return "explain", "", err
		}
		for _, l := range strings.Split(plan, "\n") {
			if strings.Contains(l, "Range") {
				return "explain", strings.TrimSpace(l), nil
			}
		}
		return "explain", "?", nil
	}())
	report("zip", "the DataFrame interface has no Zip method", errNotOffered)
	report(func() (string, string, error) {
		df, err := spark.Sql(ctx, "SELECT id, repeat('x', 100) AS pad FROM range(200000)")
		if err != nil {
			return "big_result", "", err
		}
		rows, err := df.Collect(ctx)
		return "big_result", fmt.Sprintf("%d rows", len(rows)), err
	}())
}
