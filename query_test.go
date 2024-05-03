package query_test

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/backdeck/backdeck/query/df"
	"github.com/backdeck/backdeck/query/engine"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files")

type testCatalog struct{}

func (*testCatalog) Schema(identifier []string) (*arrow.Schema, error) {
	var (
		schema *arrow.Schema
		err    error
	)
	fqTableName := strings.Join(identifier, ".")

	switch fqTableName {
	case "test_db.main.table1":
		schema = arrow.NewSchema(
			[]arrow.Field{
				{Name: "col1", Type: engine.ArrowTypes.BooleanType},
				{Name: "col2", Type: engine.ArrowTypes.StringType},
				{Name: "col3", Type: engine.ArrowTypes.Int64Type},
				{Name: "col4", Type: engine.ArrowTypes.Decimal(38, 8)},
				{Name: "col5", Type: engine.ArrowTypes.DateType},
			},
			nil,
		)
	default:
		err = fmt.Errorf("table not found: %s", fqTableName)
	}

	return schema, err
}

var _ engine.Catalog = (*testCatalog)(nil)

// var mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
var testcases = []struct {
	Name      string
	DataFrame df.DataFrame
	Catalog   engine.Catalog
}{
	{
		Name: "simple_read",
		DataFrame: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			),
		Catalog: &testCatalog{},
	},
	{
		Name: "read_project",
		DataFrame: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Select(df.ColIdx(1)),
		Catalog: &testCatalog{},
	},
	{
		Name: "read_filter_project",
		DataFrame: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Filter(df.ColIdx(1)).
			Select(df.ColIdx(1)),
		Catalog: &testCatalog{},
	},
	{
		Name: "read_filter",
		DataFrame: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Filter(df.ColIdx(1)),
		Catalog: &testCatalog{},
	},
	{
		Name: "read_project_plus_one",
		DataFrame: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Select(df.Add(df.ColIdx(2), df.Lit(1))),
		Catalog: &testCatalog{},
	},
}

func TestDataFrame(t *testing.T) {
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			plan := tc.DataFrame.LogicalPlan()

			t.Run("serialize", func(t *testing.T) {
				runTestSerialize(t, tc.Name, plan, tc.Catalog)
			})

			t.Run("roundtrip", func(t *testing.T) {
				runTestRoundTrip(t, tc.Name, plan, tc.Catalog)
			})

		})
	}
	// mem.AssertSize(t, 0)
}

func runTestSerialize(t *testing.T, name string, relation engine.Relation, catalog engine.Catalog) {
	plan := engine.NewPlan(relation)
	unboundRelText, err := engine.FormatPlan(plan)
	if err != nil {
		unboundRelText = fmt.Sprintf("Error %s", err.Error())
	}

	engine.SetCatalogForPlan(plan, catalog)

	planSchema, err := relation.Schema()
	require.NoError(t, err)

	boundRelText, err := engine.FormatPlan(plan)
	if err != nil {
		boundRelText = fmt.Sprintf("Error: %s", err.Error())
	}

	actual := []byte(
		fmt.Sprintf(
			"Unbound Proto:\n%s\n\nBound Root Schema:\n%s\n\nBound Proto:\n%s",
			unboundRelText,
			planSchema,
			boundRelText,
		),
	)

	golden := filepath.Join("testdata", "serialization", name+".golden")
	if *update {
		err := os.WriteFile(golden, actual, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	expected, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(actual, expected) {
		t.Errorf("formatted output does not match\nexpected: %s\nfound: %s", expected, actual)
	}
}

func runTestRoundTrip(t *testing.T, name string, relation engine.Relation, catalog engine.Catalog) {
	plan := engine.NewPlan(relation)
	engine.SetCatalogForPlan(plan, catalog)

	p, err := plan.ToProto()
	require.NoError(t, err)

	planOut, err := engine.FromProto(p)
	require.NoError(t, err)

	planText, err := engine.FormatPlan(plan)
	require.NoError(t, err)

	planOutText, err := engine.FormatPlan(planOut)
	require.NoError(t, err)

	require.Equal(t, planText, planOutText)
}
