package engine_test

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
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
			},
			nil,
		)
	default:
		err = fmt.Errorf("table not found: %s", fqTableName)
	}

	return schema, err
}

var _ engine.Catalog = (*testCatalog)(nil)

func TestSimpleRead(t *testing.T) {
	name := "simple_read"
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	p := engine.NewReadOperation(table)

	runTestSerialize(
		t,
		name,
		p,
		&catalog,
	)

	runTestRoundTrip(t, name, p, &catalog)
}

func TestReadProject(t *testing.T) {
	name := "read_project"
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	r := engine.NewReadOperation(table)
	c := engine.NewColumnIndexExpr(1)
	p := engine.NewProjectionOperation(r, []engine.Expr{c})

	runTestSerialize(
		t,
		name,
		p,
		&catalog,
	)

	runTestRoundTrip(t, name, p, &catalog)
}

func TestReadFilterProject(t *testing.T) {
	name := "read_filter_project"
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	r := engine.NewReadOperation(table)
	c := engine.NewColumnIndexExpr(1)
	s := engine.NewSelectionOperation(r, c)
	p := engine.NewProjectionOperation(s, []engine.Expr{c})

	runTestSerialize(
		t,
		name,
		p,
		&catalog,
	)

	runTestRoundTrip(t, name, p, &catalog)
}

func TestReadFilter(t *testing.T) {
	name := "read_filter"
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	r := engine.NewReadOperation(table)
	c := engine.NewColumnIndexExpr(1)
	p := engine.NewSelectionOperation(r, c)

	runTestSerialize(
		t,
		name,
		p,
		&catalog,
	)

	runTestRoundTrip(t, name, p, &catalog)
}

func runTestSerialize(t *testing.T, name string, plan engine.Plan, catalog engine.Catalog) {
	unboundRelText, err := engine.FormatPlan(plan)
	if err != nil {
		unboundRelText = fmt.Sprintf("Error %s", err.Error())
	}

	engine.SetCatalogForPlan(plan, catalog)

	planSchema, err := plan.Schema()
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

func runTestRoundTrip(t *testing.T, name string, plan engine.Plan, catalog engine.Catalog) {
	engine.SetCatalogForPlan(plan, catalog)

	rel, err := plan.ToProto()
	require.NoError(t, err)

	planOut, err := engine.FromProto(rel)
	require.NoError(t, err)

	planText, err := engine.FormatPlan(plan)
	require.NoError(t, err)

	planOutText, err := engine.FormatPlan(planOut)
	require.NoError(t, err)

	require.Equal(t, planText, planOutText)
}
