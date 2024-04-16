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
	"google.golang.org/protobuf/encoding/prototext"
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
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	p := engine.NewReadOperation(table)

	runTestSerialize(
		t,
		"simple_read",
		p,
		&catalog,
	)
}

func TestReadProject(t *testing.T) {
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	r := engine.NewReadOperation(table)
	c := engine.NewColumnExpr("col2")
	p := engine.NewProjectionOperation(r, []engine.Expr{c})

	runTestSerialize(
		t,
		"read_project",
		p,
		&catalog,
	)
}

func TestReadFilterProject(t *testing.T) {
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	r := engine.NewReadOperation(table)
	c := engine.NewColumnExpr("col2")
	s := engine.NewSelectionOperation(r, c)
	p := engine.NewProjectionOperation(s, []engine.Expr{c})

	runTestSerialize(
		t,
		"read_filter_project",
		p,
		&catalog,
	)
}

func TestReadFilter(t *testing.T) {
	catalog := testCatalog{}
	table := engine.NewNamedTable([]string{"test_db", "main", "table1"}, nil)

	r := engine.NewReadOperation(table)
	c := engine.NewColumnExpr("col2")
	s := engine.NewSelectionOperation(r, c)

	runTestSerialize(
		t,
		"read_filter",
		s,
		&catalog,
	)
}

func runTestSerialize(t *testing.T, name string, plan engine.Plan, catalog engine.Catalog) {
	var unboundRelText string
	unboundRel, err := plan.ToProto()
	if err == nil {
		unboundRelText = prototext.MarshalOptions{Multiline: true}.Format(unboundRel)
	} else {
		unboundRelText = err.Error()
	}

	engine.SetCatalogForPlan(plan, catalog)

	planSchema, err := plan.Schema()
	require.NoError(t, err)

	var boundRelText string
	boundRel, err := plan.ToProto()
	if err == nil {
		boundRelText = prototext.MarshalOptions{Multiline: true}.Format(boundRel)
	} else {
		boundRelText = err.Error()
	}

	actual := []byte(
		fmt.Sprintf(
			"Proto[Unbound]:\n%s\n\nRoot Schema:\n%s\n\nProto[Bound]:\n%s",
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
