package query_test

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/joellubi/bonobo/engine"
	"github.com/joellubi/bonobo/sql"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"
)

var updateSQL = flag.Bool("update-sql", false, "update sql golden files")

type sqlTestCatalog struct{}

func (*sqlTestCatalog) Schema(identifier []string) (*arrow.Schema, error) {
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

var _ engine.Catalog = (*sqlTestCatalog)(nil)

var sqltestcases = []struct {
	Name  string
	Query string
}{
	{
		Name:  "simple_read",
		Query: "FROM test_db.main.table1",
	},
	{
		Name:  "read_project",
		Query: "SELECT col1, col2 FROM test_db.main.table1",
	},
	{
		Name:  "read_project_add",
		Query: "SELECT col3 + 3 FROM test_db.main.table1",
	},
	{
		Name:  "math_expr",
		Query: "SELECT 1 + 2",
	},
	{
		Name:  "math_multi_expr",
		Query: "SELECT 1 + 2 + 3",
	},
	{
		Name:  "read_project_filter",
		Query: "SELECT col1, col2 FROM test_db.main.table1 WHERE col1",
	},
	{
		Name:  "read_project_filter_subquery",
		Query: "SELECT col1, col2 FROM (SELECT col1, col2 FROM test_db.main.table1 WHERE col1)",
	},
	{
		Name:  "alias_column_names",
		Query: "SELECT col1 AS first, col2 second FROM test_db.main.table1",
	},
	{
		Name:  "alias_addition_expr",
		Query: "SELECT 1 + 2 AS three",
	},
}

func TestSqlToSubstrait(t *testing.T) {
	var catalog sqlTestCatalog
	for _, tc := range sqltestcases {
		t.Run(tc.Name, func(t *testing.T) {
			plan, err := sql.Parse(tc.Query)
			require.NoError(t, err)

			engine.SetCatalogForPlan(plan, &catalog)

			planText, err := engine.FormatPlan(plan)
			require.NoError(t, err)

			actual := []byte(fmt.Sprintf("SQL Query:\n\n%s\n\nSubstrait Plan:\n\n%s\n", tc.Query, planText))

			golden := filepath.Join("testdata", "sql_to_substrait", tc.Name+".golden")
			if *updateSQL {
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
		})
	}
}
