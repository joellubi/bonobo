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
	"github.com/substrait-io/substrait-go/proto"
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

var testcases = []struct {
	Name           string
	Input          df.DataFrame
	ExpectedOutput df.DataFrame
	Catalog        engine.Catalog
}{
	{
		Name: "simple_read",
		Input: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			),
		ExpectedOutput: df.QueryContext().
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
		Input: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Select(df.ColIdx(1)),
		ExpectedOutput: df.QueryContext().
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
		Input: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Filter(df.ColIdx(1)).
			Select(df.ColIdx(1)),
		ExpectedOutput: df.QueryContext().
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
		Input: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Filter(df.ColIdx(1)),
		ExpectedOutput: df.QueryContext().
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
		Name: "read_project_plus_one", // TODO: Test serialize_deserialize
		Input: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Select(df.Add(df.ColIdx(2), df.Lit(1))),
		ExpectedOutput: df.QueryContext().
			Read(
				engine.NewNamedTable(
					[]string{"test_db", "main", "table1"},
					nil,
				),
			).
			Select(df.Add(df.ColIdx(2), df.Lit(1))),
		Catalog: &testCatalog{},
	},
	// {
	// 	Name: "read_project_plus_one_alias",
	// 	Input: df.QueryContext().
	// 		Read(
	// 			engine.NewNamedTable(
	// 				[]string{"test_db", "main", "table1"},
	// 				nil,
	// 			),
	// 		).
	// 		Select(
	// 			df.As(
	// 				df.Add(df.ColIdx(2), df.Lit(1)),
	// 				"custom_name",
	// 			),
	// 		),
	// 	ExpectedOutput: df.QueryContext().
	// 		Read(
	// 			engine.NewNamedTable(
	// 				[]string{"test_db", "main", "table1"},
	// 				nil,
	// 			),
	// 		).
	// 		Select(
	// 			df.As(
	// 				df.Add(df.ColIdx(2), df.Lit(1)),
	// 				"custom_name",
	// 			),
	// 		),
	// 	Catalog: &testCatalog{},
	// },
}

func TestDataFrame(t *testing.T) {
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			plan := engine.NewPlan(tc.Input.LogicalPlan())
			engine.SetCatalogForPlan(plan, tc.Catalog)

			var planProto *proto.Plan
			t.Run("serialize", func(t *testing.T) {
				schema, err := plan.Relations()[0].Schema() // Expecting first relation is root, TODO: cleanup
				require.NoError(t, err)

				planProto, err = plan.ToProto()
				require.NoError(t, err)

				formatted, err := engine.FormatPlanProto(planProto)
				require.NoError(t, err)

				actual := []byte(
					fmt.Sprintf(
						"Root Schema:\n%s\n\nProto:\n%s",
						schema,
						formatted,
					),
				)

				golden := filepath.Join("testdata", "serialization", tc.Name+".golden")
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
			})

			t.Run("deserialize", func(t *testing.T) {
				deserializedPlan, err := engine.FromProto(planProto)
				require.NoError(t, err)
				deserializedText, err := engine.FormatPlan(deserializedPlan)
				require.NoError(t, err)

				expectedPlan := engine.NewPlan(tc.ExpectedOutput.LogicalPlan())
				engine.SetCatalogForPlan(expectedPlan, tc.Catalog)

				expectedText, err := engine.FormatPlan(expectedPlan)
				require.NoError(t, err)

				require.Equal(t, expectedText, deserializedText)
			})

		})
	}
}
