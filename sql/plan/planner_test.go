package plan_test

import (
	"testing"

	"github.com/backdeck/backdeck/query/df"
	"github.com/backdeck/backdeck/query/engine"
	"github.com/backdeck/backdeck/query/sql/parse"
	"github.com/backdeck/backdeck/query/sql/plan"
	"github.com/stretchr/testify/require"
)

var testcases = []struct {
	Name     string
	Input    *parse.SqlQuery
	Expected engine.Relation
}{
	{
		Name:  "named_table_read",
		Input: &parse.SqlQuery{Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"a"}})},
		Expected: df.QueryContext().
			Read(engine.NewNamedTable([]string{"a"}, nil)).
			LogicalPlan(),
	},
	{
		Name: "select_from_named_table",
		Input: &parse.SqlQuery{
			Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"b"}}),
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
				},
			),
		},
		Expected: df.QueryContext().
			Read(engine.NewNamedTable([]string{"b"}, nil)).
			Select(df.Col("a")).
			LogicalPlan(),
	},
	{
		Name: "select_multiple_from_named_table",
		Input: &parse.SqlQuery{
			Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"c"}}),
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
					&parse.SqlIdentifier{Names: []string{"b"}},
				},
			),
		},
		Expected: df.QueryContext().
			Read(engine.NewNamedTable([]string{"c"}, nil)).
			Select(
				df.Col("a"),
				df.Col("b"),
			).
			LogicalPlan(),
	},
	{
		Name: "select_multiple_add_int_from_named_table",
		Input: &parse.SqlQuery{
			Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"c"}}),
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{Names: []string{"b"}},
						Op:    "+",
						Right: &parse.SqlIntLiteral{Value: 1},
					},
				},
			),
		},
		Expected: df.QueryContext().
			Read(engine.NewNamedTable([]string{"c"}, nil)).
			Select(
				df.Col("a"),
				df.Add(df.Col("b"), df.Lit(1)),
			).
			LogicalPlan(),
	},
	{
		Name: "select_from_named_table_subquery",
		Input: &parse.SqlQuery{
			Read: parse.SqlFromRelation(
				&parse.SqlQuery{
					Projection: parse.SqlSelectRelation(
						[]parse.SqlExpr{
							&parse.SqlIdentifier{Names: []string{"a"}},
						},
					),
					Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"b"}}),
				},
			),
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
				},
			),
		},
		Expected: df.QueryContext().
			Read(engine.NewNamedTable([]string{"b"}, nil)).
			Select(df.Col("a")).
			Select(df.Col("a")).
			LogicalPlan(),
	},
}

func TestPlanner(t *testing.T) {
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			p, err := plan.CreateLogicalPlan(tc.Input)
			require.NoError(t, err)
			require.Equal(t, tc.Expected, p)
		})
	}
}
