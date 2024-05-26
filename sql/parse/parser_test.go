package parse_test

import (
	"testing"

	"github.com/joellubi/bonobo/sql/parse"
	"github.com/joellubi/bonobo/sql/token"

	"github.com/stretchr/testify/require"
)

var testcases = []struct {
	Name     string
	Input    []token.Token
	Expected *parse.SqlQuery
	Error    bool
}{
	{
		Name: "SELECT a + b",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "b"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{Names: []string{"a"}},
						Op:    "+",
						Right: &parse.SqlIdentifier{Names: []string{"b"}},
					},
				},
			),
		},
	},
	{
		Name: "SELECT 1 * 2 + 3, 4 / 5",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.INT, Val: "1"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "2"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "3"},
			{Name: token.COMMA, Val: ","},
			{Name: token.INT, Val: "4"},
			{Name: token.QUO, Val: "/"},
			{Name: token.INT, Val: "5"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left: &parse.SqlBinaryExpr{
							Left:  &parse.SqlIntLiteral{Value: 1},
							Op:    "*",
							Right: &parse.SqlIntLiteral{Value: 2},
						},
						Op:    "+",
						Right: &parse.SqlIntLiteral{Value: 3},
					},
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIntLiteral{Value: 4},
						Op:    "/",
						Right: &parse.SqlIntLiteral{Value: 5},
					},
				},
			),
		},
	},
	{
		Name: "SELECT person.age + factor, person.weight FROM prod_db.public.patients",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "person"},
			{Name: token.PERIOD, Val: "."},
			{Name: token.IDENT, Val: "age"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "factor"},
			{Name: token.COMMA, Val: ","},
			{Name: token.IDENT, Val: "person"},
			{Name: token.PERIOD, Val: "."},
			{Name: token.IDENT, Val: "weight"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "prod_db"},
			{Name: token.PERIOD, Val: "."},
			{Name: token.IDENT, Val: "public"},
			{Name: token.PERIOD, Val: "."},
			{Name: token.IDENT, Val: "patients"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{Names: []string{"person", "age"}},
						Op:    "+",
						Right: &parse.SqlIdentifier{Names: []string{"factor"}},
					},
					&parse.SqlIdentifier{Names: []string{"person", "weight"}},
				},
			),
			Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"prod_db", "public", "patients"}}),
		},
	},
	{
		Name: "a + b FROM c",
		Input: []token.Token{
			{Name: token.IDENT, Val: "a"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "c"},
		},
		Error: true,
	},
	{
		Name: "SELECT a FROM b + c",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "c"},
		},
		Error: true,
	},
	{
		Name: "SELECT a + b FROM c WHERE d > 1",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "c"},
			{Name: token.WHERE, Val: "WHERE"},
			{Name: token.IDENT, Val: "d"},
			{Name: token.GTR, Val: ">"},
			{Name: token.INT, Val: "1"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{Names: []string{"a"}},
						Op:    "+",
						Right: &parse.SqlIdentifier{Names: []string{"b"}},
					},
				},
			),
			Read: parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"c"}}),
			Filter: parse.SqlWhereRelation(
				&parse.SqlBinaryExpr{
					Left:  &parse.SqlIdentifier{Names: []string{"d"}},
					Op:    ">",
					Right: &parse.SqlIntLiteral{Value: 1},
				},
			),
		},
	},
	{
		Name: "SELECT (1 + 2) * 3",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.INT, Val: "1"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "2"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "3"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left: &parse.SqlBinaryExpr{
							Left:  &parse.SqlIntLiteral{Value: 1},
							Op:    "+",
							Right: &parse.SqlIntLiteral{Value: 2},
						},
						Op:    "*",
						Right: &parse.SqlIntLiteral{Value: 3},
					},
				},
			),
		},
	},
	{
		Name: "SELECT (((1 + 2) * 3) + 4) * 5",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.LPAREN, Val: "("},
			{Name: token.LPAREN, Val: "("},
			{Name: token.INT, Val: "1"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "2"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "3"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "4"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "5"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left: &parse.SqlBinaryExpr{
							Left: &parse.SqlBinaryExpr{
								Left: &parse.SqlBinaryExpr{
									Left:  &parse.SqlIntLiteral{Value: 1},
									Op:    "+",
									Right: &parse.SqlIntLiteral{Value: 2},
								},
								Op:    "*",
								Right: &parse.SqlIntLiteral{Value: 3},
							},
							Op:    "+",
							Right: &parse.SqlIntLiteral{Value: 4},
						},
						Op:    "*",
						Right: &parse.SqlIntLiteral{Value: 5},
					},
				},
			),
		},
	},
	{
		Name: "SELECT (1 + 2) * 3)",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.INT, Val: "1"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "2"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "3"},
			{Name: token.RPAREN, Val: ")"},
		},
		Error: true,
	},
	{
		Name: "SELECT a FROM (SELECT a FROM b)",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.RPAREN, Val: ")"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
				},
			),
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
		},
	},
	{
		Name: "SELECT a FROM (SELECT a FROM (SELECT a FROM b))",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.RPAREN, Val: ")"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
				},
			),
			Read: parse.SqlFromRelation(
				&parse.SqlQuery{
					Projection: parse.SqlSelectRelation(
						[]parse.SqlExpr{
							&parse.SqlIdentifier{Names: []string{"a"}},
						},
					),
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
				},
			),
		},
	},
	{
		Name: "SELECT a FROM (SELECT a FROM (SELECT a FROM b)) WHERE b.c = 5",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.LPAREN, Val: "("},
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.RPAREN, Val: ")"},
			{Name: token.WHERE, Val: "WHERE"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.PERIOD, Val: "."},
			{Name: token.IDENT, Val: "c"},
			{Name: token.EQL, Val: "="},
			{Name: token.INT, Val: "5"},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}},
				},
			),
			Read: parse.SqlFromRelation(
				&parse.SqlQuery{
					Projection: parse.SqlSelectRelation(
						[]parse.SqlExpr{
							&parse.SqlIdentifier{Names: []string{"a"}},
						},
					),
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
				},
			),
			Filter: parse.SqlWhereRelation(
				&parse.SqlBinaryExpr{
					Left:  &parse.SqlIdentifier{Names: []string{"b", "c"}},
					Op:    "=",
					Right: &parse.SqlIntLiteral{Value: 5},
				},
			),
		},
	},
	{
		Name: "SELECT a AS b, c d FROM (SELECT t.a, t.c FROM my_schema.my_table t) AS e",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 1},
			{Name: token.IDENT, Val: "a", Pos: 2},
			{Name: token.AS, Val: "AS", Pos: 3},
			{Name: token.IDENT, Val: "b", Pos: 4},
			{Name: token.COMMA, Val: ",", Pos: 5},
			{Name: token.IDENT, Val: "c", Pos: 6},
			{Name: token.IDENT, Val: "d", Pos: 7},
			{Name: token.FROM, Val: "FROM", Pos: 8},
			{Name: token.LPAREN, Val: "(", Pos: 9},
			{Name: token.SELECT, Val: "SELECT", Pos: 10},
			{Name: token.IDENT, Val: "t", Pos: 11},
			{Name: token.PERIOD, Val: ".", Pos: 12},
			{Name: token.IDENT, Val: "a", Pos: 13},
			{Name: token.COMMA, Val: ",", Pos: 14},
			{Name: token.IDENT, Val: "t", Pos: 15},
			{Name: token.PERIOD, Val: ".", Pos: 16},
			{Name: token.IDENT, Val: "c", Pos: 17},
			{Name: token.FROM, Val: "FROM", Pos: 18},
			{Name: token.IDENT, Val: "my_schema", Pos: 19},
			{Name: token.PERIOD, Val: ".", Pos: 20},
			{Name: token.IDENT, Val: "my_table", Pos: 21},
			{Name: token.IDENT, Val: "t", Pos: 22},
			{Name: token.RPAREN, Val: ")", Pos: 23},
			{Name: token.AS, Val: "AS", Pos: 24},
			{Name: token.IDENT, Val: "e", Pos: 25},
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlIdentifier{Names: []string{"a"}, Alias: "b"},
					&parse.SqlIdentifier{Names: []string{"c"}, Alias: "d"},
				},
			),
			Read: parse.SqlFromRelation(
				&parse.SqlQuery{
					Projection: parse.SqlSelectRelation(
						[]parse.SqlExpr{
							&parse.SqlIdentifier{Names: []string{"t", "a"}},
							&parse.SqlIdentifier{Names: []string{"t", "c"}},
						},
					),
					Read:  parse.SqlFromRelation(&parse.SqlIdentifier{Names: []string{"my_schema", "my_table"}, Alias: "t"}),
					Alias: "e",
				},
			),
		},
	},
	{
		Name: "SELECT a AS",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 1},
			{Name: token.IDENT, Val: "a", Pos: 2},
			{Name: token.AS, Val: "AS", Pos: 3},
		},
		Error: true,
	},
}

func TestQueryParser(t *testing.T) {
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			tokens := token.NewListTokenStream(tc.Input)
			query, err := parse.Parse(tokens)

			if tc.Error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.Expected, query)
			}
		})
	}
}
