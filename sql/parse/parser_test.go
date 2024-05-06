package parse_test

import (
	"testing"

	"github.com/backdeck/backdeck/query/sql/parse"
	"github.com/backdeck/backdeck/query/sql/token"
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
			}

			require.Equal(t, tc.Expected, query)
		})
	}
}
