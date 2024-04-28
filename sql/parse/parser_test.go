package parse_test

import (
	"testing"

	"github.com/backdeck/backdeck/query/sql/parse"
	"github.com/backdeck/backdeck/query/sql/token"
	"github.com/stretchr/testify/require"
)

var exprParserTestcases = []struct {
	Name     string
	Input    []token.Token
	Expected parse.SqlExpr
}{
	{
		Name: "1 + 2 * 3",
		Input: []token.Token{
			token.Literal(token.INT, "1"),
			token.ADD,
			token.Literal(token.INT, "2"),
			token.MUL,
			token.Literal(token.INT, "3"),
		},
		Expected: &parse.SqlBinaryExpr{
			Left: &parse.SqlIntLiteral{Value: 1},
			Op:   "+",
			Right: &parse.SqlBinaryExpr{
				Left:  &parse.SqlIntLiteral{Value: 2},
				Op:    "*",
				Right: &parse.SqlIntLiteral{Value: 3},
			},
		},
	},
	{
		Name: "1 * 2 + 3",
		Input: []token.Token{
			token.Literal(token.INT, "1"),
			token.MUL,
			token.Literal(token.INT, "2"),
			token.ADD,
			token.Literal(token.INT, "3"),
		},
		Expected: &parse.SqlBinaryExpr{
			Left: &parse.SqlBinaryExpr{
				Left:  &parse.SqlIntLiteral{Value: 1},
				Op:    "*",
				Right: &parse.SqlIntLiteral{Value: 2},
			},
			Op:    "+",
			Right: &parse.SqlIntLiteral{Value: 3},
		},
	},
	{
		Name: "SELECT 1 * 2 + 3, 4 / 5",
		Input: []token.Token{
			token.SELECT,
			token.Literal(token.INT, "1"),
			token.MUL,
			token.Literal(token.INT, "2"),
			token.ADD,
			token.Literal(token.INT, "3"),
			token.COMMA,
			token.Literal(token.INT, "4"),
			token.QUO,
			token.Literal(token.INT, "5"),
		},
		Expected: parse.SqlSelectRelation(
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
	{
		Name: "SELECT a + b, c FROM d",
		Input: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.IDENT, "b"),
			token.COMMA,
			token.Literal(token.IDENT, "c"),
			token.FROM,
			token.Literal(token.IDENT, "d"),
		},
		// Only the SELECT is consumed by the expression parser
		Expected: parse.SqlSelectRelation(
			[]parse.SqlExpr{
				&parse.SqlBinaryExpr{
					Left:  &parse.SqlIdentifier{ID: "a"},
					Op:    "+",
					Right: &parse.SqlIdentifier{ID: "b"},
				},
				&parse.SqlIdentifier{ID: "c"},
			},
		),
	},
	// {
	// 	Name: "SELECT a FROM (SELECT a FROM x) AS x",
	// 	Input: []token.Token{
	// 		token.SELECT,
	// 		token.Literal(token.IDENT, "a"),
	// 		token.FROM,
	// 		token.LPAREN,
	// 		token.SELECT,
	// 		token.Literal(token.IDENT, "a"),
	// 		token.FROM,
	// 		token.Literal(token.IDENT, "x"),
	// 		token.RPAREN,
	// 		token.AS,
	// 		token.Literal(token.IDENT, "x"),
	// 	},
	// 	Expected: []parse.SqlExpr{
	// 		parse.SqlSelectRelation(
	// 			[]parse.SqlExpr{
	// 				&parse.SqlIdentifier{ID: "a"},
	// 			},
	// 		),
	// 		parse.SqlFromRelation(parse.SqlSelectRelation()),
	// 	},
	// },
}

func TestExprParser(t *testing.T) {
	for _, tc := range exprParserTestcases {
		t.Run(tc.Name, func(t *testing.T) {
			parser := parse.NewExprParser(tc.Input)

			output, err := parser.Parse(0)
			require.NoError(t, err)

			require.Equal(t, tc.Expected, output)
		})
	}
}

var queryParserTestcases = []struct {
	Name     string
	Input    []token.Token
	Expected *parse.SqlQuery
	Error    bool
}{
	{
		Name: "SELECT a + b",
		Input: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.IDENT, "b"),
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{ID: "a"},
						Op:    "+",
						Right: &parse.SqlIdentifier{ID: "b"},
					},
				},
			),
		},
	},
	{
		Name: "SELECT a + b FROM c",
		Input: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.IDENT, "b"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
		Expected: &parse.SqlQuery{
			Projection: parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{ID: "a"},
						Op:    "+",
						Right: &parse.SqlIdentifier{ID: "b"},
					},
				},
			),
			Read: parse.SqlFromRelation(&parse.SqlIdentifier{ID: "c"}),
		},
	},
	{
		Name: "a + b FROM c",
		Input: []token.Token{
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.IDENT, "b"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
		Error: true,
	},
	{
		Name: "SELECT a FROM b + c",
		Input: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.FROM,
			token.Literal(token.IDENT, "b"),
			token.ADD,
			token.Literal(token.IDENT, "c"),
		},
		Error: true,
	},
}

func TestQueryParser(t *testing.T) {
	var parser parse.QueryParser
	for _, tc := range queryParserTestcases {
		t.Run(tc.Name, func(t *testing.T) {
			query, err := parser.Parse(tc.Input)

			if tc.Error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.Expected, query)
		})
	}
}
