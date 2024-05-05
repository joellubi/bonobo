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
			{Name: token.INT, Val: "1"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "2"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "3"},
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
			{Name: token.INT, Val: "1"},
			{Name: token.MUL, Val: "*"},
			{Name: token.INT, Val: "2"},
			{Name: token.ADD, Val: "+"},
			{Name: token.INT, Val: "3"},
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
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.COMMA, Val: ","},
			{Name: token.IDENT, Val: "c"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "d"},
		},
		// Only the SELECT is consumed by the expression parser
		Expected: parse.SqlSelectRelation(
			[]parse.SqlExpr{
				&parse.SqlBinaryExpr{
					Left:  &parse.SqlIdentifier{Names: []string{"a"}},
					Op:    "+",
					Right: &parse.SqlIdentifier{Names: []string{"b"}},
				},
				&parse.SqlIdentifier{Names: []string{"c"}},
			},
		),
	},
	// {
	// 	Name: "SELECT a FROM (SELECT a FROM x) AS x",
	// 	Input: []token.Token{
	// 		{Name: token.SELECT, Val: "SELECT"},
	// 		{Name: token.IDENT, Val: "a"},
	// 		{Name: token.FROM, Val: "FROM"},
	// 		{Name: token.LPAREN, Val: "("},
	// 		{Name: token.SELECT, Val: "SELECT"},
	// 		{Name: token.IDENT, Val: "a"},
	// 		{Name: token.FROM, Val: "FROM"},
	// 		{Name: token.RPAREN, Val: ")"},
	// 		{Name: token.AS, Val: "AS"},
	// 		{Name: token.IDENT, Val: "x"},
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
			tokens := token.NewListTokenStream(tc.Input)
			parser := parse.NewExprParser(tokens)

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
		Name: "SELECT a + b FROM c",
		Input: []token.Token{
			{Name: token.SELECT, Val: "SELECT"},
			{Name: token.IDENT, Val: "a"},
			{Name: token.ADD, Val: "+"},
			{Name: token.IDENT, Val: "b"},
			{Name: token.FROM, Val: "FROM"},
			{Name: token.IDENT, Val: "c"},
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
}

func TestQueryParser(t *testing.T) {
	for _, tc := range queryParserTestcases {
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
