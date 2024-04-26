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
	Expected []parse.SqlExpr
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
		Expected: []parse.SqlExpr{&parse.SqlBinaryExpr{
			Left: &parse.SqlIntLiteral{Value: 1},
			Op:   "+",
			Right: &parse.SqlBinaryExpr{
				Left:  &parse.SqlIntLiteral{Value: 2},
				Op:    "*",
				Right: &parse.SqlIntLiteral{Value: 3},
			},
		}},
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
		Expected: []parse.SqlExpr{&parse.SqlBinaryExpr{
			Left: &parse.SqlBinaryExpr{
				Left:  &parse.SqlIntLiteral{Value: 1},
				Op:    "*",
				Right: &parse.SqlIntLiteral{Value: 2},
			},
			Op:    "+",
			Right: &parse.SqlIntLiteral{Value: 3},
		}},
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
		Expected: []parse.SqlExpr{parse.SqlSelectRelation(
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
		)},
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
		Expected: []parse.SqlExpr{
			parse.SqlSelectRelation(
				[]parse.SqlExpr{
					&parse.SqlBinaryExpr{
						Left:  &parse.SqlIdentifier{ID: "a"},
						Op:    "+",
						Right: &parse.SqlIdentifier{ID: "b"},
					},
					&parse.SqlIdentifier{ID: "c"},
				},
			),
			parse.SqlFromRelation(&parse.SqlIdentifier{ID: "d"}),
		},
	},
}

func TestParser(t *testing.T) {
	for _, tc := range testcases {
		// // TEMP skip
		// if tc.Name == "SELECT a + b, c FROM d" {
		// 	continue
		// }

		t.Run(tc.Name, func(t *testing.T) {
			parser := parse.NewParser(tc.Input)
			exprs := make([]parse.SqlExpr, 0)
			for {
				output, err := parser.Parse(0)
				if err == parse.ErrEndOfTokenStream {
					break
				}
				require.NoError(t, err)

				exprs = append(exprs, output)
			}

			require.Equal(t, tc.Expected, exprs)
		})
	}
}

// func TestParser2(t *testing.T) {
// 	tc := testcases[2]

// 	parser := parse.NewParser(tc.Input)
// 	output, err := parser.Parse(0)
// 	require.NoError(t, err)

// 	output, err = parser.Parse(0)
// 	require.NoError(t, err)

// 	require.Equal(t, tc.Expected, output)

// }
