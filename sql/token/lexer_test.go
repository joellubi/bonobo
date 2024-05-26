package token_test

import (
	"testing"

	"github.com/joellubi/bonobo/sql/token"

	"github.com/stretchr/testify/require"
)

var testcases = []struct {
	Name     string
	Input    string
	Expected []token.Token
}{
	{
		Name:  "compare_identifiers_one_char_op",
		Input: "SELECT a > b FROM c",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.GTR, Val: ">", Pos: 9},
			{Name: token.IDENT, Val: "b", Pos: 11},
			{Name: token.FROM, Val: "FROM", Pos: 13},
			{Name: token.IDENT, Val: "c", Pos: 18},
			{Name: token.EOF, Pos: 19},
		},
	},
	{
		Name:  "compare_identifiers_two_char_op",
		Input: "SELECT a >= b FROM c",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.GEQ, Val: ">=", Pos: 9},
			{Name: token.IDENT, Val: "b", Pos: 12},
			{Name: token.FROM, Val: "FROM", Pos: 14},
			{Name: token.IDENT, Val: "c", Pos: 19},
			{Name: token.EOF, Pos: 20},
		},
	},
	{
		Name: "add_identifiers_multiline",
		Input: `SELECT
					a + b
				FROM
					c`,
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 12},
			{Name: token.ADD, Val: "+", Pos: 14},
			{Name: token.IDENT, Val: "b", Pos: 16},
			{Name: token.FROM, Val: "FROM", Pos: 22},
			{Name: token.IDENT, Val: "c", Pos: 32},
			{Name: token.EOF, Pos: 33},
		},
	},
	{
		Name:  "add_literal_string",
		Input: "SELECT a + 'b' FROM c",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.ADD, Val: "+", Pos: 9},
			{Name: token.STRING, Val: "b", Pos: 12},
			{Name: token.FROM, Val: "FROM", Pos: 15},
			{Name: token.IDENT, Val: "c", Pos: 20},
			{Name: token.EOF, Pos: 21},
		},
	},
	{
		Name:  "add_literal_int",
		Input: "SELECT a + 5 FROM c",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.ADD, Val: "+", Pos: 9},
			{Name: token.INT, Val: "5", Pos: 11},
			{Name: token.FROM, Val: "FROM", Pos: 13},
			{Name: token.IDENT, Val: "c", Pos: 18},
			{Name: token.EOF, Pos: 19},
		},
	},
	{
		Name:  "add_literal_float",
		Input: "SELECT a + 5.67 FROM c",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.ADD, Val: "+", Pos: 9},
			{Name: token.FLOAT, Val: "5.67", Pos: 11},
			{Name: token.FROM, Val: "FROM", Pos: 16},
			{Name: token.IDENT, Val: "c", Pos: 21},
			{Name: token.EOF, Pos: 22},
		},
	},
	{
		Name:  "select_two_columns",
		Input: "SELECT a, b FROM c",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.COMMA, Val: ",", Pos: 8},
			{Name: token.IDENT, Val: "b", Pos: 10},
			{Name: token.FROM, Val: "FROM", Pos: 12},
			{Name: token.IDENT, Val: "c", Pos: 17},
			{Name: token.EOF, Pos: 18},
		},
	},
	{
		Name:  "select_from_aliased_subquery",
		Input: "SELECT a FROM (SELECT a FROM x) AS x",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.FROM, Val: "FROM", Pos: 9},
			{Name: token.LPAREN, Val: "(", Pos: 14},
			{Name: token.SELECT, Val: "SELECT", Pos: 15},
			{Name: token.IDENT, Val: "a", Pos: 22},
			{Name: token.FROM, Val: "FROM", Pos: 24},
			{Name: token.IDENT, Val: "x", Pos: 29},
			{Name: token.RPAREN, Val: ")", Pos: 30},
			{Name: token.AS, Val: "AS", Pos: 32},
			{Name: token.IDENT, Val: "x", Pos: 35},
			{Name: token.EOF, Pos: 36},
		},
	},
	{
		Name:  "select_lots_of_operators",
		Input: "SELECT a+-5-9.012/(x-.1)>=-3. FROM z;",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "a", Pos: 7},
			{Name: token.ADD, Val: "+", Pos: 8},
			{Name: token.SUB, Val: "-", Pos: 9},
			{Name: token.INT, Val: "5", Pos: 10},
			{Name: token.SUB, Val: "-", Pos: 11},
			{Name: token.FLOAT, Val: "9.012", Pos: 12},
			{Name: token.QUO, Val: "/", Pos: 17},
			{Name: token.LPAREN, Val: "(", Pos: 18},
			{Name: token.IDENT, Val: "x", Pos: 19},
			{Name: token.SUB, Val: "-", Pos: 20},
			{Name: token.FLOAT, Val: ".1", Pos: 21},
			{Name: token.RPAREN, Val: ")", Pos: 23},
			{Name: token.GEQ, Val: ">=", Pos: 24},
			{Name: token.SUB, Val: "-", Pos: 26},
			{Name: token.FLOAT, Val: "3.", Pos: 27},
			{Name: token.FROM, Val: "FROM", Pos: 30},
			{Name: token.IDENT, Val: "z", Pos: 35},
			{Name: token.SEMICOLON, Val: ";", Pos: 36},
			{Name: token.EOF, Pos: 37},
		},
	},
	{
		Name:  "select_from_namespaced_table",
		Input: "SELECT name, age FROM my_db.public.customers;",
		Expected: []token.Token{
			{Name: token.SELECT, Val: "SELECT", Pos: 0},
			{Name: token.IDENT, Val: "name", Pos: 7},
			{Name: token.COMMA, Val: ",", Pos: 11},
			{Name: token.IDENT, Val: "age", Pos: 13},
			{Name: token.FROM, Val: "FROM", Pos: 17},
			{Name: token.IDENT, Val: "my_db", Pos: 22},
			{Name: token.PERIOD, Val: ".", Pos: 27},
			{Name: token.IDENT, Val: "public", Pos: 28},
			{Name: token.PERIOD, Val: ".", Pos: 34},
			{Name: token.IDENT, Val: "customers", Pos: 35},
			{Name: token.SEMICOLON, Val: ";", Pos: 44},
			{Name: token.EOF, Pos: 45},
		},
	},
	{
		Name:  "lots_of_dots",
		Input: ".1.2.a.b.3.4c.d5 e6.7 8.9f.",
		Expected: []token.Token{
			{Name: token.FLOAT, Val: ".1", Pos: 0},
			{Name: token.FLOAT, Val: ".2", Pos: 2},
			{Name: token.PERIOD, Val: ".", Pos: 4},
			{Name: token.IDENT, Val: "a", Pos: 5},
			{Name: token.PERIOD, Val: ".", Pos: 6},
			{Name: token.IDENT, Val: "b", Pos: 7},
			{Name: token.FLOAT, Val: ".3", Pos: 8},
			{Name: token.FLOAT, Val: ".4", Pos: 10},
			{Name: token.IDENT, Val: "c", Pos: 12},
			{Name: token.PERIOD, Val: ".", Pos: 13},
			{Name: token.IDENT, Val: "d5", Pos: 14},
			{Name: token.IDENT, Val: "e6", Pos: 17},
			{Name: token.FLOAT, Val: ".7", Pos: 19},
			{Name: token.FLOAT, Val: "8.9", Pos: 22},
			{Name: token.IDENT, Val: "f", Pos: 25},
			{Name: token.PERIOD, Val: ".", Pos: 26},
			{Name: token.EOF, Pos: 27},
		},
	},
}

func TestLexer(t *testing.T) {
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			var tokens []token.Token
			lex := token.Lex(tc.Input)

			var err string
			for {
				tok := lex.NextToken()
				tokens = append(tokens, tok)
				if tok.Name == token.EOF {
					break
				}
				if tok.Name == token.ERROR {
					err = tok.Val
					break
				}

			}

			require.Empty(t, err)
			require.Equal(t, tc.Expected, tokens)
		})
	}
}

var result []token.Token

func BenchmarkLexer(b *testing.B) {
	input := "SELECT a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' + a + 'b' FROM c"

	var tokens []token.Token
	for i := 0; i < b.N; i++ {
		tokens = make([]token.Token, 0)
		lex := token.Lex(input)
		for {
			tok := lex.NextToken()
			tokens = append(tokens, tok)
			if tok.Name == token.EOF {
				break
			}
			if tok.Name == token.ERROR {
				b.FailNow()
				break
			}
		}
	}
	result = tokens
}
