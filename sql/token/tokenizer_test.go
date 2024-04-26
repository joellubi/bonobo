package token_test

import (
	"strings"
	"testing"

	"github.com/backdeck/backdeck/query/sql/token"
	"github.com/stretchr/testify/require"
)

var tokenizer = token.NewTokenizer()

var testcases = []struct {
	Name     string
	Input    string
	Expected []token.Token
}{
	{
		Name:  "add_identifiers",
		Input: "SELECT a + b FROM c",
		Expected: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.IDENT, "b"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
	},
	{
		Name: "add_identifiers_multiline",
		Input: `SELECT
					a + b
				FROM
					c`,
		Expected: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.IDENT, "b"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
	},
	{
		Name:  "add_literal_string",
		Input: "SELECT a + 'b' FROM c",
		Expected: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.STRING, "b"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
	},
	{
		Name:  "sub_literal_int",
		Input: "SELECT a + 5 FROM c",
		Expected: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.INT, "5"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
	},
	{
		Name:  "mult_literal_float",
		Input: "SELECT a + 5.67 FROM c",
		Expected: []token.Token{
			token.SELECT,
			token.Literal(token.IDENT, "a"),
			token.ADD,
			token.Literal(token.FLOAT, "5.67"),
			token.FROM,
			token.Literal(token.IDENT, "c"),
		},
	},
}

func TestTokenizer(t *testing.T) {
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			tokens, err := tokenizer.Tokenize(strings.NewReader(tc.Input))
			require.NoError(t, err)

			require.Equal(t, tc.Expected, tokens)
		})
	}
}
