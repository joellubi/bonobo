package token

import (
	"fmt"
	"strings"
)

type Token interface {
	ID() token
	Value() string
	Precedence() int

	IsLiteral() bool
	IsOperator() bool
	IsKeyword() bool
}

type token int

// The list of tokens.
const (
	// Special tokens
	ILLEGAL token = iota // TODO: Remove unneeded tokens
	EOF
	COMMENT

	literal_beg
	// Identifiers and basic type literals
	// (these tokens stand for classes of literals)
	IDENT  // main
	INT    // 12345
	FLOAT  // 123.45
	STRING // 'abc'
	literal_end

	operator_beg
	// Operators and delimiters
	ADD // +
	SUB // -
	MUL // *
	QUO // /
	REM // %

	AND // AND
	OR  // OR
	NOT // NOT

	EQL // =
	NEQ // !=
	LSS // <
	GTR // >
	LEQ // <=
	GEQ // >=

	LPAREN // (
	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RPAREN    // )
	RBRACK    // ]
	RBRACE    // }
	SEMICOLON // ;
	COLON     // :
	operator_end

	keyword_beg
	// Keywords
	SELECT
	FROM
	WHERE
	AS
	keyword_end
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",

	EOF:     "EOF",
	COMMENT: "COMMENT",

	IDENT:  "IDENT",
	INT:    "INT",
	FLOAT:  "FLOAT",
	STRING: "STRING",

	ADD: "+",
	SUB: "-",
	MUL: "*",
	QUO: "/",
	REM: "%",

	AND: "AND",
	OR:  "OR",
	NOT: "NOT",

	EQL: "=",
	NEQ: "!=",
	LSS: "<",
	GTR: ">",
	LEQ: "<=",
	GEQ: ">=",

	LPAREN: "(",
	LBRACK: "[",
	LBRACE: "{",
	COMMA:  ",",
	PERIOD: ".",

	RPAREN:    ")",
	RBRACK:    "]",
	RBRACE:    "}",
	SEMICOLON: ";",
	COLON:     ":",

	SELECT: "SELECT",
	FROM:   "FROM",
	WHERE:  "WHERE",
	AS:     "AS",
}

func (tok token) Value() string {
	if tok < literal_end || tok >= token(len(tokens)) {
		panic(fmt.Sprintf("cannot determine value for token: %s", tok))
	}
	return tokens[tok]
}

func (tok token) ID() token {
	return tok
}

func (tok token) String() string {
	return tok.Value()
}

var tokenLookup map[string]Token

func init() {
	tokenLookup = make(map[string]Token, len(tokens)) // TODO: Kind of the right size...
	for i := literal_beg + 1; i < literal_end; i++ {
		tokenLookup[tokens[i]] = i
	}
	for i := operator_beg + 1; i < operator_end; i++ {
		tokenLookup[tokens[i]] = i
	}
	for i := keyword_beg + 1; i < keyword_end; i++ {
		tokenLookup[tokens[i]] = i
	}
}

// LookupToken maps an identifier to a token if it is a valid token.
func LookupToken(ident string) (Token, bool) {
	tok, found := tokenLookup[strings.ToUpper(ident)]
	return tok, found
}

const (
	LowestPrec = 0 // non-operators
	// UnaryPrec   = 6
	HighestPrec = 100
)

// Precedence returns the operator precedence of the binary
// operator op. If op is not a binary operator, the result
// is LowestPrecedence.
func (tok token) Precedence() int {
	switch tok {
	case ADD, SUB:
		return 50
	case MUL, QUO:
		return 60
	}
	return LowestPrec
}

// IsLiteral returns true for tokens corresponding to identifiers
// and basic type literals; it returns false otherwise.
func (tok token) IsLiteral() bool { return literal_beg < tok && tok < literal_end }

// IsOperator returns true for tokens corresponding to operators and
// delimiters; it returns false otherwise.
func (tok token) IsOperator() bool { return operator_beg < tok && tok < operator_end }

// IsKeyword returns true for tokens corresponding to keywords;
// it returns false otherwise.
func (tok token) IsKeyword() bool { return keyword_beg < tok && tok < keyword_end }

type literalToken struct {
	token
	value string
}

func Literal(tok token, value string) literalToken {
	if !tok.IsLiteral() {
		panic(fmt.Sprintf("cannot assign literal value to token: %s", tok))
	}
	return literalToken{token: tok, value: value}
}

func (tok literalToken) Value() string {
	return tok.value
}

func (tok literalToken) String() string {
	return tok.Value()
}

var _ Token = (*token)(nil)
var _ Token = (*literalToken)(nil)
