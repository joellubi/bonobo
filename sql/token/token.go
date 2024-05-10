package token

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type Token struct {
	Name TokenName
	Val  string
	Pos  int
}

func (tok Token) IsLiteral() bool  { return literal_beg < tok.Name && tok.Name < literal_end }
func (tok Token) IsOperator() bool { return operator_beg < tok.Name && tok.Name < operator_end }
func (tok Token) IsKeyword() bool  { return keyword_beg < tok.Name && tok.Name < keyword_end }

// Precedence implements Token.
func (tok *Token) Precedence() int {
	switch tok.Name {
	case LSS, GTR, EQL, LEQ, GEQ:
		return 40
	case ADD, SUB:
		return 50
	case MUL, QUO:
		return 60
	}
	return LowestPrec
}

func (tok *Token) String() string {
	return fmt.Sprintf("'%s' @ location %d", tok.Val, tok.Pos)
}

type TokenName int

// The list of tokens.
const (
	// Special tokens
	ERROR TokenName = iota // TODO: Remove unneeded tokens
	EOF
	COMMENT
	PERIOD // .

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

	OPAND // &&
	OPOR  // ||
	OPNOT // !

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
	AND
	OR
	NOT
	keyword_end
)

var tokens = [...]string{
	ERROR: "ERROR",

	EOF:     "EOF",
	COMMENT: "COMMENT",
	PERIOD:  ".",

	IDENT:  "IDENT",
	INT:    "INT",
	FLOAT:  "FLOAT",
	STRING: "STRING",

	ADD: "+",
	SUB: "-",
	MUL: "*",
	QUO: "/",
	REM: "%",

	OPAND: "&&",
	OPOR:  "||",
	OPNOT: "!",

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

	RPAREN:    ")",
	RBRACK:    "]",
	RBRACE:    "}",
	SEMICOLON: ";",
	COLON:     ":",

	SELECT: "SELECT",
	FROM:   "FROM",
	WHERE:  "WHERE",
	AS:     "AS",
	AND:    "AND",
	OR:     "OR",
	NOT:    "NOT",
}

func (tok TokenName) String() string {
	return tokens[tok]
}

var (
	keywordLookup  map[string]TokenName
	operatorLookup [][]TokenName
)

func operatorsStartingWith(r ...rune) []TokenName {
	if len(r) == 0 || len(r) > 2 {
		return nil
	}

	if int(r[0]) >= len(operatorLookup) {
		return nil
	}

	tokens := operatorLookup[r[0]]
	if len(tokens) < 2 || len(r) < 2 {
		return tokens
	}

	if int(r[1]) >= len(tokens) {
		return nil
	}

	tok := tokens[r[1]]
	if tok == ERROR {
		return nil
	}

	return []TokenName{tok}
}

func init() {
	keywordLookup = make(map[string]TokenName, keyword_end-keyword_beg)
	for i := keyword_beg + 1; i < keyword_end; i++ {
		keywordLookup[tokens[i]] = i
	}

	operatorLookup = make([][]TokenName, 0)
	for i := operator_beg + 1; i < operator_end; i++ {
		opString := tokens[i]
		r1, w1 := utf8.DecodeRuneInString(opString)

		if len(operatorLookup) <= int(r1) {
			operatorLookup = append(operatorLookup, make([][]TokenName, int(r1)-len(operatorLookup)+1)...)
		}

		var r2 rune
		if w1 < len(opString) {
			var w2 int
			r2, w2 = utf8.DecodeRuneInString(opString[w1:])

			if w1+w2 < len(opString) {
				panic(fmt.Sprintf("cannot initialize operator lookup table, ops can have 2 chars max but found: %s", opString))
			}
		}

		if len(operatorLookup[r1]) <= int(r2) {
			operatorLookup[r1] = append(operatorLookup[r1], make([]TokenName, int(r2)-len(operatorLookup[r1])+1)...)
		}

		operatorLookup[r1][r2] = i

	}
}

func LookupKeyword(val string) (TokenName, bool) {
	tok, found := keywordLookup[strings.ToUpper(val)]
	return tok, found
}

const (
	LowestPrec  = 0
	HighestPrec = 100
)
