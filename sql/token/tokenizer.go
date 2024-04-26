package token

import (
	"bufio"
	"fmt"
	"io"
	"unicode"
)

type Tokenizer interface {
	Tokenize(r io.Reader) ([]Token, error)
}

func NewTokenizer() *tokenizer {
	return &tokenizer{}
}

type tokenizer struct{}

func (t *tokenizer) Tokenize(r io.Reader) ([]Token, error) {
	scanner := bufio.NewScanner(r) // TODO: Reset scanner after complete?
	scanner.Split(bufio.ScanWords)

	tokens := make([]Token, 0)
	for scanner.Scan() {
		text := scanner.Text() // TODO: Can alloc actually be avoided?
		token, err := ScanToken(text)
		if err != nil {
			return tokens, err
		}
		tokens = append(tokens, token)
	}

	if scanner.Err() != nil {
		return tokens, scanner.Err()
	}

	return tokens, nil
}

func ScanToken(value string) (Token, error) {
	tok, found := LookupToken(value)
	if found {
		return tok, nil
	}

	chars := []rune(value)
	if isIdentifier(chars) {
		return Literal(IDENT, string(chars)), nil // TODO: Check for invalid chars
	}

	return ScanLiteralToken(chars)
}

func ScanLiteralToken(chars []rune) (Token, error) {
	if isStringLiteral(chars) {
		unquoted := chars[1 : len(chars)-1]
		return Literal(STRING, string(unquoted)), nil
		// return Token{Value: string(unquoted), Kind: STRING}, nil
	}

	return ScanLiteralNumericToken(chars)
}

func ScanLiteralNumericToken(chars []rune) (Token, error) {
	var hasDecimal bool
	for _, ch := range chars {
		if ch == '.' {
			if hasDecimal {
				return ILLEGAL, fmt.Errorf("unrecognized token: %s", string(chars))
			}
			hasDecimal = true
			continue
		}
		if !unicode.IsDigit(ch) {
			return ILLEGAL, fmt.Errorf("unrecognized token: %s", string(chars))
		}
	}

	kind := INT
	if hasDecimal {
		kind = FLOAT
	}

	return Literal(kind, string(chars)), nil
	// return Token{Value: string(chars), Kind: kind}, nil
}

func isIdentifier(chars []rune) bool {
	return unicode.IsLetter(chars[0])
}

func isStringLiteral(chars []rune) bool {
	return chars[0] == '\'' && chars[len(chars)-1] == '\''
}

var _ Tokenizer = (*tokenizer)(nil)
