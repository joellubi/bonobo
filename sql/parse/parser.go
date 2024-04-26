package parse

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/backdeck/backdeck/query/sql/token"
)

var ErrEndOfTokenStream = errors.New("parse: end of token stream")

type TokenStream struct {
	tokens []token.Token
	cur    int
}

func (ts *TokenStream) Next() (token.Token, bool) {
	tok, more := ts.Peek()
	if !more {
		return token.ILLEGAL, false
	}

	ts.cur++
	return tok, true
}

func (ts *TokenStream) Peek() (token.Token, bool) {
	if ts.cur >= len(ts.tokens) {
		return token.ILLEGAL, false
	}

	tok := ts.tokens[ts.cur]
	return tok, true
}

type Parser interface {
	Parse(precedence int) (SqlExpr, error)
	NextPrecedence() int
	ParsePrefix() (SqlExpr, error)
	ParseInfix(left SqlExpr, precedence int) (SqlExpr, error)
}

func NewParser(tokens []token.Token) Parser {
	return &parser{tokens: TokenStream{tokens: tokens}}
}

type parser struct {
	tokens TokenStream
}

// Parse implements Parser.
func (p *parser) Parse(precedence int) (SqlExpr, error) {
	expr, err := p.ParsePrefix()
	if err != nil {
		return nil, err
	}
	for precedence < p.NextPrecedence() {
		expr, err = p.ParseInfix(expr, p.NextPrecedence())
		if err != nil {
			return nil, err
		}
	}
	return expr, nil
}

// NextPrecedence implements Parser.
func (p *parser) NextPrecedence() int {
	tok, more := p.tokens.Peek()
	if !more {
		return token.LowestPrec
	}

	return tok.Precedence()
}

// ParsePrefix implements Parser.
func (p *parser) ParsePrefix() (SqlExpr, error) {
	tok, more := p.tokens.Next()
	if !more {
		return nil, ErrEndOfTokenStream
	}

	switch tok.ID() {
	case token.SELECT:
		return p.parseSelect()
	case token.FROM:
		return p.parseFrom()
	case token.IDENT:
		return &SqlIdentifier{ID: tok.Value()}, nil
	case token.INT:
		val, err := strconv.Atoi(tok.Value())
		if err != nil {
			return nil, err
		}
		return &SqlIntLiteral{Value: val}, nil
	default:
		return nil, fmt.Errorf("parse: unexpected token: %s", tok)
	}
}

// ParseInfix implements Parser.
func (p *parser) ParseInfix(left SqlExpr, precedence int) (SqlExpr, error) {
	tok, more := p.tokens.Peek()
	if !more {
		return nil, ErrEndOfTokenStream
	}
	if !tok.IsOperator() {
		return nil, fmt.Errorf("parse: unexpected token: expected operator, found: %s", tok)
	}

	p.tokens.Next() // TODO: Can we just do Next above?
	right, err := p.Parse(precedence)
	if err != nil {
		return nil, err
	}

	return &SqlBinaryExpr{
		Left:  left,
		Op:    tok.Value(),
		Right: right,
	}, nil
}

func (p *parser) parseSelect() (*sqlSelectRelation, error) {
	projection, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	return SqlSelectRelation(projection), nil
}

func (p *parser) parseFrom() (*sqlFromRelation, error) {
	table, err := p.parseTableExpr()
	if err != nil {
		return nil, err
	}

	return SqlFromRelation(table), nil
}

func (p *parser) parseTableExpr() (SqlExpr, error) {
	tok, more := p.tokens.Peek()
	if !more {
		return nil, ErrEndOfTokenStream
	}

	switch tok.ID() {
	case token.IDENT:
		return p.parseExpr()
	case token.LPAREN:
		return nil, fmt.Errorf("unimplemented: FROM subquery")
	}

	return nil, fmt.Errorf("parse: unexpected token: %s", tok)
}

func (p *parser) parseExprList() ([]SqlExpr, error) {
	exprs := make([]SqlExpr, 0)
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("expected at least one expression in SELECT: %w", err)
	}
	exprs = append(exprs, expr)

	for tok, more := p.tokens.Peek(); more && tok == token.COMMA; tok, more = p.tokens.Peek() {
		p.tokens.Next()

		expr, err = p.parseExpr()
		if err == ErrEndOfTokenStream {
			break
		}
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

func (p *parser) parseExpr() (SqlExpr, error) {
	return p.Parse(0)
}

var _ Parser = (*parser)(nil)
