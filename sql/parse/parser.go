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

// TODO: TokenStream handle merging tokens if needed during call to peek()
func (ts *TokenStream) Peek() (token.Token, bool) {
	if ts.cur >= len(ts.tokens) {
		return token.ILLEGAL, false
	}

	tok := ts.tokens[ts.cur]
	return tok, true
}

type SqlNode interface {
	Children() []SqlNode
}

type Parser interface {
	Parse(tokens []token.Token) (*SqlQuery, error)
}

type PrattParser interface {
	Parse(precedence int) (SqlExpr, error)
	NextPrecedence() int
	ParsePrefix() (SqlExpr, error)
	ParseInfix(left SqlExpr, precedence int) (SqlExpr, error)
}

type QueryParser struct {
	bldr SqlQueryBuilder
}

// Parse implements QueryParser.
func (p *QueryParser) Parse(tokens []token.Token) (*SqlQuery, error) {
	parser := NewExprParser(tokens)
	for {
		block, err := parser.Parse(token.HighestPrec)
		if err == ErrEndOfTokenStream {
			break
		}
		if err != nil {
			return nil, err
		}

		switch b := block.(type) {
		case *sqlSelectRelation:
			p.bldr.Select(b)
		case *sqlFromRelation:
			p.bldr.From(b)
		default:
			return nil, fmt.Errorf("parse: expected valid sql relation, found %[1]T: %[1]s", b)
		}
	}

	return p.bldr.Query(), nil
}

func NewExprParser(tokens []token.Token) PrattParser {
	return &exprParser{tokens: TokenStream{tokens: tokens}}
}

type exprParser struct {
	tokens TokenStream
}

// Parse implements Parser.
func (p *exprParser) Parse(precedence int) (SqlExpr, error) {
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
func (p *exprParser) NextPrecedence() int {
	tok, more := p.tokens.Peek()
	if !more {
		return token.LowestPrec
	}

	return tok.Precedence()
}

// ParsePrefix implements Parser.
func (p *exprParser) ParsePrefix() (SqlExpr, error) {
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
func (p *exprParser) ParseInfix(left SqlExpr, precedence int) (SqlExpr, error) {
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

func (p *exprParser) parseSelect() (*sqlSelectRelation, error) {
	projection, err := p.parseExprList()
	if err != nil {
		return nil, err
	}

	return SqlSelectRelation(projection), nil
}

func (p *exprParser) parseFrom() (*sqlFromRelation, error) {
	table, err := p.parseTableExpr()
	if err != nil {
		return nil, err
	}

	return SqlFromRelation(table), nil
}

func (p *exprParser) parseTableExpr() (SqlExpr, error) {
	tok, more := p.tokens.Peek()
	if !more {
		return nil, ErrEndOfTokenStream
	}

	switch tok.ID() {
	case token.IDENT:
		return p.parseTableIdentifier()
	case token.LPAREN:
		return nil, fmt.Errorf("unimplemented: FROM subquery")
	}

	return nil, fmt.Errorf("parse: unexpected token: %s", tok)
}

func (p *exprParser) parseTableIdentifier() (SqlExpr, error) {
	tok, err := p.expectToken(token.IDENT)
	if err != nil {
		return nil, err
	}

	// TODO: expect PERIOD to check for namespaced tables

	return &SqlIdentifier{ID: tok.Value()}, nil
}

func (p *exprParser) parseExprList() ([]SqlExpr, error) {
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

func (p *exprParser) parseExpr() (SqlExpr, error) {
	return p.Parse(0)
}

// func (p *exprParser) consumeToken(tok token.Token) bool {
// 	t, more := p.tokens.Peek()
// 	if !more {
// 		return false
// 	}

// 	if t != tok {
// 		return false
// 	}

// 	p.tokens.Next()
// 	return true
// }

func (p *exprParser) expectToken(tok token.Token) (token.Token, error) {
	t, more := p.tokens.Peek()
	if !more {
		return t, ErrEndOfTokenStream
	}

	if t.ID() != tok.ID() {
		return t, fmt.Errorf("parse: expected %s token but found %s", tok, t)
	}

	p.tokens.Next()
	return t, nil
}

var _ PrattParser = (*exprParser)(nil)
var _ Parser = (*QueryParser)(nil)
