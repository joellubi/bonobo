package parse

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/backdeck/backdeck/query/sql/token"
)

var ErrEndOfTokenStream = errors.New("parse: end of token stream")

type SqlNode interface {
	Children() []SqlNode
}

type PrattParser interface {
	Parse(precedence int) (SqlExpr, error)
	NextPrecedence() int
	ParsePrefix() (SqlExpr, error)
	ParseInfix(left SqlExpr, precedence int) (SqlExpr, error)
}

func Parse(tokens token.TokenStream) (*SqlQuery, error) {
	var bldr SqlQueryBuilder
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
			bldr.Select(b)
		case *sqlFromRelation:
			bldr.From(b)
		case *sqlWhereRelation:
			bldr.Where(b)
		default:
			return nil, fmt.Errorf("parse: expected valid sql relation, found %[1]T: %[1]s", b)
		}
	}

	return bldr.Query(), nil
}

func NewExprParser(tokens token.TokenStream) PrattParser {
	return &exprParser{tokens: tokens}
}

type exprParser struct {
	tokens token.TokenStream
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

	switch tok.Name {
	case token.SELECT:
		return p.parseSelect()
	case token.FROM:
		return p.parseFrom()
	case token.WHERE:
		return p.parseWhere()
	case token.IDENT:
		return p.parseIdentifier(tok.Val)
	case token.INT:
		val, err := strconv.Atoi(tok.Val)
		if err != nil {
			return nil, err
		}
		return &SqlIntLiteral{Value: val}, nil
	default:
		return nil, fmt.Errorf("parse: unexpected token: %s", tok.String())
	}
}

// ParseInfix implements Parser.
func (p *exprParser) ParseInfix(left SqlExpr, precedence int) (SqlExpr, error) {
	tok, more := p.tokens.Peek()
	if !more {
		return nil, ErrEndOfTokenStream
	}
	if !tok.IsOperator() {
		return nil, fmt.Errorf("parse: unexpected token: expected operator, found: %s", tok.String())
	}

	p.tokens.Next() // TODO: Can we just do Next above?
	right, err := p.Parse(precedence)
	if err != nil {
		return nil, err
	}

	return &SqlBinaryExpr{
		Left:  left,
		Op:    tok.Val,
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

	switch tok.Name {
	case token.IDENT:
		return p.parseIdentifier()
	case token.LPAREN:
		return nil, fmt.Errorf("unimplemented: FROM subquery")
	}

	return nil, fmt.Errorf("parse: unexpected token: %s", tok.String())
}

func (p *exprParser) parseWhere() (*sqlWhereRelation, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("expected expression to follow WHERE: %w", err)
	}
	return SqlWhereRelation(expr), nil
}

func (p *exprParser) parseIdentifier(names ...string) (SqlExpr, error) {
	if len(names) != 0 {
		// Partially-consumed identifier; already at end or period delimits next part
		_, err := p.expectToken(token.PERIOD)
		if err != nil {
			return &SqlIdentifier{Names: names}, nil
		}
	}

	for {
		tok, err := p.expectToken(token.IDENT)
		if err != nil {
			return nil, err
		}
		names = append(names, tok.Val)

		_, err = p.expectToken(token.PERIOD)
		if err != nil {
			break
		}
	}

	return &SqlIdentifier{Names: names}, nil
}

func (p *exprParser) parseExprList() ([]SqlExpr, error) {
	exprs := make([]SqlExpr, 0)
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("expected at least one expression in SELECT: %w", err)
	}
	exprs = append(exprs, expr)

	for tok, more := p.tokens.Peek(); more && tok.Name == token.COMMA; tok, more = p.tokens.Peek() {
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

func (p *exprParser) expectToken(tok token.TokenName) (token.Token, error) {
	t, more := p.tokens.Peek()
	if !more {
		return t, ErrEndOfTokenStream
	}

	if t.Name != tok {
		return t, fmt.Errorf("parse: expected %s token but found %s", tok.String(), t.String())
	}

	p.tokens.Next()
	return t, nil
}

var _ PrattParser = (*exprParser)(nil)
