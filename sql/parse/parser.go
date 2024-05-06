package parse

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/backdeck/backdeck/query/sql/token"
)

var (
	ErrEndOfTokenStream     = errors.New("parse: end of token stream")
	ErrUnexpectedOpenParen  = errors.New("parse: unexpected opening paren")
	ErrUnexpectedCloseParen = errors.New("parse: unexpected closing paren")
)

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
	var (
		block SqlExpr
		bldr  SqlQueryBuilder
		err   error
	)

	parser := NewExprParser(tokens)
	for {
		block, err = parser.Parse(token.HighestPrec)
		if err == ErrEndOfTokenStream {
			return bldr.Query(), nil
		}
		if err != nil {
			return bldr.Query(), err
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
}

func NewExprParser(tokens token.TokenStream) PrattParser {
	return &exprParser{tokens: tokens}
}

type exprParser struct {
	tokens token.TokenStream
	depth  int
}

// Parse implements Parser.
func (p *exprParser) Parse(precedence int) (SqlExpr, error) {
	if err := p.consumeLeftParens(); err != nil {
		return nil, err
	}

	expr, err := p.ParsePrefix()
	if err != nil {
		return nil, err
	}

	if err := p.consumeRightParens(); err != nil {
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

	return tok.Precedence() + (p.depth * token.HighestPrec)
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

func (p *exprParser) consumeLeftParens() error {
	for {
		tok, _ := p.tokens.Peek()
		switch tok.Name {
		case token.LPAREN:
			p.depth++
			p.tokens.Next()
		case token.RPAREN:
			return fmt.Errorf("%w: %s", ErrUnexpectedCloseParen, tok.String())
		default:
			return nil
		}
	}
}

func (p *exprParser) consumeRightParens() error {
	for p.depth > 0 {
		tok, _ := p.tokens.Peek()
		switch tok.Name {
		case token.LPAREN:
			return fmt.Errorf("%w: %s", ErrUnexpectedOpenParen, tok.String())
		case token.RPAREN:
			p.depth--
			p.tokens.Next()
		default:
			return nil
		}
	}
	return nil
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
		if _, err := p.expectToken(token.LPAREN); err != nil {
			return nil, err
		}

		subquery, err := Parse(p.tokens)
		if err == nil {
			return nil, fmt.Errorf("parse: subquery was not closed")
		}
		if !errors.Is(err, ErrUnexpectedCloseParen) {
			return nil, err
		}

		if _, err := p.expectToken(token.RPAREN); err != nil {
			return nil, err
		}

		alias, err := p.tryParseAlias()
		if err == nil {
			subquery.Alias = alias
		}

		return subquery, nil
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
	var err error

	if len(names) != 0 { // TODO
		// Partially-consumed identifier; already at end or period delimits next part
		_, err = p.expectToken(token.PERIOD)
		// if err != nil {
		// 	return &SqlIdentifier{Names: names}, nil
		// }
	}

	if err == nil {
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
	}

	identifier := SqlIdentifier{Names: names}

	alias, err := p.tryParseAlias()
	if err == nil {
		identifier.Alias = alias
	}

	return &identifier, nil
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
	depthStart := p.depth

	expr, err := p.Parse(token.LowestPrec)
	if err != nil {
		return nil, err
	}

	if p.depth != depthStart {
		return nil, fmt.Errorf("parse: invalid expression, unmatched parentheses")
	}

	return expr, nil
}

func (p *exprParser) tryParseAlias() (string, error) {
	p.expectToken(token.AS) // Doesn't matter if there is an error or not
	tok, err := p.expectToken(token.IDENT)
	return tok.Val, err
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
