package sql

import (
	"github.com/backdeck/backdeck/query/engine"
	"github.com/backdeck/backdeck/query/sql/parse"
	"github.com/backdeck/backdeck/query/sql/plan"
	"github.com/backdeck/backdeck/query/sql/token"
)

type SQLParser interface {
	Parse(sql string) (*engine.Plan, error)
}

type Parser struct{}

func (*Parser) Parse(sql string) (*engine.Plan, error) {
	lex := token.Lex(sql)
	tokens := token.NewTokenStream(lex)

	var parser parse.QueryParser
	ast, err := parser.Parse(tokens)
	if err != nil {
		return nil, err
	}

	rel, err := plan.CreateLogicalPlan(ast)
	if err != nil {
		return nil, err
	}

	return engine.NewPlan(rel), nil
}

var _ SQLParser = (*Parser)(nil)
