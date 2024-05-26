package sql

import (
	"github.com/joellubi/bonobo/engine"
	"github.com/joellubi/bonobo/sql/parse"
	"github.com/joellubi/bonobo/sql/plan"
	"github.com/joellubi/bonobo/sql/token"
)

func Parse(sql string) (*engine.Plan, error) {
	lex := token.Lex(sql)
	tokens := token.NewTokenStream(lex)

	ast, err := parse.Parse(tokens)
	if err != nil {
		return nil, err
	}

	rel, err := plan.CreateLogicalPlan(ast)
	if err != nil {
		return nil, err
	}

	return engine.NewPlan(rel), nil
}
