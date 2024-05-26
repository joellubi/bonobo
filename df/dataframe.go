package df

import (
	"github.com/joellubi/bonobo/engine"

	"github.com/apache/arrow/go/v17/arrow"
)

type DataFrame interface {
	Select(exprs ...engine.Expr) DataFrame
	Filter(expr engine.Expr) DataFrame

	Schema() (*arrow.Schema, error)
	LogicalPlan() engine.Relation
}

func QueryContext() *queryContext {
	return &queryContext{}
}

type queryContext struct{}

func (execCtx *queryContext) Read(table engine.Table) DataFrame {
	r := engine.NewReadOperation(table)

	return dataframe{plan: r, exec: execCtx}
}

type dataframe struct {
	plan engine.Relation
	exec *queryContext
}

func (df dataframe) Select(exprs ...engine.Expr) DataFrame {
	df.plan = engine.NewProjectionOperation(df.plan, exprs)
	return df
}

func (df dataframe) Filter(expr engine.Expr) DataFrame {
	df.plan = engine.NewSelectionOperation(df.plan, expr)
	return df
}

func (df dataframe) Schema() (*arrow.Schema, error) { return df.plan.Schema() }

func (df dataframe) LogicalPlan() engine.Relation { return df.plan }

var (
	ColIdx = engine.NewColumnIndexExpr
	Lit    = engine.NewLiteralExpr
	Col    = engine.NewColumnExpr
	As     = engine.NewAliasExpr

	Add = engine.NewAddFunctionExpr
)
