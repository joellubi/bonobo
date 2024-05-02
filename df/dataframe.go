package df

import (
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/backdeck/backdeck/query/engine"
)

type DataFrame interface {
	Select(exprs ...engine.Expr) DataFrame
	Filter(expr engine.Expr) DataFrame

	Schema() (*arrow.Schema, error)
	LogicalPlan() engine.Relation
}

func QueryContext() *executionContext {
	return &executionContext{}
}

type executionContext struct {
	// ctx   context.Context
	// alloc memory.Allocator
}

func (execCtx *executionContext) Read(table engine.Table) DataFrame {
	r := engine.NewReadOperation(table)

	return dataframeImpl{plan: r, exec: execCtx}
}

type dataframeImpl struct {
	plan engine.Relation
	exec *executionContext
}

func (df dataframeImpl) Select(exprs ...engine.Expr) DataFrame {
	df.plan = engine.NewProjectionOperation(df.plan, exprs)
	return df
}

func (df dataframeImpl) Filter(expr engine.Expr) DataFrame {
	df.plan = engine.NewSelectionOperation(df.plan, expr)
	return df
}

func (df dataframeImpl) Schema() (*arrow.Schema, error) { return df.plan.Schema() }

func (df dataframeImpl) LogicalPlan() engine.Relation { return df.plan }

var (
	ColIdx = engine.NewColumnIndexExpr
	Lit    = engine.NewLiteralExpr

	Add = engine.Add
)
