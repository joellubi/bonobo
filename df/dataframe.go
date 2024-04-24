package df

import (
	"context"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/backdeck/backdeck/query/engine"
)

type DataFrame interface {
	Select(exprs ...engine.Expr) DataFrame
	Filter(expr engine.Expr) DataFrame

	Schema() (*arrow.Schema, error)
	LogicalPlan() engine.Plan
}

func QueryContext(ctx context.Context, alloc memory.Allocator) *executionContext {
	return &executionContext{
		ctx:   ctx,
		alloc: alloc,
	}
}

type executionContext struct {
	ctx   context.Context
	alloc memory.Allocator
}

func (execCtx *executionContext) Read(table engine.Table) DataFrame {
	r := engine.NewReadOperation(table)

	return dataframeImpl{plan: r, exec: execCtx}
}

type dataframeImpl struct {
	plan engine.Plan
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

func (df dataframeImpl) LogicalPlan() engine.Plan { return df.plan }

var (
	ColIdx = engine.NewColumnIndexExpr
)