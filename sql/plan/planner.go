package plan

import (
	"fmt"

	"github.com/backdeck/backdeck/query/engine"
	"github.com/backdeck/backdeck/query/sql/parse"
)

func CreateLogicalExpr(expr parse.SqlExpr) (engine.Expr, error) {
	switch e := expr.(type) {
	case *parse.SqlIdentifier:
		return engine.NewColumnExpr(e.ID), nil
	default:
		return nil, fmt.Errorf("plan: unrecognized SqlExpr type: %T", e)
	}
}

func CreateLogicalPlan(query *parse.SqlQuery) (engine.Plan, error) {
	var (
		plan engine.Plan
		// err  error
	)

	if query.Read != nil {
		switch t := query.Read.Table.(type) {
		case *parse.SqlIdentifier:
			table := engine.NewNamedTable([]string{t.ID}, nil) // TODO: Handle namespace splitting
			plan = engine.NewReadOperation(table)
		case *parse.SqlQuery:
			return nil, fmt.Errorf("plan: Read from subquery unimplemented")
		default:
			return nil, fmt.Errorf("plan: unrecognized SqlExpr type for Read operation: %T", t)
		}
	}

	if query.Projection != nil {
		exprs := make([]engine.Expr, len(query.Projection.Exprs))
		for i, expr := range query.Projection.Exprs {
			e, err := CreateLogicalExpr(expr)
			if err != nil {
				return nil, fmt.Errorf("parse: failed to plan SQL query: %w", err)
			}
			exprs[i] = e
		}
		plan = engine.NewProjectionOperation(plan, exprs)
	}
	// plan := engine.NewReadOperation(e)
	return plan, nil
}
