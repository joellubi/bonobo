package plan

import (
	"fmt"

	"github.com/backdeck/backdeck/query/engine"
	"github.com/backdeck/backdeck/query/sql/parse"
)

func CreateLogicalExpr(expr parse.SqlExpr) (engine.Expr, error) {
	switch e := expr.(type) {
	case *parse.SqlIdentifier:
		if len(e.Names) != 1 {
			return nil, fmt.Errorf("unimplemented: multi-part column identifier: %s", e.Names)
		}

		return engine.NewColumnExpr(e.Names[0]), nil
	case *parse.SqlIntLiteral:
		return engine.NewLiteralExpr(e.Value), nil
	case *parse.SqlBinaryExpr:
		left, err := CreateLogicalExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := CreateLogicalExpr(e.Right)
		if err != nil {
			return nil, err
		}

		f, err := engine.NewFunctionWithArgs(e.Op, left, right)
		if err != nil {
			return nil, err
		}

		return f, nil
	default:
		return nil, fmt.Errorf("plan: unrecognized SqlExpr type: %T", e)
	}
}

func CreateLogicalPlan(query *parse.SqlQuery) (engine.Relation, error) {
	var plan engine.Relation

	if query.Read != nil {
		switch t := query.Read.Table.(type) {
		case *parse.SqlIdentifier:
			table := engine.NewNamedTable(t.Names, nil)
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

	return plan, nil
}
