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

		var ident engine.Expr
		ident = engine.NewColumnExpr(e.Names[0])
		if e.Alias != "" {
			ident = engine.NewAliasExpr(ident, e.Alias)
		}

		return ident, nil
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
	var (
		plan engine.Relation
		err  error
	)

	if query.Read != nil {
		switch t := query.Read.Table.(type) {
		case *parse.SqlIdentifier:
			table := engine.NewNamedTable(t.Names, nil)
			plan = engine.NewReadOperation(table)
		case *parse.SqlQuery:
			plan, err = CreateLogicalPlan(t)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("plan: unrecognized SqlExpr type for Read operation: %T", t)
		}
	} else {
		table := engine.NewVirtualTable(nil)
		plan = engine.NewReadOperation(table)
	}

	if query.Filter != nil {
		expr, err := CreateLogicalExpr(query.Filter.Expr)
		if err != nil {
			return nil, fmt.Errorf("parse: failed to plan SQL query: %w", err)
		}
		plan = engine.NewSelectionOperation(plan, expr)
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
