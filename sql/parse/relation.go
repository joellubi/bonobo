package parse

import (
	"fmt"
	"strings"
)

type SqlRelation interface {
	SqlExpr
}

func SqlSelectRelation(exprs []SqlExpr) *sqlSelectRelation {
	return &sqlSelectRelation{exprs: exprs}
}

type sqlSelectRelation struct {
	exprs []SqlExpr
}

func (r *sqlSelectRelation) String() string {
	s := make([]string, 0, len(r.exprs))
	for _, expr := range r.exprs {
		s = append(s, expr.String())
	}

	return fmt.Sprintf("SELECT\n\t%s", strings.Join(s, ",\n\t"))
}

func SqlFromRelation(table SqlExpr) *sqlFromRelation {
	return &sqlFromRelation{table: table}
}

type sqlFromRelation struct {
	table SqlExpr
}

func (r *sqlFromRelation) String() string {
	return fmt.Sprintf("FROM\n\t%s", r.table.String())
}

var _ SqlRelation = (*sqlSelectRelation)(nil)
var _ SqlRelation = (*sqlFromRelation)(nil)
