package parse

import (
	"fmt"
	"strings"
)

type SqlRelation interface {
	SqlExpr
	Name() string
}

func SqlSelectRelation(exprs []SqlExpr) *sqlSelectRelation {
	return &sqlSelectRelation{Exprs: exprs}
}

type sqlSelectRelation struct {
	Exprs []SqlExpr
}

// Children implements SqlRelation.
func (r *sqlSelectRelation) Children() []SqlNode {
	children := make([]SqlNode, len(r.Exprs))
	for i, expr := range r.Exprs {
		children[i] = expr
	}
	return children
}

// Name implements SqlRelation.
func (*sqlSelectRelation) Name() string {
	return "SELECT"
}

func (r *sqlSelectRelation) String() string {
	s := make([]string, 0, len(r.Exprs))
	for _, expr := range r.Exprs {
		s = append(s, expr.String())
	}

	return fmt.Sprintf("%s\n\t%s", r.Name(), strings.Join(s, ",\n\t"))
}

func SqlFromRelation(table SqlExpr) *sqlFromRelation {
	return &sqlFromRelation{Table: table}
}

type sqlFromRelation struct {
	Table SqlExpr
}

// Children implements SqlRelation.
func (r *sqlFromRelation) Children() []SqlNode {
	return []SqlNode{r.Table}
}

// Name implements SqlRelation.
func (*sqlFromRelation) Name() string {
	return "FROM"
}

func (r *sqlFromRelation) String() string {
	return fmt.Sprintf("%s\n\t%s", r.Name(), r.Table.String())
}

var _ SqlRelation = (*sqlSelectRelation)(nil)
var _ SqlRelation = (*sqlFromRelation)(nil)
