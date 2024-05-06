package parse

import "fmt"

type SqlQuery struct {
	Alias      string
	Read       *sqlFromRelation
	Projection *sqlSelectRelation
	Filter     *sqlWhereRelation
}

// Children implements SqlExpr.
func (q *SqlQuery) Children() []SqlNode {
	children := make([]SqlNode, 0)
	if q.Read != nil {
		children = append(children, q.Read)
	}
	if q.Projection != nil {
		children = append(children, q.Projection)
	}
	return children
}

func (q *SqlQuery) String() string {
	return "TODO"
}

// func NewQueryBuilder() *SqlQ

type SqlQueryBuilder struct {
	query SqlQuery
}

func (bldr *SqlQueryBuilder) Select(rel *sqlSelectRelation) error {
	if bldr.query.Projection != nil {
		return fmt.Errorf("parse: query cannot have more than one SELECT")
	}

	bldr.query.Projection = rel
	return nil
}

func (bldr *SqlQueryBuilder) From(rel *sqlFromRelation) error {
	if bldr.query.Read != nil {
		return fmt.Errorf("parse: query cannot have more than one FROM")
	}

	bldr.query.Read = rel
	return nil
}

func (bldr *SqlQueryBuilder) Where(rel *sqlWhereRelation) error {
	if bldr.query.Filter != nil {
		return fmt.Errorf("parse: query cannot have more than one WHERE")
	}

	bldr.query.Filter = rel
	return nil
}

func (bldr *SqlQueryBuilder) Query() *SqlQuery {
	query := bldr.query
	bldr.query = SqlQuery{}
	return &query
}

var _ SqlExpr = (*SqlQuery)(nil)
