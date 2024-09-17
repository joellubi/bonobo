package engine

import (
	"errors"
	"fmt"

	"github.com/joellubi/bonobo"
	"github.com/joellubi/bonobo/substrait"

	"github.com/substrait-io/substrait-go/proto"
)

type Relation interface { // TODO: Plan implements Table?
	fmt.Stringer
	ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error)

	Schema() (*bonobo.Schema, error)
	Children() []Relation
}

func NewReadOperation(table Table) *Read {
	return &Read{table: table}
}

type Read struct {
	table Table
}

func (*Read) Children() []Relation {
	return nil
}

func (r *Read) Schema() (*bonobo.Schema, error) {
	return r.table.Schema()
}

func (r *Read) String() string {
	schema, err := r.Schema()
	if err != nil && !errors.Is(err, ErrUnboundTable) {
		panic(err)
	}
	return fmt.Sprintf("Read: schema=[%s], projection=%s", formatSchema(schema), "None") // TODO: Projection
}

func (r *Read) ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error) {
	p, err := r.table.ToProto(extensions)
	if err != nil {
		return nil, err
	}

	// TODO: Schema projection stuff
	return p, err
}

func NewProjectionOperation(input Relation, exprs []Expr) *Projection {
	return &Projection{input: input, exprs: exprs}
}

type Projection struct {
	input Relation
	exprs ExprList
}

func (p *Projection) Schema() (*bonobo.Schema, error) {
	fields := make([]bonobo.Field, len(p.exprs))
	fieldNames := make(map[string]bool, len(p.exprs))
	for i, expr := range p.exprs {
		if expr == nil {
			return nil, fmt.Errorf("invalid Projection, expr is nil")
		}
		f, err := expr.Field(p.input)
		if err != nil {
			return nil, err
		}

		// TODO: Broader duplicate detection, outside of only projection
		if _, found := fieldNames[f.Name]; found {
			return nil, fmt.Errorf("invalid Projection, duplicate field name %s", f.Name)
		}

		fieldNames[f.Name] = true
		fields[i] = f
	}

	return bonobo.NewSchema(fields), nil
}

func (p *Projection) Children() []Relation {
	return []Relation{p.input}
}

// String implements Plan.
func (p *Projection) String() string {
	return fmt.Sprintf("Projection: %s", p.exprs)
}

// ToProto implements Plan.
func (p *Projection) ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error) {
	var err error

	exprs := make([]*proto.Expression, len(p.exprs))
	for i, expr := range p.exprs {
		exprs[i], err = expr.ToProto(p.input, extensions)
		if err != nil {
			return nil, err
		}
	}

	childRel, err := p.input.ToProto(extensions)
	if err != nil {
		return nil, err
	}

	return &proto.Rel{
		RelType: &proto.Rel_Project{
			Project: &proto.ProjectRel{
				Input:       childRel,
				Expressions: exprs,
			},
		},
	}, nil
}

func NewSelectionOperation(input Relation, expr Expr) *Selection {
	return &Selection{input: input, expr: expr}
}

type Selection struct {
	input Relation
	expr  Expr
}

func (s *Selection) ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error) {
	expr, err := s.expr.ToProto(s.input, extensions)
	if err != nil {
		return nil, err
	}

	childRel, err := s.input.ToProto(extensions)
	if err != nil {
		return nil, err
	}

	return &proto.Rel{
		RelType: &proto.Rel_Filter{
			Filter: &proto.FilterRel{
				Input:     childRel,
				Condition: expr,
			},
		},
	}, nil
}

func (s *Selection) Schema() (*bonobo.Schema, error) {
	return s.input.Schema()
}

func (s *Selection) Children() []Relation {
	return []Relation{s.input}
}

func (s *Selection) String() string {
	return fmt.Sprintf("Selection: %s", s.expr)
}

func (s *Selection) Child() Relation {
	return s.input
}

func SetCatalogForPlan(plan *Plan, catalog Catalog) {
	for _, relation := range plan.Relations() {
		SetCatalogForRelation(relation, catalog)
	}
}

func SetCatalogForRelation(plan Relation, catalog Catalog) {
	r, ok := plan.(*Read)
	if ok {
		SetCatalogForTable(r.table, catalog)
		return
	}

	for _, child := range plan.Children() {
		SetCatalogForRelation(child, catalog)
	}
}

func SetCatalogForTable(table Table, catalog Catalog) {
	t, ok := table.(NamedTable)
	if !ok {
		return
	}

	t.SetCatalog(catalog)
}

var _ Relation = (*Read)(nil)
var _ Relation = (*Projection)(nil)
var _ Relation = (*Selection)(nil)
