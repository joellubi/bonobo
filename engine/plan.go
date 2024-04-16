package engine

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/substrait-io/substrait-go/proto"
)

type Plan interface { // TODO: Plan implements Table?
	fmt.Stringer
	ToProto() (*proto.Rel, error)

	Schema() (*arrow.Schema, error)
	Children() []Plan
}

func NewReadOperation(table Table) *Read {
	return &Read{table: table}
}

type Read struct {
	table Table
}

func (*Read) Children() []Plan {
	return nil
}

func (r *Read) Schema() (*arrow.Schema, error) {
	return r.table.Schema()
}

func (r *Read) String() string {
	schema, err := r.Schema()
	if err != nil && !errors.Is(err, ErrUnboundTable) {
		panic(err)
	}
	return fmt.Sprintf("Read: schema=[%s], projection=%s", formatSchema(schema), "None") // TODO: Projection
}

func (r *Read) ToProto() (*proto.Rel, error) {
	p, err := r.table.ToProto()
	if err != nil {
		return nil, err
	}

	// TODO: Schema projection stuff
	return p, err
}

func NewProjectionOperation(input Plan, exprs []Expr) *Projection {
	return &Projection{input: input, exprs: exprs}
}

type Projection struct {
	input Plan
	exprs ExprList
}

func (p *Projection) evaluateSchema() (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(p.exprs))
	fieldNames := make(map[string]bool, len(p.exprs))
	for _, expr := range p.exprs {
		if expr == nil {
			return nil, fmt.Errorf("invalid Projection, expr is nil")
		}
		f, err := expr.Field(p.input)
		if err != nil {
			return nil, err
		}

		if _, found := fieldNames[f.Name]; found {
			return nil, fmt.Errorf("invalid Projection, duplicate field name %s", f.Name)
		}

		fieldNames[f.Name] = true
		fields = append(fields, f)
	}

	return arrow.NewSchema(fields, nil), nil
}

func (p *Projection) Children() []Plan {
	return []Plan{p.input}
}

func (p *Projection) Schema() (*arrow.Schema, error) {
	return p.evaluateSchema() // TODO: Consolidate
}

// String implements Plan.
func (p *Projection) String() string {
	return fmt.Sprintf("Projection: %s", p.exprs)
}

// ToProto implements Plan.
func (p *Projection) ToProto() (*proto.Rel, error) {
	var err error

	exprs := make([]*proto.Expression, len(p.exprs))
	for i, expr := range p.exprs {
		exprs[i], err = expr.ToProto(p.input)
		if err != nil {
			return nil, err
		}
	}

	childRel, err := p.input.ToProto()
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

func NewSelectionOperation(input Plan, expr Expr) *Selection {
	return &Selection{input: input, expr: expr}
}

type Selection struct {
	input Plan
	expr  Expr
}

func (s *Selection) ToProto() (*proto.Rel, error) {
	expr, err := s.expr.ToProto(s.input)
	if err != nil {
		return nil, err
	}

	childRel, err := s.input.ToProto()
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

func (s *Selection) Schema() (*arrow.Schema, error) {
	return s.input.Schema()
}

func (s *Selection) Children() []Plan {
	return []Plan{s.input}
}

func (s *Selection) String() string {
	return fmt.Sprintf("Selection: %s", s.expr)
}

func (s *Selection) Child() Plan {
	return s.input
}

func SetCatalogForPlan(plan Plan, catalog Catalog) {
	r, ok := plan.(*Read)
	if ok {
		SetCatalogForTable(r.table, catalog)
		return
	}

	for _, child := range plan.Children() {
		SetCatalogForPlan(child, catalog)
	}
}

func SetCatalogForTable(table Table, catalog Catalog) {
	t, ok := table.(NamedTable)
	if !ok {
		return
	}

	t.SetCatalog(catalog)
}

var _ Plan = (*Read)(nil)
var _ Plan = (*Projection)(nil)
var _ Plan = (*Selection)(nil)
