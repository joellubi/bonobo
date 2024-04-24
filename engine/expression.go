package engine

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/substrait-io/substrait-go/proto"
)

type Expr interface {
	fmt.Stringer
	ToProto(input Plan) (*proto.Expression, error)

	Field(input Plan) (arrow.Field, error)
}

type ExprList []Expr

func (exprs ExprList) String() string {
	s := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		s = append(s, expr.String())
	}

	return strings.Join(s, ", ")
}

func NewColumnExpr(name string) *Column {
	return &Column{name: name}
}

type Column struct {
	name string
}

func (expr *Column) ToProto(input Plan) (*proto.Expression, error) {
	var structField *proto.Expression_ReferenceSegment_StructField

	// We cannot represent a named column ref
	// without knowing the underlying schema
	schema, err := input.Schema()
	if err != nil {
		return nil, fmt.Errorf("input schema required to serialize Column expr: %w", err)
	}

	for i, field := range schema.Fields() {
		if field.Name == expr.name {
			structField = &proto.Expression_ReferenceSegment_StructField{Field: int32(i)}
			break
		}
	}

	if structField == nil {
		return nil, fmt.Errorf("cannot marshal Column expr to proto: input does not contain field: %s", expr.name)
	}

	return &proto.Expression{
		RexType: &proto.Expression_Selection{
			Selection: &proto.Expression_FieldReference{
				ReferenceType: &proto.Expression_FieldReference_DirectReference{
					DirectReference: &proto.Expression_ReferenceSegment{
						ReferenceType: &proto.Expression_ReferenceSegment_StructField_{
							StructField: structField,
						},
					},
				},
			},
		},
	}, nil
}

func (expr *Column) Field(input Plan) (arrow.Field, error) {
	inputSchema, err := input.Schema()
	if err != nil {
		return arrow.Field{}, err
	}

	fields, ok := inputSchema.FieldsByName(expr.name)
	if !ok {
		return arrow.Field{}, fmt.Errorf("no column named %s", expr.name)
	}

	if len(fields) != 1 {
		return arrow.Field{}, fmt.Errorf("column expression with %d fields: %s, unimplemented", len(fields), fields)
	}

	return fields[0], nil
}

func (expr *Column) String() string {
	return fmt.Sprintf("#%s", expr.name)
}

func NewColumnIndexExpr(index int) *ColumnIndex {
	return &ColumnIndex{index: index}
}

type ColumnIndex struct {
	index int
}

func (expr *ColumnIndex) ToProto(input Plan) (*proto.Expression, error) {
	return &proto.Expression{
		RexType: &proto.Expression_Selection{
			Selection: &proto.Expression_FieldReference{
				ReferenceType: &proto.Expression_FieldReference_DirectReference{
					DirectReference: &proto.Expression_ReferenceSegment{
						ReferenceType: &proto.Expression_ReferenceSegment_StructField_{
							StructField: &proto.Expression_ReferenceSegment_StructField{
								Field: int32(expr.index),
							},
						},
					},
				},
			},
		},
	}, nil
}

func (expr *ColumnIndex) Field(input Plan) (arrow.Field, error) {
	inputSchema, err := input.Schema()
	if err != nil {
		return arrow.Field{}, err
	}

	return inputSchema.Field(expr.index), nil
}

func (expr *ColumnIndex) String() string {
	return fmt.Sprintf("#%d", expr.index)
}

var _ Expr = (*Column)(nil)
var _ Expr = (*ColumnIndex)(nil)
