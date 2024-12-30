package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/joellubi/bonobo"
	"github.com/joellubi/bonobo/substrait"

	"github.com/substrait-io/substrait-go/v3/proto"
)

type Expr interface {
	fmt.Stringer
	ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error)

	Field(input Relation) (bonobo.Field, error)
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

func (expr *Column) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
	var structField *proto.Expression_ReferenceSegment_StructField

	// We cannot represent a named column ref
	// without knowing the underlying schema
	schema, err := input.Schema()
	if err != nil {
		return nil, fmt.Errorf("input schema required to serialize Column expr: %w", err)
	}

	for i, name := range schema.Names {
		if name == expr.name {
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

func (expr *Column) Field(input Relation) (bonobo.Field, error) {
	inputSchema, err := input.Schema()
	if err != nil {
		return bonobo.Field{}, err
	}

	for _, field := range inputSchema.Fields() {
		if field.Name == expr.name {
			return field, nil
		}
	}
	return bonobo.Field{}, fmt.Errorf("no column named %s", expr.name)
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

func (expr *ColumnIndex) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
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

func (expr *ColumnIndex) Field(input Relation) (bonobo.Field, error) {
	inputSchema, err := input.Schema()
	if err != nil {
		return bonobo.Field{}, err
	}

	fields := inputSchema.Fields()
	if expr.index >= len(fields) {
		return bonobo.Field{}, fmt.Errorf("column index %d out of range for input with %d fields", expr.index, len(fields))
	}

	return fields[expr.index], nil
}

func (expr *ColumnIndex) String() string {
	return fmt.Sprintf("#%d", expr.index)
}

func NewLiteralExpr(val any) *Literal {
	var typ bonobo.Type

	switch v := val.(type) {
	case bool:
		typ = bonobo.Types.BooleanType(false)
	case int8:
		typ = bonobo.Types.Int8Type(false)
	case int16:
		typ = bonobo.Types.Int16Type(false)
	case int32:
		typ = bonobo.Types.Int32Type(false)
	case int64:
		typ = bonobo.Types.Int64Type(false)
	case int:
		typ = bonobo.Types.Int64Type(false)
		val = int64(v)
	case float32:
		typ = bonobo.Types.FloatType(false)
	case float64:
		typ = bonobo.Types.DoubleType(false)
	case string:
		typ = bonobo.Types.StringType(false)
	default:
		panic(fmt.Sprintf("invalid literal type: %T", v))
	}

	return &Literal{val: val, typ: typ}
}

type Literal struct {
	val any
	typ bonobo.Type
}

func (expr *Literal) Field(input Relation) (bonobo.Field, error) {
	return bonobo.Field{Name: expr.Name(), Type: expr.typ}, nil
}

func (expr *Literal) String() string {
	return fmt.Sprintf("%s::%s", expr.Name(), expr.typ)
}

func (expr *Literal) Name() string {
	switch v := expr.val.(type) {
	case bool:
		return strconv.FormatBool(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v
	default:
		panic(fmt.Sprintf("invalid literal type: %T", v))
	}
}

func (expr *Literal) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
	var exprLiteral *proto.Expression_Literal

	switch v := expr.val.(type) {
	case bool:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_Boolean{
				Boolean: v,
			},
		}
	case int8:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_I8{
				I8: int32(v),
			},
		}
	case int16:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_I16{
				I16: int32(v),
			},
		}
	case int32:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_I32{
				I32: v,
			},
		}
	case int64:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_I64{
				I64: v,
			},
		}
	case float32:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_Fp32{
				Fp32: v,
			},
		}
	case float64:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_Fp64{
				Fp64: v,
			},
		}
	case string:
		exprLiteral = &proto.Expression_Literal{
			LiteralType: &proto.Expression_Literal_String_{
				String_: v,
			},
		}
	default:
		panic(fmt.Sprintf("invalid literal type: %T", v))
	}

	return &proto.Expression{
		RexType: &proto.Expression_Literal_{
			Literal: exprLiteral,
		},
	}, nil
}

func NewAliasExpr(expr Expr, name string) *Alias {
	return &Alias{child: expr, alias: name}
}

type Alias struct {
	child Expr
	alias string
}

// Field implements Expr.
func (expr *Alias) Field(input Relation) (bonobo.Field, error) {
	field, err := expr.child.Field(input)
	if err != nil {
		return bonobo.Field{}, err
	}

	field.Name = expr.alias
	return field, nil
}

// String implements Expr.
func (expr *Alias) String() string {
	return fmt.Sprintf("%s AS %s", expr.child.String(), expr.alias)
}

// ToProto implements Expr.
func (expr *Alias) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
	return expr.child.ToProto(input, extensions)
}

var _ Expr = (*Column)(nil)
var _ Expr = (*ColumnIndex)(nil)
var _ Expr = (*Literal)(nil)
var _ Expr = (*Alias)(nil)
