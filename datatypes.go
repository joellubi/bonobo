package bonobo

import (
	"fmt"

	"github.com/substrait-io/substrait-go/v3/proto"
	"github.com/substrait-io/substrait-go/v3/types"
)

type Type types.Type

type Field struct {
	Name string
	Type Type
}

func (f Field) String() string {
	return fmt.Sprintf("%s::%s", f.Name, f.Type)
}

type Schema types.NamedStruct

func NewSchema(fields []Field) *Schema {
	fieldNames := make([]string, len(fields))
	fieldTypes := make([]types.Type, len(fields))
	for i, field := range fields {
		fieldNames[i] = field.Name
		fieldTypes[i] = field.Type
	}

	return &Schema{
		Names: fieldNames,
		Struct: types.StructType{
			Nullability: types.NullabilityRequired,
			Types:       fieldTypes,
		},
	}
}

func NewSchemaFromProto(n *proto.NamedStruct) *Schema {
	schema := Schema(types.NewNamedStructFromProto(n))
	return &schema
}

func (s *Schema) Fields() []Field {
	fields := make([]Field, len(s.Names))
	for i := range fields {
		typ := s.Struct.Types[i]
		fields[i] = Field{Name: s.Names[i], Type: typ}
	}
	return fields
}

func (s *Schema) Len() int {
	return len(s.Names)
}

func (s *Schema) String() string {
	return (*types.NamedStruct)(s).String()
}

var Types = struct {
	BooleanType func(nullable bool) Type
	Int8Type    func(nullable bool) Type
	Int16Type   func(nullable bool) Type
	Int32Type   func(nullable bool) Type
	Int64Type   func(nullable bool) Type
	FloatType   func(nullable bool) Type
	DoubleType  func(nullable bool) Type
	DateType    func(nullable bool) Type
	StringType  func(nullable bool) Type
	DecimalType func(p, s int32, nullable bool) Type
}{
	BooleanType: func(nullable bool) Type {
		return withNullability(&types.BooleanType{}, nullable)
	},
	Int8Type: func(nullable bool) Type {
		return withNullability(&types.Int8Type{}, nullable)
	},
	Int16Type: func(nullable bool) Type {
		return withNullability(&types.Int16Type{}, nullable)
	},
	Int32Type: func(nullable bool) Type {
		return withNullability(&types.Int32Type{}, nullable)
	},
	Int64Type: func(nullable bool) Type {
		return withNullability(&types.Int64Type{}, nullable)
	},
	FloatType: func(nullable bool) Type {
		return withNullability(&types.Float32Type{}, nullable)
	},
	DoubleType: func(nullable bool) Type {
		return withNullability(&types.Float64Type{}, nullable)
	},
	DateType: func(nullable bool) Type {
		return withNullability(&types.DateType{}, nullable)
	},
	StringType: func(nullable bool) Type {
		return withNullability(&types.StringType{}, nullable)
	},
	DecimalType: func(p, s int32, nullable bool) Type {
		return withNullability(&types.DecimalType{Precision: p, Scale: s}, nullable)
	},
}

func withNullability(typ Type, nullable bool) Type {
	nullability := types.NullabilityRequired
	if nullable {
		nullability = types.NullabilityNullable
	}
	return typ.WithNullability(nullability)
}
