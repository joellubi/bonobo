package engine

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/substrait-io/substrait-go/proto"
)

func FormatPlanText(plan Plan) string {
	var (
		bldr   strings.Builder
		indent int
	)
	formatPlan(plan, &bldr, indent)
	return bldr.String()
}

func formatPlan(plan Plan, bldr *strings.Builder, indent int) {
	if indent > 0 {
		bldr.WriteString("\n")
	}

	for i := 0; i < indent; i++ {
		bldr.WriteString("\t")
	}

	bldr.WriteString(plan.String())
	for _, child := range plan.Children() {
		formatPlan(child, bldr, indent+1)
	}
}

func formatSchema(schema *arrow.Schema) string {
	if schema == nil {
		return "None"
	}
	var bldr strings.Builder
	for i, f := range schema.Fields() {
		if i != 0 {
			bldr.WriteString(", ")
		}
		fmt.Fprintf(&bldr, "%s: %s", f.Name, f.Type)
	}
	return bldr.String()
}

func schemaToNamedStruct(schema *arrow.Schema) *proto.NamedStruct {
	names := make([]string, schema.NumFields())
	types := make([]*proto.Type, schema.NumFields())
	for i, field := range schema.Fields() {
		names[i] = field.Name
		types[i] = protoTypeForArrowType(field.Type, field.Nullable)
	}

	return &proto.NamedStruct{
		Names: names,
		Struct: &proto.Type_Struct{
			Types:       types,
			Nullability: proto.Type_NULLABILITY_REQUIRED,
		},
	}
}

func protoTypeForArrowType(arrowType arrow.DataType, nullable bool) *proto.Type {
	nullability := proto.Type_NULLABILITY_REQUIRED
	if nullable {
		nullability = proto.Type_NULLABILITY_NULLABLE
	}

	switch arrowType.ID() {
	case ArrowTypes.BooleanType.ID():
		return &proto.Type{
			Kind: &proto.Type_Bool{
				Bool: &proto.Type_Boolean{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.Int8Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I8_{
				I8: &proto.Type_I8{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.Int16Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I16_{
				I16: &proto.Type_I16{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.Int32Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I32_{
				I32: &proto.Type_I32{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.Int64Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I64_{
				I64: &proto.Type_I64{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.FloatType.ID():
		return &proto.Type{
			Kind: &proto.Type_Fp32{
				Fp32: &proto.Type_FP32{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.DoubleType.ID():
		return &proto.Type{
			Kind: &proto.Type_Fp64{
				Fp64: &proto.Type_FP64{
					Nullability: nullability,
				},
			},
		}
	case ArrowTypes.StringType.ID():
		return &proto.Type{
			Kind: &proto.Type_String_{
				String_: &proto.Type_String{
					Nullability: nullability,
				},
			},
		}
	default:
		panic(fmt.Sprintf("unrecognized type: %s", arrowType.Name()))
	}
}
