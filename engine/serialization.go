package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/substrait-io/substrait-go/proto"
	"google.golang.org/protobuf/encoding/protojson"
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

func FormatPlan(plan Plan) (string, error) {
	rel, err := plan.ToProto()
	if err != nil {
		return "", err
	}

	marshaller := protojson.MarshalOptions{
		UseProtoNames: true,
		// EmitUnpopulated: true,
	}

	data, err := marshaller.Marshal(rel)
	if err != nil {
		return "", err
	}

	var raw json.RawMessage = data
	b, err := json.MarshalIndent(raw, "", " ")
	if err != nil {
		return "", err
	}

	return string(b), nil
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

func schemaToNamedStruct(schema *arrow.Schema) (*proto.NamedStruct, error) {
	var err error

	names := make([]string, schema.NumFields())
	types := make([]*proto.Type, schema.NumFields())
	for i, field := range schema.Fields() {
		names[i] = field.Name
		types[i], err = protoTypeForArrowType(field.Type, field.Nullable)
		if err != nil {
			return nil, err
		}
	}

	return &proto.NamedStruct{
		Names: names,
		Struct: &proto.Type_Struct{
			Types:       types,
			Nullability: proto.Type_NULLABILITY_REQUIRED,
		},
	}, err
}

func namedStructToSchema(namedStruct *proto.NamedStruct) (*arrow.Schema, error) {
	types := namedStruct.Struct.GetTypes()
	fields := make([]arrow.Field, len(namedStruct.Names))
	for i, name := range namedStruct.Names {
		arrowType, nullable, err := arrowTypeForProtoType(types[i])
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{Name: name, Type: arrowType, Nullable: nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}

func arrowTypeForProtoType(protoType *proto.Type) (arrow.DataType, bool, error) {
	var (
		arrowType   arrow.DataType
		nullability proto.Type_Nullability
		nullable    bool
		err         error
	)

	switch t := protoType.GetKind().(type) {
	case *proto.Type_Bool:
		nullability = t.Bool.GetNullability()
		arrowType = ArrowTypes.BooleanType
	case *proto.Type_I8_:
		nullability = t.I8.GetNullability()
		arrowType = ArrowTypes.Int8Type
	case *proto.Type_I16_:
		nullability = t.I16.GetNullability()
		arrowType = ArrowTypes.Int16Type
	case *proto.Type_I32_:
		nullability = t.I32.GetNullability()
		arrowType = ArrowTypes.Int32Type
	case *proto.Type_I64_:
		nullability = t.I64.GetNullability()
		arrowType = ArrowTypes.Int64Type
	case *proto.Type_Fp32:
		nullability = t.Fp32.GetNullability()
		arrowType = ArrowTypes.FloatType
	case *proto.Type_Fp64:
		nullability = t.Fp64.GetNullability()
		arrowType = ArrowTypes.DoubleType
	case *proto.Type_String_:
		nullability = t.String_.GetNullability()
		arrowType = ArrowTypes.StringType
	default:
		err = fmt.Errorf("unsupported proto type: %s", protoType.GetKind())
	}

	nullable = nullability == proto.Type_NULLABILITY_NULLABLE
	return arrowType, nullable, err
}

func protoTypeForArrowType(arrowType arrow.DataType, nullable bool) (*proto.Type, error) {
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
		}, nil
	case ArrowTypes.Int8Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I8_{
				I8: &proto.Type_I8{
					Nullability: nullability,
				},
			},
		}, nil
	case ArrowTypes.Int16Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I16_{
				I16: &proto.Type_I16{
					Nullability: nullability,
				},
			},
		}, nil
	case ArrowTypes.Int32Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I32_{
				I32: &proto.Type_I32{
					Nullability: nullability,
				},
			},
		}, nil
	case ArrowTypes.Int64Type.ID():
		return &proto.Type{
			Kind: &proto.Type_I64_{
				I64: &proto.Type_I64{
					Nullability: nullability,
				},
			},
		}, nil
	case ArrowTypes.FloatType.ID():
		return &proto.Type{
			Kind: &proto.Type_Fp32{
				Fp32: &proto.Type_FP32{
					Nullability: nullability,
				},
			},
		}, nil
	case ArrowTypes.DoubleType.ID():
		return &proto.Type{
			Kind: &proto.Type_Fp64{
				Fp64: &proto.Type_FP64{
					Nullability: nullability,
				},
			},
		}, nil
	case ArrowTypes.StringType.ID():
		return &proto.Type{
			Kind: &proto.Type_String_{
				String_: &proto.Type_String{
					Nullability: nullability,
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized arrow type: %s", arrowType.Name())
	}
}

func FromProto(rel *proto.Rel) (Plan, error) {
	switch r := rel.GetRelType().(type) {
	case *proto.Rel_Read:
		return FromProtoRead(r.Read)
	case *proto.Rel_Project:
		return FromProtoProject(r.Project)
	case *proto.Rel_Filter:
		return FromProtoFilter(r.Filter)
	default:
		return nil, fmt.Errorf("cannot construct Plan from proto: unrecognized rel type: %T", r)
	}
}

func FromProtoExpr(expr *proto.Expression) (Expr, error) {
	switch e := expr.GetRexType().(type) {
	case *proto.Expression_Literal_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_Literal") // TODO
	case *proto.Expression_Selection:
		return FromProtoFieldReferenceExpr(e.Selection)
	case *proto.Expression_ScalarFunction_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_ScalarFunction") // TODO
	case *proto.Expression_WindowFunction_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_WindowFunction") // TODO
	case *proto.Expression_IfThen_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_IfThen") // TODO
	case *proto.Expression_SwitchExpression_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_SwitchExpression") // TODO
	case *proto.Expression_SingularOrList_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_SingularOrList") // TODO
	case *proto.Expression_MultiOrList_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_MultiOrList") // TODO
	case *proto.Expression_Cast_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_Cast") // TODO
	case *proto.Expression_Subquery_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_Subquery") // TODO
	case *proto.Expression_Nested_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_Nested") // TODO
	case *proto.Expression_Enum_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_Enum") // TODO
	default:
		return nil, fmt.Errorf("unrecognized proto.Expression type: %T", e)
	}
}

func FromProtoFieldReferenceExpr(expr *proto.Expression_FieldReference) (Expr, error) {
	if expr.RootType != nil {
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_FieldReference.RootType")
	}

	switch e := expr.GetReferenceType().(type) {
	case *proto.Expression_FieldReference_DirectReference:
		return FromProtoReferenceSegmentExpr(e.DirectReference)
	case *proto.Expression_FieldReference_MaskedReference:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_FieldReference_MaskedReference")
	default:
		return nil, fmt.Errorf("unrecognized proto.Expression_FieldReference type: %T", e)
	}
}

func FromProtoReferenceSegmentExpr(expr *proto.Expression_ReferenceSegment) (Expr, error) {
	switch e := expr.GetReferenceType().(type) {
	case *proto.Expression_ReferenceSegment_ListElement_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_ReferenceSegment_ListElement")
	case *proto.Expression_ReferenceSegment_MapKey_:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_ReferenceSegment_MapKey")
	case *proto.Expression_ReferenceSegment_StructField_:
		if e.StructField.Child != nil {
			return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_ReferenceSegment.Child")
		}
		return NewColumnIndexExpr(int(e.StructField.Field)), nil
	default:
		return nil, fmt.Errorf("unrecognized proto.Expression_ReferenceSegment type: %T", e)
	}
}

func FromProtoRead(rel *proto.ReadRel) (*Read, error) {
	schema, err := namedStructToSchema(rel.GetBaseSchema())
	if err != nil {
		return nil, err
	}

	var table Table
	switch t := rel.GetReadType().(type) {
	case *proto.ReadRel_NamedTable_:
		table = NewNamedTable(t.NamedTable.GetNames(), NewAnonymousCatalog(schema))
	case *proto.ReadRel_VirtualTable_:
		return nil, fmt.Errorf("cannot construct Read operation from proto: unimplemented VirtualTable")
	case *proto.ReadRel_LocalFiles_:
		return nil, fmt.Errorf("cannot construct Read operation from proto: unimplemented LocalFiles")
	case *proto.ReadRel_ExtensionTable_:
		return nil, fmt.Errorf("cannot construct Read operation from proto: unimplemented ExtensionTable")
	}

	return NewReadOperation(table), nil
}

func FromProtoProject(rel *proto.ProjectRel) (*Projection, error) {
	var err error

	exprs := make([]Expr, len(rel.GetExpressions()))
	for i, expr := range rel.GetExpressions() {
		exprs[i], err = FromProtoExpr(expr)
		if err != nil {
			return nil, err
		}
	}

	input, err := FromProto(rel.GetInput())
	if err != nil {
		return nil, err
	}

	return NewProjectionOperation(input, exprs), nil
}

func FromProtoFilter(rel *proto.FilterRel) (*Selection, error) {
	var err error

	expr, err := FromProtoExpr(rel.GetCondition())
	if err != nil {
		return nil, err
	}

	input, err := FromProto(rel.GetInput())
	if err != nil {
		return nil, err
	}

	return NewSelectionOperation(input, expr), nil
}
