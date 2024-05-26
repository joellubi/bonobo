package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/joellubi/bonobo/substrait"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/substrait-io/substrait-go/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

func FormatPlanText(plan Relation) string {
	var (
		bldr   strings.Builder
		indent int
	)
	formatPlan(plan, &bldr, indent)
	return bldr.String()
}

func formatPlan(plan Relation, bldr *strings.Builder, indent int) {
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

func FormatPlan(plan *Plan) (string, error) {
	planProto, err := plan.ToProto()
	if err != nil {
		return "", err
	}

	return FormatPlanProto(planProto)
}

func FormatPlanProto(plan *proto.Plan) (string, error) {
	marshaller := protojson.MarshalOptions{
		UseProtoNames: true,
		// EmitUnpopulated: true,
	}

	data, err := marshaller.Marshal(plan)
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
		types[i], err = ProtoTypeForArrowType(field.Type, field.Nullable)
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
	case *proto.Type_Decimal_:
		nullability = t.Decimal.GetNullability()
		arrowType = ArrowTypes.Decimal(t.Decimal.GetPrecision(), t.Decimal.GetScale())
	case *proto.Type_Date_:
		nullability = t.Date.GetNullability()
		arrowType = ArrowTypes.DateType
	default:
		err = fmt.Errorf("unsupported proto type: %s", protoType.GetKind())
	}

	nullable = nullability == proto.Type_NULLABILITY_NULLABLE
	return arrowType, nullable, err
}

func ProtoTypeForArrowType(arrowType arrow.DataType, nullable bool) (*proto.Type, error) {
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
	case ArrowTypes.DateType.ID():
		return &proto.Type{
			Kind: &proto.Type_Date_{
				Date: &proto.Type_Date{
					Nullability: nullability,
				},
			},
		}, nil
	case arrow.DECIMAL128:
		dec, ok := arrowType.(*arrow.Decimal128Type)
		if !ok {
			return nil, fmt.Errorf("cannot convert arrow to substrait type: invalid Decimal128: %s", arrowType)
		}

		return &proto.Type{
			Kind: &proto.Type_Decimal_{
				Decimal: &proto.Type_Decimal{
					Precision: dec.GetPrecision(),
					Scale:     dec.GetScale(),
				},
			},
		}, nil
	case arrow.DECIMAL256:
		dec, ok := arrowType.(*arrow.Decimal256Type)
		if !ok {
			return nil, fmt.Errorf("cannot convert arrow to substrait type: invalid Decimal256: %s", arrowType)
		}

		return &proto.Type{
			Kind: &proto.Type_Decimal_{
				Decimal: &proto.Type_Decimal{
					Precision: dec.GetPrecision(),
					Scale:     dec.GetScale(),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized arrow type: %s", arrowType.Name())
	}
}

func FromProto(plan *proto.Plan) (*Plan, error) {
	var err error

	extensions, err := substrait.NewExtensionRegistryFromProto(plan)
	if err != nil {
		return nil, err
	}

	bldr := planBuilder{extensions: extensions}

	relations := make([]Relation, 0)
	var rootRelation Relation
	for _, planRel := range plan.GetRelations() {
		switch t := planRel.GetRelType().(type) {
		case *proto.PlanRel_Root:
			if rootRelation != nil {
				return nil, fmt.Errorf("cannot unmarshall plan with multiple root relations: unsupported")
			}

			rootRelation, err = bldr.RelRoot(t.Root.Input, t.Root.Names)
			if err != nil {
				return nil, err
			}
		case *proto.PlanRel_Rel:
			rel, err := bldr.Rel(t.Rel)
			if err != nil {
				return nil, err
			}
			relations = append(relations, rel)
		default:
			return nil, fmt.Errorf("unrecognized proto.PlanRel type: %T", t)
		}
	}

	return NewPlan(rootRelation, relations...), nil
}

type planBuilder struct {
	extensions substrait.ExtensionRegistry
}

func (bldr *planBuilder) RelRoot(rel *proto.Rel, names []string) (Relation, error) {
	r, err := bldr.Rel(rel)
	if err != nil {
		return nil, err
	}

	schema, err := r.Schema()
	if err != nil {
		return nil, err
	}

	var aliasing bool
	for i, field := range schema.Fields() {
		if names[i] != field.Name {
			aliasing = true
			break
		}
	}

	if aliasing {
		exprs := make([]Expr, schema.NumFields())
		for i := 0; i < schema.NumFields(); i++ {
			exprs[i] = NewAliasExpr(NewColumnIndexExpr(i), names[i])
		}

		r = NewProjectionOperation(r, exprs)
	}

	return r, nil
}

func (bldr *planBuilder) Rel(rel *proto.Rel) (Relation, error) {
	switch r := rel.GetRelType().(type) {
	case *proto.Rel_Read:
		return bldr.Read(r.Read)
	case *proto.Rel_Project:
		return bldr.Project(r.Project)
	case *proto.Rel_Filter:
		return bldr.Filter(r.Filter)
	default:
		return nil, fmt.Errorf("cannot construct Plan from proto: unrecognized rel type: %T", r)
	}
}

func (bldr *planBuilder) Expr(expr *proto.Expression) (Expr, error) {
	switch e := expr.GetRexType().(type) {
	case *proto.Expression_Literal_:
		return bldr.LiteralExpr(e.Literal)
	case *proto.Expression_Selection:
		return bldr.FieldReferenceExpr(e.Selection)
	case *proto.Expression_ScalarFunction_:
		return bldr.ScalarFunctionExpr(e.ScalarFunction)
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

func (bldr *planBuilder) LiteralExpr(expr *proto.Expression_Literal) (Expr, error) {
	switch e := expr.GetLiteralType().(type) {
	case *proto.Expression_Literal_Boolean:
		return NewLiteralExpr(e.Boolean), nil
	case *proto.Expression_Literal_I8:
		return NewLiteralExpr(e.I8), nil
	case *proto.Expression_Literal_I16:
		return NewLiteralExpr(e.I16), nil
	case *proto.Expression_Literal_I32:
		return NewLiteralExpr(e.I32), nil
	case *proto.Expression_Literal_I64:
		return NewLiteralExpr(e.I64), nil
	case *proto.Expression_Literal_Fp32:
		return NewLiteralExpr(e.Fp32), nil
	case *proto.Expression_Literal_Fp64:
		return NewLiteralExpr(e.Fp64), nil
	case *proto.Expression_Literal_String_:
		return NewLiteralExpr(e.String_), nil
	default:
		return nil, fmt.Errorf("unrecognized proto.Expression_Literal type: %T", e)
	}
}

func (bldr *planBuilder) ScalarFunctionExpr(expr *proto.Expression_ScalarFunction) (Expr, error) {
	// TODO: expr.Options

	ext, uri, err := bldr.extensions.GetExtensionByReference(expr.GetFunctionReference())
	if err != nil {
		return nil, err
	}

	output, _, err := arrowTypeForProtoType(expr.GetOutputType())
	if err != nil {
		return nil, err
	}

	args := make([]Expr, len(expr.Arguments))
	for i, arg := range expr.Arguments {
		args[i], err = bldr.FunctionArgumentExpr(arg)
		if err != nil {
			return nil, err
		}
	}

	return NewAnonymousFunction(uri, ext.Name, output, args...)
}

func (bldr *planBuilder) FunctionArgumentExpr(expr *proto.FunctionArgument) (Expr, error) {
	switch e := expr.GetArgType().(type) {
	case *proto.FunctionArgument_Enum:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: FunctionArgument_Enum")
	case *proto.FunctionArgument_Type:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: FunctionArgument_Type")
	case *proto.FunctionArgument_Value:
		return bldr.Expr(e.Value)
	default:
		return nil, fmt.Errorf("unrecognized proto.FunctionArgument type: %T", e)
	}
}

func (bldr *planBuilder) FieldReferenceExpr(expr *proto.Expression_FieldReference) (Expr, error) {
	if expr.RootType != nil {
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_FieldReference.RootType")
	}

	switch e := expr.GetReferenceType().(type) {
	case *proto.Expression_FieldReference_DirectReference:
		return bldr.ReferenceSegmentExpr(e.DirectReference)
	case *proto.Expression_FieldReference_MaskedReference:
		return nil, fmt.Errorf("failed to build Expr: FromProto not implemented: Expression_FieldReference_MaskedReference")
	default:
		return nil, fmt.Errorf("unrecognized proto.Expression_FieldReference type: %T", e)
	}
}

func (bldr *planBuilder) ReferenceSegmentExpr(expr *proto.Expression_ReferenceSegment) (Expr, error) {
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

func (bldr *planBuilder) Read(rel *proto.ReadRel) (*Read, error) {
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

func (bldr *planBuilder) Project(rel *proto.ProjectRel) (*Projection, error) {
	var err error

	exprs := make([]Expr, len(rel.GetExpressions()))
	for i, expr := range rel.GetExpressions() {
		exprs[i], err = bldr.Expr(expr)
		if err != nil {
			return nil, err
		}
	}

	input, err := bldr.Rel(rel.GetInput())
	if err != nil {
		return nil, err
	}

	return NewProjectionOperation(input, exprs), nil
}

func (bldr *planBuilder) Filter(rel *proto.FilterRel) (*Selection, error) {
	var err error

	expr, err := bldr.Expr(rel.GetCondition())
	if err != nil {
		return nil, err
	}

	input, err := bldr.Rel(rel.GetInput())
	if err != nil {
		return nil, err
	}

	return NewSelectionOperation(input, expr), nil
}
