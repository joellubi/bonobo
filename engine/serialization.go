package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/joellubi/bonobo"
	"github.com/joellubi/bonobo/substrait"

	"github.com/substrait-io/substrait-go/v3/proto"
	"github.com/substrait-io/substrait-go/v3/types"
	"google.golang.org/protobuf/encoding/protojson"
)

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

func formatSchema(schema *bonobo.Schema) string {
	if schema == nil {
		return "None"
	}
	var bldr strings.Builder
	for i, typ := range schema.Struct.Types {
		if i != 0 {
			bldr.WriteString(", ")
		}
		fmt.Fprintf(&bldr, "%s: %s", schema.Names[i], typ)
	}
	return bldr.String()
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
		exprs := make([]Expr, schema.Len())
		for i := range exprs {
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

	output := types.TypeFromProto(expr.GetOutputType())

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
	schema := bonobo.NewSchemaFromProto(rel.GetBaseSchema())

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
