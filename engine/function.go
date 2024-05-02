package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/backdeck/backdeck/query/substrait"
	"github.com/substrait-io/substrait-go/proto"
)

type Function interface {
	Expr
	// Signature(input Relation) (string, error)
}

type FunctionImplementation struct {
	URI    string
	Inputs []arrow.DataType
	Output arrow.DataType
}

type function struct { // TODO: Make concrete
	name  string
	args  []Expr
	impls []FunctionImplementation
	impl  *functionImpl
}

type functionImpl struct {
	uri    string
	inputs []arrow.DataType
	output arrow.DataType
}

func (f *function) resolveImpl(input Relation) (*functionImpl, error) {
	if f.impl != nil {
		return f.impl, nil
	}

	var err error
	inputFields := make([]arrow.Field, len(f.args))
	for i, arg := range f.args {
		inputFields[i], err = arg.Field(input)
		if err != nil {
			return nil, err
		}
	}

outer:
	for _, impl := range f.impls {
		if len(inputFields) != len(impl.Inputs) {
			continue
		}
		for i, field := range inputFields {
			if field.Type.ID() != impl.Inputs[i].ID() {
				continue outer
			}
		}

		// Made it through the loop without any mismatches
		f.impl = &functionImpl{uri: impl.URI, inputs: impl.Inputs, output: impl.Output}
		return f.impl, nil
	}

	return nil, fmt.Errorf("no valid implementation for function: %s", f.String())
}

// Field implements Function.
func (f *function) Field(input Relation) (arrow.Field, error) {
	impl, err := f.resolveImpl(input)
	if err != nil {
		return arrow.Field{}, err
	}

	return arrow.Field{Name: f.name, Type: impl.output}, nil
}

func getFunctionSignature(name string, args ...arrow.DataType) (string, error) {
	var err error
	sigs := make([]string, len(args))
	for i, arg := range args {
		sigs[i], err = signatureNameForArgumentType(arg)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%s:%s", name, strings.Join(sigs, "_")), nil
}

// String implements Function.
func (f *function) String() string {
	args := make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ", "))
}

// ToProto implements Function.
func (f *function) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
	impl, err := f.resolveImpl(input)
	if err != nil {
		return nil, err
	}

	outputField, err := f.Field(input)
	if err != nil {
		return nil, err
	}

	outputType, err := protoTypeForArrowType(outputField.Type, outputField.Nullable)
	if err != nil {
		return nil, err
	}

	args := make([]*proto.FunctionArgument, len(f.args))
	for i, arg := range f.args {
		expr, err := arg.ToProto(input, extensions)
		if err != nil {
			return nil, err
		}

		args[i] = &proto.FunctionArgument{
			ArgType: &proto.FunctionArgument_Value{
				Value: expr,
			},
		}
	}

	sig, err := getFunctionSignature(f.name, impl.inputs...)
	if err != nil {
		return nil, err
	}

	ref := extensions.RegisterFunction(impl.uri, sig)

	return &proto.Expression{
		RexType: &proto.Expression_ScalarFunction_{
			ScalarFunction: &proto.Expression_ScalarFunction{
				FunctionReference: ref,
				Arguments:         args,
				OutputType:        outputType,
				Options:           []*proto.FunctionOption{}, // TODO
			},
		},
	}, nil
}

func getCanonicalFunctionName(name string) string {
	for canonical, aliases := range FunctionAliases {
		if name == canonical || slices.Contains(aliases, name) {
			return canonical
		}
	}
	return name
}

func getFunctionImplementations(name string) []FunctionImplementation {
	switch name {
	case "add":
		return []FunctionImplementation{
			{
				URI: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
				Inputs: []arrow.DataType{
					ArrowTypes.Int8Type,
					ArrowTypes.Int8Type,
				},
				Output: ArrowTypes.Int8Type,
			},
			{
				URI: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
				Inputs: []arrow.DataType{
					ArrowTypes.Int16Type,
					ArrowTypes.Int16Type,
				},
				Output: ArrowTypes.Int16Type,
			},
			{
				URI: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
				Inputs: []arrow.DataType{
					ArrowTypes.Int32Type,
					ArrowTypes.Int32Type,
				},
				Output: ArrowTypes.Int32Type,
			},
			{
				URI: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
				Inputs: []arrow.DataType{
					ArrowTypes.Int64Type,
					ArrowTypes.Int64Type,
				},
				Output: ArrowTypes.Int64Type,
			},
			{
				URI: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
				Inputs: []arrow.DataType{
					ArrowTypes.FloatType,
					ArrowTypes.FloatType,
				},
				Output: ArrowTypes.FloatType,
			},
			{
				URI: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
				Inputs: []arrow.DataType{
					ArrowTypes.DoubleType,
					ArrowTypes.DoubleType,
				},
				Output: ArrowTypes.DoubleType,
			},
		}
	default:
		return nil
	}
}

func NewFunctionWithArgs(name string, args ...Expr) (Function, error) {
	canonicalName := getCanonicalFunctionName(name)
	impls := getFunctionImplementations(canonicalName)

	return &function{
		name:  canonicalName,
		args:  args,
		impls: impls,
	}, nil
}

func NewFunctionWithImplAndArgs(name string, impl FunctionImplementation, args ...Expr) (Function, error) {
	canonicalName := getCanonicalFunctionName(name)

	return &function{
		name:  canonicalName,
		args:  args,
		impls: []FunctionImplementation{impl},
	}, nil
}

func Add(left, right Expr) Function {
	name := "add"
	f, err := NewFunctionWithArgs(name, left, right)
	if err != nil {
		panic(fmt.Sprintf("invalid definition for function %s: %s", name, err))
	}

	return f
}

type ScalarFunction struct {
	Name        string               `json:"name"`
	Description string               `json:"description"`
	Impls       []ScalarFunctionImpl `json:"impls"`
}

type ScalarFunctionImpl struct {
	Args   []ValueArgument `json:"args"`
	Return string          `json:"return"`
}

type ValueArgument struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

var KnownFunctions = []ScalarFunction{
	{
		Name:        "add",
		Description: "Add two values.",
		Impls: []ScalarFunctionImpl{
			{
				Args: []ValueArgument{
					{
						Name:  "x",
						Value: "i8",
					},
					{
						Name:  "y",
						Value: "i8",
					},
				},
			},
		},
	},
}

var FunctionAliases = map[string][]string{
	"add": {"+"},
}

func signatureNameForArgumentType(typ arrow.DataType) (string, error) {
	switch typ.ID() {
	case ArrowTypes.Int8Type.ID():
		return "i8", nil
	case ArrowTypes.Int16Type.ID():
		return "i16", nil
	case ArrowTypes.Int32Type.ID():
		return "i32", nil
	case ArrowTypes.Int64Type.ID():
		return "i64", nil
	default:
		return "", fmt.Errorf("function argument: unrecognized arrow type: %s", typ.Name())
	}
}

var signatureNameToType = map[string]arrow.DataType{
	"i8":  ArrowTypes.Int8Type,
	"i16": ArrowTypes.Int16Type,
	"i32": ArrowTypes.Int32Type,
	"i64": ArrowTypes.Int64Type,
}

func argumentTypeForSignatureName(name string) (arrow.DataType, error) {
	typ, found := signatureNameToType[name]
	if !found {
		return nil, fmt.Errorf("unrecognized type signature name: %s", name)
	}

	return typ, nil
}

func parseFunctionSignature(signature string) (string, []arrow.DataType, error) {
	name, argsPart, found := strings.Cut(signature, ":")
	if !found {
		return "", nil, fmt.Errorf("failed to parse function signature: %s", signature)
	}

	var err error
	args := strings.Split(argsPart, "_")
	types := make([]arrow.DataType, len(args))
	for i, arg := range args {
		types[i], err = argumentTypeForSignatureName(arg)
		if err != nil {
			return "", nil, err
		}
	}

	return name, types, nil
}

var _ Function = (*function)(nil)
