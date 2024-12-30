package engine

import (
	"fmt"
	"strings"

	"github.com/joellubi/bonobo"
	"github.com/joellubi/bonobo/substrait"

	"github.com/substrait-io/substrait-go/v3/proto"
	"github.com/substrait-io/substrait-go/v3/types"
)

var DefaultFunctionRepository = substrait.NewFunctionRepository()

func init() {
	// TODO: Improve
	DefaultFunctionRepository.RegisterImplementation("https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml", "add", &addI8Impl{})
	DefaultFunctionRepository.RegisterImplementation("https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml", "add", &addI64Impl{})
}

func NewFunctionExpr(uri, name string, args ...Expr) *Function {
	return &Function{
		uri:        uri,
		name:       name,
		args:       args,
		repository: DefaultFunctionRepository,
	}
}

func NewAddFunctionExpr(left, right Expr) *Function {
	return NewFunctionExpr(
		"https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml",
		"add",
		left,
		right,
	)
}

func NewAnonymousFunction(uri, signature string, output bonobo.Type, args ...Expr) (*Function, error) {
	repo := substrait.NewAnonymousFunctionRepository(signature, output)

	// TODO: consolidate with type parsing?
	name, _, found := strings.Cut(signature, ":")
	if !found {
		return nil, fmt.Errorf("invalid function signature: %s", signature)
	}

	return &Function{
		uri:        uri,
		name:       name,
		args:       args,
		repository: repo,
	}, nil
}

// TODO: SetFunctionRepository
type Function struct {
	uri, name  string
	args       []Expr
	repository substrait.FunctionRepository
}

// Field implements Expr.
func (f *Function) Field(input Relation) (bonobo.Field, error) {
	args := make([]bonobo.Type, len(f.args))
	for i, arg := range f.args {
		field, err := arg.Field(input)
		if err != nil {
			return bonobo.Field{}, err
		}
		args[i] = field.Type
	}

	// TODO
	// id := extensions.ID{URI: f.uri, Name: f.name}
	// variant, found := extensions.DefaultCollection.GetScalarFunc(id)
	// if !found {
	// 	return arrow.Field{}, fmt.Errorf("scalar function not known: %s", id)
	// }

	// variant.ResolveType()

	impl, err := f.repository.GetImplementation(f.uri, f.name, args...)
	if err != nil {
		return bonobo.Field{}, err
	}

	returnType, err := impl.ReturnType(args...)
	if err != nil {
		return bonobo.Field{}, err
	}

	return bonobo.Field{Name: f.String(), Type: returnType}, nil
}

// String implements Expr.
func (f *Function) String() string {
	args := make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}

	return fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ", "))
}

// TODO: Consolidate with Field()?
// ToProto implements Expr.
func (f *Function) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
	args := make([]bonobo.Type, len(f.args))
	for i, arg := range f.args {
		field, err := arg.Field(input)
		if err != nil {
			return nil, err
		}
		args[i] = field.Type
	}

	impl, err := f.repository.GetImplementation(f.uri, f.name, args...)
	if err != nil {
		return nil, err
	}

	returnType, err := impl.ReturnType(args...)
	if err != nil {
		return nil, err
	}

	outputType := types.TypeToProto(returnType)

	functionArgs := make([]*proto.FunctionArgument, len(f.args))
	for i, arg := range f.args {
		expr, err := arg.ToProto(input, extensions)
		if err != nil {
			return nil, err
		}

		functionArgs[i] = &proto.FunctionArgument{
			ArgType: &proto.FunctionArgument_Value{
				Value: expr,
			},
		}
	}

	ref := extensions.RegisterFunction(f.uri, impl.Signature())

	return &proto.Expression{
		RexType: &proto.Expression_ScalarFunction_{
			ScalarFunction: &proto.Expression_ScalarFunction{
				FunctionReference: ref,
				Arguments:         functionArgs,
				OutputType:        outputType,
				Options:           []*proto.FunctionOption{}, // TODO
			},
		},
	}, nil
}

type addI8Impl struct{}

func (impl *addI8Impl) Name() string {
	return "add"
}

func (impl *addI8Impl) Signature() string {
	return "add:i8_i8"
}

func (impl *addI8Impl) ReturnType(inputs ...bonobo.Type) (bonobo.Type, error) {
	expectedType := bonobo.Types.Int8Type(false)

	if len(inputs) != 2 {
		return nil, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)

	}

	if !inputs[0].Equals(expectedType) {
		return nil, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	if !inputs[1].Equals(expectedType) {
		return nil, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	return expectedType, nil
}

type addI64Impl struct{}

func (impl *addI64Impl) Name() string {
	return "add"
}

func (impl *addI64Impl) Signature() string {
	return "add:i64_i64"
}

func (impl *addI64Impl) ReturnType(inputs ...bonobo.Type) (bonobo.Type, error) {
	expectedType := bonobo.Types.Int64Type(false)

	if len(inputs) != 2 {
		return nil, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)

	}

	if !inputs[0].Equals(expectedType) {
		return nil, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	if !inputs[1].Equals(expectedType) {
		return nil, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	return expectedType, nil
}

var _ Expr = (*Function)(nil)
