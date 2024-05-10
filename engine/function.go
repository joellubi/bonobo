package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/backdeck/backdeck/query/substrait"
	"github.com/substrait-io/substrait-go/proto"
)

var DefaultFunctionRepository = NewFunctionRepository()

var ErrNoMatchingImplementation = errors.New("function: no implementation matching the provided arguments")

func init() {
	// TODO: Improve
	DefaultFunctionRepository.RegisterImplementation("https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml", "add", &addI8Impl{})
	DefaultFunctionRepository.RegisterImplementation("https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml", "add", &addI64Impl{})
}

func NewFunctionRepository() *functionRepository {
	return &functionRepository{definitions: make(map[string]map[string][]FunctionImplementation, 0)}
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

func NewAnonymousFunction(uri, signature string, outputType arrow.DataType, args ...Expr) (*Function, error) {
	repo := anonymousRepository{
		impl: &anonymousFunctionImplementation{
			signature:  signature,
			outputType: outputType,
		},
	}

	name, _, found := strings.Cut(signature, ":")
	if !found {
		return nil, fmt.Errorf("invalid function signature: %s", signature)
	}

	return &Function{
		uri:        uri,
		name:       name,
		args:       args,
		repository: &repo,
	}, nil
}

// TODO: SetFunctionRepository
type Function struct {
	uri, name  string
	args       []Expr
	repository FunctionRepository
}

// Field implements Expr.
func (f *Function) Field(input Relation) (arrow.Field, error) {
	args := make([]arrow.DataType, len(f.args))
	for i, arg := range f.args {
		field, err := arg.Field(input)
		if err != nil {
			return arrow.Field{}, err
		}
		args[i] = field.Type
	}

	impl, err := f.repository.GetImplementation(f.uri, f.name, args...)
	if err != nil {
		return arrow.Field{}, err
	}

	typ, nullable, err := impl.ReturnType(args...)
	if err != nil {
		return arrow.Field{}, err
	}

	return arrow.Field{Name: f.String(), Type: typ, Nullable: nullable}, nil
}

// String implements Expr.
func (f *Function) String() string {
	args := make([]string, len(f.args))
	for i, arg := range f.args {
		args[i] = arg.String()
	}

	return fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ", "))
}

// ToProto implements Expr.
func (f *Function) ToProto(input Relation, extensions *substrait.ExtensionRegistry) (*proto.Expression, error) {
	args := make([]arrow.DataType, len(f.args))
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

	typ, nullable, err := impl.ReturnType(args...)
	if err != nil {
		return nil, err
	}

	outputType, err := ProtoTypeForArrowType(typ, nullable)
	if err != nil {
		return nil, err
	}

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

type FunctionImplementation interface {
	Signature() string
	ReturnType(inputs ...arrow.DataType) (arrow.DataType, bool, error)
}

type addI8Impl struct{}

func (impl *addI8Impl) Signature() string {
	return "add:i8_i8"
}

func (impl *addI8Impl) ReturnType(inputs ...arrow.DataType) (arrow.DataType, bool, error) {
	if len(inputs) != 2 {
		return nil, false, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)

	}

	if inputs[0].ID() != arrow.INT8 {
		return nil, false, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	if inputs[1].ID() != arrow.INT8 {
		return nil, false, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	return ArrowTypes.Int8Type, false, nil
}

type addI64Impl struct{}

func (impl *addI64Impl) Signature() string {
	return "add:i64_i64"
}

func (impl *addI64Impl) ReturnType(inputs ...arrow.DataType) (arrow.DataType, bool, error) {
	if len(inputs) != 2 {
		return nil, false, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)

	}

	if inputs[0].ID() != arrow.INT64 {
		return nil, false, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	if inputs[1].ID() != arrow.INT64 {
		return nil, false, fmt.Errorf("provided arguments do not match the signature %s: %s", impl.Signature(), inputs)
	}

	return ArrowTypes.Int64Type, false, nil
}

type functionRepository struct {
	definitions map[string]map[string][]FunctionImplementation
}

func (r *functionRepository) FunctionsForURI(uri string) map[string][]FunctionImplementation {
	return r.definitions[uri]
}

func (r *functionRepository) FunctionImplementations(uri, name string) []FunctionImplementation {
	return r.definitions[uri][name]
}

func (r *functionRepository) GetImplementation(uri, name string, args ...arrow.DataType) (FunctionImplementation, error) {
	impls := r.definitions[uri][name]
	for _, impl := range impls {
		_, _, err := impl.ReturnType(args...)
		if err == nil {
			return impl, nil
		}
	}

	return nil, ErrNoMatchingImplementation
}

func (r *functionRepository) RegisterImplementation(uri, name string, impl FunctionImplementation) {
	_, ok := r.definitions[uri]
	if !ok {
		r.definitions[uri] = make(map[string][]FunctionImplementation, 0)
	}

	_, ok = r.definitions[uri][name]
	if !ok {
		r.definitions[uri][name] = make([]FunctionImplementation, 0)
	}

	r.definitions[uri][name] = append(r.definitions[uri][name], impl)
}

type FunctionRepository interface {
	GetImplementation(uri, name string, args ...arrow.DataType) (FunctionImplementation, error)
}

type anonymousRepository struct {
	impl FunctionImplementation
}

// GetImplementation implements FunctionRepository.
func (r *anonymousRepository) GetImplementation(uri string, name string, args ...arrow.DataType) (FunctionImplementation, error) {
	return r.impl, nil
}

var _ FunctionRepository = (*functionRepository)(nil)
var _ FunctionRepository = (*anonymousRepository)(nil)

type anonymousFunctionImplementation struct {
	signature  string
	outputType arrow.DataType
}

// ReturnType implements FunctionImplementation.
func (f *anonymousFunctionImplementation) ReturnType(inputs ...arrow.DataType) (arrow.DataType, bool, error) {
	return f.outputType, false, nil
}

// Signature implements FunctionImplementation.
func (f *anonymousFunctionImplementation) Signature() string {
	return f.signature
}

var _ Expr = (*Function)(nil)
var _ FunctionImplementation = (*anonymousFunctionImplementation)(nil)
var _ FunctionImplementation = (*addI8Impl)(nil)
var _ FunctionImplementation = (*addI64Impl)(nil)
