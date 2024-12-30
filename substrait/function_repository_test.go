package substrait_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/joellubi/bonobo"
	"github.com/joellubi/bonobo/substrait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v3/types"
)

func TestFunctionRepository(t *testing.T) {
	repo := substrait.NewFunctionRepository()

	uri := "https://example.com/functions.yaml"
	repo.RegisterImplementation(uri, "add", &addI64Impl{})

	args := []bonobo.Type{bonobo.Types.Int64Type(false), bonobo.Types.Int64Type(false)}
	impl, err := repo.GetImplementation(uri, "add", args...)
	require.NoError(t, err)

	require.Equal(t, "add:i64_i64", impl.Signature())
	ret, err := impl.ReturnType(args...)
	require.NoError(t, err)
	require.Equal(t, bonobo.Types.Int64Type(false), ret)

	// Function name not known
	_, err = repo.GetImplementation(uri, "sub")
	require.Error(t, err)

	// No valid signature with one argument
	_, err = repo.GetImplementation(uri, "add", bonobo.Types.Int64Type(false))
	require.Error(t, err)

	// No valid signature with arguments i64_i8
	_, err = repo.GetImplementation(uri, "add", bonobo.Types.Int64Type(false), bonobo.Types.Int8Type(false))
	require.Error(t, err)
}

func TestGetLocalFunctionImplementations(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)

	uri := "file://" + path.Join(dir, "testdata/extensions/functions.yaml")
	repo := substrait.NewFunctionRepository()

	require.NoError(t, substrait.RegisterImplementationsFromURI(repo, uri))

	args := []bonobo.Type{bonobo.Types.Int64Type(false), bonobo.Types.Int64Type(false)}
	impl, err := repo.GetImplementation(uri, "add", args...)
	require.NoError(t, err)

	require.Equal(t, "add:i64_i64", impl.Signature())

	ret, err := impl.ReturnType(args...)
	require.NoError(t, err)
	ret.GetNullability()
	require.Equal(t, types.NullabilityRequired, ret.GetNullability())
	require.Equal(t, bonobo.Types.Int64Type(false), ret)
}

func TestGetDefaultFunctionImplementations(t *testing.T) {
	uri := "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
	repo := substrait.NewFunctionRepository()

	require.NoError(t, substrait.RegisterImplementationsFromURI(repo, uri))

	assert.Len(t, repo.FunctionsForURI(uri), 31)

	// TODO: assertions on contents
}

// func TestGetFunctionImplementationsFromGithub(t *testing.T) {
// 	uri := "https://raw.githubusercontent.com/substrait-io/substrait/main/extensions/functions_arithmetic.yaml"
// 	repo := substrait.NewFunctionRepository()
// 	require.NoError(t, substrait.RegisterImplementationsFromURI(repo, uri))

// 	impl, err := repo.GetImplementation(uri, "add", bonobo.ArrowTypes.Int64Type, bonobo.ArrowTypes.Int64Type)
// 	require.NoError(t, err)

// 	require.Equal(t, "add:i64_i64", impl.Signature())

// 	returnType, nullable, err := impl.ReturnType(bonobo.ArrowTypes.Int64Type, bonobo.ArrowTypes.Int64Type)
// 	require.NoError(t, err)
// 	require.False(t, nullable)
// 	require.Equal(t, bonobo.ArrowTypes.Int64Type, returnType)
// }

func TestParseGithubURI(t *testing.T) {
	input := "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"

	raw, err := substrait.RawFileFromGithubURL(input)
	require.NoError(t, err)

	expected := "https://raw.githubusercontent.com/substrait-io/substrait/main/extensions/functions_arithmetic.yaml"
	require.Equal(t, expected, raw)
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

var _ substrait.FunctionImplementation = (*addI64Impl)(nil)
