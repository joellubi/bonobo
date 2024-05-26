package substrait_test

import (
	"testing"

	"github.com/joellubi/bonobo/substrait"

	"github.com/stretchr/testify/require"
)

func TestExtensionRegistry(t *testing.T) {
	var reg substrait.ExtensionRegistry

	expectedURIs := []substrait.ExtensionURI{}
	expectedExtensions := []substrait.ExtensionDeclaration{}

	URIs, extensions := reg.Extensions()
	require.Equal(t, expectedURIs, URIs)
	require.Equal(t, expectedExtensions, extensions)

	reg.RegisterFunction("example.com/functions.yaml", "add:i64_i64")

	expectedURIs = []substrait.ExtensionURI{{URI: "example.com/functions.yaml", Anchor: uint32(1)}}
	expectedExtensions = []substrait.ExtensionDeclaration{{Reference: uint32(1), Anchor: uint32(1), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction}}

	URIs, extensions = reg.Extensions()
	require.Equal(t, expectedURIs, URIs)
	require.Equal(t, expectedExtensions, extensions)

	reg.RegisterFunction("example.com/functions_decimal.yaml", "add:i64_i64")

	expectedURIs = []substrait.ExtensionURI{
		{URI: "example.com/functions.yaml", Anchor: uint32(1)},
		{URI: "example.com/functions_decimal.yaml", Anchor: uint32(2)},
	}
	expectedExtensions = []substrait.ExtensionDeclaration{
		{Reference: uint32(1), Anchor: uint32(1), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(2), Anchor: uint32(2), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
	}

	URIs, extensions = reg.Extensions()
	require.Equal(t, expectedURIs, URIs)
	require.Equal(t, expectedExtensions, extensions)

	reg.RegisterFunction("example.com/functions_decimal.yaml", "add:i64_i64")

	expectedURIs = []substrait.ExtensionURI{
		{URI: "example.com/functions.yaml", Anchor: uint32(1)},
		{URI: "example.com/functions_decimal.yaml", Anchor: uint32(2)},
	}
	expectedExtensions = []substrait.ExtensionDeclaration{
		{Reference: uint32(1), Anchor: uint32(1), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(2), Anchor: uint32(2), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
	}

	URIs, extensions = reg.Extensions()
	require.Equal(t, expectedURIs, URIs)
	require.Equal(t, expectedExtensions, extensions)

	reg.RegisterFunction("example.com/functions_decimal.yaml", "add:i32_i32")

	expectedURIs = []substrait.ExtensionURI{
		{URI: "example.com/functions.yaml", Anchor: uint32(1)},
		{URI: "example.com/functions_decimal.yaml", Anchor: uint32(2)},
	}
	expectedExtensions = []substrait.ExtensionDeclaration{
		{Reference: uint32(1), Anchor: uint32(1), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(2), Anchor: uint32(2), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(2), Anchor: uint32(3), Name: "add:i32_i32", Kind: substrait.ExtensionKindFunction},
	}

	URIs, extensions = reg.Extensions()
	require.Equal(t, expectedURIs, URIs)
	require.Equal(t, expectedExtensions, extensions)

	reg.RegisterFunction("example.com/functions.yaml", "subtract:i32_i32")

	expectedURIs = []substrait.ExtensionURI{
		{URI: "example.com/functions.yaml", Anchor: uint32(1)},
		{URI: "example.com/functions_decimal.yaml", Anchor: uint32(2)},
	}
	expectedExtensions = []substrait.ExtensionDeclaration{
		{Reference: uint32(1), Anchor: uint32(1), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(2), Anchor: uint32(2), Name: "add:i64_i64", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(2), Anchor: uint32(3), Name: "add:i32_i32", Kind: substrait.ExtensionKindFunction},
		{Reference: uint32(1), Anchor: uint32(4), Name: "subtract:i32_i32", Kind: substrait.ExtensionKindFunction},
	}

	URIs, extensions = reg.Extensions()
	require.Equal(t, expectedURIs, URIs)
	require.Equal(t, expectedExtensions, extensions)
}
