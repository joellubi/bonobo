package substrait

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/hashicorp/go-getter"
	"github.com/hashicorp/go-getter/helper/url"
	"github.com/joellubi/bonobo"
	substraitgo "github.com/substrait-io/substrait-go"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/types"
)

const (
	defaultExtensionsDir = "https://github.com/substrait-io/substrait/blob/main/extensions/"
)

var ErrNoMatchingImplementation = errors.New("function: no implementation matching the provided arguments")

//go:embed extensions/*
var defaultExtensions embed.FS

type FunctionRepository interface {
	GetImplementation(uri, name string, args ...bonobo.Type) (FunctionImplementation, error)
}

type FunctionImplementation interface {
	Name() string
	Signature() string
	ReturnType(inputs ...bonobo.Type) (typ bonobo.Type, err error)
}

type FunctionDeclaration interface {
	Implementations() ([]FunctionImplementation, error)
}

func NewFunctionRepository() *functionRepository {
	return &functionRepository{definitions: make(map[string]map[string][]FunctionImplementation, 0)}
}

type functionRepository struct {
	definitions map[string]map[string][]FunctionImplementation
	// variants    map[string]map[string][]extensions.FunctionVariant // TODO
}

func (r *functionRepository) FunctionsForURI(uri string) map[string][]FunctionImplementation {
	return r.definitions[uri]
}

func (r *functionRepository) FunctionImplementations(uri, name string) []FunctionImplementation {
	return r.definitions[uri][name]
}

func (r *functionRepository) GetImplementation(uri, name string, args ...bonobo.Type) (FunctionImplementation, error) {
	impls := r.definitions[uri][name]
	for _, impl := range impls {
		_, err := impl.ReturnType(args...)
		if err == nil {
			// Found a valid implementation
			return impl, nil
		}
		// Temp fix
		if strings.Contains(err.Error(), "provided arguments do not match the signature") {
			continue
		}
		if !(errors.Is(err, substraitgo.ErrInvalidExpr) || errors.Is(err, substraitgo.ErrInvalidType)) {
			// Some other error besides a mismatch with the function signature
			return nil, err
		}
		// Did not match the function signature, continue...
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

func RegisterImplementationsFromURI(repo *functionRepository, uri string, getterOpts ...getter.ClientOption) error {
	r, err := getExtensionFile(uri)
	if err != nil {
		return err
	}

	impls, err := ReadScalarFunctionImplementations(r, uri)
	if err != nil {
		return err
	}

	for _, impl := range impls {
		repo.RegisterImplementation(uri, impl.Name(), &variantFunctionImplementation{variant: impl})
	}

	return nil
}

func getExtensionFile(uri string) (io.ReadCloser, error) {
	if path.Ext(uri) != ".yaml" {
		return nil, fmt.Errorf("invalid extension URI, expected YAML file, found: %s", uri)
	}

	dir, base := path.Split(uri)
	if dir == defaultExtensionsDir {
		return defaultExtensions.Open(path.Join("extensions", base))
	}

	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "file":
		u.Scheme = ""
		return os.Open(u.String())
	case "http", "https":
		res, err := http.Get(u.String())
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	default:
		return nil, fmt.Errorf("unrecognized scheme: %s", u.Scheme)
	}
}

type variantFunctionImplementation struct {
	variant extensions.FunctionVariant
}

// Name implements FunctionImplementation.
func (impl *variantFunctionImplementation) Name() string {
	return impl.variant.Name()
}

// ReturnType implements FunctionImplementation.
func (impl *variantFunctionImplementation) ReturnType(inputs ...bonobo.Type) (typ bonobo.Type, err error) {
	types := make([]types.Type, len(inputs))
	for i, arg := range inputs {
		types[i] = arg
	}

	return impl.variant.ResolveType(types)
}

// Signature implements FunctionImplementation.
func (impl *variantFunctionImplementation) Signature() string {
	return impl.variant.CompoundName()
}

func ReadScalarFunctionImplementations(r io.Reader, uri string) ([]*extensions.ScalarFunctionVariant, error) {
	var (
		buf              bytes.Buffer
		simpleExtensions extensions.SimpleExtensionFile
	)
	_, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(buf.Bytes(), &simpleExtensions); err != nil {
		return nil, err
	}

	// implementations := make([]FunctionImplementation, 0)
	variants := make([]*extensions.ScalarFunctionVariant, 0)
	for _, scalarFunc := range simpleExtensions.ScalarFunctions {
		variants = append(variants, scalarFunc.GetVariants(uri)...)
		// impls, err := scalarFunc.Implementations()
		// if err != nil {
		// 	return nil, err
		// }
		// implementations = append(implementations, impls...)
	}

	return variants, nil
}

func NewAnonymousFunctionRepository(signature string, returnType bonobo.Type) *anonymousRepository {
	return &anonymousRepository{
		impl: &anonymousFunctionImplementation{
			signature: signature,
			typ:       returnType,
		},
	}
}

type anonymousRepository struct {
	impl FunctionImplementation
}

func (r *anonymousRepository) GetImplementation(uri string, name string, args ...bonobo.Type) (FunctionImplementation, error) {
	return r.impl, nil
}

type anonymousFunctionImplementation struct {
	name, signature string
	typ             bonobo.Type
}

func (f *anonymousFunctionImplementation) ReturnType(inputs ...bonobo.Type) (bonobo.Type, error) {
	return f.typ, nil
}

func (f *anonymousFunctionImplementation) Name() string {
	return f.name
}

func (f *anonymousFunctionImplementation) Signature() string {
	return f.signature
}

func RawFileFromGithubURL(url string) (string, error) {
	body, ok := strings.CutPrefix(url, "https://github.com/")
	if !ok {
		return "", fmt.Errorf("invalid github uri: %s", url)
	}

	parts := strings.SplitN(body, "/", 5)
	if len(parts) != 5 {
		return "", fmt.Errorf("invalid github uri: %s", url)
	}

	// TODO: Maybe recursively handle "tree"
	if parts[2] != "blob" {
		return "", fmt.Errorf("invalid github uri: %s", url)
	}

	user := parts[0]
	repo := parts[1]
	branch := parts[3]
	filename := parts[4]

	return fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", user, repo, branch, filename), nil
}

var _ FunctionRepository = (*functionRepository)(nil)
var _ FunctionRepository = (*anonymousRepository)(nil)
var _ FunctionImplementation = (*anonymousFunctionImplementation)(nil)
var _ FunctionImplementation = (*variantFunctionImplementation)(nil)
