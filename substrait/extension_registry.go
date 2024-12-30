package substrait

import (
	"fmt"
	"slices"

	"github.com/substrait-io/substrait-go/v3/proto"
	"github.com/substrait-io/substrait-go/v3/proto/extensions"
)

type ExtensionRegistry struct {
	extensions []Extension
}

func (reg *ExtensionRegistry) RegisterFunction(uri, signature string) uint32 {
	index := slices.IndexFunc(reg.extensions, func(e Extension) bool {
		return functionUniqueIdentifier(e.URI, e.Name) == functionUniqueIdentifier(uri, signature)
	})

	// If not found, add the new extension and set index to its offset
	if index < 0 {
		reg.extensions = append(reg.extensions, Extension{URI: uri, Name: signature, Kind: ExtensionKindFunction})
		index = len(reg.extensions) - 1
	}

	return uint32(index + 1)
}

func (reg *ExtensionRegistry) GetExtensionByReference(ref uint32) (ExtensionDeclaration, string, error) {
	if ref == 0 || ref > uint32(len(reg.extensions)) {
		return ExtensionDeclaration{}, "", fmt.Errorf("invalid extension reference: %d", ref)
	}

	URIs, exts := reg.Extensions()
	// Extensions are kept in order, but are indexed starting from 1
	ext := exts[ref-1]
	uri := URIs[ext.Reference-1]

	return ext, uri.URI, nil
}

func (reg *ExtensionRegistry) Extensions() ([]ExtensionURI, []ExtensionDeclaration) {
	URIs := make([]ExtensionURI, 0)
	extensions := make([]ExtensionDeclaration, len(reg.extensions))

	var anchorURI uint32 = 1
	for i, ext := range reg.extensions {
		var referenceURI uint32
		indexURI := slices.IndexFunc(URIs, func(e ExtensionURI) bool { return e.URI == ext.URI })
		if indexURI < 0 {
			URIs = append(URIs, ExtensionURI{Anchor: anchorURI, URI: ext.URI})
			referenceURI = anchorURI
			anchorURI++
		} else {
			referenceURI = URIs[indexURI].Anchor
		}
		extensions[i] = ExtensionDeclaration{Reference: referenceURI, Anchor: uint32(i + 1), Name: ext.Name, Kind: ext.Kind}
	}

	return URIs, extensions

}

func (reg *ExtensionRegistry) ToProto() ([]*extensions.SimpleExtensionURI, []*extensions.SimpleExtensionDeclaration, error) {
	URIs, extDecls := reg.Extensions()

	protoURIs := make([]*extensions.SimpleExtensionURI, len(URIs))
	for i, uri := range URIs {
		protoURIs[i] = &extensions.SimpleExtensionURI{
			ExtensionUriAnchor: uri.Anchor,
			Uri:                uri.URI,
		}
	}

	protoExt := make([]*extensions.SimpleExtensionDeclaration, len(extDecls))
	for i, ext := range extDecls {
		if ext.Kind != ExtensionKindFunction {
			return nil, nil, fmt.Errorf("serialization unimplemented: ExtensionKind %d", ext.Kind)
		}
		protoExt[i] = &extensions.SimpleExtensionDeclaration{
			MappingType: &extensions.SimpleExtensionDeclaration_ExtensionFunction_{
				ExtensionFunction: &extensions.SimpleExtensionDeclaration_ExtensionFunction{
					ExtensionUriReference: ext.Reference,
					FunctionAnchor:        ext.Anchor,
					Name:                  ext.Name,
				},
			},
		}
	}

	return protoURIs, protoExt, nil
}

func NewExtensionRegistryFromProto(plan *proto.Plan) (ExtensionRegistry, error) {
	var reg ExtensionRegistry

	URIs := plan.GetExtensionUris()
	uriByRef := make(map[uint32]string, len(URIs))
	for _, uri := range URIs {
		uriByRef[uri.GetExtensionUriAnchor()] = uri.GetUri()
	}

	for _, ext := range plan.GetExtensions() {
		switch t := ext.GetMappingType().(type) {
		case *extensions.SimpleExtensionDeclaration_ExtensionFunction_:
			name := t.ExtensionFunction.Name
			URI, found := uriByRef[t.ExtensionFunction.ExtensionUriReference]
			if !found {
				return reg, fmt.Errorf("unable to resolve extension URI reference for %s", t)
			}
			reg.RegisterFunction(URI, name)
		case *extensions.SimpleExtensionDeclaration_ExtensionType_:
			name := t.ExtensionType.Name
			URI, found := uriByRef[t.ExtensionType.ExtensionUriReference]
			if !found {
				return reg, fmt.Errorf("unable to resolve extension URI reference for %s", t)
			}
			return reg, fmt.Errorf("cannot register ExtensionType %s/%s: unimplemented", URI, name)
		case *extensions.SimpleExtensionDeclaration_ExtensionTypeVariation_:
			name := t.ExtensionTypeVariation.Name
			URI, found := uriByRef[t.ExtensionTypeVariation.ExtensionUriReference]
			if !found {
				return reg, fmt.Errorf("unable to resolve extension URI reference for %s", t)
			}
			return reg, fmt.Errorf("cannot register ExtensionTypeVariation %s/%s: unimplemented", URI, name)
		default:
			return reg, fmt.Errorf("invalid SimpleExtensionDeclaration type: %T", t)
		}
	}

	return reg, nil
}

type ExtensionKind int

const (
	ExtensionKindType ExtensionKind = iota
	ExtensionKindTypeVariation
	ExtensionKindFunction
)

type ExtensionDeclaration struct {
	Reference uint32
	Anchor    uint32
	Name      string
	Kind      ExtensionKind
}

type ExtensionURI struct {
	URI    string
	Anchor uint32
}

type Extension struct {
	Name, URI string
	Kind      ExtensionKind
}

func functionUniqueIdentifier(uri, signature string) string {
	return fmt.Sprintf("%s/%s", uri, signature)
}
