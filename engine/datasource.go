package engine

import (
	"errors"
	"fmt"

	"github.com/joellubi/bonobo"
	"github.com/joellubi/bonobo/substrait"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/substrait-io/substrait-go/proto"
	"github.com/substrait-io/substrait-go/types"
)

var (
	ErrUnboundTable = errors.New("engine: attempted to determine schema of table that has not been bound to a catalog yet")
)

type Table interface {
	Schema() (*bonobo.Schema, error)
	ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error)
}

type Catalog interface {
	Schema(identifier Identifier) (*bonobo.Schema, error)
}

type Identifier = []string

type NamedTable interface {
	Table

	Identifier() Identifier
	SetCatalog(catalog Catalog)
}

func NewNamedTable(identifier Identifier, catalog Catalog) *namedTable {
	return &namedTable{identifier: identifier, catalog: catalog}
}

type namedTable struct {
	identifier Identifier

	catalog Catalog
}

func (t *namedTable) Identifier() []string {
	return t.identifier
}

func (t *namedTable) SetCatalog(catalog Catalog) {
	t.catalog = catalog
}

func (t *namedTable) Schema() (*bonobo.Schema, error) {
	if t.catalog == nil {
		return nil, ErrUnboundTable
	}

	return t.catalog.Schema(t.identifier)
}

func (t *namedTable) ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error) {
	var baseSchema *proto.NamedStruct

	// If the schema cannot be determined, we can still
	// serialize the plan and just omit the schema
	schema, err := t.Schema()
	if err == nil {
		baseSchema = types.NamedStruct(*schema).ToProto()
	}

	return &proto.Rel{
		RelType: &proto.Rel_Read{
			Read: &proto.ReadRel{
				BaseSchema: baseSchema,
				ReadType: &proto.ReadRel_NamedTable_{
					NamedTable: &proto.ReadRel_NamedTable{
						Names: t.identifier,
					},
				},
			},
		},
	}, nil
}

func NewVirtualTable(rec arrow.Record) *virtualTable {
	return &virtualTable{rec: rec}
}

type virtualTable struct {
	rec arrow.Record
}

func (t *virtualTable) Schema() (*bonobo.Schema, error) {
	if t.rec == nil {
		return bonobo.NewSchema(nil), nil
	}

	return nil, fmt.Errorf("engine: virtual table from arrow record is not yet implemented")
}

func (t *virtualTable) ToProto(extensions *substrait.ExtensionRegistry) (*proto.Rel, error) {
	if t.rec != nil {
		return nil, fmt.Errorf("engine: virtual table from arrow record is not yet implemented")
	}

	return &proto.Rel{
		RelType: &proto.Rel_Read{
			Read: &proto.ReadRel{
				ReadType: &proto.ReadRel_VirtualTable_{
					VirtualTable: &proto.ReadRel_VirtualTable{
						Values: nil,
					},
				},
			},
		},
	}, nil
}

func NewAnonymousCatalog(schema *bonobo.Schema) *anonymousCatalog {
	return &anonymousCatalog{schema: schema}
}

type anonymousCatalog struct {
	schema *bonobo.Schema
}

func (c *anonymousCatalog) Schema(identifier []string) (*bonobo.Schema, error) {
	return c.schema, nil
}

var _ NamedTable = (*namedTable)(nil)
var _ Table = (*virtualTable)(nil)
var _ Catalog = (*anonymousCatalog)(nil)
