package engine

import (
	"errors"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/substrait-io/substrait-go/proto"
)

var (
	ErrUnboundTable = errors.New("engine: attempted to determine schema of table that has not been bound to a catalog yet")
)

type Table interface {
	Schema() (*arrow.Schema, error)
	ToProto() (*proto.Rel, error)
}

type Catalog interface {
	Schema(identifier Identifier) (*arrow.Schema, error)
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

func (t *namedTable) Schema() (*arrow.Schema, error) {
	if t.catalog == nil {
		return nil, ErrUnboundTable
	}

	return t.catalog.Schema(t.identifier)
}

func (t *namedTable) ToProto() (*proto.Rel, error) {
	var baseSchema *proto.NamedStruct

	// If the schema cannot be determined, we can still
	// serialize the plan and just omit the schema
	schema, err := t.Schema()
	if err == nil {
		if baseSchema, err = schemaToNamedStruct(schema); err != nil {
			return nil, err
		}
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

func NewAnonymousCatalog(schema *arrow.Schema) *anonymousCatalog {
	return &anonymousCatalog{schema: schema}
}

type anonymousCatalog struct {
	schema *arrow.Schema
}

func (c *anonymousCatalog) Schema(identifier []string) (*arrow.Schema, error) {
	return c.schema, nil
}

var _ NamedTable = (*namedTable)(nil)
var _ Catalog = (*anonymousCatalog)(nil)
