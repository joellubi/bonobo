package engine

import (
	"github.com/backdeck/backdeck/query/substrait"
	"github.com/substrait-io/substrait-go/proto"
)

func NewPlan(root Relation, rels ...Relation) *Plan {
	return &Plan{root: root, relations: rels}
}

type Plan struct {
	extensions substrait.ExtensionRegistry
	root       Relation
	relations  []Relation
}

func (p *Plan) Relations() []Relation {
	return append([]Relation{p.root}, p.relations...)
}

func (p *Plan) ToProto() (*proto.Plan, error) {
	relations := make([]*proto.PlanRel, len(p.Relations()))
	for i, rel := range p.Relations() {
		schema, err := rel.Schema()
		if err != nil {
			return nil, err
		}

		names := make([]string, schema.NumFields())
		for i, field := range schema.Fields() {
			names[i] = field.Name
		}

		protoRel, err := rel.ToProto(&p.extensions)
		if err != nil {
			return nil, err
		}
		relations[i] = &proto.PlanRel{
			RelType: &proto.PlanRel_Root{
				Root: &proto.RelRoot{
					Input: protoRel,
					Names: names,
				},
			},
		}
	}

	extensionURIs, extensions, err := p.extensions.ToProto()
	if err != nil {
		return nil, err
	}

	return &proto.Plan{
		Version:       &proto.Version{}, // TODO
		ExtensionUris: extensionURIs,
		Extensions:    extensions,
		Relations:     relations,
	}, nil
}
