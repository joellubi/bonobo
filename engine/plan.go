package engine

import (
	"github.com/joellubi/bonobo/substrait"

	"github.com/substrait-io/substrait-go/proto"
)

func NewPlan(root Relation, rels ...Relation) *Plan {
	return &Plan{root: root, relations: rels}
}

type Plan struct {
	root      Relation
	relations []Relation

	extensions substrait.ExtensionRegistry
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

		protoRel, err := rel.ToProto(&p.extensions)
		if err != nil {
			return nil, err
		}
		relations[i] = &proto.PlanRel{
			RelType: &proto.PlanRel_Root{
				Root: &proto.RelRoot{
					Input: protoRel,
					Names: schema.Names,
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
