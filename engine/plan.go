package engine

import (
	"github.com/backdeck/backdeck/query/substrait"
	"github.com/substrait-io/substrait-go/proto"
)

func NewPlan(rels ...Relation) *Plan {
	return &Plan{relations: rels}
}

type Plan struct {
	extensions substrait.ExtensionRegistry
	relations  []Relation
}

func (p *Plan) ToProto() (*proto.Plan, error) {
	relations := make([]*proto.PlanRel, len(p.relations))
	for i, rel := range p.relations {
		protoRel, err := rel.ToProto(&p.extensions)
		if err != nil {
			return nil, err
		}
		relations[i] = &proto.PlanRel{
			RelType: &proto.PlanRel_Rel{
				Rel: protoRel,
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
