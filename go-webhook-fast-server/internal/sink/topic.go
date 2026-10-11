package sink

import (
	"strings"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// topic is a Redis stream name or a NATS subject with the {space} and {type}
// placeholders of SPEC §3.3 and §3.8, expanded per envelope with the
// envelope's own values: the subject convention of the JLH, loghub.{space}.
// {type}, so a consumer of either bus finds the envelope where the LogHub
// convention says it is. A template without placeholders is used as is.
type topic struct {
	template string
	// own is the template expanded for this instance's Workspace and type:
	// every envelope this server writes, except one reingested from a dump a
	// life with another Workspace left behind. Precomputing it keeps the hot
	// path free of allocation.
	space, own string
}

func newTopic(template, workspace string) topic {
	return topic{template: template, space: workspace,
		own: expandTopic(template, workspace, envelope.Type)}
}

// For returns the topic of one envelope.
func (t topic) For(space, typ string) string {
	if space == t.space && typ == envelope.Type {
		return t.own
	}
	return expandTopic(t.template, space, typ)
}

func expandTopic(template, space, typ string) string {
	if strings.IndexByte(template, '{') < 0 {
		return template
	}
	return strings.NewReplacer("{space}", space, "{type}", typ).Replace(template)
}
