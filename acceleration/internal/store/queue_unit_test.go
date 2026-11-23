package store

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// unclaim reads each claimed row's key and attempts off its model: a queue whose model names
// either otherwise would panic as a router stops.
func TestEveryQueuesModelHasItsKeyAndAttempts(t *testing.T) {
	tables := pgdialect.New().Tables()
	for model, q := range map[reflect.Type]queue{
		reflect.TypeFor[ChatInbound]():                 chatInbounds,
		reflect.TypeFor[ChannelInbound]():              channelInbounds,
		reflect.TypeFor[ImageJob]():                    imageJobs,
		reflect.TypeFor[ConnectionEventSubscription](): connectionEventSubscriptions,
	} {
		table := tables.Get(model)
		require.Contains(t, table.FieldMap, q.key, q.name)
		require.Contains(t, table.FieldMap, "attempts", q.name)
	}
}
