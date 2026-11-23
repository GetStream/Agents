package routing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A socket's session counts its own rows, and the socket still counts every row of each of its
// sessions, the other session's and its own included.
func TestAPartOfASpentAddsToTheWholeAndNotToItsSiblings(t *testing.T) {
	socket := &Spent{}
	first, second := socket.Part(), socket.Part()
	socket.add(5, false)
	first.add(100, false)
	second.add(20, true)

	micros, rows, estimated := first.Total()
	require.Equal(t, []any{int64(100), int64(1), false}, []any{micros, rows, estimated})
	micros, rows, estimated = second.Total()
	require.Equal(t, []any{int64(20), int64(1), true}, []any{micros, rows, estimated})
	micros, rows, estimated = socket.Total()
	require.Equal(t, []any{int64(125), int64(3), true}, []any{micros, rows, estimated})
}

// Work opened with no Spent of its caller's still adds up its own.
func TestAPartOfNoSpentAddsUpItsOwn(t *testing.T) {
	var none *Spent
	part := none.Part()
	part.add(7, false)

	micros, rows, _ := part.Total()
	require.Equal(t, []any{int64(7), int64(1)}, []any{micros, rows})
}
