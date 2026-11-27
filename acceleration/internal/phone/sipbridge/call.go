package sipbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrReleased is what Err returns for a call let go without BYE, so another node can continue it.
var ErrReleased = errors.New("sipbridge: call released to another node")

var errCallEnded = errors.New("call ended")

// errNotEstablished refuses a request that arrives while Dial is still setting the call up:
// the other leg may not have a dialog yet to pass it on in.
var errNotEstablished = errors.New("call not set up yet")

// callState is the lifecycle state of a Call, moving strictly forward:
// callSettingUp -> callEstablished -> callEnding -> callEnded
// (or callSettingUp -> callEnding -> callEnded if ended during setup).
type callState int32

const (
	callSettingUp callState = iota
	callEstablished
	callEnding
	callEnded
)

// Call is an established outbound call: two dialogs, and whatever one side sends in its
// dialog is passed on to the other.
type Call struct {
	// ctx is the context the call was dialled or resumed in, without its cancellation: what
	// the other side asks of the call later (a BYE, a re-INVITE, a keepalive) is traced under it.
	ctx  context.Context
	legs [2]leg
	log  *slog.Logger
	// media is where each side sent its audio from at setup, fixed before Dial returns.
	media MediaAddrs
	// onChange is told when a passed-on re-INVITE changed what either leg last sent.
	onChange func()
	// trunkAddr is Config.trunkAddr, where Stream is sent RTP when a customer re-INVITE names no
	// public address.
	trunkAddr netip.Addr

	state atomic.Int32
	done  chan struct{}
	err   error
}

func newCall(ctx context.Context, customer, stream leg, log *slog.Logger) *Call {
	return &Call{ctx: context.WithoutCancel(ctx), legs: [2]leg{customer, stream}, log: log, done: make(chan struct{})}
}

// Done is closed when the call has ended.
func (c *Call) Done() <-chan struct{} { return c.done }

// Media is where each side sent its audio from when the call was set up.
func (c *Call) Media() MediaAddrs { return c.media }

// Err says why the call ended, nil for a normal hangup. Read it after Done is closed.
func (c *Call) Err() error { return c.err }

// Hangup sends BYE on both legs at once, each bounded by its own cleanup timeout, so a
// customer trunk that does not answer cannot hold up the BYE to Stream, and ends the call.
func (c *Call) Hangup(ctx context.Context) error {
	if !c.claim() {
		return nil
	}
	var errs [2]error
	var wg sync.WaitGroup
	for s, l := range c.legs {
		wg.Go(func() {
			byeCtx, cancel := context.WithTimeout(ctx, cleanupTimeout)
			defer cancel()
			errs[s] = l.Bye(byeCtx)
		})
	}
	wg.Wait()
	c.end(nil)
	return errors.Join(errs[:]...)
}

// Release ends the call without a BYE on either leg. The dialogs stay up for the node that
// takes the call over.
func (c *Call) Release() {
	if !c.claim() {
		return
	}
	c.end(ErrReleased)
}

// State is both legs' dialogs and where the audio flows.
func (c *Call) State() CallState {
	return CallState{Customer: c.legs[customerSide].State(), Stream: c.legs[streamSide].State(), Media: c.media}
}

// establish lets requests from one side through to the other. It returns false if the
// call already began ending during setup.
func (c *Call) establish() bool {
	return c.state.CompareAndSwap(int32(callSettingUp), int32(callEstablished))
}

func (c *Call) claim() bool {
	for {
		s := callState(c.state.Load())
		if s != callSettingUp && s != callEstablished {
			return false
		}
		if c.state.CompareAndSwap(int32(s), int32(callEnding)) {
			return true
		}
	}
}

func (c *Call) end(err error) {
	if c.state.CompareAndSwap(int32(callEnding), int32(callEnded)) {
		c.err = err
		close(c.done)
	}
}

func (c *Call) other(s side) leg { return c.legs[1-s] }

func (c *Call) onBye(ctx context.Context, from side) {
	if !c.claim() {
		return
	}
	c.log.Info("BYE received, hanging up the other leg", "from", from)
	if err := c.other(from).Bye(ctx); err != nil {
		c.log.Warn("BYE to the other leg failed", "leg", 1-from, "error", err)
	}
	c.end(nil)
}

func (c *Call) onReinvite(ctx context.Context, from side, offer []byte) ([]byte, error) {
	switch callState(c.state.Load()) {
	case callSettingUp:
		return nil, stack.Wrap(errNotEstablished)
	case callEstablished:
	default:
		return nil, stack.Wrap(errCallEnded)
	}
	c.log.Info("re-INVITE received, passing it on", media(from, offer)...)
	sent, err := c.passOn(from, offer)
	if err != nil {
		return nil, err
	}
	answer, err := c.other(from).Reinvite(ctx, sent)
	if err != nil {
		return nil, err
	}
	c.log.Info("re-INVITE answered", media(1-from, answer)...)
	if answer, err = c.passOn(1-from, answer); err != nil {
		return nil, err
	}
	c.changed()
	return answerFor(offer, answer), nil
}

// passOn is what the other leg gets of an SDP body from side s. Stream gets the customer's body rebuilt
// by toStream, never its bytes; an empty body (a re-INVITE that asks for an offer) carries nothing.
func (c *Call) passOn(s side, body []byte) ([]byte, error) {
	if s == streamSide || len(body) == 0 {
		return body, nil
	}
	return toStream(body, c.trunkAddr)
}

func (c *Call) onInfo(ctx context.Context, from side, contentType string, body []byte) error {
	switch callState(c.state.Load()) {
	case callSettingUp:
		return stack.Wrap(errNotEstablished)
	case callEstablished:
	default:
		return stack.Wrap(errCallEnded)
	}
	c.log.Info("INFO received, passing it on", "from", from, "content_type", contentType)
	return c.other(from).Info(ctx, contentType, body)
}

// lost ends the call when one leg can no longer be reached, so the other is not left open.
func (c *Call) lost(ctx context.Context, s side, err error) {
	if !c.claim() {
		return
	}
	c.log.Warn("leg lost, hanging up the other", "leg", s, "error", err)
	if byeErr := c.other(s).Bye(ctx); byeErr != nil {
		c.log.Warn("BYE to the other leg failed", "leg", 1-s, "error", byeErr)
	}
	c.end(stack.Wrap(fmt.Errorf("%s leg lost: %w", s, err)))
}

func (c *Call) changed() {
	if c.onChange != nil {
		c.onChange()
	}
}
