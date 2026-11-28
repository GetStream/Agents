package appkeys

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Every kind of sealed record shares these two counters, with its kind as the record label. They
// have no app label, because a label per app would add one series per app in every region. The
// warning line of a fallback names the app.
var (
	sealFallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "app_key_seal_fallbacks_total",
		Help:      "Secrets sealed under the router's key because the app's data key was unavailable, by kind of record.",
	}, []string{"record"})
	// kekOpens counts opens of secrets still sealed under the router's key. A record nobody uses
	// is never counted, so a zero rate says only that no use needs that key. The rows with no
	// data key version say which records still do.
	kekOpens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "sealed_kek_opens_total",
		Help:      "Sealed records that opened under the router's key rather than an app's data key, by kind of record.",
	}, []string{"record"})
)

// StartCounting serves both counters for kind at 0 until its first count. A labelled series
// appears only on its first count, so without this a dashboard or an alert on a kind that has
// not fallen back yet would find no series at all. Call it once for each kind of record.
func StartCounting(kind string) {
	sealFallbacks.WithLabelValues(kind)
	kekOpens.WithLabelValues(kind)
}

// ErrNoKeys says a secret is sealed under the app's data key, and the router was built without
// data keys. The setting is missing, not the record's secret.
var ErrNoKeys = errors.New("appkeys: the secret is sealed under the app's data key, " +
	"and auth.app_keys_kms_key is not set")

// Record is one sealed record, as the counters and the log lines name it.
type Record struct {
	// Kind is the record label of the counters, such as "connector_credentials". It names a kind
	// of record and never one row, so each kind adds one series.
	Kind string
	// CustomerID is the app the record belongs to. Its data key seals the record.
	CustomerID string
	// Attrs name the row in log lines, such as its connection id.
	Attrs []slog.Attr
}

// Sealed is a secret as SealRecord sealed it.
type Sealed struct {
	Blob []byte
	// KEKVersion is the version of the router's key that sealed Blob, and 0 when the app's data
	// key sealed it.
	KEKVersion int
	// DataKeyVersion is the version of the app's data key that sealed Blob, and nil when the
	// router's key sealed it.
	DataKeyVersion *int
}

// SealRecord seals plaintext, a new secret of record. With sealing on, it seals under the app's
// data key with aad. With sealing off, or a nil k, routerKey seals it. When KMS is unavailable,
// routerKey also seals it, so a write never fails because of KMS, and
// router_app_key_seal_fallbacks_total counts that. Any other failure of the data key is
// returned. A secret that is already sealed moves to the data key with ResealRecord instead.
//
// The caller writes the data key version in the same statement as the blob, so a row never names
// a key other than the one that sealed it.
func (k *Keys) SealRecord(ctx context.Context, record Record, plaintext, aad []byte,
	routerKey func() (blob []byte, version int, err error)) (Sealed, error) {
	bound, err := record.aad(aad)
	if err != nil {
		return Sealed{}, err
	}
	if k == nil || !k.options.Seal {
		return sealUnderRouterKey(routerKey)
	}
	blob, version, err := k.Seal(ctx, record.CustomerID, plaintext, bound)
	if err == nil {
		return Sealed{Blob: blob, DataKeyVersion: &version}, nil
	}
	if !errors.Is(err, ErrUnavailable) {
		return Sealed{}, err
	}
	sealFallbacks.WithLabelValues(record.Kind).Inc()
	// Seal already logged the KMS error with its cause, so this line only says what was done
	// about it.
	args := []any{"record", record.Kind, "app_id", record.CustomerID}
	for _, attr := range record.Attrs {
		args = append(args, attr)
	}
	k.options.Logger.WarnContext(ctx, "sealed a secret under the router's key, since the app's data key is unavailable",
		append(args, "reason", fallbackReason(err))...)
	return sealUnderRouterKey(routerKey)
}

// ResealRecord seals plaintext again under the app's data key, for a record that opened under
// the router's key. It has no fallback: when KMS is unavailable it returns the error, and the
// caller leaves the row as it is, since the secret is already sealed and a rewrite under the
// router's key would change nothing. With sealing off or a nil k it seals nothing and returns no
// data key version. Use SealRecord for a new secret.
func (k *Keys) ResealRecord(ctx context.Context, record Record, plaintext, aad []byte) (Sealed, error) {
	bound, err := record.aad(aad)
	if err != nil {
		return Sealed{}, err
	}
	if k == nil || !k.options.Seal {
		return Sealed{}, nil
	}
	blob, version, err := k.Seal(ctx, record.CustomerID, plaintext, bound)
	if err != nil {
		return Sealed{}, err
	}
	return Sealed{Blob: blob, DataKeyVersion: &version}, nil
}

func sealUnderRouterKey(routerKey func() ([]byte, int, error)) (Sealed, error) {
	blob, version, err := routerKey()
	if err != nil {
		return Sealed{}, err
	}
	return Sealed{Blob: blob, KEKVersion: version}, nil
}

// fallbackReason is the kind of KMS failure that made a seal fall back to the router's key:
// "kms_timeout" when KMS did not answer within auth.app_keys_kms_timeout, else "kms_error".
func fallbackReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "kms_timeout"
	}
	return "kms_error"
}

// OpenRecord opens sealed, the secret of record. A nil dataKeyVersion means the router's key
// sealed it: routerKey opens it, and router_sealed_kek_opens_total counts each open that
// succeeds. Otherwise it opens under that version of the app's data key with aad. A nil k then
// fails with ErrNoKeys.
func (k *Keys) OpenRecord(ctx context.Context, record Record, dataKeyVersion *int, sealed, aad []byte,
	routerKey func() ([]byte, error)) ([]byte, error) {
	bound, err := record.aad(aad)
	if err != nil {
		return nil, err
	}
	if dataKeyVersion == nil {
		plain, err := routerKey()
		if err == nil {
			kekOpens.WithLabelValues(record.Kind).Inc()
		}
		return plain, err
	}
	if k == nil {
		return nil, stack.Wrap(ErrNoKeys)
	}
	return k.Open(ctx, record.CustomerID, *dataKeyVersion, sealed, bound)
}

// aad is what a secret of record is sealed with under the app's data key: the record's kind and
// app, then rowAAD. One data key seals every kind of record of an app, so without the kind a
// blob of one kind would open as another kind of the same app. Each string is length-prefixed,
// so no kind and app split the same bytes two ways. The router's key keeps rowAAD alone, since
// the secrets already sealed under it must still open. v1 changes with the layout.
func (record Record) aad(rowAAD []byte) ([]byte, error) {
	if record.Kind == "" || record.CustomerID == "" || len(rowAAD) == 0 {
		return nil, stack.Wrap(errIncompleteRecord)
	}
	bound := fmt.Appendf(nil, "accelerate:record:v1:%d:%s:%d:%s:", len(record.Kind), record.Kind,
		len(record.CustomerID), record.CustomerID)
	return append(bound, rowAAD...), nil
}

// errIncompleteRecord is a caller's bug: a secret sealed without its kind, app or row could be
// moved to another row and still open.
var errIncompleteRecord = errors.New("appkeys: a sealed record needs a kind, an app and an AAD that names its row")
