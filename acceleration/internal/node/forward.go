package node

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ForwardedHeader marks a request a peer handed over, so the node it arrives at answers
// it rather than looking for somewhere else to send it.
const ForwardedHeader = "X-Acceleration-Forwarded"

// CredentialPurpose is what a deployment's keyring derives the credential its nodes present
// to each other for. The version is there so a change to the credential's form can be told
// from a wrong key.
const CredentialPurpose = "node-forward-v1"

// maxForward is the largest call a node reads from a peer: a request whose body is the most an
// operation reads (maxInlineAudioBody in internal/api, 12 MiB) and whose URL and headers are the
// most net/http does (http.DefaultMaxHeaderBytes, 1 MiB). gRPC's default is 4 MiB.
const maxForward = 14 << 20

// credentialKey is the metadata a peer's credential travels in.
const credentialKey = "x-acceleration-node-credential"

// shardHeader carries the shard a peer handed a request to (ToShard). It is read only on a
// request a peer forwarded, and never passed on.
const shardHeader = "X-Acceleration-Shard"

type forwardedKey struct{}

type toShardKey struct{}

type handedToShardKey struct{}

// ToShard marks the requests a Forwarder hands over on ctx as handed to shard, because their
// work is done there rather than because the peer runs something they are about.
func ToShard(ctx context.Context, shard string) context.Context {
	return context.WithValue(ctx, toShardKey{}, shard)
}

// HandedToShard reports whether a peer handed a request here because its work is done in this
// node's shard (ToShard). The node that handed it over did not answer any of it, so a request
// marked so may still be carried on once, to the node running what it is about or to a worker,
// and never to another shard.
func HandedToShard(ctx context.Context) bool {
	handed, _ := ctx.Value(handedToShardKey{}).(bool)
	return handed
}

// Forwarded reports whether a request was handed over by a peer.
//
// Unlike ForwardedHeader, which anybody can send, this is only ever set by the node
// replaying a peer's request, so it can be trusted to mean the peer already acted on what
// arrived there first, such as recording a hook as delivered.
func Forwarded(ctx context.Context) bool {
	forwarded, _ := ctx.Value(forwardedKey{}).(bool)
	return forwarded
}

// Serve returns a handler that answers this deployment's own requests and the Forward
// calls its peers make, on one port.
//
// One port rather than two because a second listener is a second thing to open between
// nodes, and the only traffic here is between a deployment's own processes. gRPC wants
// HTTP/2, which a peer speaks without TLS by prior knowledge alone, so h2c takes the connection
// preface: an HTTP/1.1 request, sockets included, never reaches it.
//
// Anyone who reaches that port can speak gRPC to it, so every call must carry one of the
// accepted credentials, which only the deployment's own nodes can derive. With none
// accepted, every call is refused.
func Serve(api http.Handler, accepted [][]byte) http.Handler {
	check := peerCredentials{}
	for _, credential := range accepted {
		check.accepted = append(check.accepted, []byte(hex.EncodeToString(credential)))
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxForward))
	RegisterNodeServer(server, &forwarded{api: api})

	served := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			// Here rather than in an interceptor, which runs once the whole call has been read.
			if !check.allows(r.Header.Values(credentialKey)) {
				w.Header().Set("Content-Type", "application/grpc")
				w.Header().Set("Grpc-Status", strconv.Itoa(int(codes.Unauthenticated)))
				w.Header().Set("Grpc-Message", "node: not one of this deployment's nodes")
				return
			}
			server.ServeHTTP(w, r)
			return
		}
		api.ServeHTTP(w, r)
	})
	prior := h2c.NewHandler(served, &http2.Server{})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// What an HTTP/2 connection opens with (RFC 9113 section 3.4).
		if r.Method == "PRI" && len(r.Header) == 0 {
			prior.ServeHTTP(w, r)
			return
		}
		// An offer to upgrade to HTTP/2 is not taken up, so the request is an ordinary one: the
		// h2c handler would read its body whole, with no limit, before the handler runs.
		if strings.EqualFold(r.Header.Get("Upgrade"), "h2c") {
			r.Header.Del("Upgrade")
		}
		served.ServeHTTP(w, r)
	})
}

// Address is where this node's peers reach it, given where it listens.
//
// The port is the one being listened on, and the host is whichever of this machine's
// addresses reaches the outside: in a pod that is the address its peers use, and on a
// laptop it is the one on the network. Nothing is dialled -- a UDP socket is given a
// destination and asked which address it would leave from.
func Address(listen string) (string, error) {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	// Any routable address will do; this one is never sent to.
	outbound, err := net.Dial("udp", "192.0.2.1:9")
	if err != nil {
		return "", err
	}
	defer outbound.Close()

	host, _, err := net.SplitHostPort(outbound.LocalAddr().String())
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}

// Forwarder hands requests to this deployment's other nodes, keeping one connection to
// each for as long as the process runs.
type Forwarder struct {
	logger     *slog.Logger
	credential string

	mu    sync.Mutex
	peers map[string]*grpc.ClientConn
}

// NewForwarder returns a Forwarder that has not connected to anything yet, presenting
// credential to every peer it hands a request to.
func NewForwarder(logger *slog.Logger, credential []byte) *Forwarder {
	if logger == nil {
		logger = slog.Default()
	}
	return &Forwarder{
		logger:     logger,
		credential: hex.EncodeToString(credential),
		peers:      map[string]*grpc.ClientConn{},
	}
}

// Forward hands a request and the body read off it to the node at an address and writes back
// what it answered.
func (f *Forwarder) Forward(ctx context.Context, address string, w http.ResponseWriter, r *http.Request, body []byte) error {
	answer, err := f.Ask(ctx, address, r, body)
	if err != nil {
		return err
	}

	for _, header := range answer.GetHeaders() {
		w.Header()[http.CanonicalHeaderKey(header.GetName())] = header.GetValues()
	}
	// The length is set because the answer is whole and because middleware here would
	// otherwise take the body for one of its own and stamp its timing into it a second
	// time. The node that answered has already said how long it took.
	w.Header().Set("Content-Length", strconv.Itoa(len(answer.GetBody())))
	w.WriteHeader(int(answer.GetStatus()))
	if _, err := w.Write(answer.GetBody()); err != nil {
		// The answer is already going out, so a failure here is the caller having left
		// rather than anything the caller can be told about. Reporting it would have the
		// caller told twice.
		f.logger.Debug("could not write back what another node answered",
			"peer", address, "error", err)
	}

	return nil
}

// Ask hands a request over and returns the answer without writing it, which is what the
// callers that took the body apart before they knew where it belonged need.
func (f *Forwarder) Ask(
	ctx context.Context,
	address string,
	r *http.Request,
	body []byte,
) (*ForwardResponse, error) {
	peer, err := f.peer(address)
	if err != nil {
		return nil, err
	}

	sent := r.Header
	if sent.Get("Content-Length") != "" {
		// What is carried may not be the body as it arrived: inflated, or left out.
		sent = sent.Clone()
		sent.Set("Content-Length", strconv.Itoa(len(body)))
	}
	headers := headersOf(sent)
	headers = slices.DeleteFunc(headers, func(header *Header) bool {
		return http.CanonicalHeaderKey(header.GetName()) == shardHeader
	})
	if shard, ok := ctx.Value(toShardKey{}).(string); ok {
		headers = append(headers, &Header{Name: shardHeader, Values: []string{shard}})
	}
	// Carried as a header because it is one, and because the request is rebuilt on the
	// other side from nothing but these: a handler reading the host it was reached at
	// would otherwise see none.
	if r.Host != "" {
		headers = append(headers, &Header{Name: "Host", Values: []string{r.Host}})
	}

	ctx = metadata.AppendToOutgoingContext(ctx, credentialKey, f.credential)
	return NewNodeClient(peer).Forward(ctx, &ForwardRequest{
		Method:     r.Method,
		Url:        r.URL.RequestURI(),
		Headers:    headers,
		Body:       body,
		RemoteAddr: r.RemoteAddr,
	})
}

// peer returns the connection to a node, opening one the first time it is asked for.
// Nothing is dialled here: a gRPC client connects when it is first used and reconnects
// on its own, so a peer that is down costs the request that needs it rather than this.
//
// A connection whose last dial failed is replaced by a new one. gRPC fails every call on it
// at once, without dialling, until its wait before the next reconnect is over, which grows to
// 2 minutes; a node that came back at that address in the meantime would be refused for as
// long. A new connection dials for the call that needs it, so it reaches a node that is back
// and fails as fast as before for one that is not.
func (f *Forwarder) peer(address string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, ok := f.peers[address]; ok {
		if existing.GetState() != connectivity.TransientFailure {
			return existing, nil
		}
		_ = existing.Close()
		delete(f.peers, address)
	}
	// Insecure because both ends are this deployment's own processes on its own network,
	// which is also what reaching them without TLS on the API port depends on.
	// An answer of any size is taken, as a caller of that node takes it: gRPC's default is 4 MiB.
	peer, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	f.peers[address] = peer

	return peer, nil
}

// peerCredentials checks that a call came from one of this deployment's own nodes.
type peerCredentials struct {
	accepted [][]byte
}

// allows compares what was presented with every accepted credential, in constant time and
// without stopping at a match, so how long it takes says nothing about which came close.
// Nothing presented, or more than one value, is never a match, whatever is accepted.
func (p peerCredentials) allows(presented []string) bool {
	matched := 0
	if len(presented) == 1 && presented[0] != "" {
		for _, credential := range p.accepted {
			matched |= subtle.ConstantTimeCompare([]byte(presented[0]), credential)
		}
	}
	return matched != 0
}

// forwarded answers what a peer hands over by running it through this node's own handler.
type forwarded struct {
	UnimplementedNodeServer
	api http.Handler
}

func (f *forwarded) Forward(ctx context.Context, request *ForwardRequest) (*ForwardResponse, error) {
	ctx = context.WithValue(ctx, forwardedKey{}, true)
	headers := http.Header{}
	for _, header := range request.GetHeaders() {
		headers[http.CanonicalHeaderKey(header.GetName())] = header.GetValues()
	}
	if headers.Get(shardHeader) != "" {
		ctx = context.WithValue(ctx, handedToShardKey{}, true)
		headers.Del(shardHeader)
	}
	replayed, err := http.NewRequestWithContext(ctx, request.GetMethod(), request.GetUrl(), bytes.NewReader(request.GetBody()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	replayed.Header = headers
	if host := replayed.Header.Get("Host"); host != "" {
		replayed.Host = host
		replayed.Header.Del("Host")
	}
	replayed.Header.Set(ForwardedHeader, "1")
	replayed.RemoteAddr = request.GetRemoteAddr()
	replayed.ContentLength = int64(len(request.GetBody()))

	answered := &answer{headers: http.Header{}}
	f.api.ServeHTTP(answered, replayed)

	return &ForwardResponse{
		Status:  int32(answered.status),
		Headers: headersOf(answered.headers),
		Body:    answered.body.Bytes(),
	}, nil
}

// answer collects what a handler answered a forwarded request with. net/http has one of
// these in httptest, which is not for a server to import.
type answer struct {
	headers http.Header
	body    bytes.Buffer
	status  int
}

func (a *answer) Header() http.Header { return a.headers }

func (a *answer) WriteHeader(status int) {
	if a.status == 0 {
		a.status = status
	}
}

func (a *answer) Write(body []byte) (int, error) {
	a.WriteHeader(http.StatusOK)
	return a.body.Write(body)
}

// headersOf renders headers for the wire, which protobuf has no map of lists for.
func headersOf(headers http.Header) []*Header {
	carried := make([]*Header, 0, len(headers))
	for name, values := range headers {
		carried = append(carried, &Header{Name: name, Values: values})
	}
	return carried
}
