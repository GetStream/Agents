package egress

import (
	"context"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// BrokerName is the code_sandbox.broker setting that picks Broker.
const BrokerName = "proxy"

// caPath is where the sandbox finds the proxy CA's certificate, and bundlePath the sandbox's
// own roots with it: /tmp, since every user code may run as can write there.
const (
	caPath     = "/tmp/va-egress-ca.pem"
	bundlePath = "/tmp/va-egress-ca-bundle.pem"
)

// bundle writes bundlePath from the first roots file the sandbox has, and fails without one: the
// system roots files Go reads on Linux (https://go.dev/src/crypto/x509/root_linux.go). Daytona's
// own image is Debian, whose file is the first (live probe, 2026-10-21: 150 roots there). A host
// of the config's allowed domains, which the proxy tunnels to as it is, is verified with them.
const bundle = `for roots in /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/ca-bundle.pem ` +
	`/etc/pki/tls/cacert.pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/ssl/cert.pem; do ` +
	`if [ -s "$roots" ]; then { cat "$roots"; echo; cat ` + caPath + `; } > ` + bundlePath + ` && exit 0; exit 1; fi; done; ` +
	`echo "no system CA bundle" >&2; exit 1`

// recordTTL is how long a run's record lasts when nothing deletes it: the router stopped during
// the run, or the store refused AfterRun's delete. Each run puts its own, so it need only outlast
// the longest run.
const recordTTL = sandbox.MaxTimeout + time.Minute

// bundleEnv are the env vars that point OpenSSL, Python requests, curl and git at the roots they
// trust instead of the system's, so they name the bundle: the ones Daytona's netleash sets (live
// probe, 2026-10-21). Node's NODE_EXTRA_CA_CERTS adds to its own roots, so it names the CA alone.
var bundleEnv = []string{"SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "GIT_SSL_CAINFO"}

// proxyEnv are the env vars that send curl, Python and git through the proxy, in both cases:
// curl reads http_proxy in lower case only (https://curl.se/docs/manpage.html#ENVIRONMENT). The
// sandbox reaches the proxy alone and has no DNS, so a client that ignores them reaches nothing.
var proxyEnv = []string{"HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"}

// gitEnv has git send the proxy credential on its first CONNECT. By default it sends none,
// waits for the 407 and connects again (http.proxyAuthMethod «anyauth»: «It is assumed that
// the proxy answers an unauthenticated request with a 407»); basic saves that round trip
// (tunnel probe 2, 2026-10-22: one CONNECT without auth before each authenticated one).
// GIT_HTTP_PROXY_AUTHMETHOD overrides http.proxyAuthMethod, and a user's own GIT_CONFIG_* does not
// replace it: git-config(1), http.proxyAuthMethod,
// https://git-scm.com/docs/git-config#Documentation/git-config.txt-httpproxyAuthMethod.
var gitEnv = map[string]string{"GIT_HTTP_PROXY_AUTHMETHOD": "basic"}

// noProxy keeps the sandbox's own loopback off the proxy, as Daytona's netleash does (live probe,
// 2026-10-21).
const noProxy = "localhost,127.0.0.1,::1"

// BrokerOptions are what Broker needs: the proxy and its store, and the session's tokens.
type BrokerOptions struct {
	Store *Store
	// URL is the proxy as a sandbox reaches it, code_sandbox.proxy_public_url: http or https, an
	// IPv4 address and a port. With https the sandbox's clients reach it through its TLS tunnel.
	URL string
	// CACert is the PEM certificate of the CA the proxy signs each host's certificate with,
	// which the sandbox trusts. Never its key.
	CACert []byte
	// Session is the session the sandbox is for, and Allowed its agent config's allowed_domains,
	// which the proxy tunnels to without a token.
	Session string
	Allowed []string
	Tokens  sandbox.Tokens
	// Bind gives a late grant its connection before each run. A late grant needs it.
	Bind   sandbox.Bind
	Logger *slog.Logger
}

// Broker is the sandbox.CredentialBroker over the router's egress proxy: the sandbox holds a
// placeholder per grant, each run of code a proxy credential of its own that ends with it, and
// the proxy asks for the token on each request. Nothing the provider keeps holds a token.
type Broker struct {
	options BrokerOptions
	proxy   *url.URL
	// allowed are options.Allowed as a Record's hosts.
	allowed []string

	mu sync.Mutex
	// held are the grants each run's record holds while recorded: from a Prepare that gave the
	// sandbox the proxy (proxied) until Close. credential is the running code's proxy credential
	// while its record is in the store, or one whose delete the store refused, which the next
	// call tries again.
	held       []held
	recorded   bool
	proxied    bool
	credential string
}

// held is a grant in the record, and the sandbox's grant it comes from.
type held struct {
	grant sandbox.Grant
	entry Grant
}

func NewBroker(o BrokerOptions) (*Broker, error) {
	// The sandbox gets the CA's certificates alone, whatever else the setting holds.
	o.CACert = certificates(o.CACert)
	if o.Store == nil || len(o.CACert) == 0 || o.Session == "" || o.Tokens == nil {
		return nil, stack.Wrap(errors.New("egress: the proxy broker needs redis, the proxy's CA certificate, a session and tokens"))
	}
	proxy, ok := config.ProxyURL(o.URL)
	if !ok {
		return nil, stack.Wrap(errors.New("egress: the proxy's public URL is http or https://<public IPv4 address>:port, with no user, path, query or fragment"))
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	var allowed []string
	for _, domain := range o.Allowed {
		// A domain with no name in it (*, *., ..) takes no host, since *. would take any name
		// that ends in a dot.
		if host := allowedHost(domain); host != "" {
			allowed = append(allowed, host)
		}
	}
	return &Broker{options: o, proxy: proxy, allowed: allowed}, nil
}

// allowedHost is a config's allowed domain as a Record's host: lower case, without a trailing
// dot, and a leading *. or . as *., which takes the domain and every name under it. A domain
// with no name left is "".
func allowedHost(domain string) string {
	domain = strings.TrimSuffix(strings.ToLower(domain), ".")
	name := strings.TrimLeft(domain, "*.")
	if name != domain && name != "" {
		return "*." + name
	}
	return name
}

func (b *Broker) Prepare(ctx context.Context, grants []sandbox.Grant) (sandbox.Mount, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.release(ctx); err != nil {
		return sandbox.Mount{}, err
	}
	b.held, b.recorded, b.proxied = nil, false, false
	env := map[string]string{}
	var prepared []held
	for _, grant := range grants {
		if grant.Late && b.options.Bind == nil {
			return sandbox.Mount{}, stack.Wrap(fmt.Errorf("egress: connector %q is bound late, and the broker has no Bind", grant.Binding))
		}
		if grant.Late && grant.Required {
			return sandbox.Mount{}, stack.Wrap(fmt.Errorf("egress: connector %q is bound late, so it cannot be required", grant.Binding))
		}
		// A provider's refresh error may quote its answer, so only its class is logged, and a
		// required grant's error carries none of it. A late grant has no connection to ask yet:
		// the proxy refuses its placeholder until BeforeRun binds it.
		var err error
		if !grant.Late {
			_, err = b.options.Tokens(ctx, grant)
		}
		if errors.Is(err, sandbox.ErrNoCredential) {
			if grant.Required {
				return sandbox.Mount{}, fmt.Errorf("egress: required connector %q gives the sandbox no credential: %w", grant.Binding, sandbox.ErrNoCredential)
			}
			b.options.Logger.Warn("the sandbox is created without a connector's credential", "connector", grant.Binding,
				"connection", grant.Connection.ConnectionID, "class", errorClass(err))
			continue
		}
		if err != nil {
			b.options.Logger.Warn("a connector's credential cannot be had just now; the proxy asks again on each request",
				"connector", grant.Binding, "connection", grant.Connection.ConnectionID, "class", errorClass(err))
		}
		placeholder := "va_ph_" + rand.Text()
		entry := Grant{Connection: grant.Connection, Binding: grant.Binding, Config: grant.Config, Connector: grant.Connector,
			Selection: grant.Selection, Scheme: grant.Scheme, Placeholder: placeholder, Hosts: grant.Hosts, Reads: grant.Reads}
		if grant.Late {
			// Whatever connection the caller gave it: the proxy gives no token until a bind.
			entry.Connection, entry.Scheme = core.ConnectionRef{}, ""
		}
		prepared = append(prepared, held{grant: grant, entry: entry})
		env[grant.Env] = placeholder
	}
	if len(prepared) == 0 {
		return sandbox.Mount{}, nil
	}
	// No credential yet: each run gets its own (BeforeRun), so the sandbox holds none between
	// runs.
	b.held, b.recorded, b.proxied = prepared, true, true
	proxy := *b.proxy
	run := map[string]string{"NODE_EXTRA_CA_CERTS": caPath, "NO_PROXY": noProxy, "no_proxy": noProxy}
	maps.Copy(run, gitEnv)
	for _, name := range bundleEnv {
		run[name] = bundlePath
	}
	mount := sandbox.Mount{Env: env, RunEnv: run, Proxy: &proxy, Files: map[string][]byte{caPath: b.options.CACert}, Setup: []string{bundle}}
	if proxy.Scheme == "https" {
		start := startTunnel(b.proxy)
		mount.Files[tunnelPath] = forwarder
		mount.Setup = append(mount.Setup, start)
		mount.EachRun = []string{start}
		// Node's fetch and https ignore the proxy env vars without it
		// (https://nodejs.org/api/cli.html#node_use_env_proxy1; live probe, 2026-10-22).
		run["NODE_USE_ENV_PROXY"] = "1"
	}
	return mount, nil
}

// BeforeRun binds each late grant Bind has a connection for, withdraws a grant whose connection
// gives sandbox.ErrNoCredential, and stores the record under a new proxy credential, which the
// run's proxy env vars send, so the proxy sends a bound grant's token from the run's first
// request on. A newer revision needs nothing: the proxy asks for the token on every request.
func (b *Broker) BeforeRun(ctx context.Context) (map[string]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// The last credential ends before a new one is made: a run tried once more in a sandbox
	// made again calls BeforeRun twice, and an AfterRun the store refused left its credential.
	if err := b.release(ctx); err != nil {
		return nil, err
	}
	if !b.proxied {
		return nil, nil
	}
	if !b.recorded {
		return b.deadEnv(), nil
	}
	var kept []held
	for _, h := range b.held {
		if h.grant.Late {
			bound, ok := b.options.Bind(ctx, h.grant)
			if !ok || !h.grant.Binds(bound) {
				kept = append(kept, h)
				continue
			}
			h.grant = h.grant.Bound(bound)
			h.entry.Connection, h.entry.Scheme = h.grant.Connection, h.grant.Scheme
		}
		_, err := b.options.Tokens(ctx, h.grant)
		if errors.Is(err, sandbox.ErrNoCredential) {
			b.options.Logger.Warn("a connector's credential is withdrawn from the sandbox", "connector", h.grant.Binding,
				"class", errorClass(err))
			continue
		}
		if err != nil {
			b.options.Logger.Warn("a connector's credential cannot be had just now; the proxy asks again on each request",
				"connector", h.grant.Binding, "class", errorClass(err))
		}
		kept = append(kept, h)
	}
	// The record keeps the allowed domains reachable once no grant is left.
	if len(kept) == 0 && len(b.allowed) == 0 {
		b.held = nil
		return b.deadEnv(), nil
	}
	credential := NewCredential()
	if err := b.options.Store.Put(ctx, credential, b.record(kept), recordTTL); err != nil {
		return nil, err
	}
	b.credential, b.held = credential, kept
	return b.runEnv(credential), nil
}

// AfterRun deletes the run's record, so its credential, wherever the code sent it, reaches
// nothing once the run has ended. A store that refuses leaves the record until recordTTL, and
// the next BeforeRun or Close deletes it.
func (b *Broker) AfterRun(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.release(ctx)
}

func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.held, b.recorded = nil, false
	return b.release(ctx)
}

// release deletes the running code's record, keeping its credential for the next try when the
// store refuses. The caller holds mu.
func (b *Broker) release(ctx context.Context) error {
	if b.credential == "" {
		return nil
	}
	if err := b.options.Store.Delete(ctx, b.credential); err != nil {
		return err
	}
	b.credential = ""
	return nil
}

// runEnv are the proxy env vars of a run whose credential is credential. With an https proxy
// they name the sandbox's TLS tunnel to it.
func (b *Broker) runEnv(credential string) map[string]string {
	clients := *b.proxy
	if clients.Scheme == "https" {
		clients = url.URL{Scheme: "http", Host: "127.0.0.1:" + tunnelPort}
	}
	clients.User = url.UserPassword("sbx", credential)
	env := map[string]string{}
	for _, name := range proxyEnv {
		env[name] = clients.String()
	}
	return env
}

// deadEnv are the proxy env vars of a run with nothing to reach, after a Close or once no grant
// and no allowed domain is left: its credential is one no record holds, so the proxy answers
// each request 407, as it does once a record is deleted.
func (b *Broker) deadEnv() map[string]string {
	return b.runEnv(NewCredential())
}

func (b *Broker) record(grants []held) Record {
	record := Record{Session: b.options.Session, Allowed: b.allowed}
	for _, h := range grants {
		record.Grants = append(record.Grants, h.entry)
	}
	return record
}

// certificates are the CERTIFICATE blocks of raw, in PEM.
func certificates(raw []byte) []byte {
	var certs []byte
	for block, rest := pem.Decode(raw); block != nil; block, rest = pem.Decode(rest) {
		if block.Type == "CERTIFICATE" {
			certs = append(certs, pem.EncodeToMemory(block)...)
		}
	}
	return certs
}
