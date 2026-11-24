//go:build integration

package daytona

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/egress"
	_ "github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// ProxySuite is the sandbox Daytona is asked for when the egress proxy is the broker.
type ProxySuite struct {
	suite.Suite
	ctx      context.Context
	platform *platform
	redis    rueidis.Client
	// timeout is the sandbox's Timeout; zero is the default. session is the test's own session,
	// whose records alone it reads, store the store its broker writes, and logs its sandbox's log.
	timeout time.Duration
	session string
	store   *egress.Store
	logs    *lockedLog
}

func TestProxySuite(t *testing.T) {
	suite.Run(t, new(ProxySuite))
}

func (s *ProxySuite) SetupSuite() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR is not set")
	}
	var err error
	s.redis, err = redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(s.redis.Close)
}

func (s *ProxySuite) SetupTest() {
	s.ctx = context.Background()
	s.platform = newPlatform()
	s.T().Cleanup(s.platform.server.Close)
	s.timeout, s.session = 0, "session-"+rand.Text()
	s.store, s.logs = egress.NewStore(s.redis), &lockedLog{}
}

// disconnected is a grant whose connection gives the sandbox no credential.
var disconnected = sandbox.Grant{Connection: core.ConnectionRef{CustomerID: "app", ConnectionID: "connection-2"},
	Binding: "linear", Env: "LINEAR_TOKEN", Hosts: []string{"api.linear.app"}}

// caCert is the proxy CA's certificate as the broker is given it.
const caCert = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n"

// proxied is a sandbox of config whose github grant the egress proxy broker at proxy holds.
func (s *ProxySuite) proxied(config sandbox.Config, proxy string, more ...sandbox.Grant) *Sandbox {
	broker, err := egress.NewBroker(egress.BrokerOptions{Store: s.store, URL: proxy,
		CACert: []byte(caCert), Session: s.session,
		Tokens: func(_ context.Context, grant sandbox.Grant) (sandbox.Token, error) {
			if grant.Binding == disconnected.Binding {
				return sandbox.Token{}, sandbox.ErrNoCredential
			}
			return sandbox.Token{Value: "gho_first", Revision: 1}, nil
		},
		Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	box, err := New(Options{APIKey: "key-1", APIURL: s.platform.server.URL + "/api", Config: config, Grants: append([]sandbox.Grant{github}, more...),
		Broker: broker, Timeout: s.timeout, Logger: slog.New(slog.NewTextHandler(s.logs, nil))})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = box.Close() })
	return box
}

// TestTheSandboxReachesTheProxyAlone: Daytona's network allow list is the proxy's address
// alone, with no domain list and no OutboundProxyUrl, which would bring Daytona's netleash in,
// whatever domains the config allows. The sandbox's own env holds the placeholder alone, every
// command and run gets the CA env, which netleash would replace in the sandbox's, and each run
// of code alone gets the proxy, with a credential of its own that ends with the run. The CA file
// and its bundle come before any code, once per sandbox.
func (s *ProxySuite) TestTheSandboxReachesTheProxyAlone() {
	const proxy = "http://203.0.113.7:8091"
	cases := []sandbox.Config{
		{},
		{NetworkAllowList: []string{"10.0.0.0/8"}},
		{Network: true, AllowedDomains: []string{"pypi.org", "api.github.com", "github.com", "*.pythonhosted.org"}},
	}
	for _, config := range cases {
		box := s.proxied(config, proxy)
		s.Require().NoError(box.Write(s.ctx, "dir/f.txt", []byte("x")))
		_, err := box.Run(s.ctx, "print(1)", nil)
		s.Require().NoError(err)
		_, err = box.Run(s.ctx, "print(2)", nil)
		s.Require().NoError(err)
	}

	for i, created := range s.platform.requests() {
		s.Equal([]string{"GITHUB_TOKEN"}, slices.Collect(maps.Keys(created["env"].(map[string]any))), i)
		s.Regexp(`^va_ph_[A-Z2-7]{26}$`, created["env"].(map[string]any)["GITHUB_TOKEN"])
		s.Equal("203.0.113.7/32", created["networkAllowList"], i)
		s.NotContains(created, "domainAllowList", i)
		s.NotContains(created, "outboundProxyUrl", i)
		s.Equal(false, created["networkBlockAll"])
		s.NotContains(created, "secrets")
	}
	s.Len(s.platform.requests(), len(cases))
	bundle := "/tmp/va-egress-ca-bundle.pem"
	runEnv := map[string]any{"SSL_CERT_FILE": bundle, "REQUESTS_CA_BUNDLE": bundle, "CURL_CA_BUNDLE": bundle, "GIT_SSL_CAINFO": bundle,
		"NODE_EXTRA_CA_CERTS": "/tmp/va-egress-ca.pem", "NO_PROXY": "localhost,127.0.0.1,::1", "no_proxy": "localhost,127.0.0.1,::1",
		"GIT_HTTP_PROXY_AUTHMETHOD": "basic"}
	withProxy := func(env map[string]any) map[string]any {
		proxied := maps.Clone(runEnv)
		for _, name := range []string{"HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"} {
			s.Regexp(`^http://sbx:[A-Z2-7]{26}@203\.0\.113\.7:8091$`, env[name], name)
			proxied[name] = env[name]
		}
		return proxied
	}
	s.Len(s.platform.executed(), 2*len(cases))
	credentials := map[string]bool{}
	for _, ran := range s.platform.executed() {
		envs, _ := ran["envs"].(map[string]any)
		s.Equal(withProxy(envs), envs, "every run")
		credentials[envs["HTTPS_PROXY"].(string)] = true
	}
	s.Len(credentials, 2*len(cases), "a credential of its own")
	s.Empty(s.records(), "each ended with its run")
	s.Len(s.platform.rawCommands, 2*len(cases))
	for _, raw := range s.platform.rawCommands {
		var command struct{ Envs map[string]any }
		s.Require().NoError(json.Unmarshal([]byte(raw), &command))
		s.Equal(runEnv, command.Envs, "every command, with no proxy credential")
	}
	var uploads []string
	for i := range cases {
		uploads = append(uploads, "/tmp/va-egress-ca.pem after "+strconv.Itoa(2*i)+" runs", "dir/f.txt after "+strconv.Itoa(2*i)+" runs")
	}
	s.Equal(uploads, s.platform.uploads, "once per sandbox, before its first file and run")
	s.Equal(caCert, string(s.platform.files["/tmp/va-egress-ca.pem"]))
	for i := range cases {
		s.Equal([]string{"upload", "command", "command", "upload"}, s.platform.steps[4*i:4*i+4],
			"the CA file is written before the setup uses it, and both before the sandbox's first file")
		s.Regexp(`^sh -c '.*> /tmp/va-egress-ca-bundle\.pem.*' after `+strconv.Itoa(2*i)+` runs$`, s.platform.commands[2*i],
			"once per sandbox, after the CA file")
	}
}

// TestAnHTTPSProxyIsReachedThroughATunnelCheckedBeforeEveryRun: the tunnel starts with the
// setup, after its file is written, and is checked before each run but the first after the
// setup, which has just started it, whose clients it serves; a check that fails runs no code.
// The sandbox still reaches the proxy's address alone.
func (s *ProxySuite) TestAnHTTPSProxyIsReachedThroughATunnelCheckedBeforeEveryRun() {
	box := s.proxied(sandbox.Config{}, "https://203.0.113.7:8091")

	_, err := box.Run(s.ctx, "print(1)", nil)
	s.Require().NoError(err)
	_, err = box.Run(s.ctx, "print(2)", nil)
	s.Require().NoError(err)

	s.Equal("203.0.113.7/32", s.platform.requests()[0]["networkAllowList"])
	s.Contains(s.platform.uploads, "/tmp/va-egress-tunnel.py after 0 runs")
	start := "sh -c " + quoted(box.mount.EachRun[0])
	s.Equal([]string{"sh -c " + quoted(box.mount.Setup[0]) + " after 0 runs", start + " after 0 runs", start + " after 1 runs"},
		s.platform.commands)
	for _, ran := range s.platform.executed() {
		s.Regexp(`^http://sbx:[A-Z2-7]{26}@127\.0\.0\.1:3128$`, ran["envs"].(map[string]any)["HTTPS_PROXY"])
	}
	for _, raw := range s.platform.rawCommands {
		s.NotContains(raw, "sbx:", "the setup and the tunnel's check get no proxy credential")
	}
	s.platform.mu.Lock()
	s.platform.commandExit = 1
	s.platform.commandOutput = "the TLS tunnel to the proxy did not start: OSError"
	s.platform.mu.Unlock()
	_, err = box.Run(s.ctx, "print(3)", nil)
	s.ErrorContains(err, "readying the sandbox for a run: exit code 1: the TLS tunnel to the proxy did not start: OSError")
	s.Len(s.platform.executed(), 2)
}

// TestTheRunAfterASetupBeforeItSkipsTheCheck: run_code makes the sandbox ready and writes the
// skills' files before it runs code, so the setup runs there, and the run after it has nothing
// to check. Only that one run skips it.
func (s *ProxySuite) TestTheRunAfterASetupBeforeItSkipsTheCheck() {
	box := s.proxied(sandbox.Config{}, "https://203.0.113.7:8091")
	_, err := box.Ready(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(box.Write(s.ctx, "f.txt", []byte("x")))

	_, err = box.Run(s.ctx, "print(1)", nil)
	s.Require().NoError(err)
	_, err = box.Run(s.ctx, "print(2)", nil)
	s.Require().NoError(err)

	start := "sh -c " + quoted(box.mount.EachRun[0])
	s.Equal([]string{"sh -c " + quoted(box.mount.Setup[0]) + " after 0 runs", start + " after 0 runs", start + " after 1 runs"},
		s.platform.commands)
}

// TestALapsedSandboxChecksTheTunnelOnTheRunAfterItsSetup: a sandbox Daytona may have stopped
// for idling has lost the forwarder its setup started, so the next run checks it.
func (s *ProxySuite) TestALapsedSandboxChecksTheTunnelOnTheRunAfterItsSetup() {
	box := s.proxied(sandbox.Config{}, "https://203.0.113.7:8091")
	meter := &told{}
	box.meter, box.lifecycle.IdleStop = meter, 100*time.Millisecond
	_, err := box.Ready(s.ctx)
	s.Require().NoError(err)
	s.Eventually(func() bool { return slices.Contains(meter.all(), "stopped") }, 5*time.Second, 10*time.Millisecond)

	_, err = box.Run(s.ctx, "print(1)", nil)
	s.Require().NoError(err)

	start := "sh -c " + quoted(box.mount.EachRun[0])
	s.Equal([]string{"sh -c " + quoted(box.mount.Setup[0]) + " after 0 runs", start + " after 0 runs", start + " after 0 runs"},
		s.platform.commands)
	s.Equal([]string{"created sandbox-1 2 vCPU 4 GiB 10 GiB container", "stopped", "started"}, meter.all())
}

// slowed is a sandbox of config behind an HTTPS proxy, that has run once, after which each
// check before a run takes check and each run of code takes code. A run's budget is its
// timeout and 5 s (daytona.go, Run): 5.01 s for code that may run 10 ms.
func (s *ProxySuite) slowed(config sandbox.Config, check, code time.Duration) *Sandbox {
	box := s.proxied(config, "https://203.0.113.7:8091")
	_, err := box.Run(s.ctx, "print(1)", nil)
	s.Require().NoError(err)
	s.platform.mu.Lock()
	s.platform.slowCommand, s.platform.slowRun = check, code
	s.platform.mu.Unlock()
	return box
}

// TestASlowCheckLeavesTheCodeItsWholeBudget: the check before a run is bounded on its own, so
// code that ends within the run's budget runs to its end however long the check took, even
// past the sandbox's timeout.
func (s *ProxySuite) TestASlowCheckLeavesTheCodeItsWholeBudget() {
	s.timeout = 5 * time.Second
	box := s.slowed(sandbox.Config{TimeoutMs: 10}, 3500*time.Millisecond, 2500*time.Millisecond)

	result, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().NoError(err)
	s.Equal("12.63\n", result.Output)
	s.Len(s.platform.executed(), 2)
}

// TestAHungCheckIsCutAtTheSandboxsTimeout: the check is bounded as any other call to Daytona,
// by the sandbox's timeout, and a check cut short runs no code.
func (s *ProxySuite) TestAHungCheckIsCutAtTheSandboxsTimeout() {
	s.timeout = 2 * time.Second
	box := s.slowed(sandbox.Config{TimeoutMs: 10}, 3*time.Second, 0)

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.ErrorContains(err, `/process/execute": context deadline exceeded`)
	s.Len(s.platform.executed(), 1, "no code ran after the check was cut")
}

// TestABuiltSandboxsCheckIsCutAtTheDefaultTimeout: a built sandbox's timeout is the 20 minutes
// a build may take, and a hung check holds the gate, so the check is cut at defaultTimeout.
func (s *ProxySuite) TestABuiltSandboxsCheckIsCutAtTheDefaultTimeout() {
	box := s.slowed(sandbox.Config{TimeoutMs: 10, CPU: 1}, 3*time.Second, 0)
	s.Require().Equal(buildTimeout, box.timeout, "a built sandbox")
	s.Equal(defaultTimeout, box.check)
	box.check = 2 * time.Second // defaultTimeout, shorter in tests

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.ErrorContains(err, `/process/execute": context deadline exceeded`)
	s.Len(s.platform.executed(), 1, "no code ran after the check was cut")
}

// TestABuiltSandboxsWriteIsCutAtTheDefaultTimeout: a write holds the gate as a check does, and a
// built sandbox's timeout is the 20 minutes a build may take, so an upload Daytona never answers
// is cut at defaultTimeout, not held for 20 minutes per skill file.
func (s *ProxySuite) TestABuiltSandboxsWriteIsCutAtTheDefaultTimeout() {
	box := s.slowed(sandbox.Config{TimeoutMs: 10, CPU: 1}, 0, 0)
	s.Require().Equal(buildTimeout, box.timeout, "a built sandbox")
	box.check = 2 * time.Second // defaultTimeout, shorter in tests
	s.platform.mu.Lock()
	s.platform.slowUpload = time.Minute
	s.platform.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	s.T().Cleanup(cancel)
	started := time.Now()

	err := box.Write(ctx, "f.txt", []byte("x"))

	s.ErrorContains(err, `/files/upload-v2?path=f.txt": context deadline exceeded`)
	s.Less(time.Since(started), 20*time.Second, "cut at the check's bound, not the caller's 30 s")
}

// TestABuiltSandboxsSetupIsCutAtTheDefaultTimeout: the setup holds the gate as a check does, and
// a built sandbox's timeout is the 20 minutes a build may take, so a setup Daytona never answers
// is cut at defaultTimeout once the sandbox has started.
func (s *ProxySuite) TestABuiltSandboxsSetupIsCutAtTheDefaultTimeout() {
	box := s.proxied(sandbox.Config{CPU: 1}, "https://203.0.113.7:8091")
	s.Require().Equal(buildTimeout, box.timeout, "a built sandbox")
	box.check = 2 * time.Second // defaultTimeout, shorter in tests
	s.platform.mu.Lock()
	s.platform.slowCommand = time.Minute
	s.platform.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	s.T().Cleanup(cancel)
	started := time.Now()

	_, err := box.Run(ctx, "print(1)", nil)

	s.ErrorContains(err, `/process/execute": context deadline exceeded`)
	s.Less(time.Since(started), 20*time.Second, "cut at the check's bound, not the caller's 30 s")
	s.Empty(s.platform.executed(), "no code ran")
}

// TestABuiltSandboxsFilesAreCutAtTheDefaultTimeout: the same for the files the setup needs, which
// are written before it.
func (s *ProxySuite) TestABuiltSandboxsFilesAreCutAtTheDefaultTimeout() {
	box := s.proxied(sandbox.Config{CPU: 1}, "https://203.0.113.7:8091")
	s.Require().Equal(buildTimeout, box.timeout, "a built sandbox")
	box.check = 2 * time.Second // defaultTimeout, shorter in tests
	s.platform.mu.Lock()
	s.platform.slowUpload = time.Minute
	s.platform.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	s.T().Cleanup(cancel)
	started := time.Now()

	_, err := box.Run(ctx, "print(1)", nil)

	s.ErrorContains(err, `/files/upload-v2`)
	s.ErrorContains(err, "context deadline exceeded")
	s.Less(time.Since(started), 20*time.Second, "cut at the check's bound, not the caller's 30 s")
	s.Empty(s.platform.executed(), "no code ran")
}

// TestACallerGivingUpDuringABuiltSandboxsSetupEndsTheRun: the setup runs on the caller's
// context, so a caller that gives up during a hung setup is answered then, not at the check's
// bound.
func (s *ProxySuite) TestACallerGivingUpDuringABuiltSandboxsSetupEndsTheRun() {
	box := s.proxied(sandbox.Config{CPU: 1}, "https://203.0.113.7:8091")
	box.check = 10 * time.Second // defaultTimeout, longer than the caller waits
	s.platform.mu.Lock()
	s.platform.slowCommand = time.Minute
	s.platform.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	s.T().Cleanup(cancel)
	started := time.Now()

	_, err := box.Run(ctx, "print(1)", nil)

	s.ErrorContains(err, `/process/execute": context deadline exceeded`)
	s.Less(time.Since(started), 6*time.Second, "answered when the caller gave up, long before the check's bound")
	s.Empty(s.platform.executed(), "no code ran")
}

// TestACallerGivingUpDuringTheCheckEndsTheRun: the check runs on the caller's context, so a
// caller that gives up during a hung check is answered then, not at the check's 60 s bound.
func (s *ProxySuite) TestACallerGivingUpDuringTheCheckEndsTheRun() {
	box := s.slowed(sandbox.Config{}, 30*time.Second, 0)
	checking := make(chan struct{}, 1)
	s.platform.mu.Lock()
	s.platform.checking = checking
	s.platform.mu.Unlock()
	ctx, cancel := context.WithCancel(s.ctx)
	s.T().Cleanup(cancel)
	go func() {
		<-checking
		cancel()
	}()
	started := time.Now()

	_, err := box.Run(ctx, "print(2)", nil)

	s.ErrorContains(err, `/process/execute": context canceled`)
	s.Less(time.Since(started), 20*time.Second, "answered long before the check's end")
	s.Len(s.platform.executed(), 1, "no code ran after the caller gave up")
}

// TestACallerGivingUpDuringTheCodeEndsTheRun: the code runs on the caller's context, so a caller
// that gives up while it runs is answered then, not at the run's 35 s budget.
func (s *ProxySuite) TestACallerGivingUpDuringTheCodeEndsTheRun() {
	box := s.slowed(sandbox.Config{}, 0, 0)
	stall, arrived := make(chan struct{}), make(chan struct{}, 1)
	s.T().Cleanup(func() { close(stall) })
	s.platform.mu.Lock()
	s.platform.stall, s.platform.arrived = stall, arrived
	s.platform.mu.Unlock()
	ctx, cancel := context.WithCancel(s.ctx)
	s.T().Cleanup(cancel)
	go func() {
		<-arrived
		cancel()
	}()
	started := time.Now()

	_, err := box.Run(ctx, "print(2)", nil)

	s.ErrorContains(err, `/process/code-run": context canceled`)
	s.Less(time.Since(started), 20*time.Second, "answered long before the run's budget")
}

// TestCodeOverItsBudgetIsCutAsOnBase is the control: with a quick check, code still has the
// run's 5.01 s and no more. Its expected error is the one base (54fb4b288) gives.
func (s *ProxySuite) TestCodeOverItsBudgetIsCutAsOnBase() {
	box := s.slowed(sandbox.Config{TimeoutMs: 10}, 0, 6*time.Second)
	started := time.Now()

	_, err := box.Run(s.ctx, "print(2)", nil)

	s.ErrorContains(err, `/process/code-run": context deadline exceeded`)
	s.Less(time.Since(started), 6*time.Second, "cut at the run's budget, before the code's end")
}

// TestRunCodeTellsTheModelGitTakesThePlaceholder: the proxy takes it as Basic auth's password.
// The grant's env var is not GITHUB_TOKEN, so the sentence names the grant's own, and only a
// grant that reaches github.com gets it.
func (s *ProxySuite) TestRunCodeTellsTheModelGitTakesThePlaceholder() {
	git := sandbox.Grant{Connection: core.ConnectionRef{CustomerID: "app", ConnectionID: "connection-3"},
		Binding: "github-pat", Env: "GH_PAT", Hosts: []string{"api.github.com", "github.com"}}

	description := sandbox.Tool(s.proxied(sandbox.Config{}, "http://203.0.113.7:8091", git)).Description

	s.Contains(description, " git over HTTPS takes it as the password in the remote's URL, such as "+
		"https://x-access-token:$GH_PAT@github.com/<owner>/<repo>.git.")
	s.Equal(1, strings.Count(description, "git over HTTPS"), "the grant with no git host gets none")
	s.Contains(description, "Code may call api.github.com with $GH_PAT: send it over HTTPS as `Authorization: Bearer $GH_PAT`; "+
		"the value is a placeholder that works only for api.github.com, github.com")
	s.NotContains(description, "cannot use")
}

// TestAGrantTheBrokerLeavesOutHasNoEnvVar: the sandbox still reaches the proxy alone.
func (s *ProxySuite) TestAGrantTheBrokerLeavesOutHasNoEnvVar() {
	box := s.proxied(sandbox.Config{}, "http://203.0.113.7:8091", disconnected)

	_, err := box.Run(s.ctx, "print(1)", nil)

	s.Require().NoError(err)
	created := s.platform.requests()[0]
	s.Equal("203.0.113.7/32", created["networkAllowList"])
	s.NotContains(created, "domainAllowList")
	s.Equal([]string{"GITHUB_TOKEN"}, slices.Collect(maps.Keys(created["env"].(map[string]any))))
}

// TestASetupThatFailsRunsNoCode: a sandbox whose image has no roots to bundle the CA with runs
// nothing, and the setup runs again before the next run.
func (s *ProxySuite) TestASetupThatFailsRunsNoCode() {
	box := s.proxied(sandbox.Config{}, "http://203.0.113.7:8091")
	s.platform.mu.Lock()
	s.platform.commandExit = 1
	s.platform.mu.Unlock()

	_, err := box.Run(s.ctx, "print(1)", nil)
	s.Require().ErrorContains(err, "setting the sandbox up")
	s.Empty(s.platform.executed())
	s.platform.mu.Lock()
	s.platform.commandExit = 0
	s.platform.mu.Unlock()
	_, err = box.Run(s.ctx, "print(1)", nil)

	s.Require().NoError(err)
	s.Len(s.platform.commands, 2)
	s.Len(s.platform.executed(), 1)
}

// TestACAFileThatCouldNotBeWrittenIsWrittenBeforeTheNextRun: no code runs without it.
func (s *ProxySuite) TestACAFileThatCouldNotBeWrittenIsWrittenBeforeTheNextRun() {
	box := s.proxied(sandbox.Config{}, "http://203.0.113.7:8091")
	s.platform.mu.Lock()
	s.platform.refuseUpload = true
	s.platform.mu.Unlock()

	_, err := box.Run(s.ctx, "print(1)", nil)
	s.Require().Error(err)
	s.platform.mu.Lock()
	s.platform.refuseUpload = false
	s.platform.mu.Unlock()
	_, err = box.Run(s.ctx, "print(1)", nil)

	s.Require().NoError(err)
	s.Equal([]string{"/tmp/va-egress-ca.pem after 0 runs"}, s.platform.uploads)
	s.Len(s.platform.ran, 1)
}

// TestTheEndOfARunEndsItsProxyCredential: its record is gone once the run has ended, which the
// proxy answers with a 407, whether its code ran or failed.
func (s *ProxySuite) TestTheEndOfARunEndsItsProxyCredential() {
	box := s.proxied(sandbox.Config{}, "http://203.0.113.7:8091")
	held, release := s.platform.outlasting(s.T(), box)
	s.Require().Len(s.records(), 1, "during the run")
	release()
	s.Require().NoError(<-held)
	s.Empty(s.records(), "after it")

	s.platform.mu.Lock()
	s.platform.runStatus, s.platform.runBody = http.StatusInternalServerError, `{"message":"the toolbox failed"}`
	s.platform.mu.Unlock()
	_, err := box.Run(s.ctx, "print(2)", nil)

	s.Require().Error(err)
	s.Len(s.platform.executed(), 2)
	s.Empty(s.records(), "after a run that failed")

	ctx, cancel := context.WithCancel(s.ctx)
	stall, arrived, ran := make(chan struct{}), make(chan struct{}, 1), make(chan error, 1)
	s.platform.mu.Lock()
	s.platform.stall, s.platform.arrived = stall, arrived
	s.platform.mu.Unlock()
	go func() {
		_, err := box.Run(ctx, "print(3)", nil)
		ran <- err
	}()
	<-arrived
	s.Require().Len(s.records(), 1)
	cancel()
	s.ErrorContains(<-ran, "context canceled")
	close(stall)
	s.Empty(s.records(), "after a run its caller gave up on")
}

// TestARunsEndTheStoreRefusedLeavesItsRecordToExpire: the run answers with its code's result,
// and the log says the credential lasts until its record expires.
func (s *ProxySuite) TestARunsEndTheStoreRefusedLeavesItsRecordToExpire() {
	refusing, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{os.Getenv("ROUTER_REDIS_ADDR")}, DisableCache: true})
	s.Require().NoError(err)
	s.store = egress.NewStore(refusing)
	box := s.proxied(sandbox.Config{}, "http://203.0.113.7:8091")
	held, release := s.platform.outlasting(s.T(), box)
	left := s.records()
	s.Require().Len(left, 1)
	s.T().Cleanup(func() { s.NoError(s.redis.Do(context.Background(), s.redis.B().Del().Key(left[0]).Build()).Error()) })

	refusing.Close()
	release()

	s.Require().NoError(<-held, "the code's answer")
	s.Equal(left, s.records(), "until it expires")
	s.Contains(s.logs.String(), `msg="the run's credential could not be ended; it lasts until it expires"`)
}

// TestACloseARunOutlastsStillEndsItsProxyCredential: Close waits for the run until its timeout,
// and the record goes then, not at its expiry, so the proxy refuses the running code.
func (s *ProxySuite) TestACloseARunOutlastsStillEndsItsProxyCredential() {
	box := s.proxied(sandbox.Config{}, "http://203.0.113.7:8091")
	box.timeout = time.Second
	ran, release := s.platform.outlasting(s.T(), box)
	s.Require().Len(s.records(), 1)

	s.ErrorIs(box.Close(), context.DeadlineExceeded)

	s.Empty(s.records())
	release()
	s.NoError(<-ran)
	s.Empty(s.records())
}

// records are the keys of the egress records of the test's session.
func (s *ProxySuite) records() []string {
	keys, err := s.redis.Do(s.ctx, s.redis.B().Keys().Pattern("sandbox:egress:*").Build()).AsStrSlice()
	s.Require().NoError(err)
	var held []string
	for _, key := range keys {
		raw, err := s.redis.Do(s.ctx, s.redis.B().Get().Key(key).Build()).AsBytes()
		var record egress.Record
		if err == nil && json.Unmarshal(raw, &record) == nil && record.Session == s.session {
			held = append(held, key)
		}
	}
	return held
}
