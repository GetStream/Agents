package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

// FetchTimeoutsSuite covers how long the file client waits on a host, and how long it keeps a connection, on a fake clock.
type FetchTimeoutsSuite struct {
	suite.Suite
	// open is how many connections the client holds.
	open atomic.Int64
}

func TestFetchTimeoutsSuite(t *testing.T) {
	suite.Run(t, new(FetchTimeoutsSuite))
}

// transport is the one the router fetches a file with when no test set how, to dial as a test says.
func (s *FetchTimeoutsSuite) transport() *http.Transport {
	server, err := NewServer(Options{
		Routers:  map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Auth:     auth.Func(func(context.Context, *http.Request) (auth.Principal, error) { return auth.Principal{}, nil }),
		AuthMode: auth.Custom,
	})
	s.Require().NoError(err)
	return server.imageHTTP.Transport.(*http.Transport).Clone()
}

// hosted runs test in a bubble with a client whose every connection reaches a host, which
// answers each request as host does.
func (s *FetchTimeoutsSuite) hosted(host func(conn net.Conn, request *http.Request), test func(client *http.Client)) {
	synctest.Test(s.T(), func(t *testing.T) {
		s.open.Store(0)
		transport := s.transport()
		transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			s.open.Add(1)
			go func() {
				defer s.open.Add(-1)
				defer server.Close()
				reader := bufio.NewReader(server)
				for {
					request, err := http.ReadRequest(reader)
					if err != nil {
						return
					}
					host(server, request)
				}
			}()
			return client, nil
		}
		defer transport.CloseIdleConnections()
		test(&http.Client{Transport: transport})
	})
}

// answerWithHeaders is a host whose answer carries one header of size bytes.
func answerWithHeaders(size int) func(net.Conn, *http.Request) {
	return func(conn net.Conn, _ *http.Request) {
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Filler: %s\r\n\r\nhi", strings.Repeat("a", size))
	}
}

func answerHi(conn net.Conn, _ *http.Request) {
	_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi")
}

func (s *FetchTimeoutsSuite) get(client *http.Client, address string) (string, error) {
	response, err := client.Get(address)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return string(body), err
}

func (s *FetchTimeoutsSuite) TestAHostThatNeverAnswersIsGivenUpOnAfterNinetySeconds() {
	s.hosted(func(net.Conn, *http.Request) {}, func(client *http.Client) {
		started := time.Now()
		client.Timeout = 300 * time.Second

		_, err := s.get(client, "http://host.example/file")

		s.Require().Error(err)
		s.Equal(90*time.Second, time.Since(started))
	})
}

func (s *FetchTimeoutsSuite) TestAnImageIsGivenUpOnAfterItsOwnFifteenSeconds() {
	s.hosted(func(net.Conn, *http.Request) {}, func(client *http.Client) {
		started := time.Now()

		err := fetchImages(context.Background(), client, []llm.ImagePart{{URL: "http://host.example/photo.png"}})

		s.Require().Error(err)
		s.Equal(15*time.Second, time.Since(started))
	})
}

func (s *FetchTimeoutsSuite) TestATLSHandshakeThatNeverEndsIsGivenUpOnAfterTenSeconds() {
	synctest.Test(s.T(), func(t *testing.T) {
		dialled := make(chan net.Conn, 1)
		transport := s.transport()
		transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go func() { _, _ = io.Copy(io.Discard, server) }()
			dialled <- server
			return client, nil
		}
		// Closed whatever the client still waits for, so the bubble can end.
		defer func() { _ = (<-dialled).Close() }()
		started := time.Now()

		_, err := s.get(&http.Client{Transport: transport, Timeout: 80 * time.Second}, "https://host.example/file")

		s.Require().Error(err)
		s.Equal(10*time.Second, time.Since(started))
	})
}

func (s *FetchTimeoutsSuite) TestADocumentWhoseHostTakesEightyFiveSecondsToAnswerIsRead() {
	s.hosted(func(conn net.Conn, request *http.Request) {
		time.Sleep(85 * time.Second)
		answerHi(conn, request)
	}, func(client *http.Client) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		data, _, err := (&Server{imageHTTP: client}).fetchDocument(ctx, FileSource{Url: "http://host.example/file"})

		s.Require().NoError(err)
		s.Equal("hi", string(data))
	})
}

func (s *FetchTimeoutsSuite) TestABodyThatArrivesSlowlyIsReadToItsEnd() {
	s.hosted(func(conn net.Conn, _ *http.Request) {
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
		for range 4 {
			time.Sleep(25 * time.Second)
			_, _ = io.WriteString(conn, "4\r\npart\r\n")
		}
		_, _ = io.WriteString(conn, "0\r\n\r\n")
	}, func(client *http.Client) {
		body, err := s.get(client, "http://host.example/file")
		s.Require().NoError(err)
		s.Equal("partpartpartpart", body)
	})
}

func (s *FetchTimeoutsSuite) TestAnIdleConnectionIsClosedAfterNinetySeconds() {
	s.hosted(answerHi, func(client *http.Client) {
		_, err := s.get(client, "http://host.example/file")
		s.Require().NoError(err)
		time.Sleep(89 * time.Second)
		synctest.Wait()
		s.Equal(int64(1), s.open.Load(), "the connection is kept for the next file of its host")
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		s.Zero(s.open.Load())
	})
}

func (s *FetchTimeoutsSuite) TestAtMostAHundredIdleConnectionsAreKept() {
	s.hosted(answerHi, func(client *http.Client) {
		for i := range 110 {
			_, err := s.get(client, fmt.Sprintf("http://host%d.example/file", i))
			s.Require().NoError(err)
		}
		synctest.Wait()
		s.Equal(int64(100), s.open.Load())
	})
}

func (s *FetchTimeoutsSuite) TestResponseHeadersPastTheCapAreRefusedAndOnesWithinItPass() {
	s.hosted(answerWithHeaders(512<<10), func(client *http.Client) {
		body, err := s.get(client, "http://host.example/file")
		s.Require().NoError(err)
		s.Equal("hi", body)
	})
	s.hosted(answerWithHeaders(2<<20), func(client *http.Client) {
		_, err := s.get(client, "http://host.example/file")
		s.ErrorContains(err, "response headers exceeded")
	})
}
