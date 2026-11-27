package video

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
)

// TimeoutsSuite covers how long the clip client keeps a connection, and how many, on a fake clock.
type TimeoutsSuite struct {
	suite.Suite
	// open is how many connections the client holds.
	open atomic.Int64
}

func TestTimeoutsSuite(t *testing.T) {
	suite.Run(t, new(TimeoutsSuite))
}

// transport is publicClient's, to dial as a test says.
func (s *TimeoutsSuite) transport() *http.Transport {
	return publicClient.Transport.(*http.Transport).Clone()
}

// hosted runs test in a bubble with a client whose every connection reaches a host, which
// answers each request as host does.
func (s *TimeoutsSuite) hosted(host func(conn net.Conn, request *http.Request), test func(client *http.Client)) {
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

func (s *TimeoutsSuite) get(client *http.Client, address string) (string, error) {
	response, err := client.Get(address)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return string(body), err
}

func (s *TimeoutsSuite) TestAnIdleConnectionIsClosedAfterNinetySeconds() {
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

func (s *TimeoutsSuite) TestAtMostAHundredIdleConnectionsAreKept() {
	s.hosted(answerHi, func(client *http.Client) {
		for i := range 110 {
			_, err := s.get(client, fmt.Sprintf("http://host%d.example/file", i))
			s.Require().NoError(err)
		}
		synctest.Wait()
		s.Equal(int64(100), s.open.Load())
	})
}

func (s *TimeoutsSuite) TestResponseHeadersPastTheCapAreRefusedAndOnesWithinItPass() {
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
