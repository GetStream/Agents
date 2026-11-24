package fakeprovider

import (
	"bytes"
	"cmp"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// PathAPI is the root of the fake's HTTP API, which an http connector's tools call
// (internal/connectors/sources/http): every method, on every path under it. Not /api, which is
// the fake Slack's (PathSlackAPI).
const PathAPI = "/rest"

// APIKeyHeader is the header the API takes APIKey in, as an api_key connection sends it
// (schemes/apikey). X-Api-Key is the scheme contract's own example header.
const APIKeyHeader = "X-Api-Key"

// APIRequest is one request that reached the API, as it arrived.
type APIRequest struct {
	Method string
	// URI is the request line's target: the path as it was sent, escaped, and the query.
	URI    string
	Header http.Header
	Body   string
}

// APIRequests are the requests that reached the API, in order, whatever they carried.
func (s *Server) APIRequests() []APIRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]APIRequest{}, s.apiRequests...)
}

// api records the request, then answers 401 unless it carries APIKey in APIKeyHeader, and
// otherwise by path:
//
//	/rest/bytes/<n>         200 with n bytes of text ("a" each), then the number of bytes the
//	                        query's then names (none when it names none) that are not UTF-8
//	                        (0xff each), so a reader that reads past n reads no text
//	/rest/echo-credential   403 whose body says the APIKeyHeader and Authorization it got, as an
//	                        API that echoes the request's credential in its error does; with
//	                        ?status=200, the same body as a 200, as one that echoes it in an
//	                        answer does
//	anything else           200 "ok"
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.apiRequests = append(s.apiRequests, APIRequest{Method: r.Method, URI: r.RequestURI, Header: r.Header.Clone(), Body: string(body)})
	s.mu.Unlock()
	if r.Header.Get(APIKeyHeader) != s.APIKey {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, PathAPI)
	if size, found := strings.CutPrefix(path, "/bytes/"); found {
		n, err := strconv.Atoi(size)
		then, thenErr := strconv.Atoi(cmp.Or(r.URL.Query().Get("then"), "0"))
		if err != nil || n < 0 || thenErr != nil || then < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, strings.Repeat("a", n))
		_, _ = w.Write(bytes.Repeat([]byte{0xff}, then))
		return
	}
	if path == "/echo-credential" {
		status := http.StatusForbidden
		if r.URL.Query().Get("status") == "200" {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"error":"key `+r.Header.Get(APIKeyHeader)+` and `+r.Header.Get("Authorization")+` may not read this"}`)
		return
	}
	_, _ = io.WriteString(w, "ok")
}
