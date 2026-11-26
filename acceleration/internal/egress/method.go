package egress

import "net/http"

// MethodLabel is method if it is one of RFC 9110 section 9.1's or PATCH, else "other": a caller
// sends any method it likes, and a metric label must not grow with it. The router's HTTP
// metrics and the sandbox egress proxy's both label a request by it.
func MethodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete,
		http.MethodConnect, http.MethodOptions, http.MethodTrace, http.MethodPatch:
		return method
	}
	return "other"
}
