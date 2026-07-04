/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import "go.arpabet.com/value"

// DebugPayloadInErrors controls whether the offending request/argument payload is
// embedded in error messages and protocol-error responses. It defaults to false
// so potentially-sensitive payload data never leaks into errors or logs in
// production; enable it only for local debugging. The function name and message
// type are always included (they identify the call, not its data).
var DebugPayloadInErrors = false

// reqDetail returns the request rendered for an error message, or "" unless
// DebugPayloadInErrors is set.
func reqDetail(req value.Map) string {
	if DebugPayloadInErrors {
		return ": " + req.String()
	}
	return ""
}

// valDetail returns a value rendered for an error message, or "" unless
// DebugPayloadInErrors is set.
func valDetail(v value.Value) string {
	if DebugPayloadInErrors {
		return ": " + value.Jsonify(v)
	}
	return ""
}

// SessionCount reports how many client sessions the server currently retains
// (connected plus disconnected-but-within-retention). It is a diagnostics hook:
// use it to observe session accumulation/reaping and to alert on unexpected
// growth. Returns 0 for a server not created by this package.
func SessionCount(s Server) int {
	t, ok := s.(*rpcServer)
	if !ok {
		return 0
	}
	n := 0
	t.clientMap.Range(func(_, _ interface{}) bool {
		n++
		return true
	})
	return n
}
