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
