/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import "testing"

func TestSplitWSPath(t *testing.T) {
	cases := []struct {
		in, host, path string
	}{
		{":9000/rpc", ":9000", "/rpc"},
		{"host:9000/rpc", "host:9000", "/rpc"},
		{"host:9000", "host:9000", "/"},
		{":9000", ":9000", "/"},
		{"host/a/b", "host", "/a/b"},
		{":9000/", ":9000", "/"},
	}
	for _, c := range cases {
		host, path := splitWSPath(c.in)
		if host != c.host || path != c.path {
			t.Errorf("splitWSPath(%q) = (%q, %q), want (%q, %q)", c.in, host, path, c.host, c.path)
		}
	}
}
