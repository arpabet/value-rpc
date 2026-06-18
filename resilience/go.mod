module go.arpabet.com/value-rpc/resilience

go 1.25.0

require (
	go.arpabet.com/value v1.2.0
	go.arpabet.com/value-rpc v1.3.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/coder/websocket v1.8.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)

// Build/test this submodule against the local value-rpc working tree rather than
// the last published release, so cross-module changes (e.g. the client
// interceptor seam) are validated before a tag. Replace directives in a
// dependency are ignored by downstream consumers, so this only affects building
// the resilience module itself. Drop on release (see ../RELEASE.md).
replace go.arpabet.com/value-rpc => ../
