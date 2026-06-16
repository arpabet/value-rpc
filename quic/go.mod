module go.arpabet.com/value-rpc/quic

go 1.25.0

require (
	github.com/quic-go/quic-go v0.60.0
	go.arpabet.com/value v1.2.0
	go.arpabet.com/value-rpc v1.3.0
	go.uber.org/zap v1.28.0
)

// Build/test this submodule against the local value-rpc working tree rather than
// the last published release, so cross-module changes are validated before a
// tag. Replace directives in a dependency are ignored by downstream consumers,
// so this only affects building the quic module itself.
replace go.arpabet.com/value-rpc => ../

require (
	github.com/coder/websocket v1.8.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)
