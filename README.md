# http-replay-bench

A load generator for benchmarking distributed storage via http protocol.

It includes both replay and pattern-based load generation. It uses a
multi-thread multi-event loop approach to generate high loads with a minimal CPU
overhead. It is easily able to keep tens of thousands of concurrent HTTP
connections active.

## Build

To build the project, use the following command:

```sh
cargo build --release
```

## Usage

See `http-replay-bench --help` for usage information.

We recommend seting `ulimit -n 65536` to allow a large number of concurrent
connections.  The authentication is handled via WLCG bearer tokens, discovered
according to the standard WLCG token discovery protocol. Non-standard CAs can
be specified via the usual OpenSSL environment variables.