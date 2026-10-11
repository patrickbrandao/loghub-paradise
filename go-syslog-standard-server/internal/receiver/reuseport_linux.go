//go:build linux

package receiver

// SO_REUSEPORT on Linux, identical across every architecture.
const soReusePort = 0xf
