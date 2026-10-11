//go:build linux

package server

// SO_REUSEPORT on Linux, identical across every architecture.
const soReusePort = 0xf
