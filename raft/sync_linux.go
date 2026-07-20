//go:build linux

package raft

import (
	"os"
	"syscall"
)

// osSync uses fdatasync: like fsync but skips flushing file metadata
// (size changes for an append-only log are still forced when needed).
func osSync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
