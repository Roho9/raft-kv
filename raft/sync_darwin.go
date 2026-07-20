//go:build darwin

package raft

import (
	"os"
	"syscall"
)

// osSync issues a plain fsync. On darwin, os.File.Sync uses F_FULLFSYNC,
// which forces a full disk cache flush and costs 10ms or more per call;
// plain fsync pushes data to the drive and is what most databases default
// to here. Process crashes lose nothing either way; replication covers
// whole-machine loss.
func osSync(f *os.File) error {
	return syscall.Fsync(int(f.Fd()))
}
