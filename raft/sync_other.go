//go:build !darwin && !linux

package raft

import "os"

func osSync(f *os.File) error {
	return f.Sync()
}
