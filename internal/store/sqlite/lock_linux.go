//go:build linux

package sqlite

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func lockStateDir(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return fmt.Errorf("lock state directory: %w", err)
	}
	return nil
}
