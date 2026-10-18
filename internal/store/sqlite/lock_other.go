//go:build !linux

package sqlite

import (
	"errors"
	"os"
)

func lockStateDir(_ *os.File) error {
	return errors.New("SQLite state directory locking requires Linux")
}
