package embeddb

import (
	"fmt"
	"io"
	"os"
)

func (db *DB) Backup(destPath string) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return fmt.Errorf("database is closed")
	}

	if db.database == nil {
		return fmt.Errorf("database not initialized")
	}

	srcFile := db.database.file
	srcPath := db.filename

	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer func() {
		if destFile != nil {
			destFile.Close()
			os.Remove(destPath)
		}
	}()

	if _, err := io.Copy(destFile, srcFile); err != nil {
		return fmt.Errorf("failed to copy database file: %w", err)
	}

	if err := destFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync backup file: %w", err)
	}

	if db.database.wal != nil {
		walPath := srcPath + ".wal"
		if _, err := os.Stat(walPath); err == nil {
			walDest := destPath + ".wal"
			walSrc, err := os.Open(walPath)
			if err != nil {
				return fmt.Errorf("failed to open WAL file: %w", err)
			}
			defer walSrc.Close()

			walDestFile, err := os.OpenFile(walDest, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
			if err != nil {
				return fmt.Errorf("failed to create backup WAL file: %w", err)
			}
			defer func() {
				if walDestFile != nil {
					walDestFile.Close()
					os.Remove(walDest)
				}
			}()

			if _, err := io.Copy(walDestFile, walSrc); err != nil {
				return fmt.Errorf("failed to copy WAL file: %w", err)
			}

			if err := walDestFile.Sync(); err != nil {
				return fmt.Errorf("failed to sync backup WAL file: %w", err)
			}

			walDestFile.Close()
			walDestFile = nil
		}
	}

	destFile.Close()
	destFile = nil

	return nil
}
