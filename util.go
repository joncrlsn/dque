package dque

import (
	"os"
)

// dirExists returns true or false
func dirExists(path string) bool {
	fileInfo, err := os.Stat(path)
	if err == nil {
		return fileInfo.IsDir()
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

// fileExists returns true or false
func fileExists(path string) bool {
	fileInfo, err := os.Stat(path)
	if err == nil {
		return !fileInfo.IsDir()
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}
