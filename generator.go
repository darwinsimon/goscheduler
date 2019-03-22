package goscheduler

import (
	"crypto/rand"
	"fmt"
)

// generate id from random hex
func generateID(size int) string {
	if size < 0 {
		return ""
	}

	bytes := make([]byte, size)
	rand.Read(bytes)

	return fmt.Sprintf("%x", bytes)
}
