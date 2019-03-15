package goscheduler

import (
	"crypto/rand"
	"fmt"
)

// generate id from random hex
func generateID(size int) string {
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}

	return fmt.Sprintf("%x", bytes)
}
