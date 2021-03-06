package goscheduler

import (
	"crypto/rand"
	"fmt"
	"regexp"
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

var isAlphaNumeric = regexp.MustCompile(`^\w+$`).MatchString

func isValidChannelName(channel string) bool {

	if !isAlphaNumeric(channel) || len(channel) == 0 || len(channel) > 100 {
		return false
	}
	return true
}
