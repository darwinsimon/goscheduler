package goscheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateID(t *testing.T) {
	assert.Len(t, generateID(-1), 0)
	assert.Len(t, generateID(0), 0)
	assert.Len(t, generateID(1), 2)
	assert.Len(t, generateID(2), 4)
	assert.Len(t, generateID(3), 6)
	assert.Len(t, generateID(1000), 2000)
}
