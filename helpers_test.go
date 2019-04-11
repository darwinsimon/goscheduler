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

func TestIsValidChannelName(t *testing.T) {
	assert.False(t, isValidChannelName(""))
	assert.False(t, isValidChannelName(" "))
	assert.False(t, isValidChannelName("~"))
	assert.False(t, isValidChannelName("$"))
	assert.False(t, isValidChannelName("$a"))
	assert.False(t, isValidChannelName("aa123%"))
	assert.False(t, isValidChannelName("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901"))

	assert.True(t, isValidChannelName("a"))
	assert.True(t, isValidChannelName("7"))
	assert.True(t, isValidChannelName("87a"))
	assert.True(t, isValidChannelName("aaa"))
	assert.True(t, isValidChannelName("_"))
	assert.True(t, isValidChannelName("_foo"))
}
