package goscheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogString(t *testing.T) {
	assert.Equal(t, "DBG", LogLevelDebug.String())
	assert.Equal(t, "INF", LogLevelInfo.String())
	assert.Equal(t, "WRN", LogLevelWarning.String())
	assert.Equal(t, "ERR", LogLevelError.String())
}
