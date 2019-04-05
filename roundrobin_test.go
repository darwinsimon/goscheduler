package goscheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundRobinAdd(t *testing.T) {
	r := newRoundRobin()
	r.Add(100)
	r.Add(200)
	r.Add(300)

	tcs := []struct {
		name   string
		picked int
		err    error
	}{
		{
			name:   "100",
			picked: 100,
			err:    nil,
		},
		{
			name:   "200",
			picked: 200,
			err:    nil,
		},
		{
			name:   "300",
			picked: 300,
			err:    nil,
		},
		{
			name:   "100",
			picked: 100,
			err:    nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			picked, err := r.Pick()
			assert.Equal(t, tc.picked, picked)
			assert.Equal(t, tc.err, err)

		})
	}

}

func TestRoundRobinSinglePick(t *testing.T) {
	r := newRoundRobin()
	r.Add(100)

	picked, err := r.Pick()
	assert.Equal(t, 100, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 100, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 100, picked)
	assert.Nil(t, err)

}

func TestRoundRobinWithRemoveByIndex(t *testing.T) {
	r := newRoundRobin()

	picked, err := r.Pick()
	assert.Equal(t, 0, picked)
	assert.EqualError(t, err, "No item")

	r.Add(100)
	r.Add(200)

	picked, err = r.Pick()
	assert.Equal(t, 100, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 200, picked)
	assert.Nil(t, err)

	r.Add(300)
	r.Add(400)

	picked, err = r.Pick()
	assert.Equal(t, 300, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByIndex(0)) // RemoveByIndex 100

	picked, err = r.Pick()
	assert.Equal(t, 400, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 200, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 300, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByIndex(2)) // RemoveByIndex 400

	picked, err = r.Pick()
	assert.Equal(t, 200, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByIndex(0)) // RemoveByIndex 200

	r.Add(500)
	r.Add(600)
	r.Add(700)

	picked, err = r.Pick()
	assert.Equal(t, 300, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 500, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByIndex(2)) // RemoveByIndex 600

	picked, err = r.Pick()
	assert.Equal(t, 700, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByIndex(0)) // RemoveByIndex 300
	assert.Nil(t, r.RemoveByIndex(0)) // RemoveByIndex 500
	assert.Nil(t, r.RemoveByIndex(0)) // RemoveByIndex 700

	assert.EqualError(t, r.RemoveByIndex(999), "Index out of range")
}

func TestRoundRobinWithRemoveByValue(t *testing.T) {
	r := newRoundRobin()

	picked, err := r.Pick()
	assert.Equal(t, 0, picked)
	assert.EqualError(t, err, "No item")

	r.Add(100)
	r.Add(200)

	assert.EqualError(t, r.RemoveByValue(999), "Item not found")

	picked, err = r.Pick()
	assert.Equal(t, 100, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 200, picked)
	assert.Nil(t, err)

	r.Add(300)
	r.Add(400)

	picked, err = r.Pick()
	assert.Equal(t, 300, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByValue(100))

	picked, err = r.Pick()
	assert.Equal(t, 400, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 200, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 300, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByValue(400))

	picked, err = r.Pick()
	assert.Equal(t, 200, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByValue(200))

	r.Add(500)
	r.Add(600)
	r.Add(700)

	picked, err = r.Pick()
	assert.Equal(t, 300, picked)
	assert.Nil(t, err)

	picked, err = r.Pick()
	assert.Equal(t, 500, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByValue(600))

	picked, err = r.Pick()
	assert.Equal(t, 700, picked)
	assert.Nil(t, err)

	assert.Nil(t, r.RemoveByValue(300))
	assert.Nil(t, r.RemoveByValue(500))
	assert.Nil(t, r.RemoveByValue(700))

}
func TestRoundRobinIsDuplicate(t *testing.T) {
	r := newRoundRobin()

	r.Add(100)
	r.Add(200)

	assert.False(t, r.IsDuplicate(300))

	r.Add(300)

	assert.True(t, r.IsDuplicate(300))

	assert.Nil(t, r.RemoveByIndex(2))

	assert.False(t, r.IsDuplicate(300))

	r.Add(300)

	assert.True(t, r.IsDuplicate(300))

}
