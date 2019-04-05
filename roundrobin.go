package goscheduler

import (
	"errors"
	"sync"
)

type roundRobin struct {
	items      []int
	lastPicked int
	total      int
	mtx        sync.Mutex
}

// new round robin picker for integer
func newRoundRobin() *roundRobin {
	return &roundRobin{
		lastPicked: -1,
		items:      []int{},
	}
}

func (r *roundRobin) Add(newItem int) {
	r.mtx.Lock()
	r.items = append(r.items, newItem)
	r.total++
	r.mtx.Unlock()
}

func (r *roundRobin) IsDuplicate(item int) bool {

	r.mtx.Lock()
	defer r.mtx.Unlock()

	for i := range r.items {
		if r.items[i] == item {
			return true
		}
	}
	return false
}

func (r *roundRobin) RemoveByIndex(index int) error {

	if r.total == 0 || r.total < index {
		return errors.New("Index out of range")
	}

	r.mtx.Lock()

	r.items = append(r.items[:index], r.items[index+1:]...)

	r.total--
	if r.lastPicked >= index {
		r.lastPicked--
	}

	r.mtx.Unlock()

	return nil
}

func (r *roundRobin) RemoveByValue(value int) error {

	if r.total == 0 {
		return errors.New("No item")
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	for i := range r.items {
		if r.items[i] == value {
			r.items = append(r.items[:i], r.items[i+1:]...)
			r.total--
			if r.lastPicked >= i {
				r.lastPicked--
			}
			return nil
		}
	}

	return errors.New("Item not found")
}

// Pick a value from the item list
func (r *roundRobin) Pick() (int, error) {

	if r.total == 0 {
		return 0, errors.New("No item")
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.total == 1 {
		return r.items[0], nil
	}

	r.lastPicked++
	if r.lastPicked == r.total {
		r.lastPicked = 0
	}

	return r.items[r.lastPicked], nil
}
