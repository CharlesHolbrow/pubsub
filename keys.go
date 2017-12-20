package pubsub

import "sync"

// keys is a collection of strings. methods are safe for concurrent calls.
type keys struct {
	db    map[string]bool
	mutex sync.RWMutex
}

func newKeys() *keys {
	return &keys{
		db: make(map[string]bool),
	}
}

// return a slice if strigs in check, but not keys.db. nil if none.
//
// needs read lock
func (k *keys) missing(check ...string) (result []string) {
	k.mutex.RLock()
	for _, c := range check {
		if _, ok := k.db[c]; !ok {
			if result == nil {
				result = make([]string, 0, 8)
			}
			result = append(result, c)
		}
	}
	k.mutex.RUnlock()
	return
}

// Return a slice of strings in both keys.db and check. If none, return nil.
//
// needs ReadLock
func (k *keys) intersection(check ...string) (result []string) {
	k.mutex.RLock()
	for _, c := range check {
		if _, ok := k.db[c]; ok {
			if result == nil {
				result = make([]string, 0, 8)
			}
			result = append(result, c)
		}
	}
	return result
}

// needs write lock
func (k *keys) add(strings ...string) {
	for _, key := range strings {
		k.db[key] = true
	}
}

// needs write lock
func (k *keys) rem(strings ...string) {
	for _, key := range strings {
		delete(k.db, key)
	}
}

// Remove all keys, and return a slice of them
//
// needs write lock
func (k *keys) clear() []interface{} {
	k.mutex.Lock()
	result := make([]interface{}, 0, len(k.db))
	for key := range k.db {
		result = append(result, key)
		delete(k.db, key)
	}
	k.mutex.Unlock()
	return result
}