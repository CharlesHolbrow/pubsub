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
// acquires read lock
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
// acquires ReadLock
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

// aquires write lock
func (k *keys) add(strings ...string) {
	k.mutex.Lock()
	for _, key := range strings {
		k.db[key] = true
	}
	k.mutex.Unlock()
}

// aquires write lock
func (k *keys) rem(strings ...string) {
	k.mutex.Lock()
	for _, key := range strings {
		delete(k.db, key)
	}
	k.mutex.Unlock()
}

// Remove all keys, and return a slice of them
//
// aquires write lock
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
