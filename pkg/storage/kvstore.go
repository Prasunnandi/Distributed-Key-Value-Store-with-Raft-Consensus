package storage

import (
	"encoding/json"
	"fmt"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}

func (kv *KVStore) Set(key string, val string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = val
}

func (kv *KVStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.data[key]; ok {
		delete(kv.data, key)
		return true
	}
	return false
}

func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return json.Marshal(kv.data)
}

func (kv *KVStore) Restore(snapshot []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var fresh map[string]string
	if err := json.Unmarshal(snapshot, &fresh); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}
	kv.data = fresh
	return nil
}

func (kv *KVStore) All() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	res := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		res[k] = v
	}
	return res
}
