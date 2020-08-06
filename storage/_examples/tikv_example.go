package main

import (
	"fmt"
	"github.com/tikv/client-go/config"
	. "rawkv-demo/storage"
	"rawkv-demo/storage/tikv"
)

func main() {
	// Create a tikv based storage
	var store Store
	var err error
	store, err = tikv.NewtikvStore([]string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err.Error())
	}

	// Set some key-value pair with different timestamp
	key := Key("milvus")
	store.Set(key, Value("milvus_v1"), 1)
	store.Set(key, Value("milvus_v2"), 2)
	store.Set(key, Value("milvus_v3"), 3)
	store.Set(key, Value("milvus_v4"), 4)

	search := func(key Key, timestamp uint64) {
		v, err := store.Get(key, timestamp)
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("Get result for key: %s, version:%d, value:%s \n", key, timestamp, v)
	}

	search(key, 0)
	search(key, 3)
	search(key, 10)

	// Batch set key-value pairs with same timestamp
	keys := []Key{Key("milvus_batch"), Key("milvus_batch1")}
	values := []Value{Value("milvus_batch_v1"), Value("milvus_batch1_v1")}
	store.BatchSet(keys, values, 0)

	batchSearch := func(keys []Key, timestamp uint64) {
		vs, err := store.BatchGet(keys, timestamp)
		if err != nil {
			panic(err.Error())
		}
		for i, v := range vs {
			fmt.Printf("Get result for key: %s, version:%d, value:%s \n", keys[i], timestamp, v)
		}
	}

	// Batch get keys
	keys = []Key{Key("milvus_batch"), Key("milvus_batch1")}
	batchSearch(keys, 1)

	//Delete outdated key-value pairs for a key
	store.Set(key, Value("milvus_v3"), 6)
	store.Set(key, Value("milvus_v4"), 7)
	err = store.Delete(key, 5)
	search(key, 5)
}
