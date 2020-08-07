package main

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/config"
	"math"
	. "tikv-store/storage"
	"tikv-store/storage/tikv"
)

func main() {
	// Create a tikv based storage
	var store Store
	var err error
	ctx := context.Background()
	store, err = tikv.NewtikvStore([]string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err.Error())
	}

	// Set some key-value pair with different timestamp
	key := Key("milvus")
	store.Set(ctx, key, Value("milvus_1"), 1)
	store.Set(ctx, key, Value("milvus_2"), 2)
	store.Set(ctx, key, Value("milvus_3"), 3)
	store.Set(ctx, key, Value("milvus_4"), 4)

	search := func(key Key, timestamp uint64) {
		v, err := store.Get(ctx, key, timestamp)
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("Get result for key: %s, version:%d, value:%s \n", key, timestamp, v)
	}

	search(key, 0)
	search(key, 3)
	search(key, 10)

	// Batch set key-value pairs with same timestamp
	keys := []Key{Key("milvus"), Key("milvus-foo")}
	values := []Value{Value("milvus_5"), Value("milvus_foo_5")}
	store.BatchSet(ctx, keys, values, 5)

	batchSearch := func(keys []Key, timestamp uint64) {
		vs, err := store.BatchGet(ctx, keys, timestamp)
		if err != nil {
			panic(err.Error())
		}
		for i, v := range vs {
			fmt.Printf("Get result for key: %s, version:%d, value:%s \n", keys[i], timestamp, v)
		}
	}

	// Batch get keys
	keys = []Key{Key("milvus"), Key("milvus-foo")}
	batchSearch(keys, 5)

	//Delete outdated key-value pairs for a key
	store.Set(ctx, key, Value("milvus_6"), 6)
	store.Set(ctx, key, Value("milvus_7"), 7)
	err = store.Delete(ctx, key, 5)
	search(key, 5)

	// use BatchDelete all keys
	keys = []Key{Key("milvus"), Key("milvus_foo")}
	store.BatchDelete(ctx, keys , math.MaxUint64)
	batchSearch(keys, math.MaxUint64)
}
