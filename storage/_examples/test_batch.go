package main

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/config"
	. "tikv-store/storage"
	"tikv-store/storage/tikv"
)

func main(){
	// Create a tikv based storage
	var err error
	ctx := context.Background()
	store, err := tikv.NewtikvStore(ctx, []string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err.Error())
	}

	keys := []Key{Key("milvus"), Key("foo"), Key("bar"), Key("a"), Key("b"), Key("c")}
	vals := []Key{Key("milvus"), Key("foo"), Key("bar"), Key("a"), Key("b"), Key("c")}

	store.BatchSet(ctx, keys, vals, 0)
	store.BatchSet(ctx, keys, vals, 1)
	store.BatchSet(ctx, keys, vals, 2)
	store.BatchSet(ctx, keys, vals, 3)

	err = store.BatchDeleteMultiRoutine(ctx, keys, 2)
	if err!= nil{
		fmt.Println(err.Error())
	}

	batchSearch := func(keys []Key, timestamp uint64) {
		vs, err := store.BatchGet(ctx, keys, timestamp)
		if err != nil {
			panic(err.Error())
		}
		for i, v := range vs {
			fmt.Printf("Get result for key: %s, version:%d, value:%s \n", keys[i], timestamp, v)
		}
	}
	batchSearch(keys, 2)
}
