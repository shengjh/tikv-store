package main

import (
	"context"
	"fmt"
	"github.com/tikv/client-go/config"
	"math"
	. "tikv-store/storage"
	"tikv-store/storage/tikv"
	"time"
)

func main() {
	var err error
	ctx := context.Background()

	var (
		pdAddr = []string{"127.0.0.1:2379"}
		conf   = config.Default()
	)

	store, err := tikv.NewtikvStore(ctx, pdAddr, conf)
	if err != nil {
		panic(err.Error())
	}

	// Prepare test data
	size := 0
	var testKeys []Key
	var testValues []Value
	for i := 0; size/conf.Raw.MaxBatchPutSize < 400; i++ {
		key := fmt.Sprint("key", i)
		size += len(key)
		testKeys = append(testKeys, []byte(key))
		value := fmt.Sprint("value", i)
		size += len(value)
		testValues = append(testValues, []byte(value))
	}

	// Set kv data
	allTs := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Now()
	for _, ts:= range allTs {
		err = store.BatchSet(ctx, testKeys, testValues, ts)
		if err != nil {
			panic(err.Error())
		}
	}
	fmt.Printf("Prepared test data %d kv pairs, total size %dKB,", len(testKeys) * len(allTs), size * len(allTs)/1024)
	fmt.Printf(" cost %s\n", time.Since(now))

	// Bench get
	maxTime := time.Duration(0)
	minTime := time.Duration(math.MaxInt64)

	keyMax := Key{}
	keyMin := Key{}

	for _, key := range testKeys {
		now := time.Now()
		_, err = store.Get(ctx, key, 2)
		cost := time.Since(now)
		if maxTime < cost {
			maxTime = cost
			keyMax = key
		}
		if minTime > cost {
			minTime = cost
			keyMin = key
		}
	}
	fmt.Printf("Max cost %s, key %s \n", maxTime, keyMax)
	fmt.Printf("Min cost %s, key %s \n", minTime, keyMin)

	// Delete test data
	now = time.Now()
	err = store.BatchDeleteMultiRoutine(ctx, testKeys, math.MaxUint64)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Batch delete all test data cost %s", time.Since(now))
}
