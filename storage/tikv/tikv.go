package tikv

import (
	"context"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"math"
	. "tikv-store/storage"
	. "tikv-store/storage/codec"
)

type tikvStore struct {
	client *rawkv.Client
}

func NewtikvStore(pdAddrs []string, conf config.Config) (*tikvStore, error) {
	client, err := rawkv.NewClient(context.TODO(), pdAddrs, conf)
	if err != nil {
		return nil, err
	}
	return &tikvStore{
		client: client,
	}, nil
}

func (s *tikvStore) Name() string {
	return "TiKV storage"
}

func (s *tikvStore) Get(ctx context.Context, key Key, timestamp uint64) (Value, error) {
	end := keyAddDelimiter(key)
	keys, vals, err := s.client.Scan(ctx, MvccEncode(key, timestamp), MvccEncode(end, math.MaxUint64), 1)
	if err != nil {
		return nil, err
	}
	if keys == nil {
		return nil, nil
	}
	return vals[0], err
}

func (s *tikvStore) Set(ctx context.Context, key Key, v Value, timestamp uint64) error {
	codedKey := MvccEncode(key, timestamp)
	err := s.client.Put(ctx, codedKey, v)
	return err
}

func (s *tikvStore) BatchSet(ctx context.Context, keys []Key, v []Value, timestamp uint64) error {
	for i, key := range keys {
		keys[i] = MvccEncode(key, timestamp)
	}
	err := s.client.BatchPut(ctx, keys, v)
	return err
}

func (s *tikvStore) BatchGet(ctx context.Context, keys []Key, timestamp uint64) ([]Value, error) {
	var key Key
	var val Value
	var err error
	var vals []Value
	// TODO: When batch size is too large, should use go routine and chain to multi get?
	for _, key = range keys {
		val, err = s.Get(ctx, key, timestamp)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, err
}

func (s *tikvStore) Delete(ctx context.Context, key Key, timestamp uint64) error {
	end := keyAddDelimiter(key)
	err := s.client.DeleteRange(ctx, MvccEncode(key, timestamp), MvccEncode(end, math.MaxUint64))
	return err
}

func (s *tikvStore) BatchDelete(ctx context.Context, keys []Key, timestamp uint64) error {
	var key Key
	var err error

	for _, key = range keys{
		err = s.Delete(ctx, key, timestamp)
		if err != nil {
			return err
		}
	}
	return err
}

func (s *tikvStore) Scan(ctx context.Context, start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {
	panic("implement me")
}

func (s *tikvStore) ReverseScan(ctx context.Context, start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {
	panic("implement me")
}

func keyAddDelimiter(key Key) Key{
	// TODO: decide delimiter byte, currently set 0x00
	return append(key, byte(0x00))
}
