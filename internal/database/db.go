// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package database implements a checksum database
// backed by an underlying transactional key-value store.
package database

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"golang.org/x/mod/sumdb/storage"
	"golang.org/x/mod/sumdb/tlog"
)

// A DB is a Go checksum database.
type DB struct {
	store storage.Storage
}

// The key space is partitioned by leading prefix:
//
//	"size" holds the overall log size
//	"config:key" holds the value for the given configuration key.
//	"module:module@version" maps to "<record-id><record-hash>"
//	"record:<record-id>" maps to record data.
//	"hash:<storage-id>" maps to a stored tlog.Hash.
//
// IDs are stored as 8-byte big-endian integers encoded with encodeInt.
// Hashes are stored as 32-byte binary values.
// Both must be encoded to text for use in certain databases (like Spanner),
// so we use hex.

const encodedIntSize = 16

func encodeInt(x int64) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return hex.EncodeToString(buf[:])
}

func decodeInt(val string) (int64, error) {
	b, err := hex.DecodeString(val)
	if err != nil {
		return 0, fmt.Errorf("invalid int: %s", val)
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid encoded integer")
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// encodeHash encodes the tlog.Hash into a string value.
func encodeHash(h tlog.Hash) string {
	return hex.EncodeToString(h[:])
}

// decodeHash decodes the string value into a tlog.Hash.
// The bytes are stored directly, so the only decoding is checking the length.
func decodeHash(val string) (tlog.Hash, error) {
	b, err := hex.DecodeString(val)
	if err != nil {
		return tlog.Hash{}, fmt.Errorf("invalid hash: %s", val)
	}
	var h tlog.Hash
	if len(b) != len(h) {
		return tlog.Hash{}, fmt.Errorf("wrong-size hash %d != %d", len(b), len(h))
	}
	copy(h[:], b)
	return h, nil
}

// Create initializes a new checksum database in empty storage.
func Create(ctx context.Context, store storage.Storage) (*DB, error) {
	// Only initialization is to record tree size 0.
	err := store.ReadWrite(ctx, func(ctx context.Context, tx storage.Transaction) error {
		if val, err := tx.ReadValue(ctx, "size"); err != nil {
			return err
		} else if val != "" {
			return fmt.Errorf("database already populated")
		}
		return tx.BufferWrites([]storage.Write{{Key: "size", Value: encodeInt(0)}})
	})
	if err != nil {
		return nil, err
	}

	return &DB{store: store}, nil
}

// Open opens an existing checksum database in the storage.
func Open(ctx context.Context, store storage.Storage) (*DB, error) {
	// Check that the database is initialized,
	// by reading the tree size.
	db := &DB{store: store}
	err := store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		_, err := db.readTreeSize(ctx, tx)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("invalid database: %v", err)
	}
	return db, nil
}

// NumRecords returns the number of records in the database.
func (db *DB) NumRecords(ctx context.Context) (int64, error) {
	var size int64
	err := db.store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		var err error
		size, err = db.readTreeSize(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return size, nil
}

// writeTreeSize buffers a write of the tree size within a transaction.
func (db *DB) writeTreeSize(ctx context.Context, tx storage.Transaction, size int64) error {
	return tx.BufferWrites([]storage.Write{{Key: "size", Value: encodeInt(size)}})
}

// readTreeSize reads and returns the current tree size within a transaction.
func (db *DB) readTreeSize(ctx context.Context, tx storage.Transaction) (int64, error) {
	val, err := tx.ReadValue(ctx, "size")
	if err != nil {
		return 0, err
	}
	if val == "" {
		return 0, fmt.Errorf("database not initialized")
	}
	return decodeInt(val)
}

// A hashMapLogger pretends the read and return hashes
// but actually just logs the indexes of the requested hashes in a map.
// We can run an algorithm like tlog.TreeHash once with a hashMapLogger
// to determine all the needed hashes, then read them all into a hashMap
// in one database read, and then rerun the algorithm with the retrieved
// hashes to get the final result.
type hashMapLogger map[int64]bool

func (h hashMapLogger) ReadHashes(indexes []int64) ([]tlog.Hash, error) {
	for _, x := range indexes {
		h[x] = true
	}
	return make([]tlog.Hash, len(indexes)), nil
}

// A hashMap implements tlog.HashReader using a fixed set of hashes in a map.
type hashMap map[int64]tlog.Hash

func (h hashMap) ReadHashes(indexes []int64) ([]tlog.Hash, error) {
	out := make([]tlog.Hash, len(indexes))
	for i, x := range indexes {
		th, ok := h[x]
		if !ok {
			return nil, fmt.Errorf("missing hash")
		}
		out[i] = th
	}
	return out, nil
}

// ReadHashes reads and returns the hashes with the given storage indexes.
func (db *DB) ReadHashes(ctx context.Context, indexes []int64) ([]tlog.Hash, error) {
	need := make(hashMapLogger)
	for _, x := range indexes {
		need[x] = true
	}
	var hashes hashMap
	err := db.store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		var err error
		hashes, err = db.readHashesInTx(ctx, tx, need)
		return err
	})
	if err != nil {
		return nil, err
	}
	list := make([]tlog.Hash, len(indexes))
	for i, x := range indexes {
		list[i] = hashes[x]
	}
	return list, nil
}

// readHashes reads and returns the needed hashes.
func (db *DB) readHashes(ctx context.Context, need hashMapLogger) (hashMap, error) {
	var hashes hashMap
	err := db.store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		var err error
		hashes, err = db.readHashesInTx(ctx, tx, need)
		return err
	})
	if err != nil {
		return nil, err
	}
	return hashes, nil
}

// readHashesInTx reads and returns the needed hashes, within an existing transaction.
func (db *DB) readHashesInTx(ctx context.Context, tx storage.Transaction, need hashMapLogger) (hashMap, error) {
	var keys []string
	for id := range need {
		keys = append(keys, "hash:"+encodeInt(id))
	}
	hashes := make(hashMap)
	vals, err := tx.ReadValues(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		id, err := decodeInt(key[len("hash:"):])
		if err != nil {
			return nil, err
		}
		h, err := decodeHash(vals[i])
		if err != nil {
			return nil, fmt.Errorf("invalid hash %q => %q", key, vals[i])
		}
		hashes[id] = h
	}
	return hashes, err
}

func (db *DB) hashReader(ctx context.Context) tlog.HashReader {
	return tlog.HashReaderFunc(func(indexes []int64) ([]tlog.Hash, error) {
		return db.ReadHashes(ctx, indexes)
	})
}

// TreeHash returns the top-level tree hash for the tree with n records.
func (db *DB) TreeHash(ctx context.Context, n int64) (tlog.Hash, error) {
	return tlog.TreeHash(n, db.hashReader(ctx))
}

// ProveRecord returns the proof that the tree of size t contains the record with index n.
func (db *DB) ProveRecord(ctx context.Context, t, n int64) (tlog.RecordProof, error) {
	return tlog.ProveRecord(t, n, db.hashReader(ctx))
}

// ProveTree returns the proof that the tree of size t contains as a prefix
// all the records from the tree of smaller size n.
func (db *DB) ProveTree(ctx context.Context, t, n int64) (tlog.TreeProof, error) {
	return tlog.ProveTree(t, n, db.hashReader(ctx))
}

// ReadTileData reads the hashes for the tile t and returns the corresponding tile data.
func (db *DB) ReadTileData(ctx context.Context, t tlog.Tile) ([]byte, error) {
	return tlog.ReadTileData(t, db.hashReader(ctx))
}

// Lookup looks up a record by its associated key ("module@version"),
// returning the record ID.
func (db *DB) Lookup(ctx context.Context, key string) (int64, error) {
	var id int64
	err := db.store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		id = -1
		val, err := tx.ReadValue(ctx, "module:"+key)
		if err != nil {
			return err
		}
		if val == "" {
			return os.ErrNotExist
		}
		if len(val) > encodedIntSize {
			val = val[:encodedIntSize]
		}
		id, err = decodeInt(val)
		return err
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Config returns the database configuration value for the given key.
func (db *DB) Config(ctx context.Context, key string) (string, error) {
	var v string
	err := db.store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		val, err := tx.ReadValue(ctx, "config:"+key)
		if err != nil {
			return err
		}
		v = val
		return nil
	})
	if err != nil {
		return "", err
	}
	return v, nil
}

// SetConfig sets the database configuration value for the given key to the value.
func (db *DB) SetConfig(ctx context.Context, key, value string) error {
	return db.store.ReadWrite(ctx, func(ctx context.Context, tx storage.Transaction) error {
		return tx.BufferWrites([]storage.Write{{Key: "config:" + key, Value: value}})
	})
}

// ReadRecords returns the content for the n records starting at id.
func (db *DB) ReadRecords(ctx context.Context, id, n int64) ([][]byte, error) {
	var list [][]byte
	err := db.store.ReadOnly(ctx, func(ctx context.Context, tx storage.Transaction) error {
		var keys []string
		for i := int64(0); i < n; i++ {
			keys = append(keys, "record:"+encodeInt(id+i))
		}
		vals, err := tx.ReadValues(ctx, keys)
		if err != nil {
			return err
		}
		list = nil
		for _, val := range vals {
			list = append(list, []byte(val))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

// maxKeyLen is the maximum length record key the database accepts.
const maxKeyLen = 4096

var errKeyTooLong = errors.New("key too long")

// A NewRecord tracks a new record to be added to the database.
// The caller initializes Key and Content, and Add sets ID and Err.
type NewRecord struct {
	Key     string // record key ("module@version")
	Content []byte // record content
	ID      int64  // record log ID sequence number
	Err     error  // error inserting record, if any
}

// A newRecord tracks additional information about a NewRecord during Add.
type newRecord struct {
	*NewRecord
	rhash tlog.Hash // record hash of record content
	rkey  string    // storage key - "module:"+r.Key
	next  *newRecord
	dup   bool // is duplicate of another record
}

// Add adds a new record with the given content and associated key.
// It returns the record id for the new record.
// If the key is the empty string, the new record has no key.
//
// If a record already exists with the same content and key,
// Add returns the record id for the existing record
// instead of adding a new one.
//
// If a record already exists with the same content but a different key,
// or the same key but different content,
// Add returns an error.
func (db *DB) Add(ctx context.Context, records []NewRecord) error {
	// Build list of records being written, with computed hashes.
	recs := make([]*newRecord, 0, len(records))
	byKey := make(map[string]*newRecord)
	for i := range records {
		r := &newRecord{NewRecord: &records[i]}
		r.rkey = "module:" + r.Key
		r.rhash = tlog.RecordHash(r.Content)
		recs = append(recs, r)
		if old := byKey[r.rkey]; old != nil {
			// Multiple writes of same record in one batch. Track all.
			if r.rhash != old.rhash {
				r.Err = fmt.Errorf("different content for preexisting record")
			}
			// Chain this record onto first record.
			r.next = old.next
			r.dup = true
			old.next = r
			continue
		}
		byKey[r.rkey] = r
	}

	// Add any new records to the tree in a single database transaction.
	err := db.store.ReadWrite(ctx, func(ctx context.Context, tx storage.Transaction) error {
		// Clear state in case transaction is being retried.
		for _, r := range recs {
			r.ID = -1
			r.Err = nil
		}

		// Read existing entries for all requested module@versions.
		var keys []string
		for _, r := range recs {
			if !r.dup {
				keys = append(keys, r.rkey)
			}
		}
		nfound := 0
		vals, err := tx.ReadValues(ctx, keys)
		if err != nil {
			return err
		}
		for i, val := range vals {
			if val == "" {
				continue
			}
			key := keys[i]
			if len(val) < encodedIntSize {
				return fmt.Errorf("corrupt database")
			}
			id, err := decodeInt(val[:encodedIntSize])
			if err != nil {
				return err
			}
			rhash, err := decodeHash(val[encodedIntSize:])
			if err != nil {
				return err
			}

			// Record preexisting ID for duplicate write.
			rec := byKey[key]
			if rec == nil {
				return fmt.Errorf("unexpected key hash")
			}
			rec.ID = id
			if rhash != rec.rhash {
				rec.Err = fmt.Errorf("different content for preexisting record")
			}
			nfound++
			for dup := rec.next; dup != nil; dup = dup.next {
				dup.ID = rec.ID
				// Note dup.Err may be non-nil already, if dup.Content differs from rec.Content.
				if dup.Err == nil {
					dup.Err = rec.Err
				}
				nfound++
			}
			return nil
		}

		// If we found all the records we were trying to write, we're done.
		if nfound == len(recs) {
			return nil
		}

		// Now that we know which records we're writing,
		// prepare new hashes for tree.
		treeSize, err := db.readTreeSize(ctx, tx)
		if err != nil {
			return err
		}
		storageID := tlog.StoredHashCount(treeSize)

		// To compute the new permanent hashes,
		// we need the existing hashes along the right-most fringe
		// of the tree. Those happen to be the same ones that
		// tlog.TreeHash needs, so use tlog.TreeHash to identify them.
		need := make(hashMapLogger)
		tlog.TreeHash(treeSize, need)

		// Read all those hashes in one read operation.
		hashes, err := db.readHashesInTx(ctx, tx, need)
		if err != nil {
			return err
		}

		// Queue the writes of the new records,
		// including their new permanent hashes.
		var writes []storage.Write
		for _, rec := range recs {
			if rec.ID >= 0 || rec.dup {
				continue
			}
			rec.ID = treeSize
			for dup := rec.next; dup != nil; dup = dup.next {
				dup.ID = treeSize
			}
			treeSize++

			// Queue data writes.
			writes = append(writes,
				storage.Write{Key: "record:" + encodeInt(rec.ID), Value: string(rec.Content)},
				storage.Write{Key: "module:" + rec.Key, Value: encodeInt(rec.ID) + encodeHash(rec.rhash)},
			)

			// Queue hash writes.
			toStore, err := tlog.StoredHashesForRecordHash(rec.ID, rec.rhash, hashes)
			if err != nil {
				return err
			}
			for _, h := range toStore {
				writes = append(writes, storage.Write{Key: "hash:" + encodeInt(storageID), Value: encodeHash(h)})
				hashes[storageID] = h
				storageID++
			}

			// Sanity check; can't happen unless this code's logic is wrong.
			if storageID != tlog.StoredHashCount(treeSize) {
				return fmt.Errorf("out of sync %d %d", storageID, tlog.StoredHashCount(treeSize))
			}
		}
		if err := tx.BufferWrites(writes); err != nil {
			return err
		}

		// Record new tree size.
		return db.writeTreeSize(ctx, tx, treeSize)
	})
	if err != nil {
		return err
	}
	return nil
}
