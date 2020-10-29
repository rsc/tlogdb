// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Tlogdb is a trivial transparent log client and server.
// It is meant as more a starting point to be customized
// than a tool to be used directly.
//
// A transparent log is a tamper-proof, append-only,
// immutable log of data records. That is, if the server
// were to violate the “append-only, immutable” properties,
// that tampering would be detected by the client.
// For more about transparent logs, see https://research.swtch.com/tlog.
//
// Server Operations
//
// To create a new log (new server state):
//
//	tlogdb [-f file] newlog $servername
//
// The newlog command creates a new database in file (default tlog.db)
// containing an empty log and a newly generated public/private key pair
// for the server using the given name.
//
// The newlog command prints
// the newly generated public key. To see it again:
//
//	tlogdb [-f file] publickey
//
// To add a record named name to the log:
//
//	cat data | tlogdb [-f file] add name
//
// To serve the authenticated log data:
//
//	tlogdb [-a addr] [-f file] serve
//
// The default server address is localhost:6655.
//
// Client Operations
//
// The client maintains a cache database both for performance
// (avoiding duplicate downloads) and for storing the server's
// public key and the most recently seen log head.
//
// To create a new client cache:
//
//	tlogdb [-c file] newcache key
//
// The newcache command creates a new database in file (default tlogclient.db)
// and stores the given public key for later use. The key should be the output
// of the tlogdb's server commands newlog or publickey, described above.
//
// To look up a record in the log:
//
//	tlogdb [-a addr] [-c file] lookup name
//
// The default server address is again localhost:6655.
//
// HTTP Protocol
//
// The protocol between client and server is the same as used in
// the Go module checksum database, documented at
// https://golang.org/design/25530-sumdb#checksum-database.
//
// There are three endpoints: /latest serves a signed tree head;
// /lookup/NAME looks up the given name,
// and /tile/* serves log tiles.
//
// Example
//
// Putting the various commands together in a Unix shell:
//
//	rm -f tlog.db tlogclient.db
//	go build
//
//	./tlogdb newlog myname
//	./tlogdb publickey
//	echo hello world | ./tlogdb add greeting
//	./tlogdb serve &
//
//	./tlogdb newcache $(./tlogdb publickey)
//	./tlogdb lookup greeting
//
//	kill $!
//
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"golang.org/x/mod/sumdb/note"
	"golang.org/x/mod/sumdb/storage"
	"golang.org/x/mod/sumdb/tlog"
	_ "rsc.io/sqlite"
	"rsc.io/tlogdb/internal/database"
	"rsc.io/tlogdb/sumdb"
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: tlogdb [-a addr] [-c cache] [-f db] cmd [args...]

Server Commands:
	newlog <server-name>
	publickey
	add <name>  (content on stdin)
	serve

Client Commands:
	newcache <publickey>
	lookup <name>
`)
	os.Exit(2)
}

var (
	serverDB   = flag.String("f", "tlog.db", "store server database in `file`")
	serverAddr = flag.String("a", "localhost:6655", "server runs on `addr`")
	clientDB   = flag.String("c", "tlogclient.db", "store local client database in `file`")
)

func main() {
	flag.Usage = usage
	flag.Parse()
	log.SetFlags(0)
	log.SetPrefix("tlogdb: ")

	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	switch args[0] {
	default:
		usage()

	case "newlog":
		if len(args) != 2 {
			usage()
		}
		name := args[1]
		db := NewSrvDB()
		if _, _, err := db.ReadKey(); err == nil {
			log.Fatal("server already initialized")
		}
		_, vkey, err := db.GenerateKey(name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", vkey)

	case "publickey":
		if len(args) != 1 {
			usage()
		}
		db := NewSrvDB()
		_, vkey, err := db.ReadKey()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", vkey)

	case "add":
		if len(args) != 2 {
			usage()
		}
		db := NewSrvDB()
		key := args[1]
		data, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal(err)
		}
		r := []database.NewRecord{{Key: key, Content: data}}
		if err := db.db.Add(context.Background(), r); err != nil {
			log.Fatal(err)
		}
		if r[0].Err != nil {
			log.Fatal(r[0].Err)
		}
		fmt.Printf("added record %d\n", r[0].ID)

	case "serve":
		if len(args) != 1 {
			usage()
		}
		srv := sumdb.NewServer(NewSrvDB())
		for _, path := range sumdb.ServerPaths {
			http.Handle(path, srv)
		}
		http.ListenAndServe(*serverAddr, nil)

	case "newcache":
		if len(args) != 2 {
			usage()
		}
		vkey := args[1]
		if _, err := note.NewVerifier(vkey); err != nil {
			log.Fatalf("invalid verifier key: %v", err)
		}
		cache := NewClientCache()
		_, err := cache.ReadConfig("key")
		if err == nil {
			log.Fatal("client already initialized")
		}
		if err := cache.WriteConfig("key", nil, []byte(vkey)); err != nil {
			log.Fatal(err)
		}

	case "lookup":
		if len(args) != 2 {
			usage()
		}
		key := args[1]
		client := sumdb.NewClient(NewClientCache())
		_, data, err := client.Lookup(key)
		if err != nil {
			log.Fatal(err)
		}
		os.Stdout.Write(data)
	}
}

type clientCache struct {
	sql *sql.DB
}

func NewClientCache() *clientCache {
	client := &clientCache{}
	sdb, err := sql.Open("sqlite3", *clientDB)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := sdb.Exec(`create table if not exists kv (k primary key, v)`); err != nil {
		log.Fatal(err)
	}
	client.sql = sdb
	return client
}

func (c *clientCache) ReadRemote(path string) ([]byte, error) {
	resp, err := http.Get("http://" + *serverAddr + path)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("http get: %v", resp.Status)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *clientCache) ReadConfig(file string) (data []byte, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("read config %s: %v", file, err)
		}
	}()

	data, err = sqlRead(c.sql, "config:"+file)
	if strings.HasSuffix(file, "/latest") && err == sql.ErrNoRows {
		return nil, nil
	}
	return data, err
}

func (c *clientCache) WriteConfig(file string, old, new []byte) error {
	if old == nil {
		return sqlWrite(c.sql, "config:"+file, new)
	}
	return sqlSwap(c.sql, "config:"+file, old, new)
}

func (c *clientCache) ReadCache(file string) ([]byte, error) {
	return sqlRead(c.sql, "file:"+file)
}

func (c *clientCache) WriteCache(file string, data []byte) {
	sqlWrite(c.sql, "file:"+file, data)
}

func (c *clientCache) Log(msg string) {
	log.Print(msg)
}

func (c *clientCache) SecurityError(msg string) {
	log.Fatal(msg)
}

type srvDB struct {
	sql *sql.DB
	db  *database.DB
}

func NewSrvDB() *srvDB {
	srv := &srvDB{}
	sdb, err := sql.Open("sqlite3", *serverDB)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := sdb.Exec(`create table if not exists kv (k primary key, v)`); err != nil {
		log.Fatal(err)
	}
	srv.sql = sdb
	op := database.Create
	if _, err := sqlRead(srv.sql, "skey"); err == nil {
		op = database.Open
	}
	db, err := op(context.Background(), &sqlStorage{sdb})
	if err != nil {
		log.Fatal(err)
	}
	srv.db = db
	return srv
}

func (srv *srvDB) Signed(ctx context.Context) ([]byte, error) {
	n, err := srv.db.NumRecords(ctx)
	if err != nil {
		return nil, err
	}
	h, err := srv.db.TreeHash(ctx, n)
	if err != nil {
		return nil, err
	}
	skey, err := sqlRead(srv.sql, "skey")
	if err != nil {
		return nil, err
	}
	text := tlog.FormatTree(tlog.Tree{N: n, Hash: h})
	signer, err := note.NewSigner(string(skey))
	if err != nil {
		return nil, err
	}
	return note.Sign(&note.Note{Text: string(text)}, signer)
}

func (srv *srvDB) ReadKey() (skey, vkey string, err error) {
	bskey, err := sqlRead(srv.sql, "skey")
	if err != nil {
		return "", "", err
	}
	bvkey, err := sqlRead(srv.sql, "vkey")
	if err != nil {
		return "", "", err
	}
	return string(bskey), string(bvkey), nil
}

func (srv *srvDB) GenerateKey(name string) (skey, vkey string, err error) {
	if _, err := sqlRead(srv.sql, "skey"); err == nil {
		return "", "", fmt.Errorf("already have key in server database")
	}
	skey, vkey, err = note.GenerateKey(rand.Reader, name)
	if err != nil {
		return "", "", err
	}
	if err := sqlWrite(srv.sql, "skey", []byte(skey)); err != nil {
		return "", "", err
	}
	if err := sqlWrite(srv.sql, "vkey", []byte(vkey)); err != nil {
		return "", "", err
	}
	return skey, vkey, nil
}

func (srv *srvDB) ReadRecords(ctx context.Context, id, n int64) ([][]byte, error) {
	return srv.db.ReadRecords(ctx, id, n)
}

func (srv *srvDB) Lookup(ctx context.Context, key string) (int64, error) {
	return srv.db.Lookup(ctx, key)
}

func (srv *srvDB) ReadTileData(ctx context.Context, t tlog.Tile) ([]byte, error) {
	return srv.db.ReadTileData(ctx, t)
}

type sqlTx struct {
	tx     *sql.Tx
	writes []storage.Write
}

type sqlStorage struct {
	db *sql.DB
}

func (s *sqlStorage) ReadOnly(ctx context.Context, f func(context.Context, storage.Transaction) error) error {
	return s.tx(ctx, f, nil)
}

func (s *sqlStorage) ReadWrite(ctx context.Context, f func(context.Context, storage.Transaction) error) error {
	return s.tx(ctx, f, []storage.Write{})
}

func (s *sqlStorage) tx(ctx context.Context, f func(context.Context, storage.Transaction) error, writes []storage.Write) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stx := &sqlTx{tx, writes}
	if err := f(ctx, stx); err != nil {
		return err
	}
	for _, w := range stx.writes {
		if _, err := tx.Exec(`insert or replace into kv (k, v) values (?, ?)`, w.Key, []byte(w.Value)); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (tx *sqlTx) ReadValue(ctx context.Context, key string) (value string, err error) {
	err = tx.tx.QueryRow(`select v from kv where k = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func (tx *sqlTx) ReadValues(ctx context.Context, keys []string) (values []string, err error) {
	for _, k := range keys {
		v, err := tx.ReadValue(ctx, k)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func (tx *sqlTx) BufferWrites(writes []storage.Write) error {
	if tx.writes == nil {
		panic("BufferWrites called with ReadOnly transaction")
	}
	tx.writes = append(tx.writes, writes...)
	return nil
}

func sqlRead(db *sql.DB, key string) ([]byte, error) {
	var value []byte
	err := db.QueryRow(`select v from kv where k = ?`, key).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func sqlWrite(db *sql.DB, key string, value []byte) error {
	_, err := db.Exec(`insert or replace into kv (k, v) values (?, ?)`, key, value)
	return err
}

func sqlSwap(db *sql.DB, key string, old, value []byte) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var txOld []byte
	if err := tx.QueryRow(`select v from kv where k = ?`, key).Scan(&txOld); err != nil {
		return err
	}
	if !bytes.Equal(txOld, old) {
		return sumdb.ErrWriteConflict
	}
	if _, err := tx.Exec(`insert or replace into kv (k, v) values (?, ?)`, key, value); err != nil {
		return err
	}
	return tx.Commit()
}
