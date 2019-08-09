// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sumdb implements the HTTP protocols for serving or accessing
// a generalized checksum database. This package is forked from
// golang.org/x/mod/sumdb.
package sumdb

import (
	"context"
	"net/http"
	"os"
	"regexp"
	"strings"

	"golang.org/x/mod/sumdb/tlog"
)

// A ServerOps provides the external operations
// (underlying database access and so on) needed by the Server.
type ServerOps interface {
	// Signed returns the signed hash of the latest tree.
	Signed(ctx context.Context) ([]byte, error)

	// ReadRecords returns the content for the n records id through id+n-1.
	ReadRecords(ctx context.Context, id, n int64) ([][]byte, error)

	// Lookup looks up a record for the given key,
	// returning the record ID.
	Lookup(ctx context.Context, key string) (int64, error)

	// ReadTileData reads the content of tile t.
	// It is only invoked for hash tiles (t.L ≥ 0).
	ReadTileData(ctx context.Context, t tlog.Tile) ([]byte, error)
}

// A Server is the checksum database HTTP server,
// which implements http.Handler and should be invoked
// to serve the paths listed in ServerPaths.
type Server struct {
	ops ServerOps
}

// NewServer returns a new Server using the given operations.
func NewServer(ops ServerOps) *Server {
	return &Server{ops: ops}
}

// ServerPaths are the URL paths the Server can (and should) serve.
//
// Typically a server will do:
//
//	srv := sumdb.NewServer(ops)
//	for _, path := range sumdb.ServerPaths {
//		http.Handle(path, srv)
//	}
//
var ServerPaths = []string{
	"/lookup/",
	"/latest",
	"/tile/",
}

var modVerRE = regexp.MustCompile(`^[^@]+@v[0-9]+\.[0-9]+\.[0-9]+(-[^@]*)?(\+incompatible)?$`)

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	switch {
	default:
		http.NotFound(w, r)

	case strings.HasPrefix(r.URL.Path, "/lookup/"):
		key := strings.TrimPrefix(r.URL.Path, "/lookup/")
		id, err := s.ops.Lookup(ctx, key)
		if err != nil {
			reportError(w, r, err)
			return
		}
		records, err := s.ops.ReadRecords(ctx, id, 1)
		if err != nil {
			// This should never happen - the lookup says the record exists.
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(records) != 1 {
			http.Error(w, "invalid record count returned by ReadRecords", http.StatusInternalServerError)
			return
		}
		msg, err := tlog.FormatRecord(id, records[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		signed, err := s.ops.Signed(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		w.Write(msg)
		w.Write(signed)

	case r.URL.Path == "/latest":
		data, err := s.ops.Signed(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		w.Write(data)

	case strings.HasPrefix(r.URL.Path, "/tile/"):
		t, err := tlog.ParseTilePath(r.URL.Path[1:])
		if err != nil {
			http.Error(w, "invalid tile syntax", http.StatusBadRequest)
			return
		}
		if t.L == -1 {
			// Record data.
			start := t.N << uint(t.H)
			records, err := s.ops.ReadRecords(ctx, start, int64(t.W))
			if err != nil {
				reportError(w, r, err)
				return
			}
			if len(records) != t.W {
				http.Error(w, "invalid record count returned by ReadRecords", http.StatusInternalServerError)
				return
			}
			var data []byte
			for i, text := range records {
				msg, err := tlog.FormatRecord(start+int64(i), text)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				data = append(data, msg...)
			}
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.Write(data)
			return
		}

		data, err := s.ops.ReadTileData(ctx, t)
		if err != nil {
			reportError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(data)
	}
}

// reportError reports err to w.
// If it's a not-found, the reported error is 404.
// Otherwise it is an internal server error.
// The caller must only call reportError in contexts where
// a not-found err should be reported as 404.
func reportError(w http.ResponseWriter, r *http.Request, err error) {
	if os.IsNotExist(err) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
