// Package couchdb implements wrappers for the CouchDB HTTP API.
//
// Unless otherwise noted, all functions in this package
// can be called from more than one goroutine at the same time.
package couchdb

import (
	"bytes"
	"encoding/json"
	"strings"
	"context"
)

var getJsonKeys = []string{"open_revs", "atts_since"}

// ContextAwareDB represents a remote CouchDB database.
type ContextAwareDB struct {
	*transport
	name string
}

// Name returns the name of a database.
func (db *ContextAwareDB) Name() string {
	return db.name
}

// Get retrieves a document from the given database.
// The document is unmarshalled into the given object.
// Some fields (like _conflicts) will only be returned if the
// options require it. Please refer to the CouchDB HTTP API documentation
// for more information.
//
// http://docs.couchdb.org/en/latest/api/document/common.html?highlight=doc#get--db-docid
func (db *ContextAwareDB) Get(ctx context.Context, id string, doc interface{}, opts Options) error {
	path, err := optpath(opts, getJsonKeys, db.name, id)
	if err != nil {
		return err
	}
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	return readBody(resp, &doc)
}

// Rev fetches the current revision of a document.
// It is faster than an equivalent Get request because no body
// has to be parsed.
func (db *ContextAwareDB) Rev(ctx context.Context, id string) (string, error) {
	return responseRev(db.closedRequest(ctx, "HEAD", path(db.name, id), nil))
}

// Post stores a new document into the given database.
func (db *ContextAwareDB) Post(ctx context.Context, doc interface{}) (id, rev string, err error) {
	path := revpath("", db.name)
	// TODO: make it possible to stream encoder output somehow
	json, err := json.Marshal(doc)
	if err != nil {
		return "", "", err
	}
	b := bytes.NewReader(json)
	resp, err := db.request(ctx, "POST", path, b)
	if err != nil {
		return "", "", err
	}
	return responseIDRev(resp)
}

// Put stores a document into the given database.
func (db *ContextAwareDB) Put(ctx context.Context, id string, doc interface{}, rev string) (newrev string, err error) {
	path := revpath(rev, db.name, id)
	// TODO: make it possible to stream encoder output somehow
	json, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	b := bytes.NewReader(json)
	return responseRev(db.closedRequest(ctx, "PUT", path, b))
}

// Delete marks a document revision as deleted.
func (db *ContextAwareDB) Delete(ctx context.Context, id, rev string) (newrev string, err error) {
	path := revpath(rev, db.name, id)
	return responseRev(db.closedRequest(ctx, "DELETE", path, nil))
}

// Security represents database security objects.
type Security struct {
	Admins  Members `json:"admins"`
	Members Members `json:"members"`
}

// Members represents member lists in database security objects.
type Members struct {
	Names []string `json:"names,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

// Security retrieves the security object of a database.
func (db *ContextAwareDB) Security(ctx context.Context) (*Security, error) {
	secobj := new(Security)
	resp, err := db.request(ctx, "GET", path(db.name, "_security"), nil)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength == 0 {
		// empty reply means defaults
		return secobj, nil
	}
	if err = readBody(resp, secobj); err != nil {
		return nil, err
	}
	return secobj, nil
}

// PutSecurity sets the database security object.
func (db *ContextAwareDB) PutSecurity(ctx context.Context, secobj *Security) error {
	json, _ := json.Marshal(secobj)
	body := bytes.NewReader(json)
	_, err := db.request(ctx, "PUT", path(db.name, "_security"), body)
	return err
}

var viewJsonKeys = []string{"startkey", "start_key", "key", "endkey", "end_key", "keys"}

// View invokes a view.
// The ddoc parameter must be the name of the design document
// containing the view, but excluding the _design/ prefix.
//
// The output of the query is unmarshalled into the given result.
// The format of the result depends on the options. Please
// refer to the CouchDB HTTP API documentation for all the possible
// options that can be set.
//
// http://docs.couchdb.org/en/latest/api/ddoc/views.html
func (db *ContextAwareDB) View(ctx context.Context, ddoc, view string, result interface{}, opts Options) error {
	ddoc = strings.Replace(ddoc, "_design/", "", 1)
	path, err := optpath(opts, viewJsonKeys, db.name, "_design", ddoc, "_view", view)
	if err != nil {
		return err
	}
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	return readBody(resp, &result)
}

// AllDocs invokes the _all_docs view of a database.
//
// The output of the query is unmarshalled into the given result.
// The format of the result depends on the options. Please
// refer to the CouchDB HTTP API documentation for all the possible
// options that can be set.
//
// http://docs.couchdb.org/en/latest/api/database/bulk-api.html#db-all-docs
func (db *ContextAwareDB) AllDocs(ctx context.Context, result interface{}, opts Options) error {
	path, err := optpath(opts, viewJsonKeys, db.name, "_all_docs")
	if err != nil {
		return err
	}
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	return readBody(resp, &result)
}

// SyncDesign will attempt to create or update a design document on the provided
// database. This can be called multiple times for different databases,
// the latest Rev will always be fetched before storing the design.
func (db *ContextAwareDB) SyncDesign(ctx context.Context, d *Design) error {
	// Get the previous design doc so we can compare and extract Rev if needed
	prev := &Design{}
	if err := db.Get(ctx, d.ID, prev, nil); err != nil {
		if !NotFound(err) {
			return err
		}
	}
	if prev.Rev != "" {
		if d.ViewChecksum() == prev.ViewChecksum() {
			// nothing to do!
			d.Rev = prev.Rev
			return nil
		}
	}
	d.Rev = "" // Prevent conflicts when switching databases
	if rev, err := db.Put(ctx, d.ID, d, prev.Rev); err != nil {
		return err
	} else {
		d.Rev = rev
	}
	return nil
}

// Deprecated: Use ContextAwareDB
type DB struct {
	db *ContextAwareDB
}

func (db *DB) Name() string { return db.db.Name() }
func (db *DB) Get(id string, doc interface{}, opts Options) error {return db.db.Get(context.Background(), id, doc, opts)}
func (db *DB) Rev(id string) (string, error) { return db.db.Rev(context.Background(), id)}
func (db *DB) Post(doc interface{}) (id, rev string, err error) {return db.db.Post(context.Background(), doc)}
func (db *DB) Put(id string, doc interface{}, rev string) (newrev string, err error) {return db.db.Put(context.Background(), id, doc, rev)}
func (db *DB) Delete(id, rev string) (newrev string, err error) {return db.db.Delete(context.Background(), id, rev)}
func (db *DB) Security() (*Security, error) { return db.db.Security(context.Background()) }
func (db *DB) PutSecurity(secobj *Security) error { return db.db.PutSecurity(context.Background(), secobj) }
func (db *DB) View(ddoc, view string, result interface{}, opts Options) error { return db.db.View(context.Background(), ddoc, view, result, opts) }
func (db *DB) AllDocs(result interface{}, opts Options) error { return db.db.AllDocs(context.Background(), result, opts)}
func (db *DB) SyncDesign(d *Design) error { return db.db.SyncDesign(context.Background(), d)}
