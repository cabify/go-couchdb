package couchdb

import (
	"net/url"
	"net/http"
	"fmt"
	"context"
)

// ContextAwareClient represents a remote CouchDB server.
type ContextAwareClient struct{ *transport }

// NewContextAwareClient creates a new ContextAwareClient
// addr should contain scheme and host, and optionally port and path. All other attributes will be ignored
// If client is nil, default http.Client will be used
// If auth is nil, no auth will be set
func NewContextAwareClient(addr *url.URL, client *http.Client, auth Auth) *ContextAwareClient {
	prefixAddr := *addr
	// cleanup our address
	prefixAddr.User, prefixAddr.RawQuery, prefixAddr.Fragment = nil, "", ""
	return &ContextAwareClient{newTransport(prefixAddr.String(), client, auth)}
}

// URL returns the URL prefix of the server.
// The url will not contain a trailing '/'.
func (c *ContextAwareClient) URL() string {
	return c.prefix
}

// Ping can be used to check whether a server is alive.
// It sends an HTTP HEAD request to the server's URL.
func (c *ContextAwareClient) Ping(ctx context.Context) error {
	_, err := c.closedRequest(ctx, "HEAD", "/", nil)
	return err
}

// SetAuth sets the authentication mechanism used by the client.
// Use SetAuth(nil) to unset any mechanism that might be in use.
// In order to verify the credentials against the server, issue any request
// after the call the SetAuth.
func (c *ContextAwareClient) SetAuth(a Auth) {
	c.transport.setAuth(a)
}

// CreateDB creates a new database.
// The request will fail with status "412 Precondition Failed" if the database
// already exists. A valid DB object is returned in all cases, even if the
// request fails.
func (c *ContextAwareClient) CreateDB(ctx context.Context, name string) (*ContextAwareDB, error) {
	if _, err := c.closedRequest(ctx, "PUT", path(name), nil); err != nil {
		return c.DB(name), err
	}
	return c.DB(name), nil
}

// CreateDBWithShards creates a new database with the specified number of shards
func (c *ContextAwareClient) CreateDBWithShards(ctx context.Context, name string, shards int) (*ContextAwareDB, error) {
	_, err := c.closedRequest(ctx, "PUT", fmt.Sprintf("%s?q=%d", path(name), shards), nil)

	return c.DB(name), err
}

// EnsureDB ensures that a database with the given name exists.
func (c *ContextAwareClient) EnsureDB(ctx context.Context, name string) (*ContextAwareDB, error) {
	db, err := c.CreateDB(ctx, name)
	if err != nil && !ErrorStatus(err, http.StatusPreconditionFailed) {
		return nil, err
	}
	return db, nil
}

// DeleteDB deletes an existing database.
func (c *ContextAwareClient) DeleteDB(ctx context.Context, name string) error {
	_, err := c.closedRequest(ctx, "DELETE", path(name), nil)
	return err
}

// AllDBs returns the names of all existing databases.
func (c *ContextAwareClient) AllDBs(ctx context.Context) (names []string, err error) {
	resp, err := c.request(ctx, "GET", "/_all_dbs", nil)
	if err != nil {
		return names, err
	}
	err = readBody(resp, &names)
	return names, err
}

// DB creates a database object.
// The database inherits the authentication and http.RoundTripper
// of the client. The database's actual existence is not verified.
func (c *ContextAwareClient) DB(name string) *ContextAwareDB {
	return &ContextAwareDB{c.transport, name}
}

// Deprecated: Use ContextAwareClient
type Client struct{ c *ContextAwareClient }
func NewClient(addr *url.URL, client *http.Client, auth Auth) *Client{ return &Client{c: NewContextAwareClient(addr, client, auth) } }
func (c *Client) URL() string { return c.c.URL() }
func (c *Client) Ping() error { return c.c.Ping(context.Background()) }
func (c *Client) SetAuth(a Auth) { c.c.SetAuth(a) }
func (c *Client) CreateDB(name string) (*DB, error) {
	db, err := c.c.CreateDB(context.Background(), name)
	return &DB{ db:db}, err
}
func (c *Client) CreateDBWithShards(name string, shards int) (*DB, error) {
	db, err :=c.c.CreateDBWithShards(context.Background(), name, shards)
	return &DB{db: db}, err
}
func (c *Client) EnsureDB(name string) (*DB, error) {
	db, err := c.c.EnsureDB(context.Background(), name)
	return &DB{db:db}, err
}
func (c *Client) DeleteDB(name string) error { return c.c.DeleteDB(context.Background(), name) }
func (c *Client) AllDBs() (names []string, err error) { return c.c.AllDBs(context.Background()) }
func (c *Client) DB(name string) *DB { return &DB{db: c.c.DB(name)} }
