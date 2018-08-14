package couchdb_test

import (
	"testing"
	"net/url"
	"net/http"
	"errors"
	"io"
	"github.com/cabify/go-couchdb"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		URL                         *url.URL
		Auth                        couchdb.Auth
		SetAuth                     couchdb.Auth
		ExpectURL, ExpectAuthHeader string
	}{
		// No Auth
		{
			URL:       asURL("http://127.0.0.1:5984/"),
			ExpectURL: "http://127.0.0.1:5984",
		},
		{
			URL:       asURL("http://hostname:5984/foobar?query=1"),
			ExpectURL: "http://hostname:5984/foobar",
		},
		// Credentials in URL
		{
			URL:              asURL("http://user:password@hostname:5984/"),
			ExpectURL:        "http://hostname:5984",
			Auth:             couchdb.BasicAuth("user", "password"),
			ExpectAuthHeader: "Basic dXNlcjpwYXNzd29yZA==",
		},
		// Credentials in URL and explicit SetAuth, SetAuth credentials win
		{
			URL:              asURL("http://urluser:urlpassword@hostname:5984/"),
			SetAuth:          couchdb.BasicAuth("user", "password"),
			ExpectURL:        "http://hostname:5984",
			ExpectAuthHeader: "Basic dXNlcjpwYXNzd29yZA==",
		},
	}

	for i, test := range tests {
		rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			a := r.Header.Get("Authorization")
			if a != test.ExpectAuthHeader {
				t.Errorf("test %d: auth header mismatch: got %q, want %q", i, a, test.ExpectAuthHeader)
			}
			return nil, errors.New("nothing to see here, move along")
		})
		httpClient := &http.Client{Transport: rt}
		c := couchdb.NewClient(test.URL, httpClient, test.Auth)
		if c.URL() != test.ExpectURL {
			t.Errorf("test %d: ServerURL mismatch: got %q, want %q", i, c.URL(), test.ExpectURL)
		}
		if test.SetAuth != nil {
			c.SetAuth(test.SetAuth)
		}
		c.Ping() // trigger round trip
	}
}

func TestServerURL(t *testing.T) {
	c := newTestClient(t)
	check(t, "c.URL()", "http://testClient:5984", c.URL())
}

func TestPing(t *testing.T) {
	c := newTestClient(t)
	c.Handle("HEAD /", func(resp http.ResponseWriter, req *http.Request) {})

	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDB(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db", func(resp http.ResponseWriter, req *http.Request) {})

	db, err := c.CreateDB("db")
	if err != nil {
		t.Fatal(err)
	}

	check(t, "db.Name()", "db", db.Name())
}

func TestDeleteDB(t *testing.T) {
	c := newTestClient(t)
	c.Handle("DELETE /db", func(resp http.ResponseWriter, req *http.Request) {})
	if err := c.DeleteDB("db"); err != nil {
		t.Fatal(err)
	}
}

func TestAllDBs(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /_all_dbs", func(resp http.ResponseWriter, req *http.Request) {
		io.WriteString(resp, `["a","b","c"]`)
	})

	names, err := c.AllDBs()
	if err != nil {
		t.Fatal(err)
	}
	check(t, "returned names", []string{"a", "b", "c"}, names)
}
