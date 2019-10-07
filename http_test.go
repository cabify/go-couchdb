package couchdb_test

import (
	"errors"
	"net/http"
	. "net/http"
	"testing"

	couchdb "github.com/cabify/go-couchdb"
)

type testauth struct{ called bool }

func (a *testauth) AddAuth(*Request) {
	a.called = true
}

func TestClientSetAuth(t *testing.T) {
	c := newTestClient(t)
	c.Handle("HEAD /", func(resp ResponseWriter, req *Request) {})

	auth := new(testauth)
	c.SetAuth(auth)
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
	if !auth.called {
		t.Error("AddAuth was not called")
	}

	auth.called = false
	c.SetAuth(nil)
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
	if auth.called {
		t.Error("AddAuth was called after removing Auth instance")
	}
}

func TestErrorHandling(t *testing.T) {
	te := &couchdb.Error{Method: "GET", StatusCode: http.StatusConflict}
	fe := errors.New("Not an HTTP error")
	if !couchdb.Conflict(te) {
		t.Errorf("Expected conflict")
	}
	if couchdb.Conflict(fe) {
		t.Errorf("Did not expect a conflict")
	}
}
