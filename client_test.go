package couchdb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"testing"

	"github.com/cabify/go-couchdb"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

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

func TestContext(t *testing.T) {
	c := newTestClient(t)
	nc := c.WithContext(context.TODO())
	if c.Client == nc {
		t.Errorf("context object not replaced")
	}
	if nc.Context() == c.Context() {
		t.Errorf("expect contexts to change")
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

// those are re-used across several tests
var securityObjectJSON = regexp.MustCompile("\\s").ReplaceAllString(
	`{
		"admins": {
			"names": ["adminName1", "adminName2"]
		},
		"members": {
			"names": ["memberName1"],
			"roles": ["memberRole1"]
		}
	}`, "")
var securityObject = &couchdb.Security{
	Admins: couchdb.Members{
		Names: []string{"adminName1", "adminName2"},
		Roles: nil,
	},
	Members: couchdb.Members{
		Names: []string{"memberName1"},
		Roles: []string{"memberRole1"},
	},
}

func TestSecurity(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_security", func(resp http.ResponseWriter, req *http.Request) {
		io.WriteString(resp, securityObjectJSON)
	})

	secobj, err := c.DB("db").Security()
	if err != nil {
		t.Fatal(err)
	}
	check(t, "secobj", securityObject, secobj)
}

func TestEmptySecurity(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_security", func(resp http.ResponseWriter, req *http.Request) {
		// CouchDB returns an empty reply if no security object has been set
		resp.WriteHeader(200)
	})

	secobj, err := c.DB("db").Security()
	if err != nil {
		t.Fatal(err)
	}
	check(t, "secobj", &couchdb.Security{}, secobj)
}

func TestPutSecurity(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db/_security", func(resp http.ResponseWriter, req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		check(t, "request body", securityObjectJSON, string(body))
		resp.WriteHeader(200)
	})

	err := c.DB("db").PutSecurity(securityObject)
	if err != nil {
		t.Fatal(err)
	}
}

type testDocument struct {
	ID    string `json:"_id,omitempty"`
	Rev   string `json:"_rev,omitempty"`
	Field int    `json:"field"`
}

func TestGetExistingDoc(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/doc", func(resp http.ResponseWriter, req *http.Request) {
		io.WriteString(resp, `{
			"_id": "doc",
			"_rev": "1-619db7ba8551c0de3f3a178775509611",
			"field": 999
		}`)
	})

	var doc testDocument
	if err := c.DB("db").Get("doc", &doc, nil); err != nil {
		t.Fatal(err)
	}
	check(t, "doc.Rev", "1-619db7ba8551c0de3f3a178775509611", doc.Rev)
	check(t, "doc.Field", 999, doc.Field)
}

func TestGetNonexistingDoc(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/doc", func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(404)
		io.WriteString(resp, `{"error":"not_found","reason":"error reason"}`)
	})

	var doc testDocument
	err := c.DB("db").Get("doc", doc, nil)
	check(t, "couchdb.NotFound(err)", true, couchdb.NotFound(err))
}

func TestBulkGet(t *testing.T) {
	c := newTestClient(t)
	c.Handle("POST /db/_bulk_get", func(resp http.ResponseWriter, req *http.Request) {
		reqData := couchdb.BulkGet{}
		body, _ := ioutil.ReadAll(req.Body)
		err := json.Unmarshal(body, &reqData)
		check(t, "reqData.Docs[0].ID", "foo", reqData.Docs[0].ID)
		check(t, "reqData.Docs[1].ID", "bar", reqData.Docs[1].ID)
		check(t, "reqData.Docs[2].ID", "baz", reqData.Docs[2].ID)
		check(t, "json.Unmarshal", nil, err)

		io.WriteString(resp, `{"results":[
			{"id":"foo","docs":[{"ok":{"_id":"foo","_rev":"4-753875d51501a6b1883a9d62b4d33f91","field":1}}]},
			{"id":"bar","docs":[{"ok":{"_id":"bar","_rev":"2-9b71d36dfdd9b4815388eb91cc8fb61d","field":2}}]},
			{"id":"baz","docs":[{"error":{"id":"baz","rev":"undefined","error":"not_found","reason":"missing"}}]}]}`)
	})

	docs, notFound, err := c.DB("db").BulkGet([]string{"foo", "bar", "baz"}, testDocument{}, nil)
	check(t, "err", nil, err)
	check(t, "notFound", []string{"baz"}, notFound)

	var fooDoc testDocument
	var barDoc testDocument

	for _, doc := range docs {
		document := doc.(testDocument)
		switch document.ID {
		case "foo":
			fooDoc = document
		case "bar":
			barDoc = document
		}
	}
	check(t, "fooDoc.ID", "foo", fooDoc.ID)
	check(t, "fooDoc.Rev", "4-753875d51501a6b1883a9d62b4d33f91", fooDoc.Rev)
	check(t, "fooDoc.Field", 1, fooDoc.Field)

	check(t, "barDoc.ID", "bar", barDoc.ID)
	check(t, "barDoc.Rev", "2-9b71d36dfdd9b4815388eb91cc8fb61d", barDoc.Rev)
	check(t, "barDoc.Field", 2, barDoc.Field)
}

func TestRev(t *testing.T) {
	c := newTestClient(t)
	db := c.DB("db")
	c.Handle("HEAD /db/ok", func(resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("ETag", `"1-619db7ba8551c0de3f3a178775509611"`)
	})
	c.Handle("HEAD /db/404", func(resp http.ResponseWriter, req *http.Request) {
		http.NotFound(resp, req)
	})

	rev, err := db.Rev("ok")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "rev", "1-619db7ba8551c0de3f3a178775509611", rev)

	errorRev, err := db.Rev("404")
	check(t, "errorRev", "", errorRev)
	check(t, "couchdb.NotFound(err)", true, couchdb.NotFound(err))
	if _, ok := err.(*couchdb.Error); !ok {
		t.Errorf("expected couchdb.Error, got %#+v", err)
	}
}

func TestPut(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db/doc", func(resp http.ResponseWriter, req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		check(t, "request body", `{"field":999}`, string(body))

		resp.Header().Set("ETag", `"1-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(http.StatusCreated)
		io.WriteString(resp, `{
			"id": "doc",
			"ok": true,
			"rev": "1-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	doc := &testDocument{Field: 999}
	rev, err := c.DB("db").Put("doc", doc, "")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "returned rev", "1-619db7ba8551c0de3f3a178775509611", rev)
}

func TestBulkDocs(t *testing.T) {
	c := newTestClient(t)
	c.Handle("POST /db/_bulk_docs", func(rw http.ResponseWriter, req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		fmt.Println(string(body))
		reqData := couchdb.BulkDocsReq{}
		err := json.Unmarshal(body, &reqData)
		check(t, "json.Unmarshal", err, nil)

		check(t, "request body", "Barney", reqData.Docs[0].(map[string]interface{})["_id"])
		check(t, "request body", "Fred Flintstone", reqData.Docs[1].(map[string]interface{})["name"])
		check(t, "request body", "Pebbles", reqData.Docs[2].(map[string]interface{})["_id"])
		check(t, "request body", "Dino", reqData.Docs[3].(map[string]interface{})["name"])

		rw.WriteHeader(http.StatusOK)
		_, err = io.WriteString(rw, `[{"ok":true,"id":"Barney","rev":"1"},
    		{"ok":true,"id":"Fred","rev":"1"},
    		{"ok":true,"id":"Pebbles","rev":"2"},
			{"id":"Dino","error":"conflict","reason":"Document update conflict"}]`)
		check(t, "io.WriteString", err, nil)

	})

	type createDoc struct {
		Name string `json:"name"`
		ID   string `json:"_id"`
		Rev  string `json:"_rev"`
	}
	type updateDoc struct {
		Name string `json:"name"`
		Age  int32  `json:"age"`
	}
	type delDoc struct {
		ID      string `json:"_id"`
		Rev     string `json:"_rev"`
		Deleted bool   `json:"_deleted"`
	}

	docCreate := &createDoc{"Barney Rubble", "Barney", "1"}
	docUpdate := &updateDoc{"Fred Flintstone", 41}
	docDel := &delDoc{"Pebbles", "2", true}
	docFailUpdate := &updateDoc{Name: "Dino", Age: 5}

	res, err := c.DB("db").BulkDocs(docCreate, docUpdate, docDel, docFailUpdate)
	check(t, "BulkDocs", err, nil)

	createRes := res[0]
	check(t, "createRes.OK", true, createRes.OK)
	check(t, "createRes.ID", "Barney", createRes.ID)
	check(t, "createRes.Rev", "1", createRes.Rev)

	updateRes := res[1]
	check(t, "updateRes.OK", true, updateRes.OK)

	delRes := res[2]
	check(t, "delRes.OK", true, delRes.OK)
	check(t, "delRes.ID", "Pebbles", delRes.ID)
	check(t, "delRes.Rev", "2", delRes.Rev)

	updateFailuteRes := res[3]
	check(t, "updateFailuteRes.OK", false, updateFailuteRes.OK)
	check(t, "updateFailuteRes.ID", "Dino", updateFailuteRes.ID)
	check(t, "updateFailuteRes.Error", "conflict", updateFailuteRes.Error)
}

func TestPutWithRev(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db/doc", func(resp http.ResponseWriter, req *http.Request) {
		check(t, "request query string",
			"rev=1-619db7ba8551c0de3f3a178775509611",
			req.URL.RawQuery)

		body, _ := ioutil.ReadAll(req.Body)
		check(t, "request body", `{"field":999}`, string(body))

		resp.Header().Set("ETag", `"2-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(http.StatusCreated)
		io.WriteString(resp, `{
			"id": "doc",
			"ok": true,
			"rev": "2-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	doc := &testDocument{Field: 999}
	rev, err := c.DB("db").Put("doc", doc, "1-619db7ba8551c0de3f3a178775509611")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "returned rev", "2-619db7ba8551c0de3f3a178775509611", rev)
}

func TestDelete(t *testing.T) {
	c := newTestClient(t)
	c.Handle("DELETE /db/doc", func(resp http.ResponseWriter, req *http.Request) {
		check(t, "request query string",
			"rev=1-619db7ba8551c0de3f3a178775509611",
			req.URL.RawQuery)

		resp.Header().Set("ETag", `"2-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(http.StatusOK)
		io.WriteString(resp, `{
			"id": "doc",
			"ok": true,
			"rev": "2-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	delrev := "1-619db7ba8551c0de3f3a178775509611"
	if rev, err := c.DB("db").Delete("doc", delrev); err != nil {
		t.Fatal(err)
	} else {
		check(t, "returned rev", "2-619db7ba8551c0de3f3a178775509611", rev)
	}
}

func TestView(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_design/test/_view/testview",
		func(resp http.ResponseWriter, req *http.Request) {
			expected := url.Values{
				"offset": {"5"},
				"limit":  {"100"},
				"reduce": {"false"},
			}
			check(t, "request query values", expected, req.URL.Query())

			io.WriteString(resp, `{
				"offset": 5,
				"rows": [
					{
						"id": "SpaghettiWithMeatballs",
						"key": "meatballs",
						"value": 1
					},
					{
						"id": "SpaghettiWithMeatballs",
						"key": "spaghetti",
						"value": 1
					},
					{
						"id": "SpaghettiWithMeatballs",
						"key": "tomato sauce",
						"value": 1
					}
				],
				"total_rows": 3
			}`)
		})

	type row struct {
		ID, Key string
		Value   int
	}
	type testviewResult struct {
		TotalRows int `json:"total_rows"`
		Offset    int
		Rows      []row
	}

	var result testviewResult
	err := c.DB("db").View("test", "testview", &result, couchdb.Options{
		"offset": 5,
		"limit":  100,
		"reduce": false,
	})
	if err != nil {
		t.Fatal(err)
	}

	expected := testviewResult{
		TotalRows: 3,
		Offset:    5,
		Rows: []row{
			{"SpaghettiWithMeatballs", "meatballs", 1},
			{"SpaghettiWithMeatballs", "spaghetti", 1},
			{"SpaghettiWithMeatballs", "tomato sauce", 1},
		},
	}
	check(t, "result", expected, result)
}

func TestAllDocs(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_all_docs",
		func(resp http.ResponseWriter, req *http.Request) {
			expected := url.Values{
				"offset":   {"5"},
				"limit":    {"100"},
				"startkey": {"[\"Zingylemontart\",\"Yogurtraita\"]"},
			}
			check(t, "request query values", expected, req.URL.Query())

			io.WriteString(resp, `{
				"total_rows": 2666,
				"rows": [
					{
						"value": {
							"rev": "1-a3544d296de19e6f5b932ea77d886942"
						},
						"id": "Zingylemontart",
						"key": "Zingylemontart"
					},
					{
						"value": {
							"rev": "1-91635098bfe7d40197a1b98d7ee085fc"
						},
						"id": "Yogurtraita",
						"key": "Yogurtraita"
					}
				],
				"offset" : 5
			}`)
		})

	type alldocsResult struct {
		TotalRows int `json:"total_rows"`
		Offset    int
		Rows      []map[string]interface{}
	}

	var result alldocsResult
	err := c.DB("db").AllDocs(&result, couchdb.Options{
		"offset":   5,
		"limit":    100,
		"startkey": []string{"Zingylemontart", "Yogurtraita"},
	})
	if err != nil {
		t.Fatal(err)
	}

	expected := alldocsResult{
		TotalRows: 2666,
		Offset:    5,
		Rows: []map[string]interface{}{
			{
				"key": "Zingylemontart",
				"id":  "Zingylemontart",
				"value": map[string]interface{}{
					"rev": "1-a3544d296de19e6f5b932ea77d886942",
				},
			},
			{
				"key": "Yogurtraita",
				"id":  "Yogurtraita",
				"value": map[string]interface{}{
					"rev": "1-91635098bfe7d40197a1b98d7ee085fc",
				},
			},
		},
	}
	check(t, "result", expected, result)
}

func TestSyncDesignNoChange(t *testing.T) {
	design := couchdb.NewDesign("test")
	design.AddView("by_created_at", &couchdb.View{
		Map:    "function(d) { if (d['created_at']) { emit(d['created_at'], 1); } }",
		Reduce: "_sum",
	})
	c := newTestClient(t)
	// Getting the current version
	c.Handle("GET /db/_design/test", func(resp http.ResponseWriter, req *http.Request) {
		io.WriteString(resp, `{
			"_id": "_design/test",
			"_rev": "1-619db7ba8551c0de3f3a178775509611",
      "language": "javascript",
			"views": {
        "by_created_at": {
          "map": "function(d) { if (d['created_at']) { emit(d['created_at'], 1); } }",
          "reduce": "_sum"
        }
      }
		}`)
	})
	db := c.DB("db")
	db.SyncDesign(design)
	check(t, "design.Rev", "1-619db7ba8551c0de3f3a178775509611", design.Rev)
}

func TestSyncDesignCreate(t *testing.T) {
	design := couchdb.NewDesign("test")
	design.AddView("by_created_at", &couchdb.View{
		Map:    "function(d) { if (d['created_at']) { emit(d['created_at'], 1); } }",
		Reduce: "_sum",
	})
	c := newTestClient(t)
	// Getting the current version (which doesn't exist)
	c.Handle("GET /db/_design/test", func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(404)
		io.WriteString(resp, `{"error":"not_found","reason":"error reason"}`)
	})
	// Putting a new version
	c.Handle("PUT /db/_design/test", func(resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("ETag", `"1-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(http.StatusCreated)
		io.WriteString(resp, `{
			"id": "_design/test",
			"ok": true,
			"rev": "1-619db7ba8551c0de3f3a178775509611"
		}`)
	})
	db := c.DB("db")
	db.SyncDesign(design)
	check(t, "design.Rev", "1-619db7ba8551c0de3f3a178775509611", design.Rev)
}

func TestSyncDesignUpdate(t *testing.T) {
	design := couchdb.NewDesign("test")
	design.AddView("by_created_at", &couchdb.View{
		Map:    "function(d) { if (d['created_at']) { emit(d['created_at'], 1); } }",
		Reduce: "_sum",
	})
	c := newTestClient(t)
	// Getting the current version
	c.Handle("GET /db/_design/test", func(resp http.ResponseWriter, req *http.Request) {
		io.WriteString(resp, `{
			"_id": "_design/test",
			"_rev": "1-619db7ba8551c0de3f3a178775509611",
      "language": "javascript",
			"views": {
        "by_created_at": {
          "map": "function(d) { if (d['created_at']) { emit(d['created_at'], null); } }"
        }
      }
		}`)
	})
	// Putting a new version
	c.Handle("PUT /db/_design/test", func(resp http.ResponseWriter, req *http.Request) {
		check(t, "request query string",
			"rev=1-619db7ba8551c0de3f3a178775509611",
			req.URL.RawQuery)

		//body, _ := ioutil.ReadAll(req.Body)
		//check(t, "request body", `{"field":999}`, string(body))

		resp.Header().Set("ETag", `"2-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(http.StatusCreated)
		io.WriteString(resp, `{
			"id": "_design/test",
			"ok": true,
			"rev": "2-619db7ba8551c0de3f3a178775509611"
		}`)
	})
	db := c.DB("db")
	db.SyncDesign(design)
	check(t, "design.Rev", "2-619db7ba8551c0de3f3a178775509611", design.Rev)
}

func asURL(raw string) *url.URL {
	u, _ := url.Parse(raw)
	return u
}
