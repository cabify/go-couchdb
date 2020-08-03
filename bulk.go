package couchdb

import "encoding/json"

type bulkID struct {
	ID string `json:"id"`
}

type BulkGet struct {
	Docs []struct{ ID string } `json:"docs"`
}

type BulkDocsReq struct {
	Docs []interface{} `json:"docs"`
}

type errorWrapper struct {
	ID     string `json:"id"`
	Rev    string `json:"rev"`
	Error  string `json:"error"`
	Reason string `json:"reason"`
}

type docWrapper struct {
	Ok    json.RawMessage `json:"ok"`
	Error *errorWrapper   `json:"error"`
}

type bulkGetRes struct {
	Id   string       `json:"id"`
	Docs []docWrapper `json:"docs"`
}

type bulkGetResp struct {
	Results []bulkGetRes
}

type BulkDocsResp struct {
	OK     bool   `json:"ok,omitempty"`
	ID     string `json:"id"`
	Rev    string `json:"rev,omitempty"`
	Error  string `json:"error,omitempty"`
	Reason string `json:"reason,omitempty"`
}
