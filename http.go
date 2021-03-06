package couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// Options represents CouchDB query string parameters.
type Options map[string]interface{}

// Payload represents CouchDB body parameters.
type Payload map[string]interface{}

// clone creates a shallow copy of an Options map
func (opts Options) clone() (result Options) {
	result = make(Options)
	for k, v := range opts {
		result[k] = v
	}
	return
}

type transport struct {
	prefix string // URL prefix
	http   *http.Client
	mu     sync.RWMutex
	auth   Auth
}

func newTransport(prefix string, httpClient *http.Client, auth Auth) *transport {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &transport{
		prefix: strings.TrimRight(prefix, "/"),
		http:   httpClient,
		auth:   auth,
	}
}

func (t *transport) setAuth(a Auth) {
	t.mu.Lock()
	t.auth = a
	t.mu.Unlock()
}

func (t *transport) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, t.prefix+path, body)
	if err != nil {
		return nil, err
	}
	if ctx != context.Background() { // Save unnecessary copying
		req = req.WithContext(ctx)
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.auth != nil {
		t.auth.AddAuth(req)
	}
	return req, nil
}

// request sends an HTTP request to a CouchDB server.
// The request URL is constructed from the server's
// prefix and the given path, which may contain an
// encoded query string.
//
// Status codes >= 400 are treated as errors.
func (t *transport) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := t.newRequest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	if method != "GET" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode >= 400 {
		return nil, parseError(resp) // the Body is closed by parseError
	} else {
		return resp, nil
	}
}

// closedRequest sends an HTTP request and discards the response body.
func (t *transport) closedRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	resp, err := t.request(ctx, method, path, body)
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return resp, err
}

func path(segs ...string) string {
	r := ""
	for _, seg := range segs {
		r += "/"
		r += url.QueryEscape(seg)
	}
	return r
}

func revpath(rev string, segs ...string) string {
	r := path(segs...)
	if rev != "" {
		r += "?rev=" + url.QueryEscape(rev)
	}
	return r
}

func optpath(opts Options, jskeys []string, segs ...string) (string, error) {
	r := path(segs...)
	if len(opts) > 0 {
		os, err := encopts(opts, jskeys)
		if err != nil {
			return "", err
		}
		r += os
	}
	return r, nil
}

func encopts(opts Options, jskeys []string) (string, error) {
	buf := new(bytes.Buffer)
	buf.WriteRune('?')
	amp := false
	for k, v := range opts {
		if amp {
			buf.WriteByte('&')
		}
		buf.WriteString(url.QueryEscape(k))
		buf.WriteByte('=')
		isjson := false
		for _, jskey := range jskeys {
			if k == jskey {
				isjson = true
				break
			}
		}
		if isjson {
			jsonv, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("invalid option %q: %v", k, err)
			}
			buf.WriteString(url.QueryEscape(string(jsonv)))
		} else {
			if err := encval(buf, k, v); err != nil {
				return "", fmt.Errorf("invalid option %q: %v", k, err)
			}
		}
		amp = true
	}
	return buf.String(), nil
}

func encval(w io.Writer, k string, v interface{}) error {
	if v == nil {
		return errors.New("value is nil")
	}
	rv := reflect.ValueOf(v)
	var str string
	switch rv.Kind() {
	case reflect.String:
		str = url.QueryEscape(rv.String())
	case reflect.Bool:
		str = strconv.FormatBool(rv.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32:
		str = strconv.FormatFloat(rv.Float(), 'f', -1, 32)
	case reflect.Float64:
		str = strconv.FormatFloat(rv.Float(), 'f', -1, 64)
	default:
		return fmt.Errorf("unsupported type: %s", rv.Type())
	}
	_, err := io.WriteString(w, str)
	return err
}

// responseRev returns the unquoted Etag of a response.
func responseRev(resp *http.Response, err error) (string, error) {
	if err != nil {
		return "", err
	} else if etag := resp.Header.Get("Etag"); etag == "" {
		return "", fmt.Errorf("couchdb: missing Etag header in response")
	} else {
		return etag[1 : len(etag)-1], nil
	}
}

// responseRev returns the ID and Rev provided in the JSON body
func responseIDRev(resp *http.Response) (string, string, error) {
	var res struct {
		ID  string `json:"id"`
		Rev string `json:"rev"`
	}
	if err := readBody(resp, &res); err != nil {
		return "", "", err
	}
	return res.ID, res.Rev, nil
}

// Read the response body and decode in JSON.
// We're not using the json.Decoder struct here as it appears to cause
// problems with persistent connections.
func readBody(resp *http.Response, v interface{}) error {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(body, &v); err != nil {
		return err
	}
	return nil
}

// Error represents API-level errors, reported by CouchDB as
//    {"error": <ErrorCode>, "reason": <Reason>}
type Error struct {
	Method     string // HTTP method of the request
	URL        string // HTTP URL of the request
	StatusCode int    // HTTP status code of the response

	// These two fields will be empty for HEAD requests.
	ErrorCode string // Error reason provided by CouchDB
	Reason    string // Error message provided by CouchDB
}

func (e *Error) Error() string {
	if e.ErrorCode == "" {
		return fmt.Sprintf("%v %v: %v", e.Method, e.URL, e.StatusCode)
	}
	return fmt.Sprintf("%v %v: (%v) %v: %v",
		e.Method, e.URL, e.StatusCode, e.ErrorCode, e.Reason)
}

// NotFound checks whether the given errors is a DatabaseError
// with StatusCode == 404. This is useful for conditional creation
// of databases and documents.
func NotFound(err error) bool {
	return ErrorStatus(err, http.StatusNotFound)
}

// Unauthorized checks whether the given error is a DatabaseError
// with StatusCode == 401.
func Unauthorized(err error) bool {
	return ErrorStatus(err, http.StatusUnauthorized)
}

// Conflict checks whether the given error is a DatabaseError
// with StatusCode == 409.
func Conflict(err error) bool {
	return ErrorStatus(err, http.StatusConflict)
}

// ErrorStatus checks whether the given error is a DatabaseError
// with a matching statusCode.
func ErrorStatus(err error, statusCode int) bool {
	dberr, ok := err.(*Error)
	return ok && dberr.StatusCode == statusCode
}

func parseError(resp *http.Response) error {
	var reply struct{ Error, Reason string }
	if resp.Request.Method != "HEAD" {
		if err := readBody(resp, &reply); err != nil {
			unknown := fmt.Sprintf("unknown, couldn't decode CouchDB error: %v", err)
			reply.Error = unknown
			reply.Reason = unknown
		}
	}
	return &Error{
		Method:     resp.Request.Method,
		URL:        resp.Request.URL.String(),
		StatusCode: resp.StatusCode,
		ErrorCode:  reply.Error,
		Reason:     reply.Reason,
	}
}
