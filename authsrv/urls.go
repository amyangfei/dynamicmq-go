package main

import (
	"net/http"
	"time"
)

// Handler is a simple encapsulation for a http request and response writer
type Handler struct {
	RespWriter http.ResponseWriter
	Request    *http.Request
	StartTime  time.Time
}

type handlerFunc func(Handler)

// The Route encapsulates handler and permission to specific url
type Route struct {
	URL         string
	Permission  int
	HandlerFunc handlerFunc
}

var (
	routes = []Route{
		{"/sdk/sub/preauth", Everyone, prepareAuth},

		{"/conn/sub/auth", Everyone, authSubCli},
	}
)

// NewHandler returns a new Handler
func NewHandler(w http.ResponseWriter, r *http.Request) Handler {
	return Handler{
		RespWriter: w,
		Request:    r,
		StartTime:  time.Now(),
	}
}

// Redirect encapsulates the http.Redirect
func (h Handler) Redirect(urlStr string) {
	http.Redirect(h.RespWriter, h.Request, urlStr, http.StatusFound)
}
