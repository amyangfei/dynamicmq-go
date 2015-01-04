package main

import (
	"net/http"
	"time"
)

type Handler struct {
	RespWriter http.ResponseWriter
	Request    *http.Request
	StartTime  time.Time
}

type HandlerFunc func(Handler)

type Route struct {
	URL         string
	Permission  int
	HandlerFunc HandlerFunc
}

var (
	routes = []Route{
		{"/sdk/sub/preauth", Everyone, prepareAuth},

		{"/conn/sub/auth", Everyone, authSubCli},
	}
)

func NewHandler(w http.ResponseWriter, r *http.Request) Handler {
	return Handler{
		RespWriter: w,
		Request:    r,
		StartTime:  time.Now(),
	}
}

func (h Handler) Redirect(urlStr string) {
	http.Redirect(h.RespWriter, h.Request, urlStr, http.StatusFound)
}
