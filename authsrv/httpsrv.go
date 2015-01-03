package main

import (
	"github.com/gorilla/mux"
	"net/http"
)

func HandlerFuncWrapper(route Route) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handler := NewHandler(w, r)
		url := r.Method + " " + r.URL.Path
		if r.URL.RawQuery != "" {
			url += "?" + r.URL.RawQuery
		}
		log.Info(url)
		// TODO: Privilege validation
		route.HandlerFunc(handler)
	}
}

func StartServer(bind string) {
	r := mux.NewRouter()
	for _, route := range routes {
		r.HandleFunc(route.URL, HandlerFuncWrapper(route))
	}
	http.Handle("/", r)
	log.Info("server start listening on: %s", bind)
	err := http.ListenAndServe(bind, nil)
	if err != nil {
		log.Error("server exit with error(%v)", err)
	}
}
