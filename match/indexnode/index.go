package main

import ()

type IndexBase struct {
	dimension int
	attrbases []*AttrBase
}

type AttrBase struct {
	name      string
	use       int
	low, high int
	sigval    []string
}
