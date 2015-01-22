package main

import (
	"fmt"
	"testing"
)

// FIXME: at present, if we want to run go test with this file, we must start
// a connector fisrt. In future work we will separate the connector into a
// standalone module in order to start a connector daemon in testing.

func TestAllocateConnector(t *testing.T) {
	addr, err := AllocateConnector([]string{"http://localhost:4001"})
	if err != nil {
		t.Errorf("AllocateConnector error (%v)", err)
	}
	fmt.Printf("allocated %s\n", addr)
}
