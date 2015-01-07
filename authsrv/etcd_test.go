package main

import (
	"fmt"
	"testing"
)

func TestGetConnector(t *testing.T) {
	addr, err := GetConnector([]string{"localhost:4001"})
	if err != nil {
		t.Errorf("GetConnector error (%v)", err)
	}
	fmt.Printf("allocated %s\n", addr)
}
